package transport

import (
	"sync"
	"time"
)

// Direction indicates if a file is req or res.
type Direction string

const (
	DirReq Direction = "req"
	DirRes Direction = "res"

	// MaxTxBufferSize prevents unbounded memory growth per session.
	MaxTxBufferSize = 2 * 1024 * 1024

	// MaxWriteChunk avoids one huge write monopolizing a session buffer.
	MaxWriteChunk = 64 * 1024

	// DefaultMaxPayloadBytes keeps individual envelopes reasonably sized.
	DefaultMaxPayloadBytes = 256 * 1024
)

// txSnapshot is an immutable copy of the current TX state prepared for upload.
// It is committed only after the upload succeeds.
type txSnapshot struct {
	session    *Session
	sessionID  string
	clientID   string
	targetAddr string
	seq        uint64
	payload    []byte
	payloadLen int
	close      bool
}

// Session represents an active proxy connection mapped to transport files.
type Session struct {
	ID string

	mu sync.Mutex

	txBuf      []byte
	txSeq      uint64
	txInFlight bool

	rxSeq      uint64
	rxQueue    map[uint64]*Envelope
	rxClosed   bool
	rxDeliverM sync.Mutex

	lastActivity time.Time
	closed       bool

	TargetAddr string
	ClientID   string

	// Backpressure: blocked when txBuf is too large.
	txCond *sync.Cond

	// App channel for receiving data downloaded from remote.
	RxChan chan []byte
}

func NewSession(id string) *Session {
	s := &Session{
		ID:           id,
		rxQueue:      make(map[uint64]*Envelope),
		lastActivity: time.Now(),
		RxChan:       make(chan []byte, 1024),
	}
	s.txCond = sync.NewCond(&s.mu)
	return s
}

// EnqueueTx appends outbound data with bounded buffering.
// It chunks large writes so one writer cannot grow txBuf far beyond the limit.
func (s *Session) EnqueueTx(data []byte) {
	if len(data) == 0 {
		s.Touch()
		return
	}

	offset := 0

	for offset < len(data) {
		s.mu.Lock()

		for len(s.txBuf) >= MaxTxBufferSize && !s.closed {
			s.txCond.Wait()
		}

		if s.closed {
			s.mu.Unlock()
			return
		}

		available := MaxTxBufferSize - len(s.txBuf)
		if available > MaxWriteChunk {
			available = MaxWriteChunk
		}

		remaining := len(data) - offset
		if available > remaining {
			available = remaining
		}

		if available <= 0 {
			s.mu.Unlock()
			continue
		}

		s.txBuf = append(s.txBuf, data[offset:offset+available]...)
		s.lastActivity = time.Now()
		offset += available

		s.mu.Unlock()
	}
}

// Touch marks the session as active without adding payload bytes.
// This is useful for the initial empty open packet.
func (s *Session) Touch() {
	s.mu.Lock()
	s.lastActivity = time.Now()
	s.mu.Unlock()
}

// Close marks the session as closed. Pending TX bytes are still allowed to flush.
func (s *Session) Close() {
	s.mu.Lock()
	s.closed = true
	s.lastActivity = time.Now()
	s.txCond.Broadcast()
	s.mu.Unlock()
}

// ClearTx clears pending TX data and releases blocked writers.
func (s *Session) ClearTx() {
	s.mu.Lock()
	s.txBuf = nil
	s.txInFlight = false
	s.txCond.Broadcast()
	s.mu.Unlock()
}

// SnapshotTx copies the bytes that should be uploaded.
// The snapshot is marked in-flight so repeated flush ticks do not duplicate it.
// CommitTx must be called after successful upload.
// ReleaseTx must be called after failed upload.
func (s *Session) SnapshotTx(maxPayloadBytes int, forceInitial bool, idleTimeout time.Duration) (txSnapshot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if idleTimeout > 0 && time.Since(s.lastActivity) > idleTimeout {
		s.closed = true
	}

	if s.txInFlight {
		return txSnapshot{}, false
	}

	pending := len(s.txBuf)
	shouldSend := pending > 0 || (forceInitial && s.txSeq == 0) || s.closed
	if !shouldSend {
		return txSnapshot{}, false
	}

	payloadLen := pending
	if maxPayloadBytes > 0 && payloadLen > maxPayloadBytes {
		payloadLen = maxPayloadBytes
	}

	payload := make([]byte, payloadLen)
	copy(payload, s.txBuf[:payloadLen])

	// Only send Close when this packet includes all pending data.
	closeWithPacket := s.closed && payloadLen == pending

	s.txInFlight = true

	return txSnapshot{
		session:    s,
		sessionID:  s.ID,
		clientID:   s.ClientID,
		targetAddr: s.TargetAddr,
		seq:        s.txSeq,
		payload:    payload,
		payloadLen: payloadLen,
		close:      closeWithPacket,
	}, true
}

// CommitTx finalizes a successful upload.
// It advances txSeq only after the backend confirms the upload succeeded.
func (s *Session) CommitTx(seq uint64, sentLen int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.txInFlight || seq != s.txSeq {
		return false
	}

	if sentLen > len(s.txBuf) {
		sentLen = len(s.txBuf)
	}

	if sentLen > 0 {
		copy(s.txBuf, s.txBuf[sentLen:])
		s.txBuf = s.txBuf[:len(s.txBuf)-sentLen]
	}

	s.txSeq++
	s.txInFlight = false
	s.lastActivity = time.Now()
	s.txCond.Broadcast()

	return true
}

// ReleaseTx unlocks a failed in-flight snapshot without dropping data.
func (s *Session) ReleaseTx(seq uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.txInFlight && seq == s.txSeq {
		s.txInFlight = false
		s.txCond.Broadcast()
	}
}

// ProcessRx handles received envelopes in sequence order.
// It avoids sending to RxChan while holding the main session lock.
func (s *Session) ProcessRx(env *Envelope) {
	var deliver [][]byte
	shouldClose := false

	s.mu.Lock()

	s.lastActivity = time.Now()

	if s.rxClosed {
		s.mu.Unlock()
		return
	}

	if env.Seq == s.rxSeq {
		if len(env.Payload) > 0 {
			deliver = append(deliver, cloneBytes(env.Payload))
		}

		s.rxSeq++

		if env.Close {
			s.rxClosed = true
			s.closed = true
			shouldClose = true
		} else {
			for {
				nextEnv, ok := s.rxQueue[s.rxSeq]
				if !ok {
					break
				}

				if len(nextEnv.Payload) > 0 {
					deliver = append(deliver, cloneBytes(nextEnv.Payload))
				}

				delete(s.rxQueue, s.rxSeq)
				s.rxSeq++

				if nextEnv.Close {
					s.rxClosed = true
					s.closed = true
					shouldClose = true
					break
				}
			}
		}
	} else if env.Seq > s.rxSeq {
		queued := *env
		queued.Payload = cloneBytes(env.Payload)
		s.rxQueue[queued.Seq] = &queued
	}

	if len(deliver) == 0 && !shouldClose {
		s.mu.Unlock()
		return
	}

	// Preserve channel delivery order without holding s.mu while sending.
	s.rxDeliverM.Lock()
	s.mu.Unlock()

	for _, payload := range deliver {
		s.RxChan <- payload
	}

	if shouldClose {
		close(s.RxChan)
	}

	s.rxDeliverM.Unlock()
}

func cloneBytes(in []byte) []byte {
	if len(in) == 0 {
		return nil
	}
	out := make([]byte, len(in))
	copy(out, in)
	return out
}
