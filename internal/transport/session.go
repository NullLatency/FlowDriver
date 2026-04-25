package transport

import (
	"sync"
	"time"
)

// Direction indicates if a file is req (client to server) or res (server to client)
type Direction string

const (
	DirReq Direction = "req"
	DirRes Direction = "res"
)

// Session represents an active proxy connection mapped to files.
type Session struct {
	ID                  string
	mu                  sync.Mutex
	txBuf               []byte
	txSeq               uint64
	rxSeq               uint64
	rxQueue             map[uint64]*Envelope
	createdAt           time.Time
	firstTxQueuedAt     time.Time
	lastActivity        time.Time
	firstResponseLogged bool
	firstUploadLogged   bool
	serverSeenLogged    bool
	closed              bool
	rxClosed            bool // Safely tracks if RxChan was successfully closed
	TargetAddr          string
	TargetHost          string
	ClientID            string
	LowPriority         bool

	// Backpressure: blocked when txBuf is too large
	txCond            *sync.Cond
	backpressureBytes int

	// App channel for receiving data downloaded from remote
	RxChan chan []byte
}

func NewSession(id string) *Session {
	s := &Session{
		ID:                id,
		rxQueue:           make(map[uint64]*Envelope),
		createdAt:         time.Now(),
		lastActivity:      time.Now(),
		RxChan:            make(chan []byte, 1024),
		backpressureBytes: 2 * 1024 * 1024,
	}
	s.txCond = sync.NewCond(&s.mu)
	return s
}

func (s *Session) SetBackpressureBytes(bytes int) {
	s.mu.Lock()
	if bytes > 0 {
		s.backpressureBytes = bytes
	}
	s.mu.Unlock()
}

func (s *Session) EnqueueTx(data []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	// BACKPRESSURE: Block if txBuf is larger than the configured limit.
	// This prevents memory explosion when uploading through the proxy
	for len(s.txBuf) > s.backpressureBytes && !s.closed {
		s.txCond.Wait()
	}

	firstPacket := s.txSeq == 0 && len(s.txBuf) == 0
	if firstPacket && s.firstTxQueuedAt.IsZero() {
		s.firstTxQueuedAt = time.Now()
	}
	s.txBuf = append(s.txBuf, data...)
	s.lastActivity = time.Now()
	return firstPacket
}

func (s *Session) ClearTx() {
	s.mu.Lock()
	s.txBuf = nil
	s.txCond.Broadcast() // Wake up any writers blocked on backpressure
	s.mu.Unlock()
}

func (s *Session) ProcessRx(env *Envelope) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastActivity = time.Now()

	if s.rxClosed {
		return // Ignore packets if the channel is already safely closed
	}

	if env.Seq == s.rxSeq {
		if len(env.Payload) > 0 {
			s.RxChan <- env.Payload
		}
		s.rxSeq++
		if env.Close {
			s.rxClosed = true
			s.closed = true
			close(s.RxChan)
			return
		}

		// process any queued future packets
		for {
			if nextEnv, ok := s.rxQueue[s.rxSeq]; ok {
				if len(nextEnv.Payload) > 0 {
					s.RxChan <- nextEnv.Payload
				}
				delete(s.rxQueue, s.rxSeq)
				s.rxSeq++
				if nextEnv.Close {
					s.rxClosed = true
					s.closed = true
					close(s.RxChan)
					return
				}
			} else {
				break
			}
		}
	} else if env.Seq > s.rxSeq {
		s.rxQueue[env.Seq] = env
	}
}
