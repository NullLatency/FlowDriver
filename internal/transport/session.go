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
	ID           string
	mu           sync.Mutex
	txBuf        []byte
	txSeq        uint64
	rxSeq        uint64
	rxQueue      map[uint64]*Envelope
	lastActivity time.Time
	closed       bool
	rxClosed     bool // Safely tracks if RxChan was successfully closed
	TargetAddr   string
	ClientID     string

	// App channel for receiving data downloaded from remote
	RxChan chan []byte
}

func NewSession(id string) *Session {
	return &Session{
		ID:           id,
		rxQueue:      make(map[uint64]*Envelope),
		lastActivity: time.Now(),
		RxChan:       make(chan []byte, 1024),
	}
}

func (s *Session) EnqueueTx(data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.txBuf = append(s.txBuf, data...)
	s.lastActivity = time.Now()
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
