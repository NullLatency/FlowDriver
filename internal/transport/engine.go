package transport

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NullLatency/flow-driver/internal/storage"
)

// Engine manages the local sessions, periodically flushes Tx buffers to files,
// and polls for new Rx files.
type Engine struct {
	backend storage.Backend
	myDir   Direction // DirReq for client, DirRes for server
	peerDir Direction // DirRes for client, DirReq for server
	id      string    // ClientID for client, empty for server

	sessions  map[string]*Session
	sessionMu sync.RWMutex

	// Tombstones for recently closed sessions to prevent re-triggering on delayed packets
	closedSessions   map[string]time.Time
	closedSessionsMu sync.Mutex

	pollTicker  time.Duration
	flushTicker time.Duration

	// Server mode handler: called when a new session is discovered
	OnNewSession func(sessionID, targetAddr string, s *Session)
}

func NewEngine(backend storage.Backend, isClient bool, clientID string) *Engine {
	e := &Engine{
		backend:        backend,
		id:             clientID,
		sessions:       make(map[string]*Session),
		closedSessions: make(map[string]time.Time),
		// Default intervals: Poll (RX) fast for responsiveness, Flush (TX) slower for gathering
		pollTicker:  500 * time.Millisecond,
		flushTicker: 300 * time.Millisecond,
	}
	if isClient {
		e.myDir = DirReq
		e.peerDir = DirRes
	} else {
		e.myDir = DirRes
		e.peerDir = DirReq
	}
	return e
}

func (e *Engine) SetRefreshRate(ms int) {
	if ms > 0 {
		e.pollTicker = time.Duration(ms) * time.Millisecond
		// Legacy behavior: sets both if FlushTicker was still at default
		if e.flushTicker == 300*time.Millisecond {
			e.flushTicker = time.Duration(ms) * time.Millisecond
		}
	}
}

func (e *Engine) SetPollRate(ms int) {
	if ms > 0 {
		e.pollTicker = time.Duration(ms) * time.Millisecond
	}
}

func (e *Engine) SetFlushRate(ms int) {
	if ms > 0 {
		e.flushTicker = time.Duration(ms) * time.Millisecond
	}
}

func (e *Engine) Start(ctx context.Context) {
	go e.flushLoop(ctx)
	go e.pollLoop(ctx)
	go e.cleanupLoop(ctx) // Delete files older than 10s
}

func (e *Engine) GetSession(id string) *Session {
	e.sessionMu.RLock()
	defer e.sessionMu.RUnlock()
	return e.sessions[id]
}

func (e *Engine) AddSession(s *Session) {
	e.sessionMu.Lock()
	defer e.sessionMu.Unlock()
	e.sessions[s.ID] = s
}

func (e *Engine) flushLoop(ctx context.Context) {
	ticker := time.NewTicker(e.flushTicker)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.flushAll(ctx)
		}
	}
}

func (e *Engine) flushAll(ctx context.Context) {
	e.sessionMu.Lock()
	sessions := make([]*Session, 0, len(e.sessions))
	for _, s := range e.sessions {
		sessions = append(sessions, s)
	}
	e.sessionMu.Unlock()

	muxes := make(map[string][]Envelope)
	var closedSessionIDs []string

	for _, s := range sessions {
		s.mu.Lock()

		// Idle Timeout check
		if time.Since(s.lastActivity) > 10*time.Second {
			s.closed = true
		}

		shouldSend := len(s.txBuf) > 0 || (s.txSeq == 0 && e.myDir == DirReq) || s.closed

		if !shouldSend {
			s.mu.Unlock()
			continue
		}

		payload := s.txBuf
		s.txBuf = nil

		env := Envelope{
			SessionID:  s.ID,
			Seq:        s.txSeq,
			Payload:    payload,
			Close:      s.closed,
			TargetAddr: s.TargetAddr,
		}

		s.txSeq++
		if s.closed {
			closedSessionIDs = append(closedSessionIDs, s.ID)
		}

		cid := s.ClientID
		if cid == "" && e.myDir == DirReq {
			cid = e.id // For client requests, use our own ID
		}

		muxes[cid] = append(muxes[cid], env)
		s.mu.Unlock()
	}

	for cid, mux := range muxes {
		var b []byte
		for _, env := range mux {
			eb, err := env.MarshalBinary()
			if err != nil {
				log.Printf("binary encode error: %v", err)
				return
			}
			b = append(b, eb...)
		}

		// Filename format: {dir}-{clientID}-mux-{timestamp}.bin
		// If clientID is empty (server-side error?), fallback to generic
		fnameCID := cid
		if fnameCID == "" {
			fnameCID = "unknown"
		}
		filename := fmt.Sprintf("%s-%s-mux-%d.bin", e.myDir, fnameCID, time.Now().UnixNano())

		// Upload asynchronously
		go func(fname string, data []byte) {
			if err := e.backend.Upload(ctx, fname, data); err != nil {
				log.Printf("upload error %s: %v", fname, err)
			}
		}(filename, b)
	}

	for _, id := range closedSessionIDs {
		e.RemoveSession(id)
	}
}

func (e *Engine) pollLoop(ctx context.Context) {
	currentPollInterval := e.pollTicker
	maxPollInterval := 5 * time.Second
	timer := time.NewTimer(currentPollInterval)
	defer timer.Stop()

	// Track processed files
	processed := make(map[string]bool)
	var processedMu sync.Mutex

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		pollAgain:
			// ZERO-TRAFFIC CLIENT OPTIMIZATION:
			// SOCKS5 only initiates from the Client. If the Client has 0 active sessions,
			// it mathematically never needs to poll Google Drive! Go entirely to sleep!
			if e.myDir == DirReq {
				e.sessionMu.RLock()
				count := len(e.sessions)
				e.sessionMu.RUnlock()
				if count == 0 {
					timer.Reset(currentPollInterval)
					continue
				}
			}

			// Fetch multiplexed files
			prefix := string(e.peerDir) + "-"
			if e.myDir == DirReq {
				// Client only polls for its own responses
				prefix += e.id + "-mux-"
			} else {
				// Server polls for ALL client requests
				prefix += ""
			}
			files, err := e.backend.ListQuery(ctx, prefix)
			if err != nil {
				log.Printf("poll list error: %v", err)
				timer.Reset(currentPollInterval)
				continue
			}

			if len(files) == 0 {
				if e.myDir == DirRes { // SERVER OPTIMIZATION
					e.sessionMu.RLock()
					activeSessions := len(e.sessions)
					e.sessionMu.RUnlock()

					if activeSessions == 0 {
						// Increase polling delay step-by-step to save API calls
						currentPollInterval += 500 * time.Millisecond
						if currentPollInterval > maxPollInterval {
							currentPollInterval = maxPollInterval
						}
					} else {
						// A session is currently active, so loop fast!
						currentPollInterval = e.pollTicker
					}
				}
				// Client optimization doesn't change intervals, but needs its timer reset
				timer.Reset(currentPollInterval)
				continue
			}

			// We found data! Reset polling back to maximum speed
			currentPollInterval = e.pollTicker

			// We found files! Let's download them in parallel to boost speed massively
			var wg sync.WaitGroup
			for _, f := range files {
				processedMu.Lock()
				already := processed[f]
				if !already {
					processed[f] = true
				}
				processedMu.Unlock()

				if already {
					continue
				}

				wg.Add(1)
				go func(fname string) {
					defer wg.Done()
					b, err := e.backend.Download(ctx, fname)
					if err != nil {
						log.Printf("download error %s: %v", fname, err)
						processedMu.Lock()
						delete(processed, fname) // failed to download, retry next poll
						processedMu.Unlock()
						return
					}

					var envs []Envelope
					curr := b
					for len(curr) > 0 {
						var env Envelope
						n, err := env.UnmarshalBinary(curr)
						if err != nil {
							log.Printf("mux unmarshal error %s: %v", fname, err)
							break
						}
						envs = append(envs, env)
						curr = curr[n:]
					}

					// Extract ClientID from filename for server-side session initialization
					var fileClientID string
					parts := strings.Split(fname, "-")
					if len(parts) >= 4 && parts[2] == "mux" {
						fileClientID = parts[1]
					}

					// Process the entire MUX batch sequentially inside the go routine
					for i := range envs {
						env := &envs[i]

						// Check tombstone first
						e.closedSessionsMu.Lock()
						if _, exists := e.closedSessions[env.SessionID]; exists {
							e.closedSessionsMu.Unlock()
							continue // Ignore packets for recently closed sessions
						}
						e.closedSessionsMu.Unlock()

						e.sessionMu.Lock()
						s, exists := e.sessions[env.SessionID]
						if !exists && e.myDir == DirRes && e.OnNewSession != nil {
							// Server logic: new connection
							s = NewSession(env.SessionID)
							s.ClientID = fileClientID // Associate session with the client that sent the request
							e.sessions[env.SessionID] = s
							// Release lock before calling OnNewSession to avoid deadlocks
							e.sessionMu.Unlock()
							log.Printf("Engine: Triggering new session %s for Client %s", env.SessionID, fileClientID)
							e.OnNewSession(env.SessionID, env.TargetAddr, s)
						} else {
							e.sessionMu.Unlock()
						}

						if s != nil {
							s.ProcessRx(env)
						}
					}

					e.backend.Delete(ctx, fname)
				}(f)
			}

			// Wait for parallel batch to finish
			wg.Wait()

			// Adaptive Polling: Because we just received data, the connection is active.
			// Instead of jumping back to the select, immediately poll again after a tiny 100ms break to drain queues.
			time.Sleep(100 * time.Millisecond)
			goto pollAgain
		}
	}
}

func (e *Engine) RemoveSession(id string) {
	e.sessionMu.Lock()
	delete(e.sessions, id)
	e.sessionMu.Unlock()

	// Add to tombstone list
	e.closedSessionsMu.Lock()
	e.closedSessions[id] = time.Now()
	e.closedSessionsMu.Unlock()
}

func (e *Engine) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Cleanup old tombstones (older than 30s)
			e.closedSessionsMu.Lock()
			for id, t := range e.closedSessions {
				if time.Since(t) > 30*time.Second {
					delete(e.closedSessions, id)
				}
			}
			e.closedSessionsMu.Unlock()

			// ZERO-TRAFFIC CLIENT OPTIMIZATION:
			if e.myDir == DirReq {
				e.sessionMu.RLock()
				count := len(e.sessions)
				e.sessionMu.RUnlock()
				if count == 0 {
					continue
				}
			}

			files, _ := e.backend.ListQuery(ctx, string(e.myDir)+"-")
			for _, f := range files {
				parts := strings.Split(f, "-")
				// Formats:
				// OLD: "req", "UUID...", "Seq", "Timestamp.json" (len >= 4)
				// MUX: "req", "mux", "Timestamp.json" (len >= 3)
				if len(parts) >= 3 {
					tsStr := parts[len(parts)-1]
					tsStr = strings.TrimSuffix(tsStr, ".json")
					tsStr = strings.TrimSuffix(tsStr, ".bin")
					ts, err := strconv.ParseInt(tsStr, 10, 64)
					if err == nil {
						t := time.Unix(0, ts)
						if time.Since(t) > 10*time.Second {
							e.backend.Delete(ctx, f)
						}
					}
				}
			}
		}
	}
}
