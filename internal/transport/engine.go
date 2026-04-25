package transport

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NullLatency/flow-driver/internal/storage"
)

const (
	defaultPollTicker  = 500 * time.Millisecond
	defaultFlushTicker = 300 * time.Millisecond

	defaultIdleTimeout  = 60 * time.Second
	defaultStaleFileTTL = 5 * time.Minute

	defaultStorageConcurrency = 8
	defaultTombstoneTTL       = 30 * time.Second
)

// Engine manages local sessions, periodically flushes TX buffers,
// and polls for RX files.
type Engine struct {
	backend storage.Backend

	myDir   Direction
	peerDir Direction

	// ClientID for client mode; empty for server mode.
	id string

	sessions  map[string]*Session
	sessionMu sync.RWMutex

	// Tombstones for recently closed sessions to prevent re-triggering on delayed packets.
	closedSessions   map[string]time.Time
	closedSessionsMu sync.Mutex

	pollTicker  time.Duration
	flushTicker time.Duration

	idleTimeout    time.Duration
	staleFileTTL   time.Duration
	maxPayloadSize int

	// Server mode handler: called when a new session is discovered.
	OnNewSession func(sessionID, targetAddr string, s *Session)

	// Concurrency control for storage operations.
	sem chan struct{}

	// Track processed files to avoid duplicate processing.
	processed   map[string]time.Time
	processedMu sync.Mutex
}

func NewEngine(backend storage.Backend, isClient bool, clientID string) *Engine {
	e := &Engine{
		backend:        backend,
		id:             clientID,
		sessions:       make(map[string]*Session),
		closedSessions: make(map[string]time.Time),
		processed:      make(map[string]time.Time),

		pollTicker:  defaultPollTicker,
		flushTicker: defaultFlushTicker,

		idleTimeout:    defaultIdleTimeout,
		staleFileTTL:   defaultStaleFileTTL,
		maxPayloadSize: DefaultMaxPayloadBytes,

		sem: make(chan struct{}, defaultStorageConcurrency),
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

		// Legacy behavior: set flush too only if it is still the default.
		if e.flushTicker == defaultFlushTicker {
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

func (e *Engine) SetIdleTimeout(d time.Duration) {
	if d > 0 {
		e.idleTimeout = d
	}
}

func (e *Engine) SetStaleFileTTL(d time.Duration) {
	if d > 0 {
		e.staleFileTTL = d
	}
}

func (e *Engine) SetMaxPayloadSize(bytes int) {
	if bytes > 0 {
		e.maxPayloadSize = bytes
	}
}

func (e *Engine) Start(ctx context.Context) {
	go e.flushLoop(ctx)
	go e.pollLoop(ctx)
	go e.cleanupLoop(ctx)
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
	log.Printf("Engine.AddSession: added session %s, total=%d", s.ID, len(e.sessions))
}

func (e *Engine) RemoveSession(id string) {
	e.sessionMu.Lock()
	delete(e.sessions, id)
	e.sessionMu.Unlock()

	e.closedSessionsMu.Lock()
	e.closedSessions[id] = time.Now()
	e.closedSessionsMu.Unlock()
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
	e.sessionMu.RLock()
	sessions := make([]*Session, 0, len(e.sessions))
	for _, s := range e.sessions {
		sessions = append(sessions, s)
	}
	e.sessionMu.RUnlock()

	muxes := make(map[string][]txSnapshot)

	for _, s := range sessions {
		forceInitial := e.myDir == DirReq

		snap, ok := s.SnapshotTx(e.maxPayloadSize, forceInitial, e.idleTimeout)
		if !ok {
			continue
		}

		cid := snap.clientID
		if cid == "" && e.myDir == DirReq {
			cid = e.id
		}
		if cid == "" {
			cid = "unknown"
		}

		muxes[cid] = append(muxes[cid], snap)
	}

	for cid, snaps := range muxes {
		filename := fmt.Sprintf("%s-%s-mux-%d.bin", e.myDir, cid, time.Now().UnixNano())
		go e.uploadMux(ctx, filename, snaps)
	}
}

func (e *Engine) uploadMux(ctx context.Context, filename string, snaps []txSnapshot) {
	e.sem <- struct{}{}
	defer func() {
		<-e.sem
	}()

	var buf bytes.Buffer

	for _, snap := range snaps {
		env := Envelope{
			SessionID:  snap.sessionID,
			Seq:        snap.seq,
			Payload:    snap.payload,
			Close:      snap.close,
			TargetAddr: snap.targetAddr,
		}

		if err := env.Encode(&buf); err != nil {
			log.Printf("mux encode error %s: %v", filename, err)
			for _, failed := range snaps {
				failed.session.ReleaseTx(failed.seq)
			}
			return
		}
	}

	if err := e.backend.Upload(ctx, filename, bytes.NewReader(buf.Bytes())); err != nil {
		log.Printf("upload error %s: %v", filename, err)

		// Important: do not drop data. Release snapshots so the next flush retries.
		for _, failed := range snaps {
			failed.session.ReleaseTx(failed.seq)
		}
		return
	}

	for _, sent := range snaps {
		if ok := sent.session.CommitTx(sent.seq, sent.payloadLen); !ok {
			log.Printf("tx commit skipped: session=%s seq=%d", sent.sessionID, sent.seq)
		}

		if sent.close {
			e.RemoveSession(sent.sessionID)
		}
	}
}

func (e *Engine) pollLoop(ctx context.Context) {
	currentPollInterval := e.pollTicker
	maxPollInterval := 5 * time.Second

	timer := time.NewTimer(currentPollInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-timer.C:
		pollAgain:
			// Client mode optimization: if there are no active sessions,
			// there is no response file to poll for.
			if e.myDir == DirReq {
				e.sessionMu.RLock()
				count := len(e.sessions)
				e.sessionMu.RUnlock()

				if count == 0 {
					timer.Reset(currentPollInterval)
					continue
				}
			}

			prefix := string(e.peerDir) + "-"
			if e.myDir == DirReq {
				// Client only polls for its own responses.
				prefix += e.id + "-mux-"
			}

			files, err := e.backend.ListQuery(ctx, prefix)
			if err != nil {
				log.Printf("poll list error: %v", err)
				timer.Reset(currentPollInterval)
				continue
			}

			if len(files) == 0 {
				if e.myDir == DirRes {
					e.sessionMu.RLock()
					activeSessions := len(e.sessions)
					e.sessionMu.RUnlock()

					if activeSessions == 0 {
						currentPollInterval += 500 * time.Millisecond
						if currentPollInterval > maxPollInterval {
							currentPollInterval = maxPollInterval
						}
					} else {
						currentPollInterval = e.pollTicker
					}
				}

				timer.Reset(currentPollInterval)
				continue
			}

			currentPollInterval = e.pollTicker

			var wg sync.WaitGroup

			for _, f := range files {
				if e.isFileStale(f, e.staleFileTTL) {
					if err := e.backend.Delete(ctx, f); err != nil {
						log.Printf("stale delete error %s: %v", f, err)
					}
					continue
				}

				e.processedMu.Lock()
				_, already := e.processed[f]
				if !already {
					e.processed[f] = time.Now()
				}
				e.processedMu.Unlock()

				if already {
					continue
				}

				wg.Add(1)
				go func(fname string) {
					defer wg.Done()
					e.processIncomingFile(ctx, fname)
				}(f)
			}

			wg.Wait()

			// We received data, so drain the visible queue quickly.
			time.Sleep(100 * time.Millisecond)
			goto pollAgain
		}
	}
}

func (e *Engine) processIncomingFile(ctx context.Context, fname string) {
	e.sem <- struct{}{}
	defer func() {
		<-e.sem
	}()

	rc, err := e.backend.Download(ctx, fname)
	if err != nil {
		log.Printf("download error %s: %v", fname, err)

		e.processedMu.Lock()
		delete(e.processed, fname)
		e.processedMu.Unlock()

		return
	}
	defer rc.Close()

	fileClientID := extractClientID(fname)

	for {
		var env Envelope
		if err := env.Decode(rc); err != nil {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				log.Printf("mux decode error %s: %v", fname, err)
			}
			break
		}

		if e.isClosedSession(env.SessionID) {
			continue
		}

		s := e.getOrCreateSessionForEnvelope(env, fileClientID)
		if s != nil {
			s.ProcessRx(&env)
		}
	}

	if err := e.backend.Delete(ctx, fname); err != nil {
		log.Printf("delete processed file error %s: %v", fname, err)
	}
}

func (e *Engine) isClosedSession(sessionID string) bool {
	e.closedSessionsMu.Lock()
	defer e.closedSessionsMu.Unlock()

	_, exists := e.closedSessions[sessionID]
	return exists
}

func (e *Engine) getOrCreateSessionForEnvelope(env Envelope, fileClientID string) *Session {
	e.sessionMu.Lock()

	s, exists := e.sessions[env.SessionID]
	if !exists && e.myDir == DirRes && e.OnNewSession != nil {
		s = NewSession(env.SessionID)
		s.ClientID = fileClientID
		e.sessions[env.SessionID] = s

		e.sessionMu.Unlock()

		log.Printf("Engine: new session %s for client %s", env.SessionID, fileClientID)
		e.OnNewSession(env.SessionID, env.TargetAddr, s)

		return s
	}

	e.sessionMu.Unlock()
	return s
}

func (e *Engine) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			e.cleanupTombstones()
			e.cleanupProcessedCache()
			e.cleanupStaleOwnFiles(ctx)
		}
	}
}

func (e *Engine) cleanupTombstones() {
	e.closedSessionsMu.Lock()
	defer e.closedSessionsMu.Unlock()

	for id, t := range e.closedSessions {
		if time.Since(t) > defaultTombstoneTTL {
			delete(e.closedSessions, id)
		}
	}
}

func (e *Engine) cleanupProcessedCache() {
	e.processedMu.Lock()
	defer e.processedMu.Unlock()

	for fname, seenAt := range e.processed {
		if time.Since(seenAt) > e.staleFileTTL {
			delete(e.processed, fname)
		}
	}
}

func (e *Engine) cleanupStaleOwnFiles(ctx context.Context) {
	if e.myDir == DirReq {
		e.sessionMu.RLock()
		count := len(e.sessions)
		e.sessionMu.RUnlock()

		if count == 0 {
			return
		}
	}

	files, err := e.backend.ListQuery(ctx, string(e.myDir)+"-")
	if err != nil {
		log.Printf("cleanup list error: %v", err)
		return
	}

	for _, f := range files {
		if !e.isFileStale(f, e.staleFileTTL) {
			continue
		}

		if err := e.backend.Delete(ctx, f); err != nil {
			log.Printf("cleanup delete error %s: %v", f, err)
		}
	}
}

func (e *Engine) isFileStale(filename string, ttl time.Duration) bool {
	if ttl <= 0 {
		return false
	}

	ts, ok := fileTimestamp(filename)
	if !ok {
		return false
	}

	return time.Since(ts) > ttl
}

func fileTimestamp(filename string) (time.Time, bool) {
	parts := strings.Split(filename, "-")
	if len(parts) < 3 {
		return time.Time{}, false
	}

	tsStr := parts[len(parts)-1]
	tsStr = strings.TrimSuffix(tsStr, ".json")
	tsStr = strings.TrimSuffix(tsStr, ".bin")

	nano, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil || nano <= 0 {
		return time.Time{}, false
	}

	return time.Unix(0, nano), true
}

func extractClientID(filename string) string {
	parts := strings.Split(filename, "-")

	// Expected mux format:
	// req-{clientID}-mux-{timestamp}.bin
	// res-{clientID}-mux-{timestamp}.bin
	if len(parts) >= 4 && parts[2] == "mux" {
		return parts[1]
	}

	return ""
}
