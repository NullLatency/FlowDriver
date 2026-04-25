package transport

import (
	"context"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

	pollTicker         time.Duration
	flushTicker        time.Duration
	idlePollMax        time.Duration
	idlePollStep       time.Duration
	sessionIdleTimeout time.Duration
	cleanupFileMaxAge  time.Duration
	maxPayloadBytes    int
	backpressureBytes  int
	storageOpTimeout   time.Duration
	immediateFlush     bool
	metricsLogInterval time.Duration

	// Server mode handler: called when a new session is discovered
	OnNewSession func(sessionID, targetAddr string, s *Session)

	// Concurrency control for storage operations (Upload/Download)
	sem chan struct{}

	// Track processed files to avoid duplicates
	processed   map[string]bool
	processedMu sync.Mutex

	flushTrigger chan struct{}
	metrics      engineMetrics
}

type engineMetrics struct {
	uploads              uint64
	downloads            uint64
	deletes              uint64
	listCalls            uint64
	uploadBytes          uint64
	downloadBytes        uint64
	uploadErrors         uint64
	downloadErrors       uint64
	listErrors           uint64
	deleteErrors         uint64
	uploadLatencyMs      uint64
	downloadLatencyMs    uint64
	listLatencyMs        uint64
	deleteLatencyMs      uint64
	maxUploadLatencyMs   uint64
	maxDownloadLatencyMs uint64
	maxListLatencyMs     uint64
	maxDeleteLatencyMs   uint64
	fileAgeMs            uint64
	maxFileAgeMs         uint64
	firstResponses       uint64
	firstResponseMs      uint64
	maxFirstResponseMs   uint64
}

type MetricsSnapshot struct {
	ActiveSessions       int     `json:"active_sessions"`
	Uploads              uint64  `json:"uploads"`
	Downloads            uint64  `json:"downloads"`
	Deletes              uint64  `json:"deletes"`
	ListCalls            uint64  `json:"list_calls"`
	UploadBytes          uint64  `json:"upload_bytes"`
	DownloadBytes        uint64  `json:"download_bytes"`
	UploadErrors         uint64  `json:"upload_errors"`
	DownloadErrors       uint64  `json:"download_errors"`
	ListErrors           uint64  `json:"list_errors"`
	DeleteErrors         uint64  `json:"delete_errors"`
	AvgUploadLatencyMs   float64 `json:"avg_upload_latency_ms"`
	AvgDownloadLatencyMs float64 `json:"avg_download_latency_ms"`
	AvgListLatencyMs     float64 `json:"avg_list_latency_ms"`
	AvgDeleteLatencyMs   float64 `json:"avg_delete_latency_ms"`
	MaxUploadLatencyMs   uint64  `json:"max_upload_latency_ms"`
	MaxDownloadLatencyMs uint64  `json:"max_download_latency_ms"`
	MaxListLatencyMs     uint64  `json:"max_list_latency_ms"`
	MaxDeleteLatencyMs   uint64  `json:"max_delete_latency_ms"`
	AvgFileAgeMs         float64 `json:"avg_file_age_ms"`
	MaxFileAgeMs         uint64  `json:"max_file_age_ms"`
	FirstResponses       uint64  `json:"first_responses"`
	AvgFirstResponseMs   float64 `json:"avg_first_response_ms"`
	MaxFirstResponseMs   uint64  `json:"max_first_response_ms"`
}

func NewEngine(backend storage.Backend, isClient bool, clientID string) *Engine {
	e := &Engine{
		backend:        backend,
		id:             clientID,
		sessions:       make(map[string]*Session),
		closedSessions: make(map[string]time.Time),
		processed:      make(map[string]bool),
		// Default intervals: Poll (RX) fast for responsiveness, Flush (TX) slower for gathering
		pollTicker:         500 * time.Millisecond,
		flushTicker:        300 * time.Millisecond,
		idlePollMax:        5 * time.Second,
		idlePollStep:       500 * time.Millisecond,
		sessionIdleTimeout: 10 * time.Second,
		cleanupFileMaxAge:  10 * time.Second,
		maxPayloadBytes:    768 * 1024,
		backpressureBytes:  2 * 1024 * 1024,
		storageOpTimeout:   18 * time.Second,
		immediateFlush:     false,
		metricsLogInterval: 30 * time.Second,
		flushTrigger:       make(chan struct{}, 1),
	}
	if isClient {
		e.myDir = DirReq
		e.peerDir = DirRes
	} else {
		e.myDir = DirRes
		e.peerDir = DirReq
	}
	// Limit to 8 concurrent upload/download operations to avoid OOM and FD exhaustion
	e.sem = make(chan struct{}, 8)
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

func (e *Engine) SetIdlePollMax(ms int) {
	if ms > 0 {
		e.idlePollMax = time.Duration(ms) * time.Millisecond
	}
}

func (e *Engine) SetIdlePollStep(ms int) {
	if ms > 0 {
		e.idlePollStep = time.Duration(ms) * time.Millisecond
	}
}

func (e *Engine) SetSessionIdleTimeout(seconds int) {
	if seconds > 0 {
		e.sessionIdleTimeout = time.Duration(seconds) * time.Second
	}
}

func (e *Engine) SetCleanupFileMaxAge(seconds int) {
	if seconds > 0 {
		e.cleanupFileMaxAge = time.Duration(seconds) * time.Second
	}
}

func (e *Engine) SetMaxPayloadBytes(bytes int) {
	if bytes > 0 {
		e.maxPayloadBytes = bytes
	}
}

func (e *Engine) SetBackpressureBytes(bytes int) {
	if bytes > 0 {
		e.backpressureBytes = bytes
	}
}

func (e *Engine) SetStorageOpTimeout(seconds int) {
	if seconds > 0 {
		e.storageOpTimeout = time.Duration(seconds) * time.Second
	}
}

func (e *Engine) SetImmediateFlush(enabled bool) {
	e.immediateFlush = enabled
}

func (e *Engine) ActiveSessionCount() int {
	e.sessionMu.RLock()
	defer e.sessionMu.RUnlock()
	return len(e.sessions)
}

func (e *Engine) SetMetricsLogInterval(seconds int) {
	if seconds > 0 {
		e.metricsLogInterval = time.Duration(seconds) * time.Second
	}
}

func (e *Engine) Start(ctx context.Context) {
	go e.flushLoop(ctx)
	go e.pollLoop(ctx)
	go e.cleanupLoop(ctx) // Delete files older than 10s
	go e.metricsLoop(ctx)
}

func (e *Engine) GetSession(id string) *Session {
	e.sessionMu.RLock()
	defer e.sessionMu.RUnlock()
	return e.sessions[id]
}

func (e *Engine) AddSession(s *Session) {
	s.SetBackpressureBytes(e.backpressureBytes)
	e.sessionMu.Lock()
	defer e.sessionMu.Unlock()
	e.sessions[s.ID] = s
	log.Printf("Engine.AddSession: Added session %s (Total now: %d)", s.ID, len(e.sessions))
	e.RequestFlush()
}

func (e *Engine) RequestFlush() {
	if !e.immediateFlush {
		return
	}
	select {
	case e.flushTrigger <- struct{}{}:
	default:
	}
}

func (e *Engine) Snapshot() MetricsSnapshot {
	current := e.snapshotMetrics()
	e.sessionMu.RLock()
	activeSessions := len(e.sessions)
	e.sessionMu.RUnlock()

	return MetricsSnapshot{
		ActiveSessions:       activeSessions,
		Uploads:              current.uploads,
		Downloads:            current.downloads,
		Deletes:              current.deletes,
		ListCalls:            current.listCalls,
		UploadBytes:          current.uploadBytes,
		DownloadBytes:        current.downloadBytes,
		UploadErrors:         current.uploadErrors,
		DownloadErrors:       current.downloadErrors,
		ListErrors:           current.listErrors,
		DeleteErrors:         current.deleteErrors,
		AvgUploadLatencyMs:   averageMs(current.uploadLatencyMs, current.uploads),
		AvgDownloadLatencyMs: averageMs(current.downloadLatencyMs, current.downloads),
		AvgListLatencyMs:     averageMs(current.listLatencyMs, current.listCalls),
		AvgDeleteLatencyMs:   averageMs(current.deleteLatencyMs, current.deletes),
		MaxUploadLatencyMs:   current.maxUploadLatencyMs,
		MaxDownloadLatencyMs: current.maxDownloadLatencyMs,
		MaxListLatencyMs:     current.maxListLatencyMs,
		MaxDeleteLatencyMs:   current.maxDeleteLatencyMs,
		AvgFileAgeMs:         averageMs(current.fileAgeMs, current.downloads),
		MaxFileAgeMs:         current.maxFileAgeMs,
		FirstResponses:       current.firstResponses,
		AvgFirstResponseMs:   averageMs(current.firstResponseMs, current.firstResponses),
		MaxFirstResponseMs:   current.maxFirstResponseMs,
	}
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
		case <-e.flushTrigger:
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
	needsFollowupFlush := false

	for _, s := range sessions {
		s.mu.Lock()

		// Idle Timeout check
		if time.Since(s.lastActivity) > e.sessionIdleTimeout {
			s.closed = true
		}

		shouldSend := len(s.txBuf) > 0 || (s.txSeq == 0 && e.myDir == DirReq) || s.closed

		if !shouldSend {
			s.mu.Unlock()
			continue
		}

		payload := s.txBuf
		if e.maxPayloadBytes > 0 && len(payload) > e.maxPayloadBytes {
			payload = append([]byte(nil), payload[:e.maxPayloadBytes]...)
			s.txBuf = append([]byte(nil), s.txBuf[e.maxPayloadBytes:]...)
			needsFollowupFlush = true
		} else {
			s.txBuf = nil
		}
		s.txCond.Broadcast() // Release any blocked writers
		closePacket := s.closed && len(s.txBuf) == 0

		env := Envelope{
			SessionID:  s.ID,
			Seq:        s.txSeq,
			Payload:    payload,
			Close:      closePacket,
			TargetAddr: s.TargetAddr,
		}

		s.txSeq++
		if closePacket {
			closedSessionIDs = append(closedSessionIDs, s.ID)
		}

		cid := s.ClientID
		if cid == "" && e.myDir == DirReq {
			cid = e.id // For client requests, use our own ID
		}

		muxes[cid] = append(muxes[cid], env)
		s.mu.Unlock()
	}

	if len(muxes) > 0 {
		// log.Printf("Engine.flushAll: Prepared muxes for %d clients", len(muxes))
	}

	for cid, mux := range muxes {
		// Filename format: {dir}-{clientID}-mux-{timestamp}.bin
		fnameCID := cid
		if fnameCID == "" {
			fnameCID = "unknown"
		}
		filename := fmt.Sprintf("%s-%s-mux-%d.bin", e.myDir, fnameCID, time.Now().UnixNano())
		payloadBytes := muxPayloadBytes(mux)

		// Upload asynchronously with backpressure/limit
		go func(fname string, m []Envelope, bytes int) {
			e.sem <- struct{}{}        // Acquire
			defer func() { <-e.sem }() // Release

			pr, pw := io.Pipe()
			go func() {
				defer pw.Close()
				for _, env := range m {
					if err := env.Encode(pw); err != nil {
						log.Printf("mux encode error: %v", err)
						break
					}
				}
			}()

			start := time.Now()
			opCtx, cancel := e.storageContext(ctx)
			err := e.backend.Upload(opCtx, fname, pr)
			cancel()
			if err != nil {
				atomic.AddUint64(&e.metrics.uploadErrors, 1)
				log.Printf("upload error %s: %v", fname, err)
				return
			}
			latencyMs := uint64(time.Since(start).Milliseconds())
			atomic.AddUint64(&e.metrics.uploads, 1)
			atomic.AddUint64(&e.metrics.uploadBytes, uint64(bytes))
			atomic.AddUint64(&e.metrics.uploadLatencyMs, latencyMs)
			atomicMaxUint64(&e.metrics.maxUploadLatencyMs, latencyMs)
		}(filename, mux, payloadBytes)
	}

	for _, id := range closedSessionIDs {
		e.RemoveSession(id)
	}
	if needsFollowupFlush {
		e.RequestFlush()
	}
}

func (e *Engine) pollLoop(ctx context.Context) {
	currentPollInterval := e.pollTicker
	timer := time.NewTimer(currentPollInterval)
	defer timer.Stop()

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
			listStart := time.Now()
			opCtx, cancel := e.storageContext(ctx)
			files, err := e.backend.ListQuery(opCtx, prefix)
			cancel()
			if err != nil {
				atomic.AddUint64(&e.metrics.listErrors, 1)
				log.Printf("poll list error: %v", err)
				timer.Reset(currentPollInterval)
				continue
			}
			listLatencyMs := uint64(time.Since(listStart).Milliseconds())
			atomic.AddUint64(&e.metrics.listCalls, 1)
			atomic.AddUint64(&e.metrics.listLatencyMs, listLatencyMs)
			atomicMaxUint64(&e.metrics.maxListLatencyMs, listLatencyMs)

			if len(files) == 0 {
				if e.myDir == DirRes { // SERVER OPTIMIZATION
					e.sessionMu.RLock()
					activeSessions := len(e.sessions)
					e.sessionMu.RUnlock()

					if activeSessions == 0 {
						// Increase polling delay step-by-step to save API calls
						currentPollInterval += e.idlePollStep
						if currentPollInterval > e.idlePollMax {
							currentPollInterval = e.idlePollMax
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
				// STARTUP OPTIMIZATION: Ignore files older than 5 minutes to avoid memory spikes on restart
				parts := strings.Split(f, "-")
				if len(parts) >= 3 {
					tsStr := parts[len(parts)-1]
					tsStr = strings.TrimSuffix(tsStr, ".bin")
					ts, _ := strconv.ParseInt(tsStr, 10, 64)
					if ts > 0 && time.Since(time.Unix(0, ts)) > e.cleanupFileMaxAge {
						e.deleteAsync(ctx, f) // Silent cleanup
						continue
					}
				}

				e.processedMu.Lock()
				already := e.processed[f]
				if !already {
					e.processed[f] = true
				}
				e.processedMu.Unlock()

				if already {
					continue
				}

				wg.Add(1)
				go func(fname string) {
					defer wg.Done()

					e.sem <- struct{}{}        // Acquire
					defer func() { <-e.sem }() // Release

					// log.Printf("Engine.pollLoop: Downloading %s", fname)
					downloadStart := time.Now()
					opCtx, cancel := e.storageContext(ctx)
					rc, err := e.backend.Download(opCtx, fname)
					if err != nil {
						cancel()
						atomic.AddUint64(&e.metrics.downloadErrors, 1)
						log.Printf("download error %s: %v", fname, err)
						e.processedMu.Lock()
						delete(e.processed, fname) // failed to download, retry next poll
						e.processedMu.Unlock()
						return
					}
					defer func() {
						rc.Close()
						cancel()
					}()
					downloadLatencyMs := uint64(time.Since(downloadStart).Milliseconds())
					fileAgeMs := fileAgeMilliseconds(fname)

					// Extract ClientID from filename for server-side session initialization
					var fileClientID string
					parts := strings.Split(fname, "-")
					if len(parts) >= 4 && parts[2] == "mux" {
						fileClientID = parts[1]
					}

					// STREAMING DECODE
					count := 0
					payloadBytes := 0
					for {
						var env Envelope
						if err := env.Decode(rc); err != nil {
							if err != io.EOF && err != io.ErrUnexpectedEOF {
								log.Printf("mux decode error %s: %v", fname, err)
							}
							break
						}
						count++
						payloadBytes += len(env.Payload)

						// Process envelope immediately
						e.closedSessionsMu.Lock()
						if _, exists := e.closedSessions[env.SessionID]; exists {
							e.closedSessionsMu.Unlock()
							continue
						}
						e.closedSessionsMu.Unlock()

						e.sessionMu.Lock()
						s, exists := e.sessions[env.SessionID]
						if !exists && e.myDir == DirRes && e.OnNewSession != nil {
							s = NewSession(env.SessionID)
							s.ClientID = fileClientID
							s.TargetAddr = env.TargetAddr
							e.sessions[env.SessionID] = s
							e.sessionMu.Unlock()
							log.Printf("Engine: Triggering new session %s for Client %s", env.SessionID, fileClientID)
							e.OnNewSession(env.SessionID, env.TargetAddr, s)
						} else {
							e.sessionMu.Unlock()
						}

						if s != nil {
							if len(env.Payload) > 0 {
								e.recordFirstResponse(s)
							}
							s.ProcessRx(&env)
						}
					}

					atomic.AddUint64(&e.metrics.downloads, 1)
					atomic.AddUint64(&e.metrics.downloadBytes, uint64(payloadBytes))
					atomic.AddUint64(&e.metrics.downloadLatencyMs, downloadLatencyMs)
					atomicMaxUint64(&e.metrics.maxDownloadLatencyMs, downloadLatencyMs)
					if fileAgeMs > 0 {
						atomic.AddUint64(&e.metrics.fileAgeMs, fileAgeMs)
						atomicMaxUint64(&e.metrics.maxFileAgeMs, fileAgeMs)
					}
					e.deleteAsync(ctx, fname)
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

func (e *Engine) deleteAsync(ctx context.Context, filename string) {
	go func() {
		e.sem <- struct{}{}
		defer func() { <-e.sem }()

		start := time.Now()
		opCtx, cancel := e.storageContext(ctx)
		err := e.backend.Delete(opCtx, filename)
		cancel()
		if err != nil {
			atomic.AddUint64(&e.metrics.deleteErrors, 1)
			return
		}
		latencyMs := uint64(time.Since(start).Milliseconds())
		atomic.AddUint64(&e.metrics.deletes, 1)
		atomic.AddUint64(&e.metrics.deleteLatencyMs, latencyMs)
		atomicMaxUint64(&e.metrics.maxDeleteLatencyMs, latencyMs)
	}()
}

func (e *Engine) storageContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if e.storageOpTimeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, e.storageOpTimeout)
}

func (e *Engine) recordFirstResponse(s *Session) {
	s.mu.Lock()
	if s.firstResponseLogged {
		s.mu.Unlock()
		return
	}
	s.firstResponseLogged = true
	targetAddr := s.TargetAddr
	elapsed := time.Since(s.createdAt)
	s.mu.Unlock()

	latencyMs := uint64(elapsed.Milliseconds())
	atomic.AddUint64(&e.metrics.firstResponses, 1)
	atomic.AddUint64(&e.metrics.firstResponseMs, latencyMs)
	atomicMaxUint64(&e.metrics.maxFirstResponseMs, latencyMs)
	if latencyMs > 2000 {
		log.Printf("session first response slow: id=%s target=%s first_response_ms=%d", s.ID, targetAddr, latencyMs)
	}
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

			// Periodically clear processed map to prevent infinite growth
			e.processedMu.Lock()
			if len(e.processed) > 5000 {
				e.processed = make(map[string]bool)
			}
			e.processedMu.Unlock()

			// ZERO-TRAFFIC CLIENT OPTIMIZATION:
			if e.myDir == DirReq {
				e.sessionMu.RLock()
				count := len(e.sessions)
				e.sessionMu.RUnlock()
				if count == 0 {
					continue
				}
			}

			listStart := time.Now()
			opCtx, cancel := e.storageContext(ctx)
			files, err := e.backend.ListQuery(opCtx, string(e.myDir)+"-")
			cancel()
			if err != nil {
				atomic.AddUint64(&e.metrics.listErrors, 1)
				continue
			}
			listLatencyMs := uint64(time.Since(listStart).Milliseconds())
			atomic.AddUint64(&e.metrics.listCalls, 1)
			atomic.AddUint64(&e.metrics.listLatencyMs, listLatencyMs)
			atomicMaxUint64(&e.metrics.maxListLatencyMs, listLatencyMs)
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
						if time.Since(t) > e.cleanupFileMaxAge {
							e.deleteAsync(ctx, f)
						}
					}
				}
			}
		}
	}
}

func (e *Engine) metricsLoop(ctx context.Context) {
	if e.metricsLogInterval <= 0 {
		return
	}

	ticker := time.NewTicker(e.metricsLogInterval)
	defer ticker.Stop()

	var last engineMetrics
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			current := e.snapshotMetrics()
			e.sessionMu.RLock()
			activeSessions := len(e.sessions)
			e.sessionMu.RUnlock()

			log.Printf(
				"metrics: active=%d uploads=%d/%s up_avg_ms=%.0f downloads=%d/%s down_avg_ms=%.0f lists=%d list_avg_ms=%.0f deletes=%d file_age_avg_ms=%.0f first_resp_avg_ms=%.0f errors[u=%d d=%d l=%d del=%d]",
				activeSessions,
				current.uploads-last.uploads,
				formatBytes(current.uploadBytes-last.uploadBytes),
				averageMs(current.uploadLatencyMs-last.uploadLatencyMs, current.uploads-last.uploads),
				current.downloads-last.downloads,
				formatBytes(current.downloadBytes-last.downloadBytes),
				averageMs(current.downloadLatencyMs-last.downloadLatencyMs, current.downloads-last.downloads),
				current.listCalls-last.listCalls,
				averageMs(current.listLatencyMs-last.listLatencyMs, current.listCalls-last.listCalls),
				current.deletes-last.deletes,
				averageMs(current.fileAgeMs-last.fileAgeMs, current.downloads-last.downloads),
				averageMs(current.firstResponseMs-last.firstResponseMs, current.firstResponses-last.firstResponses),
				current.uploadErrors-last.uploadErrors,
				current.downloadErrors-last.downloadErrors,
				current.listErrors-last.listErrors,
				current.deleteErrors-last.deleteErrors,
			)
			last = current
		}
	}
}

func (e *Engine) snapshotMetrics() engineMetrics {
	return engineMetrics{
		uploads:              atomic.LoadUint64(&e.metrics.uploads),
		downloads:            atomic.LoadUint64(&e.metrics.downloads),
		deletes:              atomic.LoadUint64(&e.metrics.deletes),
		listCalls:            atomic.LoadUint64(&e.metrics.listCalls),
		uploadBytes:          atomic.LoadUint64(&e.metrics.uploadBytes),
		downloadBytes:        atomic.LoadUint64(&e.metrics.downloadBytes),
		uploadErrors:         atomic.LoadUint64(&e.metrics.uploadErrors),
		downloadErrors:       atomic.LoadUint64(&e.metrics.downloadErrors),
		listErrors:           atomic.LoadUint64(&e.metrics.listErrors),
		deleteErrors:         atomic.LoadUint64(&e.metrics.deleteErrors),
		uploadLatencyMs:      atomic.LoadUint64(&e.metrics.uploadLatencyMs),
		downloadLatencyMs:    atomic.LoadUint64(&e.metrics.downloadLatencyMs),
		listLatencyMs:        atomic.LoadUint64(&e.metrics.listLatencyMs),
		deleteLatencyMs:      atomic.LoadUint64(&e.metrics.deleteLatencyMs),
		maxUploadLatencyMs:   atomic.LoadUint64(&e.metrics.maxUploadLatencyMs),
		maxDownloadLatencyMs: atomic.LoadUint64(&e.metrics.maxDownloadLatencyMs),
		maxListLatencyMs:     atomic.LoadUint64(&e.metrics.maxListLatencyMs),
		maxDeleteLatencyMs:   atomic.LoadUint64(&e.metrics.maxDeleteLatencyMs),
		fileAgeMs:            atomic.LoadUint64(&e.metrics.fileAgeMs),
		maxFileAgeMs:         atomic.LoadUint64(&e.metrics.maxFileAgeMs),
		firstResponses:       atomic.LoadUint64(&e.metrics.firstResponses),
		firstResponseMs:      atomic.LoadUint64(&e.metrics.firstResponseMs),
		maxFirstResponseMs:   atomic.LoadUint64(&e.metrics.maxFirstResponseMs),
	}
}

func muxPayloadBytes(mux []Envelope) int {
	var total int
	for _, env := range mux {
		total += len(env.Payload)
	}
	return total
}

func formatBytes(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	value := float64(n)
	for _, suffix := range []string{"KB", "MB", "GB"} {
		value /= unit
		if value < unit {
			return fmt.Sprintf("%.1f%s", value, suffix)
		}
	}
	return fmt.Sprintf("%.1fTB", value/unit)
}

func averageMs(total, count uint64) float64 {
	if count == 0 {
		return 0
	}
	return float64(total) / float64(count)
}

func atomicMaxUint64(target *uint64, value uint64) {
	for {
		current := atomic.LoadUint64(target)
		if value <= current {
			return
		}
		if atomic.CompareAndSwapUint64(target, current, value) {
			return
		}
	}
}

func fileAgeMilliseconds(filename string) uint64 {
	parts := strings.Split(filename, "-")
	if len(parts) < 3 {
		return 0
	}
	tsStr := strings.TrimSuffix(parts[len(parts)-1], ".bin")
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil || ts <= 0 {
		return 0
	}
	age := time.Since(time.Unix(0, ts))
	if age < 0 {
		return 0
	}
	return uint64(age.Milliseconds())
}
