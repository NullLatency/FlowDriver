package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/NullLatency/flow-driver/internal/app"
	"github.com/NullLatency/flow-driver/internal/config"
	"github.com/NullLatency/flow-driver/internal/health"
	"github.com/NullLatency/flow-driver/internal/transport"
	"github.com/things-go/go-socks5"
	"github.com/things-go/go-socks5/statute"
)

func generateSessionID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

type rawResolver struct{}

func (rawResolver) Resolve(ctx context.Context, name string) (context.Context, net.IP, error) {
	// Defends comprehensively against Local DNS leaks by doing absolutely nothing.
	return ctx, nil, nil
}

func main() {
	var configPath, gcPath string
	flag.StringVar(&configPath, "c", "config.json", "Path to config file")
	flag.StringVar(&gcPath, "gc", "credentials.json", "Path to Google Service Account JSON")
	flag.Parse()

	log.Println("Starting Flow Client...")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	appCfg, err := config.Load(configPath)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	appCfg.ApplyProfile()

	backend, err := app.BuildBackend(appCfg, gcPath)
	if err != nil {
		log.Fatalf("Failed to init storage: %v", err)
	}
	if err := backend.Login(ctx); err != nil {
		log.Fatalf("Backend login failed: %v", err)
	}

	// AUTOMATION: If folder ID is missing, find or create it
	if appCfg.StorageType == "google" && len(appCfg.GoogleLanes) == 0 && appCfg.GoogleFolderID == "" {
		log.Println("Zero-Config: Searching for existing Google Drive folder 'Flow-Data'...")
		folderID, err := backend.FindFolder(ctx, "Flow-Data")
		if err != nil {
			log.Fatalf("Failed to search for folder: %v", err)
		}

		if folderID == "" {
			log.Println("Zero-Config: 'Flow-Data' not found. Creating new folder...")
			folderID, err = backend.CreateFolder(ctx, "Flow-Data")
			if err != nil {
				log.Fatalf("Failed to auto-create folder: %v", err)
			}
		} else {
			log.Printf("Zero-Config: Found existing folder with ID %s", folderID)
		}

		appCfg.GoogleFolderID = folderID
		if err := appCfg.Save(configPath); err != nil {
			log.Printf("Warning: Failed to save folder ID to %s: %v", configPath, err)
		} else {
			log.Printf("Zero-Config: Config updated with folder ID %s", folderID)
		}
	}

	cid := appCfg.ClientID
	if cid == "" {
		cid = generateSessionID()[:8] // Short random ID as fallback
	}
	engine := transport.NewEngine(backend, true, cid)
	if appCfg.RefreshRateMs > 0 {
		engine.SetPollRate(appCfg.RefreshRateMs)
	}
	if appCfg.FlushRateMs > 0 {
		engine.SetFlushRate(appCfg.FlushRateMs)
	}
	engine.SetIdlePollMax(appCfg.IdlePollMaxMs)
	engine.SetIdlePollStep(appCfg.IdlePollStepMs)
	engine.SetSessionIdleTimeout(appCfg.SessionIdleTimeoutSec)
	engine.SetCleanupFileMaxAge(appCfg.CleanupFileMaxAgeSec)
	engine.SetStartupStaleMaxAge(appCfg.StartupStaleMaxAgeSec)
	engine.SetMaxPayloadBytes(appCfg.MaxPayloadBytes)
	engine.SetBackpressureBytes(appCfg.BackpressureBytes)
	engine.SetStorageOpTimeout(appCfg.StorageOpTimeoutSec)
	engine.SetImmediateFlush(appCfg.ImmediateFlush)
	engine.SetColdStartBurst(appCfg.ColdStartBurstMs, appCfg.ColdStartPollMs)
	engine.SetMetricsLogInterval(appCfg.MetricsLogSec)
	engine.SetTargetMetricsTopN(appCfg.TargetMetricsTopN)
	engine.Start(ctx)
	health.Start(ctx, appCfg.HealthListenAddr, engine)

	listenAddr := appCfg.ListenAddr
	if listenAddr == "" {
		listenAddr = "127.0.0.1:1080"
	}
	policy := newTargetPolicy(appCfg.BlockedTargets, appCfg.LowPriorityTargets)

	// Create the library SOCKS5 server wrapping our custom Google Drive Engine tunnel
	server := socks5.NewServer(
		socks5.WithDial(func(dc context.Context, network, addr string) (net.Conn, error) {
			host, port, hasHostPort := splitTarget(addr)
			if policy.blocked(host, port, addr) {
				engine.RecordBlockedTarget(addr)
				log.Printf("Blocked target by policy: %s", addr)
				return nil, fmt.Errorf("target blocked by policy: %s", addr)
			}
			lowPriority := policy.lowPriority(host, port, addr)

			if err := waitForSessionCapacity(dc, engine, appCfg.MaxActiveSessions, appCfg.SessionWaitTimeoutSec); err != nil {
				return nil, err
			}

			sessionID := generateSessionID()

			// Intelligently parse the address string to warn users if their browser is natively leaking DNS
			priorityLabel := ""
			if lowPriority {
				priorityLabel = " LOW-PRIORITY"
			}
			if hasHostPort {
				if net.ParseIP(host) != nil {
					log.Printf("New covert session %s%s targeting RAW IP %s:%s (Warning: Local DNS Leak?)", sessionID, priorityLabel, host, port)
				} else {
					log.Printf("New covert session %s%s targeting SECURE DOMAIN %s:%s", sessionID, priorityLabel, host, port)
				}
			} else {
				log.Printf("New covert session %s%s targeting %s", sessionID, priorityLabel, addr)
			}

			session := transport.NewSession(sessionID)
			session.TargetAddr = addr
			session.TargetHost = host
			session.LowPriority = lowPriority
			engine.AddSession(session)

			// Instantly ping a blank payload so the remote end opens the actual TCP destination
			session.EnqueueTx(nil)
			if !lowPriority {
				engine.TriggerWarmPoll()
				engine.ForceFlush()
			}

			return transport.NewVirtualConn(session, engine), nil
		}),
		socks5.WithAssociateHandle(func(ctx context.Context, w io.Writer, req *socks5.Request) error {
			// Explicitly block UDP routing to confidently prevent ISP endpoint leakage
			socks5.SendReply(w, statute.RepCommandNotSupported, nil)
			return fmt.Errorf("covert UDP not supported")
		}),
		// DEFEND AGAINST LOCAL DNS LEAKS:
		// The library natively performs system DNS lookups for all FQDNs before proxying!
		// We explicitly override the resolver with a NoOp dummy to force raw strings into the pipe.
		socks5.WithResolver(rawResolver{}),
	)

	log.Printf("Listening for SOCKS5 on %s...", listenAddr)

	go func() {
		if err := server.ListenAndServe("tcp", listenAddr); err != nil {
			log.Fatalf("SOCKS5 server failed: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("Shutting down client...")
	cancel()
}

type targetPolicy struct {
	blockedTargets     []targetPattern
	lowPriorityTargets []targetPattern
}

type targetPattern struct {
	raw  string
	host string
	port string
}

func newTargetPolicy(blocked, lowPriority []string) targetPolicy {
	return targetPolicy{
		blockedTargets:     parseTargetPatterns(blocked),
		lowPriorityTargets: parseTargetPatterns(lowPriority),
	}
}

func parseTargetPatterns(values []string) []targetPattern {
	patterns := make([]targetPattern, 0, len(values))
	for _, value := range values {
		raw := strings.TrimSpace(strings.ToLower(value))
		if raw == "" {
			continue
		}
		host, port, ok := splitPattern(raw)
		if !ok {
			host = raw
		}
		patterns = append(patterns, targetPattern{raw: raw, host: host, port: port})
	}
	return patterns
}

func splitPattern(value string) (host, port string, ok bool) {
	if strings.HasPrefix(value, "[") {
		host, port, err := net.SplitHostPort(value)
		if err == nil {
			return strings.Trim(host, "[]"), port, true
		}
	}
	lastColon := strings.LastIndex(value, ":")
	if lastColon <= 0 {
		return "", "", false
	}
	host = value[:lastColon]
	port = value[lastColon+1:]
	if host == "" || port == "" {
		return "", "", false
	}
	return strings.Trim(host, "[]"), port, true
}

func (p targetPolicy) blocked(host, port, raw string) bool {
	return targetPatternsMatch(p.blockedTargets, host, port, raw)
}

func (p targetPolicy) lowPriority(host, port, raw string) bool {
	return targetPatternsMatch(p.lowPriorityTargets, host, port, raw)
}

func targetPatternsMatch(patterns []targetPattern, host, port, raw string) bool {
	host = strings.ToLower(strings.Trim(host, "[]"))
	port = strings.ToLower(port)
	raw = strings.ToLower(raw)
	for _, pattern := range patterns {
		if pattern.match(host, port, raw) {
			return true
		}
	}
	return false
}

func (p targetPattern) match(host, port, raw string) bool {
	if p.port != "" && p.port != "*" && p.port != port {
		return false
	}

	patternHost := p.host
	if patternHost == "" {
		patternHost = p.raw
	}
	if matched, err := path.Match(patternHost, host); err == nil && matched {
		return true
	}
	if p.port == "" {
		if matched, err := path.Match(p.raw, raw); err == nil && matched {
			return true
		}
	}
	return patternHost == host || (p.port == "" && p.raw == raw)
}

func splitTarget(addr string) (host, port string, ok bool) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return strings.ToLower(strings.Trim(addr, "[]")), "", false
	}
	return strings.ToLower(strings.Trim(host, "[]")), strings.ToLower(port), true
}

func waitForSessionCapacity(ctx context.Context, engine *transport.Engine, maxActive, timeoutSec int) error {
	if maxActive <= 0 {
		return nil
	}
	if engine.ActiveSessionCount() < maxActive {
		return nil
	}

	waitTimeout := 10 * time.Second
	if timeoutSec > 0 {
		waitTimeout = time.Duration(timeoutSec) * time.Second
	}
	timer := time.NewTimer(waitTimeout)
	defer timer.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return fmt.Errorf("too many active sessions: %d/%d", engine.ActiveSessionCount(), maxActive)
		case <-ticker.C:
			if engine.ActiveSessionCount() < maxActive {
				return nil
			}
		}
	}
}
