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
	"syscall"

	"github.com/NullLatency/flow-driver/internal/config"
	"github.com/NullLatency/flow-driver/internal/httpclient"
	"github.com/NullLatency/flow-driver/internal/storage"
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

	var backend storage.Backend
	if appCfg.StorageType == "google" {
		customHttpClient := httpclient.NewCustomClient(appCfg.Transport)
		backend = storage.NewGoogleBackend(customHttpClient, gcPath, appCfg.GoogleFolderID)
	} else {
		backend, err = storage.NewLocalBackend(appCfg.LocalDir)
		if err != nil {
			log.Fatalf("Failed to init local storage: %v", err)
		}
	}
	if err := backend.Login(ctx); err != nil {
		log.Fatalf("Backend login failed: %v", err)
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
	engine.Start(ctx)

	listenAddr := appCfg.ListenAddr
	if listenAddr == "" {
		listenAddr = "127.0.0.1:1080"
	}

	// Create the library SOCKS5 server wrapping our custom Google Drive Engine tunnel
	server := socks5.NewServer(
		socks5.WithDial(func(dc context.Context, network, addr string) (net.Conn, error) {
			sessionID := generateSessionID()

			// Intelligently parse the address string to warn users if their browser is natively leaking DNS
			host, port, err := net.SplitHostPort(addr)
			if err == nil {
				if net.ParseIP(host) != nil {
					log.Printf("New covert session %s targeting RAW IP %s:%s (Warning: Local DNS Leak?)", sessionID, host, port)
				} else {
					log.Printf("New covert session %s targeting SECURE DOMAIN %s:%s", sessionID, host, port)
				}
			} else {
				log.Printf("New covert session %s targeting %s", sessionID, addr)
			}

			session := transport.NewSession(sessionID)
			session.TargetAddr = addr
			engine.AddSession(session)

			// Instantly ping a blank payload so the remote end opens the actual TCP destination
			session.EnqueueTx(nil)

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
