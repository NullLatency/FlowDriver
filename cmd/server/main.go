package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/NullLatency/flow-driver/internal/app"
	"github.com/NullLatency/flow-driver/internal/config"
	"github.com/NullLatency/flow-driver/internal/health"
	"github.com/NullLatency/flow-driver/internal/transport"
)

func main() {
	var configPath, gcPath string
	flag.StringVar(&configPath, "c", "config.json", "Path to config file")
	flag.StringVar(&gcPath, "gc", "credentials.json", "Path to Google Service Account JSON")
	flag.Parse()

	log.Println("Starting Flow Server...")
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

	engine := transport.NewEngine(backend, false, "")
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
	engine.SetMaxPayloadBytes(appCfg.MaxPayloadBytes)
	engine.SetBackpressureBytes(appCfg.BackpressureBytes)
	engine.SetStorageOpTimeout(appCfg.StorageOpTimeoutSec)
	engine.SetImmediateFlush(appCfg.ImmediateFlush)
	engine.SetMetricsLogInterval(appCfg.MetricsLogSec)

	// Called by polling loop when a new incoming session file is found
	engine.OnNewSession = func(sessionID, targetAddr string, session *transport.Session) {
		log.Printf("Server received new session %s destined for %s", sessionID, targetAddr)
		go handleServerConn(sessionID, targetAddr, session, engine)
	}

	engine.Start(ctx)
	health.Start(ctx, appCfg.HealthListenAddr, engine)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("Shutting down server...")
	cancel()
}

func handleServerConn(sessionID, targetAddr string, session *transport.Session, engine *transport.Engine) {
	defer engine.RemoveSession(sessionID)

	conn, err := net.Dial("tcp", targetAddr)
	if err != nil {
		log.Printf("Dial error to %s: %v", targetAddr, err)
		// Send back a close packet? Just closing the session will notify client
		return
	}
	defer conn.Close()

	errCh := make(chan error, 2)

	// Conn -> Tx (Res)
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := conn.Read(buf)
			if n > 0 {
				session.EnqueueTx(buf[:n])
				engine.RequestFlush()
			}
			if err != nil {
				errCh <- err
				return
			}
		}
	}()

	// Rx (Req) -> Conn
	go func() {
		for {
			data, ok := <-session.RxChan
			if !ok {
				errCh <- fmt.Errorf("session closed by remote")
				return
			}
			if len(data) > 0 {
				if _, err := conn.Write(data); err != nil {
					errCh <- err
					return
				}
			}
		}
	}()

	<-errCh
}
