package main

import (
	"context"
	"flag"
	"log"
	"sync"

	"github.com/NullLatency/flow-driver/internal/app"
	"github.com/NullLatency/flow-driver/internal/config"
)

func main() {
	var configPath, gcPath string
	flag.StringVar(&configPath, "c", "config.json", "Path to config file")
	flag.StringVar(&gcPath, "gc", "credentials.json", "Path to Google credentials JSON")
	flag.Parse()

	ctx := context.Background()
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

	prefixes := []string{"req-", "res-"}
	total := 0
	for _, prefix := range prefixes {
		files, err := backend.ListQuery(ctx, prefix)
		if err != nil {
			log.Fatalf("Failed to list %s files: %v", prefix, err)
		}
		log.Printf("Purging %d files with prefix %s", len(files), prefix)

		var wg sync.WaitGroup
		sem := make(chan struct{}, 8)
		for _, file := range files {
			wg.Add(1)
			sem <- struct{}{}
			go func(file string) {
				defer wg.Done()
				defer func() { <-sem }()
				if err := backend.Delete(ctx, file); err != nil {
					log.Printf("delete failed %s: %v", file, err)
					return
				}
			}(file)
		}
		wg.Wait()
		total += len(files)
	}
	log.Printf("Purge finished. Requested delete for %d files.", total)
}
