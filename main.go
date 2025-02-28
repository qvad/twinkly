package main

import (
	"log"
	"os"
	"path/filepath"

	"github.com/qvad/twinkly/pkg/config"
	"github.com/qvad/twinkly/pkg/proxy"
)

func main() {
	// Cleanup inconsistency_reports folder on startup
	cleanupReportsFolder()

	// Determine config file path
	configPath := "config/twinkly.conf" // Default
	if os.Getenv("TWINKLY_CONFIG") != "" {
		configPath = os.Getenv("TWINKLY_CONFIG")
	}
	for i, arg := range os.Args {
		if arg == "-config" || arg == "-c" {
			if i+1 < len(os.Args) {
				configPath = os.Args[i+1]
			}
			break
		}
	}

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	p := proxy.NewDualExecutionProxy(cfg)
	log.Printf("Starting Twinkly dual proxy on :%d", cfg.Proxy.ListenPort)
	log.Fatal(p.Start())
}

// cleanupReportsFolder removes all files from the inconsistency_reports folder on startup
func cleanupReportsFolder() {
	reportDir := "inconsistency_reports"

	// Check if directory exists
	if _, err := os.Stat(reportDir); os.IsNotExist(err) {
		return // Directory doesn't exist, nothing to clean
	}

	// Read directory contents
	entries, err := os.ReadDir(reportDir)
	if err != nil {
		log.Printf("Warning: could not read %s: %v", reportDir, err)
		return
	}

	// Remove all files
	for _, entry := range entries {
		path := filepath.Join(reportDir, entry.Name())
		if err := os.RemoveAll(path); err != nil {
			log.Printf("Warning: could not remove %s: %v", path, err)
		}
	}

	if len(entries) > 0 {
		log.Printf("Cleaned up %d report(s) from %s", len(entries), reportDir)
	}
}
