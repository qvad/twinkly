package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func resolveConfigPath(flagPath string) (string, []string) {
	var tried []string
	var candidates []string
	if flagPath != "" {
		candidates = append(candidates, flagPath)
	}
	if env := os.Getenv("TWINKLY_CONFIG"); env != "" {
		candidates = append(candidates, env)
	}
	// Common defaults
	candidates = append(candidates, "config/twinkly.conf")
	if exe, err := os.Executable(); err == nil {
		dir := filepath.Dir(exe)
		candidates = append(candidates, filepath.Join(dir, "config", "twinkly.conf"))
	}
	if cwd, err := os.Getwd(); err == nil {
		candidates = append(candidates, filepath.Join(cwd, "config", "twinkly.conf"))
	}
	for _, cand := range candidates {
		tried = append(tried, cand)
		path := cand
		if strings.HasPrefix(path, "~") {
			if home, _ := os.UserHomeDir(); home != "" {
				path = filepath.Join(home, strings.TrimPrefix(path, "~"))
			}
		}
		if info, err := os.Stat(path); err == nil && !info.IsDir() {
			return path, tried
		}
	}
	return "", tried
}

func main() {
	// Parse command line arguments
	var configPath string
	flag.StringVar(&configPath, "config", "", "Path to configuration file (HOCON). You can also set TWINKLY_CONFIG env var.")
	flag.StringVar(&configPath, "c", "", "Shorthand for -config")
	flag.Parse()

	// Resolve config path with fallbacks
	resolved, tried := resolveConfigPath(configPath)
	if resolved == "" {
		log.Printf("Could not find configuration file. Tried: %s", strings.Join(tried, ", "))
		log.Printf("Tip: pass -config /path/to/twinkly.conf or set TWINKLY_CONFIG env var")
		os.Exit(2)
	}
	log.Printf("Using config file: %s", resolved)

	// Load configuration
	config, err := LoadConfig(resolved)
	if err != nil {
		log.Fatalf("Failed to load configuration %s: %v", resolved, err)
	}

	// Validate configuration
	if err := config.Validate(); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	// Validate security configuration
	if err := ValidateConfigSecurity(config); err != nil {
		log.Fatalf("Security validation failed: %v", err)
	}

	// Initialize security
	security := NewSecurityConfig()
	if err := security.CompilePatterns(); err != nil {
		log.Fatalf("Failed to compile security patterns: %v", err)
	}
	log.Printf("✓ Security validation enabled")

	// Ensure YugabyteDB is the source of truth for returning results
	if config.Comparison.SourceOfTruth != "yugabytedb" {
		log.Printf("⚠️  WARNING: Setting source of truth to 'yugabytedb' as required")
		config.Comparison.SourceOfTruth = "yugabytedb"
	}

	// Configuration from config file
	listenAddr := fmt.Sprintf(":%d", config.Proxy.ListenPort)
	log.Printf("Config summary: listen=%s, PG=%s@%s:%d/%s, YB=%s@%s:%d/%s, debug.log-all-queries=%v",
		listenAddr,
		config.Proxy.PostgreSQL.User, config.Proxy.PostgreSQL.Host, config.Proxy.PostgreSQL.Port, config.Proxy.PostgreSQL.Database,
		config.Proxy.YugabyteDB.User, config.Proxy.YugabyteDB.Host, config.Proxy.YugabyteDB.Port, config.Proxy.YugabyteDB.Database,
		config.Debug.LogAllQueries,
	)

	// Create proxy
	proxy := NewSimpleProxy(config)

	// Start the proxy server
	if err := proxy.Start(listenAddr); err != nil {
		log.Fatalf("Failed to start proxy: %v", err)
	}

	// Wait for shutdown signal
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	<-sigs
	log.Println("Received shutdown signal")

	log.Println("Shutting down proxy server...")

	// Create shutdown context with timeout
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Shutdown with timeout
	shutdownDone := make(chan struct{})
	go func() {
		proxy.Shutdown()
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
		log.Println("Proxy server shutdown completed")
	case <-shutdownCtx.Done():
		log.Println("Shutdown timeout reached, forcing exit")
	}
}
