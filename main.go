package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	// Parse command line arguments
	var configPath string
	flag.StringVar(&configPath, "config", "config/twinkly.conf", "Path to configuration file")
	flag.Parse()

	// Load configuration
	config, err := LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
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

// isCatalogQuery checks if a query is accessing PostgreSQL system catalogs
func isCatalogQuery(query string) bool {
	queryLower := strings.ToLower(query)
	
	// Check for common catalog tables
	catalogTables := []string{
		"pg_",
		"information_schema",
	}
	
	for _, prefix := range catalogTables {
		if strings.Contains(queryLower, prefix) {
			return true
		}
	}
	
	return false
}