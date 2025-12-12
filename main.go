package main

import (
	"log"
	"os"

	"github.com/qvad/twinkly/pkg/config"
	"github.com/qvad/twinkly/pkg/proxy"
)

func main() {
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
