package main

import (
	"testing"

	"github.com/qvad/twinkly/pkg/config"
)

func TestCoreConfig(t *testing.T) {
	t.Run("ValidateConfig", func(t *testing.T) {
		cfg := &config.Config{
			Proxy: config.ProxyConfig{
				ListenPort: 5432,
				PostgreSQL: config.DatabaseConfig{
					Host: "localhost",
					Port: 5432,
				},
				YugabyteDB: config.DatabaseConfig{
					Host: "localhost",
					Port: 5433,
				},
			},
		}

		if err := cfg.Validate(); err != nil {
			t.Errorf("Valid config failed validation: %v", err)
		}
	})

	t.Run("InvalidConfig", func(t *testing.T) {
		cfg := &config.Config{
			Proxy: config.ProxyConfig{
				ListenPort: 0, // Invalid port
			},
		}

		if err := cfg.Validate(); err == nil {
			t.Error("Invalid config should have failed validation")
		}
	})
}
