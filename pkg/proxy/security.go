package proxy

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/qvad/twinkly/pkg/config"
)

// SecurityConfig holds security-related configuration
type SecurityConfig struct {
	MaxQueryLength  int
	AllowedPatterns []string
	BlockedPatterns []string
	RequireAuth     bool
	ValidTokens     map[string]bool
	allowedRegex    []*regexp.Regexp
	blockedRegex    []*regexp.Regexp

	// Rate limiting
	RateLimiter *RateLimiter
}

// RateLimiter implements simple rate limiting per IP
type RateLimiter struct {
	requests map[string][]time.Time
	mutex    sync.RWMutex
	limit    int           // requests per window
	window   time.Duration // time window
}

// NewSecurityConfig creates a new security configuration with defaults
func NewSecurityConfig() *SecurityConfig {
	return &SecurityConfig{
		MaxQueryLength: 10000, // 10KB max query size
		AllowedPatterns: []string{
			"^SELECT.*",
			"^INSERT.*",
			"^UPDATE.*",
			"^DELETE.*",
			"^CREATE TABLE.*",
			"^CREATE DATABASE.*",
			"^ALTER DATABASE.*",
			"^CREATE ROLE.*",
			"^DROP TABLE.*",
			"^SHOW.*",
			"^EXPLAIN.*",
		},
		BlockedPatterns: []string{
			".*;.*--;.*",              // SQL injection patterns
			".*UNION.*SELECT.*",       // Union-based injection
			".*'.*OR.*'.*",            // OR-based injection
			".*DROP DATABASE.*",       // Dangerous operations
			".*ALTER SYSTEM.*",        // System alterations
			".*pg_sleep.*",            // Sleep functions
			".*pg_read_file.*",        // File access functions
			".*lo_import.*",           // Large object functions
			".*copy.*from.*program.*", // Command execution
		},
		RequireAuth: false,
		ValidTokens: make(map[string]bool),
		RateLimiter: NewRateLimiter(100, time.Minute), // 100 requests per minute per IP
	}
}

// NewRateLimiter creates a new rate limiter
func NewRateLimiter(limit int, window time.Duration) *RateLimiter {
	return &RateLimiter{
		requests: make(map[string][]time.Time),
		limit:    limit,
		window:   window,
	}
}

// IsAllowed checks if a request from the given IP is allowed
func (rl *RateLimiter) IsAllowed(ip string) bool {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()

	now := time.Now()

	// Clean up old requests outside the window
	if requests, exists := rl.requests[ip]; exists {
		var filtered []time.Time
		cutoff := now.Add(-rl.window)
		for _, requestTime := range requests {
			if requestTime.After(cutoff) {
				filtered = append(filtered, requestTime)
			}
		}
		rl.requests[ip] = filtered
	}

	// Check if limit exceeded
	if len(rl.requests[ip]) >= rl.limit {
		return false
	}

	// Add current request
	rl.requests[ip] = append(rl.requests[ip], now)
	return true
}

// CompilePatterns compiles regex patterns for efficient matching
func (sc *SecurityConfig) CompilePatterns() error {
	// Compile allowed patterns
	for _, pattern := range sc.AllowedPatterns {
		re, err := regexp.Compile("(?i)" + pattern)
		if err != nil {
			return fmt.Errorf("invalid allowed pattern %s: %w", pattern, err)
		}
		sc.allowedRegex = append(sc.allowedRegex, re)
	}

	// Compile blocked patterns
	for _, pattern := range sc.BlockedPatterns {
		re, err := regexp.Compile("(?i)" + pattern)
		if err != nil {
			return fmt.Errorf("invalid blocked pattern %s: %w", pattern, err)
		}
		sc.blockedRegex = append(sc.blockedRegex, re)
	}

	return nil
}

// ValidateConnectionParameters validates database connection parameters
func ValidateConnectionParameters(params map[string]string) error {
	// Check required parameters
	requiredParams := []string{"database"}
	for _, param := range requiredParams {
		if value, exists := params[param]; !exists || value == "" {
			return fmt.Errorf("missing required parameter: %s", param)
		}
	}

	// Validate database name
	dbName := params["database"]
	if len(dbName) > 64 {
		return fmt.Errorf("database name too long: %d characters (max 64)", len(dbName))
	}

	// Check for valid database name characters
	validDBName := regexp.MustCompile(`^[a-zA-Z0-9_]+$`)
	if !validDBName.MatchString(dbName) {
		return fmt.Errorf("invalid database name: %s (only alphanumeric and underscore allowed)", dbName)
	}

	// Validate user if provided
	if user, exists := params["user"]; exists {
		if len(user) > 64 {
			return fmt.Errorf("username too long: %d characters (max 64)", len(user))
		}

		validUser := regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
		if !validUser.MatchString(user) {
			return fmt.Errorf("invalid username: %s (only alphanumeric, underscore, and hyphen allowed)", user)
		}
	}

	return nil
}

// SanitizeString removes potentially dangerous characters from a string
func SanitizeString(input string) string {
	// Remove control characters except newline and tab
	var result strings.Builder
	for _, r := range input {
		if r >= 32 || r == '\n' || r == '\t' {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// ValidateConfigSecurity validates security-related configuration
func ValidateConfigSecurity(config *config.Config) error {
	// Check for hardcoded credentials (basic check)
	pgConnStr := fmt.Sprintf("host=%s port=%d", config.Proxy.PostgreSQL.Host, config.Proxy.PostgreSQL.Port)
	if strings.Contains(strings.ToLower(pgConnStr), "password=") {
		return errors.New("hardcoded password detected in PostgreSQL configuration")
	}

	ybConnStr := fmt.Sprintf("host=%s port=%d", config.Proxy.YugabyteDB.Host, config.Proxy.YugabyteDB.Port)
	if strings.Contains(strings.ToLower(ybConnStr), "password=") {
		return errors.New("hardcoded password detected in YugabyteDB configuration")
	}

	// Validate port ranges
	if config.Proxy.ListenPort < 1024 && config.Proxy.ListenPort != 0 {
		return fmt.Errorf("listen port %d requires root privileges", config.Proxy.ListenPort)
	}

	// Check for reasonable connection limits
	if config.Proxy.PostgreSQL.MaxConnections > 1000 {
		return fmt.Errorf("PostgreSQL max connections too high: %d (max 1000)", config.Proxy.PostgreSQL.MaxConnections)
	}

	if config.Proxy.YugabyteDB.MaxConnections > 1000 {
		return fmt.Errorf("YugabyteDB max connections too high: %d (max 1000)", config.Proxy.YugabyteDB.MaxConnections)
	}

	return nil
}
