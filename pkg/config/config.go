package config

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/gurkankaymak/hocon"
)

// Config represents the complete dual-proxy configuration
type Config struct {
	Proxy                ProxyConfig
	Comparison           ComparisonConfig
	Features             FeaturesConfig
	ErrorMappings        ErrorMappingsConfig
	QueryTransformations QueryTransformationsConfig
	Monitoring           MonitoringConfig
	Debug                DebugConfig
}

// ProxyConfig contains proxy server settings
type ProxyConfig struct {
	ListenPort int
	PostgreSQL DatabaseConfig
	YugabyteDB DatabaseConfig
	Routing    RoutingConfig
}

// DatabaseConfig contains database connection settings
type DatabaseConfig struct {
	Host              string
	Port              int
	User              string
	Database          string
	MaxConnections    int
	ConnectionTimeout time.Duration
}

// RoutingConfig contains query routing rules
type RoutingConfig struct {
	DefaultTarget string
}

// ComparisonConfig contains result comparison settings
type ComparisonConfig struct {
	Enabled            bool
	SourceOfTruth      string
	MaxCompareRows     int
	LogComparisons     bool
	LogDifferencesOnly bool
	FailOnDifferences  bool
	ExcludePatterns    []string

	// When false, operate in degraded mode if secondary is unavailable: route only to SourceOfTruth and do not reject queries.
	// When true, enforce that secondary must be healthy; otherwise reject queries on this connection.
	RequireSecondary bool
	// If true, and the secondary fails startup due to missing database (SQLSTATE 3D000), allow degraded mode for this connection
	// even when RequireSecondary=true. Default true to improve operability during bootstrap.
	FailOpenOnMissingDatabase bool

	// Sorting and special divergence handling
	SortBeforeCompare                    bool // When true, sort DataRow results lexicographically before comparing
	SyntaxErrorDivergenceFailAndRollback bool // When true, on syntax-error divergence fail the client transaction (no report)
	SyntaxErrorDivergenceReport          bool // When true, still report syntax-error divergence (default false)

	// Reporting options - explicit control over what gets reported
	ReportRowCountMismatch  bool // When true, report row count differences (default true)
	ReportDataValueMismatch bool // When true, report data value differences (default true)
	ReportErrorDivergence   bool // When true, report error divergence (one DB succeeds while other fails)

	// Catalog-difference suppression
	IgnoreCatalogDifferences  bool     // When true, do not report/fail differences for catalog queries
	CatalogDifferencePatterns []string // Regex patterns to detect catalog queries

	// Slow query reporting
	ReportSlowQueries bool
	SlowQueryRatio    float64
	FailOnSlowQueries bool

	// Extended protocol dual-execution control
	DualExtendedProtocol bool

	// Primary database for execution order (default: "yugabytedb")
	// When set to "yugabytedb", queries are executed on YugabyteDB first,
	// and if they fail with an ignored error code, PostgreSQL execution is skipped
	PrimaryDatabase string

	// Ignored error codes - when primary database fails with one of these codes,
	// skip execution on secondary database entirely (e.g., "0A000" for feature not supported)
	IgnoredErrorCodes []string

	// EXPLAIN configuration (legacy string form)
	ExplainSelect string // e.g., "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)"
	ExplainOther  string // e.g., "EXPLAIN (FORMAT TEXT)"
	// EXPLAIN configuration (structured form for tests)
	ExplainOptions ExplainAnalyzeOptions

	// AI Analysis configuration
	AI AIAnalysisConfig

	// Compiled exclude patterns
	excludePatterns []*regexp.Regexp
	// Compiled catalog patterns
	catalogPatterns []*regexp.Regexp
}

// AIAnalysisConfig contains settings for AI-based discrepancy analysis
type AIAnalysisConfig struct {
	Enabled  bool
	Provider string // "mock", "gemini", "openai"
	APIKey   string
	Endpoint string
	Model    string
}

// ExplainAnalyzeOptions groups per-DB EXPLAIN options
type ExplainAnalyzeOptions struct {
	PostgreSQL ExplainOptions
	YugabyteDB ExplainOptions
}

// ExplainOptions represents tunable EXPLAIN flags
type ExplainOptions struct {
	Analyze bool
	Buffers bool
	Costs   bool
	Timing  bool
	Summary bool
	Format  string // TEXT or JSON
	// YB-specific flags (ignored by PG)
	Dist  bool
	Debug bool
}

// FeaturesConfig contains feature compatibility mappings
type FeaturesConfig struct {
	IsolationLevels IsolationLevelsConfig
	IndexTypes      IndexTypesConfig
	SQLFeatures     map[string]FeatureSupport
}

// IsolationLevelsConfig contains isolation level mappings
type IsolationLevelsConfig struct {
	PostgreSQL []string
	YugabyteDB []string
	Mapping    map[string]string
}

// IndexTypesConfig contains index type support
type IndexTypesConfig struct {
	PostgreSQL        []string
	YugabyteDB        []string
	UnsupportedAction string
}

// FeatureSupport describes support for a SQL feature
type FeatureSupport struct {
	PostgreSQL    bool
	YugabyteDB    bool
	OnUnsupported string
}

// ErrorMappingsConfig contains error code mappings
type ErrorMappingsConfig struct {
	TransactionErrors map[string]ErrorMapping
	ConstraintErrors  map[string]ErrorMapping
	FeatureErrors     map[string]ErrorMapping
	ConnectionErrors  map[string]ErrorMapping
	DataErrors        map[string]ErrorMapping
}

// ErrorMapping describes how to map errors between databases
type ErrorMapping struct {
	PostgreSQLCodes  []string
	YugabyteCodes    []string
	Action           string
	MaxRetries       int
	RetryDelay       time.Duration
	TransformMessage string
	TransformCode    string
	CustomHandlers   map[string]CustomHandler
}

// CustomHandler for specific error scenarios
type CustomHandler struct {
	Action           string
	TransformCode    string
	TransformMessage string
}

// QueryTransformationsConfig contains query transformation rules
type QueryTransformationsConfig struct {
	AdvisoryLocks TransformRule
	SystemColumns SystemColumnsRule
}

// TransformRule describes a query transformation
type TransformRule struct {
	Patterns     []string
	Action       string
	ErrorMessage string

	// Compiled patterns
	compiled []*regexp.Regexp
}

// SystemColumnsRule for handling system columns
type SystemColumnsRule struct {
	Unsupported []string
	Action      string
}

// MonitoringConfig contains monitoring settings
type MonitoringConfig struct {
	LogErrors           bool
	LogErrorMappings    bool
	LogRoutingDecisions bool
	Metrics             MetricsConfig
}

// MetricsConfig contains metrics collection settings
type MetricsConfig struct {
	Enabled bool
	Port    int
	Track   MetricsTracking
}

// MetricsTracking specifies what metrics to track
type MetricsTracking struct {
	QueryCount         bool
	ErrorCount         bool
	ErrorMappings      bool
	ConnectionCount    bool
	TransactionRetries bool
}

// DebugConfig contains debug settings
type DebugConfig struct {
	Enabled        bool
	LogAllQueries  bool
	LogProtocol    bool
	DumpNetwork    bool
	SimulateErrors ErrorSimulation
}

// ErrorSimulation for testing
type ErrorSimulation struct {
	Enabled    bool
	ErrorRate  float64
	ErrorTypes []string
}

// LoadConfig loads configuration from file
func LoadConfig(filename string) (*Config, error) {
	// Read file content
	content, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Substitute environment variables
	expandedContent := os.ExpandEnv(string(content))

	// Parse the expanded content
	conf, err := hocon.ParseString(expandedContent)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config: %w", err)
	}

	cfg := &Config{}

	// Load proxy configuration
	cfg.Proxy.ListenPort = conf.GetInt("proxy.listen-port")

	// PostgreSQL config
	cfg.Proxy.PostgreSQL.Host = conf.GetString("proxy.postgresql.host")
	cfg.Proxy.PostgreSQL.Port = conf.GetInt("proxy.postgresql.port")
	cfg.Proxy.PostgreSQL.User = conf.GetString("proxy.postgresql.user")
	cfg.Proxy.PostgreSQL.Database = conf.GetString("proxy.postgresql.database")
	cfg.Proxy.PostgreSQL.MaxConnections = conf.GetInt("proxy.postgresql.max-connections")
	cfg.Proxy.PostgreSQL.ConnectionTimeout = conf.GetDuration("proxy.postgresql.connection-timeout")

	// YugabyteDB config
	cfg.Proxy.YugabyteDB.Host = conf.GetString("proxy.yugabytedb.host")
	cfg.Proxy.YugabyteDB.Port = conf.GetInt("proxy.yugabytedb.port")
	cfg.Proxy.YugabyteDB.User = conf.GetString("proxy.yugabytedb.user")
	cfg.Proxy.YugabyteDB.Database = conf.GetString("proxy.yugabytedb.database")
	cfg.Proxy.YugabyteDB.MaxConnections = conf.GetInt("proxy.yugabytedb.max-connections")
	cfg.Proxy.YugabyteDB.ConnectionTimeout = conf.GetDuration("proxy.yugabytedb.connection-timeout")

	// Routing config
	cfg.Proxy.Routing.DefaultTarget = conf.GetString("proxy.routing.default-target")

	// Load comparison config
	cfg.Comparison.Enabled = conf.GetBoolean("comparison.enabled")
	cfg.Comparison.SourceOfTruth = conf.GetString("comparison.source-of-truth")
	cfg.Comparison.MaxCompareRows = conf.GetInt("comparison.max-compare-rows")
	cfg.Comparison.LogComparisons = conf.GetBoolean("comparison.log-comparisons")
	cfg.Comparison.LogDifferencesOnly = conf.GetBoolean("comparison.log-differences-only")
	cfg.Comparison.FailOnDifferences = conf.GetBoolean("comparison.fail-on-differences")
	// Sorting and special divergence handling
	cfg.Comparison.SortBeforeCompare = conf.GetBoolean("comparison.sort-before-compare")
	// Defaults for syntax error divergence handling: fail transaction (true) and do not report (false)
	cfg.Comparison.SyntaxErrorDivergenceFailAndRollback = true
	if conf.Get("comparison.syntax-error-divergence-fail-and-rollback") != nil {
		cfg.Comparison.SyntaxErrorDivergenceFailAndRollback = conf.GetBoolean("comparison.syntax-error-divergence-fail-and-rollback")
	}
	cfg.Comparison.SyntaxErrorDivergenceReport = false
	if conf.Get("comparison.syntax-error-divergence-report") != nil {
		cfg.Comparison.SyntaxErrorDivergenceReport = conf.GetBoolean("comparison.syntax-error-divergence-report")
	}
	// Reporting options - all default to true except error divergence
	cfg.Comparison.ReportRowCountMismatch = true
	if conf.Get("comparison.report-row-count-mismatch") != nil {
		cfg.Comparison.ReportRowCountMismatch = conf.GetBoolean("comparison.report-row-count-mismatch")
	}
	cfg.Comparison.ReportDataValueMismatch = true
	if conf.Get("comparison.report-data-value-mismatch") != nil {
		cfg.Comparison.ReportDataValueMismatch = conf.GetBoolean("comparison.report-data-value-mismatch")
	}
	// Error divergence reporting (default: false - currently unstable)
	cfg.Comparison.ReportErrorDivergence = false
	if conf.Get("comparison.report-error-divergence") != nil {
		cfg.Comparison.ReportErrorDivergence = conf.GetBoolean("comparison.report-error-divergence")
	}
	// Require secondary can be absent; default to true (strict mode: reject when secondary is unavailable)
	cfg.Comparison.RequireSecondary = true
	if conf.Get("comparison.require-secondary") != nil {
		cfg.Comparison.RequireSecondary = conf.GetBoolean("comparison.require-secondary")
	}
	// Fail-open on missing database (3D000) default: true for better bootstrap UX
	cfg.Comparison.FailOpenOnMissingDatabase = true
	if conf.Get("comparison.fail-open-on-missing-database") != nil {
		cfg.Comparison.FailOpenOnMissingDatabase = conf.GetBoolean("comparison.fail-open-on-missing-database")
	}

	// Slow query config
	cfg.Comparison.ReportSlowQueries = conf.GetBoolean("comparison.report-slow-queries")
	cfg.Comparison.SlowQueryRatio = conf.GetFloat64("comparison.slow-query-ratio")
	cfg.Comparison.FailOnSlowQueries = conf.GetBoolean("comparison.fail-on-slow-queries")

	// Extended protocol dual execution (default: enabled)
	cfg.Comparison.DualExtendedProtocol = true
	if conf.Get("comparison.dual-extended-protocol") != nil {
		cfg.Comparison.DualExtendedProtocol = conf.GetBoolean("comparison.dual-extended-protocol")
	}

	// Primary database for execution order (default: yugabytedb)
	cfg.Comparison.PrimaryDatabase = "yugabytedb"
	if conf.Get("comparison.primary-database") != nil {
		cfg.Comparison.PrimaryDatabase = conf.GetString("comparison.primary-database")
	}

	// Ignored error codes - when primary fails with these, skip secondary
	// Default includes 0A000 (feature_not_supported) which is common for YugabyteDB
	defaultIgnoredCodes := []string{"0A000"}
	ignoredCodes := conf.GetStringSlice("comparison.ignored-error-codes")
	if len(ignoredCodes) == 0 {
		ignoredCodes = defaultIgnoredCodes
	}
	cfg.Comparison.IgnoredErrorCodes = make([]string, len(ignoredCodes))
	for i, code := range ignoredCodes {
		cfg.Comparison.IgnoredErrorCodes[i] = strings.Trim(code, "\"")
	}

	// EXPLAIN configuration (tunable)
	cfg.Comparison.ExplainSelect = conf.GetString("comparison.explain.select")
	if cfg.Comparison.ExplainSelect == "" {
		cfg.Comparison.ExplainSelect = "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)"
	}
	cfg.Comparison.ExplainOther = conf.GetString("comparison.explain.other")
	if cfg.Comparison.ExplainOther == "" {
		cfg.Comparison.ExplainOther = "EXPLAIN (FORMAT TEXT)"
	}
	// Structured EXPLAIN defaults for tests and future use
	cfg.Comparison.ExplainOptions.PostgreSQL = ExplainOptions{
		Analyze: true,
		Buffers: true,
		Costs:   true,
		Timing:  true,
		Summary: false,
		Format:  "TEXT",
	}
	cfg.Comparison.ExplainOptions.YugabyteDB = ExplainOptions{
		Analyze: true,
		Buffers: true,
		Costs:   true,
		Timing:  true,
		Summary: false,
		Format:  "TEXT",
		Dist:    true,
		Debug:   false,
	}

	// Load AI Analysis config
	cfg.Comparison.AI.Enabled = conf.GetBoolean("comparison.ai.enabled")
	cfg.Comparison.AI.Provider = conf.GetString("comparison.ai.provider")
	if cfg.Comparison.AI.Provider == "" {
		cfg.Comparison.AI.Provider = "mock"
	}
	cfg.Comparison.AI.APIKey = conf.GetString("comparison.ai.api-key")
	cfg.Comparison.AI.Endpoint = conf.GetString("comparison.ai.endpoint")
	cfg.Comparison.AI.Model = conf.GetString("comparison.ai.model")

	// Load and compile exclude patterns
	excludePatterns := conf.GetStringSlice("comparison.exclude-patterns")
	cfg.Comparison.ExcludePatterns = make([]string, len(excludePatterns))
	for i, pattern := range excludePatterns {
		cleaned := strings.Trim(pattern, "\"")
		cleaned = strings.ReplaceAll(cleaned, "\\\\", "\\")
		cfg.Comparison.ExcludePatterns[i] = cleaned
	}

	// Compile exclude patterns
	if err := compileExcludePatterns(&cfg.Comparison); err != nil {
		return nil, fmt.Errorf("failed to compile exclude patterns: %w", err)
	}

	// Load catalog-difference suppression config
	cfg.Comparison.IgnoreCatalogDifferences = true
	if conf.Get("comparison.ignore-catalog-differences") != nil {
		cfg.Comparison.IgnoreCatalogDifferences = conf.GetBoolean("comparison.ignore-catalog-differences")
	}
	defaultCatalogPatterns := []string{
		"(?i)\\bpg_catalog\\b",                 // pg_catalog schema
		"(?i)\\binformation_schema\\b",         // information_schema schema
		"(?i)\\bFROM\\s+pg_[a-z0-9_]+\\b",      // FROM pg_* system tables (pg_class, pg_opclass, etc.)
		"(?i)\\bJOIN\\s+pg_[a-z0-9_]+\\b",      // JOIN pg_* system tables
		"(?i)\\bFROM\\s+yb_[a-z0-9_]+\\b",      // FROM yb_* YugabyteDB-specific tables
		"(?i)\\bJOIN\\s+yb_[a-z0-9_]+\\b",      // JOIN yb_* YugabyteDB-specific tables
		"(?i)\\bFROM\\s+information_schema\\b", // FROM information_schema
	}
	catalogPatterns := conf.GetStringSlice("comparison.catalog-difference-patterns")
	if len(catalogPatterns) == 0 {
		catalogPatterns = defaultCatalogPatterns
	}
	cfg.Comparison.CatalogDifferencePatterns = make([]string, len(catalogPatterns))
	for i, pattern := range catalogPatterns {
		cleaned := strings.Trim(pattern, "\"")
		cleaned = strings.ReplaceAll(cleaned, "\\\\", "\\")
		cfg.Comparison.CatalogDifferencePatterns[i] = cleaned
	}
	// Compile catalog patterns
	if err := compileCatalogPatterns(&cfg.Comparison); err != nil {
		return nil, fmt.Errorf("failed to compile catalog difference patterns: %w", err)
	}

	// Load error mappings
	if err := loadErrorMappings(conf, cfg); err != nil {
		return nil, fmt.Errorf("failed to load error mappings: %w", err)
	}

	// Load monitoring config
	cfg.Monitoring.LogErrors = conf.GetBoolean("monitoring.log-errors")
	cfg.Monitoring.LogErrorMappings = conf.GetBoolean("monitoring.log-error-mappings")
	cfg.Monitoring.LogRoutingDecisions = conf.GetBoolean("monitoring.log-routing-decisions")

	// Load debug config
	cfg.Debug.Enabled = conf.GetBoolean("debug.enabled")
	cfg.Debug.LogAllQueries = conf.GetBoolean("debug.log-all-queries")
	cfg.Debug.LogProtocol = conf.GetBoolean("debug.log-protocol")
	cfg.Debug.DumpNetwork = conf.GetBoolean("debug.dump-network")

	return cfg, nil
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.Proxy.ListenPort <= 0 || c.Proxy.ListenPort > 65535 {
		return fmt.Errorf("invalid listen port: %d", c.Proxy.ListenPort)
	}

	if c.Proxy.PostgreSQL.Host == "" || c.Proxy.PostgreSQL.Port <= 0 {
		return fmt.Errorf("invalid PostgreSQL configuration")
	}

	if c.Proxy.YugabyteDB.Host == "" || c.Proxy.YugabyteDB.Port <= 0 {
		return fmt.Errorf("invalid YugabyteDB configuration")
	}

	return nil
}

// compileExcludePatterns compiles exclude patterns for comparison
func compileExcludePatterns(comparison *ComparisonConfig) error {
	for _, pattern := range comparison.ExcludePatterns {
		// Make patterns case-insensitive
		re, err := regexp.Compile("(?i)" + pattern)
		if err != nil {
			return fmt.Errorf("invalid exclude pattern %s: %w", pattern, err)
		}
		comparison.excludePatterns = append(comparison.excludePatterns, re)
	}

	return nil
}

// compileCatalogPatterns compiles catalog difference suppression patterns
func compileCatalogPatterns(comparison *ComparisonConfig) error {
	for _, pattern := range comparison.CatalogDifferencePatterns {
		re, err := regexp.Compile(pattern)
		if err != nil {
			return fmt.Errorf("invalid catalog difference pattern %s: %w", pattern, err)
		}
		comparison.catalogPatterns = append(comparison.catalogPatterns, re)
	}
	return nil
}

// IsCatalogQuery checks if the given query hits pg_catalog, information_schema or pg_* relations
func (c *Config) IsCatalogQuery(query string) bool {
	if query == "" {
		return false
	}
	// Evaluate against compiled patterns
	for _, re := range c.Comparison.catalogPatterns {
		if re.MatchString(query) {
			return true
		}
	}
	return false
}

// IsIgnoredErrorCode checks if the given error code should be ignored for dual execution
// When the primary database fails with an ignored error code, the secondary execution is skipped
func (c *Config) IsIgnoredErrorCode(errorCode string) bool {
	for _, code := range c.Comparison.IgnoredErrorCodes {
		if code == errorCode {
			return true
		}
	}
	return false
}

// loadErrorMappings loads error mapping configuration
func loadErrorMappings(conf *hocon.Config, cfg *Config) error {
	// Transaction errors
	cfg.ErrorMappings.TransactionErrors = make(map[string]ErrorMapping)
	loadErrorCategory(conf, "error-mappings.transaction-errors", cfg.ErrorMappings.TransactionErrors)

	// Constraint errors
	cfg.ErrorMappings.ConstraintErrors = make(map[string]ErrorMapping)
	loadErrorCategory(conf, "error-mappings.constraint-errors", cfg.ErrorMappings.ConstraintErrors)

	// Add other error categories...

	return nil
}

// loadErrorCategory loads a category of error mappings
func loadErrorCategory(conf *hocon.Config, path string, target map[string]ErrorMapping) {
	obj := conf.GetObject(path)
	for key := range obj {
		mapping := ErrorMapping{}
		fullPath := path + "." + key

		mapping.PostgreSQLCodes = conf.GetStringSlice(fullPath + ".postgresql-codes")
		mapping.YugabyteCodes = conf.GetStringSlice(fullPath + ".yugabytedb-codes")
		action := conf.GetString(fullPath + ".action")
		// Remove any quotes from action string
		mapping.Action = strings.Trim(action, "\"")
		mapping.MaxRetries = conf.GetInt(fullPath + ".max-retries")

		// Try to get retry delay
		retryDelay := conf.Get(fullPath + ".retry-delay")
		if retryDelay != nil {
			mapping.RetryDelay = conf.GetDuration(fullPath + ".retry-delay")
		}

		// Try to get transform message
		transformMsg := conf.Get(fullPath + ".transform-message")
		if transformMsg != nil {
			mapping.TransformMessage = conf.GetString(fullPath + ".transform-message")
		}

		target[key] = mapping
	}
}

// MapError maps an error code from one database to another
func (c *Config) MapError(errorCode string, fromDB string) (string, string, bool) {
	// Check all error categories
	categories := []map[string]ErrorMapping{
		c.ErrorMappings.TransactionErrors,
		c.ErrorMappings.ConstraintErrors,
		c.ErrorMappings.FeatureErrors,
		c.ErrorMappings.ConnectionErrors,
		c.ErrorMappings.DataErrors,
	}

	for _, category := range categories {
		for _, mapping := range category {
			// Check if error code matches
			var sourceCodes []string
			if fromDB == "postgresql" {
				sourceCodes = mapping.PostgreSQLCodes
			} else {
				sourceCodes = mapping.YugabyteCodes
			}

			for _, code := range sourceCodes {
				if code == errorCode {
					// Found a mapping
					if c.Monitoring.LogErrorMappings {
						log.Printf("Mapping error %s from %s: action=%s", errorCode, fromDB, mapping.Action)
					}

					// Return mapped code and action
					if mapping.TransformCode != "" {
						return mapping.TransformCode, mapping.Action, true
					}

					// Return first target code
					var targetCodes []string
					if fromDB == "postgresql" {
						targetCodes = mapping.YugabyteCodes
					} else {
						targetCodes = mapping.PostgreSQLCodes
					}

					if len(targetCodes) > 0 {
						return targetCodes[0], mapping.Action, true
					}
				}
			}
		}
	}

	return errorCode, "pass-through", false
}

// ShouldCompareQuery checks if a query should be compared between databases
func (c *Config) ShouldCompareQuery(query string) bool {
	if !c.Comparison.Enabled {
		return false
	}

	// Check exclude patterns
	for _, re := range c.Comparison.excludePatterns {
		if re.MatchString(query) {
			return false
		}
	}

	return true
}
