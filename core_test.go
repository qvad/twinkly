package main

import (
	"errors"
	"strings"
	"testing"

	_ "github.com/lib/pq"
)

// TestConfigValidation tests configuration validation
func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name        string
		config      *Config
		expectError bool
	}{
		{
			name: "valid config",
			config: &Config{
				Proxy: ProxyConfig{
					ListenPort: 5431,
					PostgreSQL: DatabaseConfig{
						Host: "localhost",
						Port: 5432,
					},
					YugabyteDB: DatabaseConfig{
						Host: "localhost",
						Port: 5433,
					},
				},
			},
			expectError: false,
		},
		{
			name: "invalid listen port",
			config: &Config{
				Proxy: ProxyConfig{
					ListenPort: 0,
					PostgreSQL: DatabaseConfig{
						Host: "localhost",
						Port: 5432,
					},
					YugabyteDB: DatabaseConfig{
						Host: "localhost",
						Port: 5433,
					},
				},
			},
			expectError: true,
		},
		{
			name: "missing postgres host",
			config: &Config{
				Proxy: ProxyConfig{
					ListenPort: 5431,
					PostgreSQL: DatabaseConfig{
						Host: "",
						Port: 5432,
					},
					YugabyteDB: DatabaseConfig{
						Host: "localhost",
						Port: 5433,
					},
				},
			},
			expectError: true,
		},
		{
			name: "missing yugabyte host",
			config: &Config{
				Proxy: ProxyConfig{
					ListenPort: 5431,
					PostgreSQL: DatabaseConfig{
						Host: "localhost",
						Port: 5432,
					},
					YugabyteDB: DatabaseConfig{
						Host: "",
						Port: 5433,
					},
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		})
	}
}

// TestSecurityValidation tests security validation
func TestSecurityValidation(t *testing.T) {
	security := NewSecurityConfig()
	if err := security.CompilePatterns(); err != nil {
		t.Fatalf("Failed to compile security patterns: %v", err)
	}

	tests := []struct {
		name        string
		query       string
		expectError bool
	}{
		{
			name:        "safe select query",
			query:       "SELECT * FROM users WHERE id = 1",
			expectError: false,
		},
		{
			name:        "sql injection attempt",
			query:       "SELECT * FROM users WHERE id = 1; DROP TABLE users; --",
			expectError: true,
		},
		{
			name:        "union injection",
			query:       "SELECT * FROM users WHERE id = 1 UNION SELECT password FROM admin",
			expectError: true,
		},
		{
			name:        "comment injection",
			query:       "SELECT * FROM users WHERE id = 1 /* comment */ OR 1=1",
			expectError: false, // Basic comment should be allowed
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := security.ValidateQuery(tt.query)
			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		})
	}
}

// TestResultValidator tests result validation functionality
func TestResultValidator(t *testing.T) {
	validator := NewResultValidator(true)

	tests := []struct {
		name           string
		pgResults      []*PGMessage
		ybResults      []*PGMessage
		expectFail     bool
		expectError    bool
	}{
		{
			name: "identical results",
			pgResults: []*PGMessage{
				{Type: msgTypeRowDescription, Data: []byte("test")},
				{Type: msgTypeDataRow, Data: []byte("row1")},
				{Type: msgTypeCommandComplete, Data: []byte("SELECT 1")},
			},
			ybResults: []*PGMessage{
				{Type: msgTypeRowDescription, Data: []byte("test")},
				{Type: msgTypeDataRow, Data: []byte("row1")},
				{Type: msgTypeCommandComplete, Data: []byte("SELECT 1")},
			},
			expectFail:  false,
			expectError: false,
		},
		{
			name: "different row counts",
			pgResults: []*PGMessage{
				{Type: msgTypeRowDescription, Data: []byte("test")},
				{Type: msgTypeDataRow, Data: []byte("row1")},
				{Type: msgTypeDataRow, Data: []byte("row2")},
				{Type: msgTypeCommandComplete, Data: []byte("SELECT 2")},
			},
			ybResults: []*PGMessage{
				{Type: msgTypeRowDescription, Data: []byte("test")},
				{Type: msgTypeDataRow, Data: []byte("row1")},
				{Type: msgTypeCommandComplete, Data: []byte("SELECT 1")},
			},
			expectFail:  true,
			expectError: false,
		},
		{
			name: "different data content",
			pgResults: []*PGMessage{
				{Type: msgTypeRowDescription, Data: []byte("test")},
				{Type: msgTypeDataRow, Data: []byte("row1")},
				{Type: msgTypeCommandComplete, Data: []byte("SELECT 1")},
			},
			ybResults: []*PGMessage{
				{Type: msgTypeRowDescription, Data: []byte("test")},
				{Type: msgTypeDataRow, Data: []byte("row2")},
				{Type: msgTypeCommandComplete, Data: []byte("SELECT 1")},
			},
			expectFail:  true,
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := validator.ValidateResults(tt.pgResults, tt.ybResults)
			
			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
			
			if result != nil {
				if tt.expectFail && !result.ShouldFail {
					t.Error("Expected validation to fail but it passed")
				}
				if !tt.expectFail && result.ShouldFail {
					t.Error("Expected validation to pass but it failed")
				}
			}
		})
	}
}

// TestPGProtocol tests PostgreSQL protocol parsing
func TestPGProtocol(t *testing.T) {
	tests := []struct {
		name        string
		data        []byte
		expectError bool
	}{
		{
			name:        "valid query message",
			data:        []byte("SELECT 1"),
			expectError: false,
		},
		{
			name:        "empty query",
			data:        []byte(""),
			expectError: false,
		},
		{
			name:        "null terminated query",
			data:        []byte("SELECT 1\x00"),
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock message
			msg := &PGMessage{
				Type: msgTypeQuery,
				Data: tt.data,
			}
			
			// Test query parsing
			query, err := ParseQuery(msg)
			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
			
			if !tt.expectError && query != strings.TrimRight(string(tt.data), "\x00") {
				t.Errorf("Expected query %q but got %q", string(tt.data), query)
			}
		})
	}

	// Test message type checking
	t.Run("message type checking", func(t *testing.T) {
		queryMsg := &PGMessage{Type: msgTypeQuery, Data: []byte("SELECT 1")}
		terminateMsg := &PGMessage{Type: msgTypeTerminate, Data: []byte{}}
		
		if !IsQueryMessage(queryMsg) {
			t.Error("Expected query message to be identified as query")
		}
		if IsQueryMessage(terminateMsg) {
			t.Error("Expected terminate message to not be identified as query")
		}
		
		if !IsTerminateMessage(terminateMsg) {
			t.Error("Expected terminate message to be identified as terminate")
		}
		if IsTerminateMessage(queryMsg) {
			t.Error("Expected query message to not be identified as terminate")
		}
	})
}

// TestConstraintDetector tests constraint divergence detection
func TestConstraintDetector(t *testing.T) {
	detector := NewConstraintDivergenceDetector()

	tests := []struct {
		name              string
		pgResult          *QueryExecutionResult
		ybResult          *QueryExecutionResult
		expectDivergence  bool
	}{
		{
			name: "both succeed",
			pgResult: &QueryExecutionResult{
				Success:      true,
				DatabaseName: "PostgreSQL",
			},
			ybResult: &QueryExecutionResult{
				Success:      true,
				DatabaseName: "YugabyteDB",
			},
			expectDivergence: false,
		},
		{
			name: "both fail with same constraint",
			pgResult: &QueryExecutionResult{
				Success:      false,
				Error:        errors.New("unique constraint violation"),
				DatabaseName: "PostgreSQL",
			},
			ybResult: &QueryExecutionResult{
				Success:      false,
				Error:        errors.New("unique constraint violation"),
				DatabaseName: "YugabyteDB",
			},
			expectDivergence: false,
		},
		{
			name: "one succeeds one fails",
			pgResult: &QueryExecutionResult{
				Success:      true,
				DatabaseName: "PostgreSQL",
			},
			ybResult: &QueryExecutionResult{
				Success:      false,
				Error:        errors.New("constraint violation"),
				DatabaseName: "YugabyteDB",
			},
			expectDivergence: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			divergence := detector.DetectDivergence("INSERT INTO test VALUES (1)", tt.pgResult, tt.ybResult)
			
			if tt.expectDivergence && divergence == nil {
				t.Error("Expected divergence but got none")
			}
			if !tt.expectDivergence && divergence != nil {
				t.Error("Expected no divergence but got one")
			}
		})
	}
}

// TestInconsistencyReporter tests inconsistency reporting
func TestInconsistencyReporter(t *testing.T) {
	reporter := NewInconsistencyReporter()

	// Test reporting different types
	reporter.ReportInconsistency(
		RowCountMismatch,
		"HIGH",
		"SELECT * FROM test",
		ResultSummary{Success: true, RowCount: 5},
		ResultSummary{Success: true, RowCount: 3},
		[]string{"Row count differs"},
	)

	reporter.ReportInconsistency(
		ErrorDivergence,
		"CRITICAL",
		"INSERT INTO test VALUES (1)",
		ResultSummary{Success: false, Error: "constraint violation"},
		ResultSummary{Success: true},
		[]string{"One succeeded, one failed"},
	)

	// Test summary generation
	summary := reporter.GenerateSummaryReport()
	if len(summary) == 0 {
		t.Error("Expected non-empty summary report")
	}

	// Check that summary contains expected information
	if !strings.Contains(summary, "ROW_COUNT_MISMATCH") {
		t.Error("Expected summary to contain ROW_COUNT_MISMATCH")
	}
	if !strings.Contains(summary, "ERROR_DIVERGENCE") {
		t.Error("Expected summary to contain ERROR_DIVERGENCE")
	}
}

// TestSlowQueryAnalyzer tests slow query analysis (mocked)
func TestSlowQueryAnalyzer(t *testing.T) {
	// Create a mock config
	config := &Config{
		Comparison: ComparisonConfig{
			SlowQueryRatio: 2.0,
		},
	}

	// Test creation with nil pools (should handle gracefully)
	analyzer := NewSlowQueryAnalyzer(config, nil, nil)
	if analyzer == nil {
		t.Error("Expected analyzer to be created even with nil pools")
	}

	// Test with config values
	if analyzer.slowQueryThreshold != 2.0 {
		t.Errorf("Expected threshold 2.0, got %f", analyzer.slowQueryThreshold)
	}
}

// TestErrorHandler tests error handling functionality
func TestErrorHandler(t *testing.T) {
	tests := []struct {
		name        string
		errorCode   string
		fromDB      string
		expectMap   bool
	}{
		{
			name:      "unmapped error",
			errorCode: "99999",
			fromDB:    "postgresql",
			expectMap: false,
		},
		{
			name:      "valid database",
			errorCode: "23505",
			fromDB:    "postgresql",
			expectMap: false, // No mappings configured in test
		},
	}

	config := &Config{
		ErrorMappings: ErrorMappingsConfig{
			TransactionErrors: make(map[string]ErrorMapping),
			ConstraintErrors:  make(map[string]ErrorMapping),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, mapped := config.MapError(tt.errorCode, tt.fromDB)
			if tt.expectMap && !mapped {
				t.Error("Expected error to be mapped but it wasn't")
			}
			if !tt.expectMap && mapped {
				t.Error("Expected error to not be mapped but it was")
			}
		})
	}
}

// TestConfigQueryRouting tests query routing configuration
func TestConfigQueryRouting(t *testing.T) {
	config := &Config{
		Proxy: ProxyConfig{
			Routing: RoutingConfig{
				PostgresOnlyPatterns: []string{"pg_.*", "SHOW.*"},
				YugabyteOnlyPatterns: []string{"yb_.*"},
			},
		},
		Monitoring: MonitoringConfig{
			LogRoutingDecisions: false,
		},
	}

	// Compile patterns
	if err := compileRoutingPatterns(&config.Proxy.Routing); err != nil {
		t.Fatalf("Failed to compile routing patterns: %v", err)
	}

	tests := []struct {
		name           string
		query          string
		expectPG       bool
		expectYB       bool
	}{
		{
			name:     "postgres catalog query",
			query:    "SELECT * FROM pg_tables",
			expectPG: true,
			expectYB: false,
		},
		{
			name:     "show command",
			query:    "SHOW server_version",
			expectPG: true,
			expectYB: false,
		},
		{
			name:     "yugabyte specific query",
			query:    "SELECT * FROM yb_servers",
			expectPG: false,
			expectYB: true,
		},
		{
			name:     "regular query",
			query:    "SELECT * FROM users",
			expectPG: false,
			expectYB: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pgRoute := config.ShouldRouteToPostgres(tt.query)
			ybRoute := config.ShouldRouteToYugabyte(tt.query)

			if tt.expectPG && !pgRoute {
				t.Error("Expected query to route to PostgreSQL")
			}
			if !tt.expectPG && pgRoute {
				t.Error("Expected query to not route to PostgreSQL")
			}

			if tt.expectYB && !ybRoute {
				t.Error("Expected query to route to YugabyteDB")
			}
			if !tt.expectYB && ybRoute {
				t.Error("Expected query to not route to YugabyteDB")
			}
		})
	}
}

// TestComparisonConfig tests query comparison configuration
func TestComparisonConfig(t *testing.T) {
	config := &Config{
		Comparison: ComparisonConfig{
			Enabled:         true,
			ExcludePatterns: []string{"SHOW.*", "SELECT.*version.*"},
		},
	}

	// Compile patterns
	if err := compileExcludePatterns(&config.Comparison); err != nil {
		t.Fatalf("Failed to compile exclude patterns: %v", err)
	}

	tests := []struct {
		name           string
		query          string
		expectCompare  bool
	}{
		{
			name:          "regular query",
			query:         "SELECT * FROM users",
			expectCompare: true,
		},
		{
			name:          "show command",
			query:         "SHOW server_version",
			expectCompare: false,
		},
		{
			name:          "version query",
			query:         "SELECT version()",
			expectCompare: false,
		},
		{
			name:          "case insensitive exclude",
			query:         "show all",
			expectCompare: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			shouldCompare := config.ShouldCompareQuery(tt.query)
			if tt.expectCompare && !shouldCompare {
				t.Error("Expected query to be compared")
			}
			if !tt.expectCompare && shouldCompare {
				t.Error("Expected query to not be compared")
			}
		})
	}
}

// TestPGMessageCreation tests PostgreSQL message creation
func TestPGMessageCreation(t *testing.T) {
	// Test error message creation
	errorMsg := CreateErrorMessage("ERROR", "XX000", "Test error message")
	if errorMsg == nil {
		t.Fatal("Expected error message to be created")
	}
	if errorMsg.Type != msgTypeErrorResponse {
		t.Errorf("Expected error message type %c, got %c", msgTypeErrorResponse, errorMsg.Type)
	}

	// Test that message contains expected fields
	data := string(errorMsg.Data)
	if !strings.Contains(data, "ERROR") {
		t.Error("Expected error message to contain severity")
	}
	if !strings.Contains(data, "XX000") {
		t.Error("Expected error message to contain error code")
	}
	if !strings.Contains(data, "Test error message") {
		t.Error("Expected error message to contain error text")
	}
}

// TestUtilityFunctions tests various utility functions
func TestUtilityFunctions(t *testing.T) {
	// Test catalog query detection
	tests := []struct {
		query    string
		expected bool
	}{
		{"SELECT * FROM pg_tables", true},
		{"SELECT * FROM information_schema.tables", true},
		{"SELECT * FROM users", false},
		{"INSERT INTO pg_user VALUES (1)", true}, // Contains pg_
	}

	for _, tt := range tests {
		result := isCatalogQuery(tt.query)
		if result != tt.expected {
			t.Errorf("isCatalogQuery(%q) = %v, expected %v", tt.query, result, tt.expected)
		}
	}
}

// TestValidateConnectionParameters tests connection parameter validation
func TestValidateConnectionParameters(t *testing.T) {
	tests := []struct {
		name        string
		params      map[string]string
		expectError bool
	}{
		{
			name: "valid parameters",
			params: map[string]string{
				"user":     "testuser",
				"database": "testdb",
			},
			expectError: false,
		},
		{
			name: "valid without user",
			params: map[string]string{
				"database": "testdb",
			},
			expectError: false,
		},
		{
			name: "missing database",
			params: map[string]string{
				"user": "testuser",
			},
			expectError: true,
		},
		{
			name:        "empty parameters",
			params:      map[string]string{},
			expectError: true,
		},
		{
			name: "empty database name",
			params: map[string]string{
				"database": "",
			},
			expectError: true,
		},
		{
			name: "invalid database name",
			params: map[string]string{
				"database": "test-db!",
			},
			expectError: true,
		},
		{
			name: "database name too long",
			params: map[string]string{
				"database": strings.Repeat("a", 65),
			},
			expectError: true,
		},
		{
			name: "invalid username",
			params: map[string]string{
				"database": "testdb",
				"user":     "test@user",
			},
			expectError: true,
		},
		{
			name: "username too long",
			params: map[string]string{
				"database": "testdb",
				"user":     strings.Repeat("a", 65),
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateConnectionParameters(tt.params)
			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		})
	}
}

// TestProxyCreation tests proxy creation and configuration
func TestProxyCreation(t *testing.T) {
	config := &Config{
		Proxy: ProxyConfig{
			ListenPort: 5431,
			PostgreSQL: DatabaseConfig{
				Host: "localhost",
				Port: 5432,
				User: "postgres",
				Database: "test",
			},
			YugabyteDB: DatabaseConfig{
				Host: "localhost",
				Port: 5433,
				User: "postgres",
				Database: "test",
			},
		},
		Comparison: ComparisonConfig{
			Enabled: true,
			SourceOfTruth: "yugabytedb",
		},
	}

	// Test simple proxy creation
	proxy := NewSimpleProxy(config)
	if proxy == nil {
		t.Fatal("Expected proxy to be created")
	}
	if proxy.config != config {
		t.Error("Expected proxy to store config reference")
	}

	// Test dual execution proxy creation
	dualProxy := NewDualExecutionProxy(config)
	if dualProxy == nil {
		t.Fatal("Expected dual proxy to be created")
	}
	if dualProxy.config != config {
		t.Error("Expected dual proxy to store config reference")
	}

	// Test dual execution proxy with reporter
	reporter := NewInconsistencyReporter()
	dualProxyWithReporter := NewDualExecutionProxyWithReporter(config, reporter)
	if dualProxyWithReporter == nil {
		t.Fatal("Expected dual proxy with reporter to be created")
	}
	if dualProxyWithReporter.reporter != reporter {
		t.Error("Expected dual proxy to use provided reporter")
	}
}

// TestConfigSecurity tests security configuration validation
func TestConfigSecurity(t *testing.T) {
	config := &Config{
		Proxy: ProxyConfig{
			ListenPort: 5431,
			PostgreSQL: DatabaseConfig{
				Host: "localhost",
				Port: 5432,
			},
			YugabyteDB: DatabaseConfig{
				Host: "localhost", 
				Port: 5433,
			},
		},
	}

	// Test that ValidateConfigSecurity works
	err := ValidateConfigSecurity(config)
	if err != nil {
		t.Errorf("Expected no error from ValidateConfigSecurity, got: %v", err)
	}
}

// TestSanitizeString tests string sanitization
func TestSanitizeString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "normal string",
			input:    "hello world",
			expected: "hello world",
		},
		{
			name:     "string with tab and newline",
			input:    "hello\tworld\n",
			expected: "hello\tworld\n",
		},
		{
			name:     "string with control characters",
			input:    "hello\x00world\x01",
			expected: "helloworld",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SanitizeString(tt.input)
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

// TestDualDatabaseResolver tests dual database resolver functionality
func TestDualDatabaseResolver(t *testing.T) {
	config := &Config{
		Proxy: ProxyConfig{
			PostgreSQL: DatabaseConfig{
				Host: "localhost",
				Port: 5432,
				User: "postgres",
				Database: "test",
			},
			YugabyteDB: DatabaseConfig{
				Host: "localhost",
				Port: 5433,
				User: "postgres", 
				Database: "test",
			},
		},
		Comparison: ComparisonConfig{
			SourceOfTruth: "postgresql",
			Enabled:       true,
		},
	}

	resolver := NewDualDatabaseResolver(config, "localhost:5432", "localhost:5433")
	if resolver == nil {
		t.Fatal("Expected resolver to be created")
	}

	// Test that pools are initially nil
	pgPool, ybPool := resolver.GetPools()
	if pgPool != nil || ybPool != nil {
		t.Error("Expected pools to be nil initially")
	}

	// Test resolver configuration
	if resolver.config != config {
		t.Error("Expected resolver to store config reference")
	}
}

// TestPGProtocolWriterReader tests protocol writer and reader
func TestPGProtocolWriterReader(t *testing.T) {
	// Test message type constants
	if msgTypeQuery != 'Q' {
		t.Errorf("Expected msgTypeQuery to be 'Q', got %c", msgTypeQuery)
	}
	if msgTypeTerminate != 'X' {
		t.Errorf("Expected msgTypeTerminate to be 'X', got %c", msgTypeTerminate)
	}
	if msgTypeReadyForQuery != 'Z' {
		t.Errorf("Expected msgTypeReadyForQuery to be 'Z', got %c", msgTypeReadyForQuery)
	}
	if msgTypeErrorResponse != 'E' {
		t.Errorf("Expected msgTypeErrorResponse to be 'E', got %c", msgTypeErrorResponse)
	}
	if msgTypeDataRow != 'D' {
		t.Errorf("Expected msgTypeDataRow to be 'D', got %c", msgTypeDataRow)
	}
}

// TestConfigAddOrderBy tests query modification for ordering
func TestConfigAddOrderBy(t *testing.T) {
	config := &Config{
		Comparison: ComparisonConfig{
			ForceOrderByCompare: true,
			DefaultOrderColumns: []string{"id", "pk", "*"},
		},
	}

	tests := []struct {
		name     string
		query    string
		expected bool // true if ORDER BY should be added
	}{
		{
			name:     "select without order by",
			query:    "SELECT * FROM users",
			expected: true,
		},
		{
			name:     "select with existing order by",
			query:    "SELECT * FROM users ORDER BY name",
			expected: false,
		},
		{
			name:     "insert statement",
			query:    "INSERT INTO users VALUES (1, 'test')",
			expected: false,
		},
		{
			name:     "update statement",
			query:    "UPDATE users SET name = 'test'",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := config.AddOrderByToQuery(tt.query)
			hasOrderBy := strings.Contains(strings.ToUpper(result), "ORDER BY")
			originalHasOrderBy := strings.Contains(strings.ToUpper(tt.query), "ORDER BY")
			
			if tt.expected {
				if !hasOrderBy {
					t.Errorf("Expected ORDER BY to be added to query: %s", tt.query)
				}
			} else {
				if hasOrderBy && !originalHasOrderBy {
					t.Errorf("Expected ORDER BY to not be added to query: %s", tt.query)
				}
			}
		})
	}
}

// TestQueryComparisonResult tests query comparison result functionality
func TestQueryComparisonResult(t *testing.T) {
	// Test comparison result structure
	result := &QueryComparisonResult{
		OriginalQuery: "SELECT * FROM test",
		PostgreSQLResult: &DatabaseResult{
			DatabaseName: "PostgreSQL",
			Columns:      []string{"id", "name"},
			Rows:         [][]interface{}{{1, "test"}},
			RowCount:     1,
		},
		YugabyteDBResult: &DatabaseResult{
			DatabaseName: "YugabyteDB",
			Columns:      []string{"id", "name"},
			Rows:         [][]interface{}{{1, "test"}},
			RowCount:     1,
		},
	}

	// Test comparison
	result.CompareResults()
	if !result.Match {
		t.Error("Expected results to match")
	}

	// Test with different results
	result.YugabyteDBResult.Rows = [][]interface{}{{1, "different"}}
	result.CompareResults()
	if result.Match {
		t.Error("Expected results to not match")
	}
}

// TestDatabaseResult tests database result functionality
func TestDatabaseResult(t *testing.T) {
	result := &DatabaseResult{
		DatabaseName: "PostgreSQL",
		Columns:      []string{"id", "name"},
		Rows:         [][]interface{}{{1, "test"}, {2, "test2"}},
		RowCount:     2,
	}

	if result.DatabaseName != "PostgreSQL" {
		t.Error("Expected database name to be PostgreSQL")
	}
	if len(result.Columns) != 2 {
		t.Error("Expected 2 columns")
	}
	if result.RowCount != 2 {
		t.Error("Expected row count to be 2")
	}
}

// TestConstraintAnalysis tests constraint error analysis
func TestConstraintAnalysis(t *testing.T) {
	detector := NewConstraintDivergenceDetector()
	
	// Test constraint error analysis
	err := errors.New("duplicate key value violates unique constraint")
	constraintErr := detector.AnalyzeConstraintError(err)
	
	if constraintErr == nil {
		t.Error("Expected constraint error to be detected")
	}
	
	// Test non-constraint error
	err = errors.New("connection refused")
	constraintErr = detector.AnalyzeConstraintError(err)
	
	if constraintErr == nil {
		t.Error("Expected constraint error struct to be returned")
	} else if constraintErr.Type != UnknownConstraint {
		t.Errorf("Expected UnknownConstraint for non-constraint error, got %v", constraintErr.Type)
	}
}

// TestValueComparison tests value comparison functionality
func TestValueComparison(t *testing.T) {
	tests := []struct {
		name     string
		a, b     interface{}
		expected bool
	}{
		{
			name:     "identical strings",
			a:        "test",
			b:        "test",
			expected: true,
		},
		{
			name:     "different strings",
			a:        "test1",
			b:        "test2",
			expected: false,
		},
		{
			name:     "both nil",
			a:        nil,
			b:        nil,
			expected: true,
		},
		{
			name:     "one nil",
			a:        nil,
			b:        "test",
			expected: false,
		},
		{
			name:     "byte slice comparison",
			a:        []byte("test"),
			b:        "test",
			expected: true,
		},
		{
			name:     "different byte slices",
			a:        []byte("test1"),
			b:        []byte("test2"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := compareValues(tt.a, tt.b)
			if result != tt.expected {
				t.Errorf("Expected %v, got %v for comparing %v and %v", tt.expected, result, tt.a, tt.b)
			}
		})
	}
}

// TestConvertToString tests string conversion utility
func TestConvertToString(t *testing.T) {
	tests := []struct {
		name     string
		input    interface{}
		expected string
	}{
		{
			name:     "string input",
			input:    "hello",
			expected: "hello",
		},
		{
			name:     "byte slice input",
			input:    []byte("hello"),
			expected: "hello",
		},
		{
			name:     "integer input",
			input:    42,
			expected: "42",
		},
		{
			name:     "nil input",
			input:    nil,
			expected: "<nil>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertToString(tt.input)
			if result != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, result)
			}
		})
	}
}

// TestLoadConfig tests configuration loading
func TestLoadConfig(t *testing.T) {
	// Test loading valid config
	config, err := LoadConfig("config/twinkly.conf")
	if err != nil {
		t.Errorf("Expected to load config successfully, got error: %v", err)
	}
	if config == nil {
		t.Error("Expected config to be loaded")
	}

	// Test loading non-existent config
	_, err = LoadConfig("non-existent.conf")
	if err == nil {
		t.Error("Expected error when loading non-existent config")
	}
}

// TestGetDetectedDivergences tests divergence getter methods
func TestGetDetectedDivergences(t *testing.T) {
	detector := NewConstraintDivergenceDetector()
	
	// Initially should be empty
	divergences := detector.GetDetectedDivergences()
	if len(divergences) != 0 {
		t.Errorf("Expected 0 divergences initially, got %d", len(divergences))
	}
	
	count := detector.GetCriticalDivergenceCount()
	if count != 0 {
		t.Errorf("Expected 0 critical divergences initially, got %d", count)
	}
	
	// Add a divergence by calling DetectDivergence
	pgResult := &QueryExecutionResult{
		Success:      true,
		DatabaseName: "PostgreSQL",
	}
	ybResult := &QueryExecutionResult{
		Success:      false,
		Error:        errors.New("constraint violation"),
		DatabaseName: "YugabyteDB",
	}
	
	divergence := detector.DetectDivergence("INSERT INTO test VALUES (1)", pgResult, ybResult)
	if divergence == nil {
		t.Error("Expected divergence to be detected")
	}
	
	// Check that divergences are stored
	divergences = detector.GetDetectedDivergences()
	if len(divergences) != 1 {
		t.Errorf("Expected 1 divergence after detection, got %d", len(divergences))
	}
	
	count = detector.GetCriticalDivergenceCount()
	if count != 1 {
		t.Errorf("Expected 1 critical divergence after detection, got %d", count)
	}
}

// TestConstraintTypeToString tests constraint type string conversion
func TestConstraintTypeToString(t *testing.T) {
	detector := NewConstraintDivergenceDetector()
	
	tests := []struct {
		constraintType ConstraintViolationType
		expected       string
	}{
		{UniqueViolation, "UNIQUE_VIOLATION"},
		{CheckViolation, "CHECK_VIOLATION"},
		{ForeignKeyViolation, "FOREIGN_KEY_VIOLATION"},
		{NotNullViolation, "NOT_NULL_VIOLATION"},
		{PrimaryKeyViolation, "PRIMARY_KEY_VIOLATION"},
		{UnknownConstraint, "UNKNOWN_CONSTRAINT"},
	}
	
	for _, tt := range tests {
		result := detector.constraintTypeToString(tt.constraintType)
		if result != tt.expected {
			t.Errorf("Expected %s for constraint type %v, got %s", tt.expected, tt.constraintType, result)
		}
	}
}

// TestPGProtocolReadWrite tests protocol reading and writing
func TestPGProtocolReadWrite(t *testing.T) {
	// Test error message creation
	errorMsg := CreateErrorMessage("ERROR", "42P01", "relation does not exist")
	if errorMsg == nil {
		t.Fatal("Expected error message to be created")
	}
	
	// Test query message creation
	queryMsg := CreateQueryMessage("SELECT 1")
	if queryMsg == nil {
		t.Fatal("Expected query message to be created")
	}
	if queryMsg.Type != msgTypeQuery {
		t.Errorf("Expected query message type 'Q', got %c", queryMsg.Type)
	}
}

// TestExtractDataRows tests data row extraction
func TestExtractDataRows(t *testing.T) {
	messages := []*PGMessage{
		{Type: msgTypeRowDescription, Data: []byte("columns")},
		{Type: msgTypeDataRow, Data: []byte("row1")},
		{Type: msgTypeDataRow, Data: []byte("row2")},
		{Type: msgTypeCommandComplete, Data: []byte("SELECT 2")},
		{Type: msgTypeReadyForQuery, Data: []byte("I")},
	}
	
	dataRows := extractDataRows(messages)
	if len(dataRows) != 2 {
		t.Errorf("Expected 2 data rows, got %d", len(dataRows))
	}
	
	for _, row := range dataRows {
		if row.Type != msgTypeDataRow {
			t.Errorf("Expected data row type 'D', got %c", row.Type)
		}
	}
}

// TestProxyReporter tests proxy reporter functionality
func TestProxyReporter(t *testing.T) {
	config := &Config{
		Comparison: ComparisonConfig{
			FailOnDifferences: true,
		},
	}
	
	reporter := NewInconsistencyReporter()
	proxy := NewDualExecutionProxyWithReporter(config, reporter)
	
	retrievedReporter := proxy.GetReporter()
	if retrievedReporter != reporter {
		t.Error("Expected proxy to return the same reporter")
	}
}

// TestAdditionalSecurityValidation tests additional security validation
func TestAdditionalSecurityValidation(t *testing.T) {
	security := NewSecurityConfig()
	err := security.CompilePatterns()
	if err != nil {
		t.Fatalf("Failed to compile patterns: %v", err)
	}
	
	// Test rate limiter creation
	rateLimiter := NewRateLimiter(10, 60)
	if rateLimiter == nil {
		t.Error("Expected rate limiter to be created")
	}
}

// TestAdditionalConfig tests additional configuration methods
func TestAdditionalConfig(t *testing.T) {
	config := &Config{
		Comparison: ComparisonConfig{
			ForceOrderByCompare: false,
		},
	}
	
	// Test AddOrderByToQuery when disabled
	query := "SELECT * FROM users"
	result := config.AddOrderByToQuery(query)
	if result != query {
		t.Error("Expected query to remain unchanged when ForceOrderByCompare is false")
	}
	
	// Test findOrderColumn with different scenarios
	config.Comparison.ForceOrderByCompare = true
	config.Comparison.DefaultOrderColumns = []string{"*"}
	
	result = config.AddOrderByToQuery("SELECT * FROM users")
	if !strings.Contains(result, "ORDER BY") {
		t.Error("Expected ORDER BY to be added when using * column")
	}
}

// TestInconsistencyReporting tests additional reporter functionality
func TestInconsistencyReporting(t *testing.T) {
	reporter := NewInconsistencyReporter()
	
	// Test impact assessment
	impact := reporter.assessImpact(ErrorDivergence, "CRITICAL")
	if impact == "" {
		t.Error("Expected impact assessment to return non-empty string")
	}
	
	// Test recommendation generation
	recommendation := reporter.getRecommendation(RowCountMismatch)
	if recommendation == "" {
		t.Error("Expected recommendation to return non-empty string")
	}
	
	// Test difference handling - we can't test formatDifferences directly as it's private
	// but we can test that the summary generation works
	summary := reporter.GenerateSummaryReport()
	if summary == "" {
		t.Error("Expected summary report to contain information")
	}
}