package proxy

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/qvad/twinkly/pkg/config"
)

// SideChannelManager handles connections and query comparison
type SideChannelManager struct {
	config *config.Config
	pgAddr string
	ybAddr string

	// Connection pools for direct database access (for comparison)
	pgPool *sql.DB
	ybPool *sql.DB
	mutex  sync.RWMutex

	// Shutdown channel for graceful cleanup
	shutdown chan struct{}
}

// NewSideChannelManager creates a new resolver with comparison capabilities
func NewSideChannelManager(config *config.Config, pgAddr, ybAddr string) *SideChannelManager {
	return &SideChannelManager{
		config:   config,
		pgAddr:   pgAddr,
		ybAddr:   ybAddr,
		shutdown: make(chan struct{}),
	}
}

// InitializePools initializes direct database connection pools for comparison
func (r *SideChannelManager) InitializePools() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	// PostgreSQL pool
	pgConnStr := fmt.Sprintf("host=%s port=%d user=%s dbname=%s sslmode=disable",
		r.config.Proxy.PostgreSQL.Host, r.config.Proxy.PostgreSQL.Port,
		r.config.Proxy.PostgreSQL.User, r.config.Proxy.PostgreSQL.Database)

	pgPool, err := sql.Open("postgres", pgConnStr)
	if err != nil {
		return fmt.Errorf("failed to create PostgreSQL pool: %w", err)
	}

	// Configure connection pool limits to prevent resource exhaustion
	pgPool.SetMaxOpenConns(10)
	pgPool.SetMaxIdleConns(5)
	pgPool.SetConnMaxLifetime(30 * time.Minute)
	pgPool.SetConnMaxIdleTime(10 * time.Minute) // Close idle connections after 10 minutes

	if err := pgPool.Ping(); err != nil {
		pgPool.Close()
		return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	r.pgPool = pgPool
	log.Printf("✓ PostgreSQL comparison pool initialized")

	// YugabyteDB pool
	ybConnStr := fmt.Sprintf("host=%s port=%d user=%s dbname=%s sslmode=disable",
		r.config.Proxy.YugabyteDB.Host, r.config.Proxy.YugabyteDB.Port,
		r.config.Proxy.YugabyteDB.User, r.config.Proxy.YugabyteDB.Database)

	ybPool, err := sql.Open("postgres", ybConnStr)
	if err != nil {
		log.Printf("⚠ YugabyteDB not available: %v", err)
		return nil // Continue without YugabyteDB
	}

	// Configure YugabyteDB pool with same settings
	ybPool.SetMaxOpenConns(10)
	ybPool.SetMaxIdleConns(5)
	ybPool.SetConnMaxLifetime(30 * time.Minute)
	ybPool.SetConnMaxIdleTime(10 * time.Minute)

	if err := ybPool.Ping(); err != nil {
		ybPool.Close()
		log.Printf("⚠ YugabyteDB not available: %v", err)
		return nil // Continue without YugabyteDB
	}

	r.ybPool = ybPool
	log.Printf("✓ YugabyteDB comparison pool initialized")

	log.Printf("✓ Dual database pools initialized successfully")

	// Start connection health monitoring
	r.startHealthMonitoring()

	return nil
}

// GetPools returns the database connection pools
func (r *SideChannelManager) GetPools() (*sql.DB, *sql.DB) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	return r.pgPool, r.ybPool
}

// startHealthMonitoring starts background health checks for database connections
func (r *SideChannelManager) startHealthMonitoring() {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				r.checkConnectionHealth()
			case <-r.shutdown:
				log.Printf("✓ Health monitoring stopped")
				return
			}
		}
	}()
}

// checkConnectionHealth performs health checks on database connections
func (r *SideChannelManager) checkConnectionHealth() {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	// Check PostgreSQL health
	if r.pgPool != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := r.pgPool.PingContext(ctx); err != nil {
			log.Printf("⚠ PostgreSQL health check failed: %v", err)
		}
		cancel()
	}

	// Check YugabyteDB health
	if r.ybPool != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := r.ybPool.PingContext(ctx); err != nil {
			log.Printf("⚠ YugabyteDB health check failed: %v", err)
		}
		cancel()
	}
}

// CompareQuery executes a query on both databases and compares results
func (r *SideChannelManager) CompareQuery(query string) (*QueryComparisonResult, error) {
	if !r.config.ShouldCompareQuery(query) {
		return nil, nil // Skip comparison
	}

	r.mutex.RLock()
	defer r.mutex.RUnlock()

	if r.pgPool == nil {
		return nil, fmt.Errorf("PostgreSQL pool not initialized")
	}

	if r.ybPool == nil {
		// Only PostgreSQL available
		log.Printf("[COMPARISON] Skipping comparison - YugabyteDB not available: %s", query)
		return nil, nil
	}

	// Use query as-is (ORDER BY handling moved to dual_proxy.go)
	modifiedQuery := query

	result := &QueryComparisonResult{
		OriginalQuery: query,
		ModifiedQuery: modifiedQuery,
		Timestamp:     time.Now(),
	}

	// Execute on both databases
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	var pgErr, ybErr error

	wg.Add(2)

	// Execute on PostgreSQL
	go func() {
		defer wg.Done()
		result.PostgreSQLResult, pgErr = r.executeQuery(ctx, r.pgPool, "PostgreSQL", modifiedQuery)
		result.PostgreSQLDuration = result.PostgreSQLResult.Duration
	}()

	// Execute on YugabyteDB
	go func() {
		defer wg.Done()
		result.YugabyteDBResult, ybErr = r.executeQuery(ctx, r.ybPool, "YugabyteDB", modifiedQuery)
		result.YugabyteDuration = result.YugabyteDBResult.Duration
	}()

	wg.Wait()

	// Handle errors
	if pgErr != nil {
		result.PostgreSQLError = pgErr.Error()
	}
	if ybErr != nil {
		result.YugabyteDError = ybErr.Error()
	}

	// Simple comparison (detailed comparison moved to dual_proxy.go)
	result.CompareResults()

	// Log results
	if r.config.Comparison.LogComparisons {
		r.logComparison(result)
	}

	return result, nil
}

// executeQuery executes a query on a specific database
func (r *SideChannelManager) executeQuery(ctx context.Context, db *sql.DB, dbName, query string) (*DatabaseResult, error) {
	start := time.Now()

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return &DatabaseResult{
			DatabaseName: dbName,
			Duration:     time.Since(start),
			Error:        err.Error(),
		}, err
	}
	defer rows.Close()

	// Get column information
	columns, err := rows.Columns()
	if err != nil {
		return &DatabaseResult{
			DatabaseName: dbName,
			Duration:     time.Since(start),
			Error:        err.Error(),
		}, err
	}

	result := &DatabaseResult{
		DatabaseName: dbName,
		Columns:      columns,
		Rows:         make([][]interface{}, 0),
		Duration:     time.Since(start),
	}

	// Read rows
	rowCount := 0
	// Get comparison config for max rows limit
	maxRows := r.config.Comparison.MaxCompareRows
	if maxRows <= 0 {
		maxRows = 1000 // Default
	}
	for rows.Next() && rowCount < maxRows {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			result.Error = err.Error()
			return result, err
		}

		// Convert byte arrays to strings for comparison
		for i, v := range values {
			if bytes, ok := v.([]byte); ok {
				values[i] = string(bytes)
			}
		}

		result.Rows = append(result.Rows, values)
		rowCount++
	}

	result.RowCount = len(result.Rows)
	result.Duration = time.Since(start)

	// Check for iteration errors
	if err := rows.Err(); err != nil {
		result.Error = err.Error()
		return result, err
	}

	return result, nil
}

// logComparison logs the comparison results
func (r *SideChannelManager) logComparison(result *QueryComparisonResult) {
	if r.config.Comparison.LogDifferencesOnly && result.Match {
		return // Skip logging matches
	}

	status := "✓ MATCH"
	if !result.Match {
		status = "✗ DIFFER"
	}

	log.Printf("[COMPARISON] %s | PG: %d rows (%.2fms) | YB: %d rows (%.2fms) | Query: %s",
		status,
		result.PostgreSQLResult.RowCount,
		float64(result.PostgreSQLDuration.Nanoseconds())/1000000,
		result.YugabyteDBResult.RowCount,
		float64(result.YugabyteDuration.Nanoseconds())/1000000,
		result.OriginalQuery)

	if !result.Match {
		if result.DifferenceReason != "" {
			log.Printf("[COMPARISON] Difference: %s", result.DifferenceReason)
		}

		// Log first few different rows for debugging
		if len(result.SampleDifferences) > 0 {
			log.Printf("[COMPARISON] Sample differences:")
			for i, diff := range result.SampleDifferences {
				if i >= 3 { // Limit to first 3 differences
					break
				}
				log.Printf("  Row %d: PG=%v | YB=%v", diff.RowIndex, diff.PostgreSQLValue, diff.YugabyteValue)
			}
		}
	}

	// Slow query analysis moved to dual_proxy.go
}

// Close cleans up database connections
func (r *SideChannelManager) Close() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	var errors []string

	// Signal shutdown to background goroutines
	close(r.shutdown)

	// Slow query reporting moved to dual_proxy.go

	if r.pgPool != nil {
		if err := r.pgPool.Close(); err != nil {
			errors = append(errors, fmt.Sprintf("PostgreSQL: %v", err))
		}
		r.pgPool = nil
	}

	if r.ybPool != nil {
		if err := r.ybPool.Close(); err != nil {
			errors = append(errors, fmt.Sprintf("YugabyteDB: %v", err))
		}
		r.ybPool = nil
	}

	if len(errors) > 0 {
		return fmt.Errorf("close errors: %s", strings.Join(errors, ", "))
	}

	return nil
}

// QueryComparisonResult holds the results of comparing a query across databases
type QueryComparisonResult struct {
	OriginalQuery string
	ModifiedQuery string
	Timestamp     time.Time

	PostgreSQLResult   *DatabaseResult
	YugabyteDBResult   *DatabaseResult
	PostgreSQLDuration time.Duration
	YugabyteDuration   time.Duration
	PostgreSQLError    string
	YugabyteDError     string

	Match             bool
	DifferenceReason  string
	SampleDifferences []RowDifference
}

// DatabaseResult holds the result from a single database
type DatabaseResult struct {
	DatabaseName string
	Columns      []string
	Rows         [][]interface{}
	RowCount     int
	Duration     time.Duration
	Error        string
}

// RowDifference represents a difference between rows
type RowDifference struct {
	RowIndex        int
	ColumnIndex     int
	ColumnName      string
	PostgreSQLValue interface{}
	YugabyteValue   interface{}
}

// CompareResults compares the results from both databases
func (r *QueryComparisonResult) CompareResults() {
	// Check for errors first
	if r.PostgreSQLError != "" || r.YugabyteDError != "" {
		r.Match = false
		if r.PostgreSQLError != "" && r.YugabyteDError != "" {
			r.DifferenceReason = fmt.Sprintf("Both databases had errors: PG=%s, YB=%s", r.PostgreSQLError, r.YugabyteDError)
		} else if r.PostgreSQLError != "" {
			r.DifferenceReason = fmt.Sprintf("PostgreSQL error: %s", r.PostgreSQLError)
		} else {
			r.DifferenceReason = fmt.Sprintf("YugabyteDB error: %s", r.YugabyteDError)
		}
		return
	}

	// Compare row counts
	if r.PostgreSQLResult.RowCount != r.YugabyteDBResult.RowCount {
		r.Match = false
		r.DifferenceReason = fmt.Sprintf("Row count differs: PG=%d, YB=%d",
			r.PostgreSQLResult.RowCount, r.YugabyteDBResult.RowCount)
		return
	}

	// Compare columns
	if !reflect.DeepEqual(r.PostgreSQLResult.Columns, r.YugabyteDBResult.Columns) {
		r.Match = false
		r.DifferenceReason = fmt.Sprintf("Column names differ: PG=%v, YB=%v",
			r.PostgreSQLResult.Columns, r.YugabyteDBResult.Columns)
		return
	}

	// Compare row data
	differences := make([]RowDifference, 0)
	for i := 0; i < r.PostgreSQLResult.RowCount; i++ {
		pgRow := r.PostgreSQLResult.Rows[i]
		ybRow := r.YugabyteDBResult.Rows[i]

		for j := 0; j < len(pgRow); j++ {
			if !compareValues(pgRow[j], ybRow[j]) {
				differences = append(differences, RowDifference{
					RowIndex:        i,
					ColumnIndex:     j,
					ColumnName:      r.PostgreSQLResult.Columns[j],
					PostgreSQLValue: pgRow[j],
					YugabyteValue:   ybRow[j],
				})

				// Limit the number of differences we track
				if len(differences) >= 10 {
					break
				}
			}
		}

		if len(differences) >= 10 {
			break
		}
	}

	if len(differences) > 0 {
		r.Match = false
		r.DifferenceReason = fmt.Sprintf("Data differs in %d locations", len(differences))
		r.SampleDifferences = differences
	} else {
		r.Match = true
	}
}

// compareValues compares two values with type tolerance
func compareValues(a, b interface{}) bool {
	// Handle nil cases
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Handle byte slice conversion
	aStr := convertToString(a)
	bStr := convertToString(b)

	return aStr == bStr
}

// convertToString converts interface{} to string, handling byte slices properly
func convertToString(val interface{}) string {
	switch v := val.(type) {
	case []byte:
		return string(v)
	case string:
		return v
	default:
		return fmt.Sprintf("%v", val)
	}
}
