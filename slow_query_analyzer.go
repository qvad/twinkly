package main

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"
)

// SlowQueryAnalyzer analyzes query performance differences between databases
type SlowQueryAnalyzer struct {
	config                *Config
	pgPool                *sql.DB
	ybPool                *sql.DB
	slowQueryThreshold    float64  // Ratio threshold (e.g., 4.0 means 4x slower)
	failOnSlowQueries     bool     // Whether to fail queries that are too slow
}

// SlowQueryResult contains performance analysis results
type SlowQueryResult struct {
	Query              string
	PostgreSQLTime     time.Duration
	YugabyteDBTime     time.Duration
	SlowRatio          float64
	PostgreSQLPlan     string
	YugabyteDBPlan     string
	IsSlowQuery        bool
	ShouldFail         bool
}

// NewSlowQueryAnalyzer creates a new slow query analyzer
func NewSlowQueryAnalyzer(config *Config, pgPool, ybPool *sql.DB) *SlowQueryAnalyzer {
	threshold := 4.0 // Default: fail if YB is 4x slower than PG
	if config.Comparison.SlowQueryRatio > 0 {
		threshold = config.Comparison.SlowQueryRatio
	}
	
	return &SlowQueryAnalyzer{
		config:             config,
		pgPool:             pgPool,
		ybPool:             ybPool,
		slowQueryThreshold: threshold,
		failOnSlowQueries:  config.Comparison.FailOnSlowQueries,
	}
}

// AnalyzeQuery analyzes query performance and returns error if it's too slow
func (s *SlowQueryAnalyzer) AnalyzeQuery(query string) (*SlowQueryResult, error) {
	result := &SlowQueryResult{
		Query: query,
	}
	
	// Execute on PostgreSQL with timing
	pgStart := time.Now()
	pgPlan, pgErr := s.getExplainAnalyzeForDB(s.pgPool, query, "postgresql")
	pgDuration := time.Since(pgStart)
	
	// Execute on YugabyteDB with timing  
	ybStart := time.Now()
	ybPlan, ybErr := s.getExplainAnalyzeForDB(s.ybPool, query, "yugabytedb")
	ybDuration := time.Since(ybStart)
	
	result.PostgreSQLTime = pgDuration
	result.YugabyteDBTime = ybDuration
	result.PostgreSQLPlan = pgPlan
	result.YugabyteDBPlan = ybPlan
	
	// Calculate slow ratio (YB time / PG time)
	if pgDuration > 0 {
		result.SlowRatio = float64(ybDuration) / float64(pgDuration)
	}
	
	// Check if this is a slow query
	result.IsSlowQuery = result.SlowRatio > s.slowQueryThreshold
	result.ShouldFail = result.IsSlowQuery && s.failOnSlowQueries
	
	// Log slow queries
	if result.IsSlowQuery {
		log.Printf("🐌 SLOW QUERY DETECTED: YugabyteDB is %.2fx slower than PostgreSQL", result.SlowRatio)
		log.Printf("Query: %s", query)
		log.Printf("PostgreSQL: %v, YugabyteDB: %v", pgDuration, ybDuration)
	}
	
	// Return error if we should fail on slow queries
	if result.ShouldFail {
		errorMsg := fmt.Sprintf("Query performance unacceptable: YugabyteDB is %.2fx slower than PostgreSQL (threshold: %.2fx)\n\nPostgreSQL Plan:\n%s\n\nYugabyteDB Plan:\n%s", 
			result.SlowRatio, s.slowQueryThreshold, pgPlan, ybPlan)
		return result, fmt.Errorf(errorMsg)
	}
	
	// Check for execution errors
	if pgErr != nil {
		return result, fmt.Errorf("PostgreSQL execution failed: %v", pgErr)
	}
	if ybErr != nil {
		return result, fmt.Errorf("YugabyteDB execution failed: %v", ybErr)
	}
	
	return result, nil
}

// getExplainAnalyzeForDB gets EXPLAIN ANALYZE output for a specific database
func (s *SlowQueryAnalyzer) getExplainAnalyzeForDB(db *sql.DB, query string, dbType string) (string, error) {
	var options ExplainOptions
	if dbType == "postgresql" {
		options = s.config.Comparison.ExplainOptions.PostgreSQL
	} else {
		options = s.config.Comparison.ExplainOptions.YugabyteDB
	}
	
	// Build EXPLAIN command with configured options
	explainParts := []string{"EXPLAIN ("}
	optionParts := []string{}
	
	if options.Analyze {
		optionParts = append(optionParts, "ANALYZE")
	}
	if options.Buffers {
		optionParts = append(optionParts, "BUFFERS")
	}
	if options.Costs {
		optionParts = append(optionParts, "COSTS")
	}
	if options.Timing {
		optionParts = append(optionParts, "TIMING")
	}
	if options.Summary {
		optionParts = append(optionParts, "SUMMARY")
	}
	
	// YugabyteDB specific options
	if dbType == "yugabytedb" {
		if options.Dist {
			optionParts = append(optionParts, "DIST")
		}
		if options.Debug {
			optionParts = append(optionParts, "DEBUG")
		}
	}
	
	// Add format
	optionParts = append(optionParts, fmt.Sprintf("FORMAT %s", options.Format))
	
	explainParts = append(explainParts, strings.Join(optionParts, ", "))
	explainParts = append(explainParts, ") ")
	explainParts = append(explainParts, query)
	
	explainQuery := strings.Join(explainParts, "")
	
	log.Printf("Executing EXPLAIN for %s: %s", dbType, explainQuery)
	
	rows, err := db.Query(explainQuery)
	if err != nil {
		return "", fmt.Errorf("failed to execute EXPLAIN ANALYZE: %v", err)
	}
	defer rows.Close()
	
	var plan string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return "", fmt.Errorf("failed to scan EXPLAIN ANALYZE result: %v", err)
		}
		plan += line + "\n"
	}
	
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("error reading EXPLAIN ANALYZE results: %v", err)
	}
	
	return plan, nil
}

// getExplainAnalyze is deprecated - use getExplainAnalyzeForDB
func (s *SlowQueryAnalyzer) getExplainAnalyze(db *sql.DB, query string) (string, error) {
	// Backward compatibility - defaults to PostgreSQL options
	return s.getExplainAnalyzeForDB(db, query, "postgresql")
}

// GetSlowQuerySummary returns a summary of slow query analysis
func (s *SlowQueryAnalyzer) GetSlowQuerySummary() string {
	return fmt.Sprintf("Slow Query Analysis - Threshold: %.2fx, Fail on slow: %v", 
		s.slowQueryThreshold, s.failOnSlowQueries)
}