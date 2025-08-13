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
	config             *Config
	pgPool             *sql.DB
	ybPool             *sql.DB
	slowQueryThreshold float64 // Ratio threshold (e.g., 4.0 means 4x slower)
	failOnSlowQueries  bool    // Whether to fail queries that are too slow
}

// SlowQueryResult contains performance analysis results
type SlowQueryResult struct {
	Query          string
	PostgreSQLTime time.Duration
	YugabyteDBTime time.Duration
	SlowRatio      float64
	PostgreSQLPlan string
	YugabyteDBPlan string
	IsSlowQuery    bool
	ShouldFail     bool
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
	// Skip DDL and utility statements that cannot or should not be EXPLAINed
	if isDDLOrUtility(query) {
		return nil, nil
	}

	result := &SlowQueryResult{
		Query: query,
	}

	// Execute on PostgreSQL with timing
	pgStart := time.Now()
	pgPlan, pgErr := s.getExplainPlan(s.pgPool, query)
	pgDuration := time.Since(pgStart)

	// Execute on YugabyteDB with timing
	ybStart := time.Now()
	ybPlan, ybErr := s.getExplainPlan(s.ybPool, query)
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

	// Do not fail on slow queries; caller will report performance issues as consistency reports
	// Previously, this returned an error when ShouldFail was true. Now, we only return execution errors below.

	// Check for execution errors
	if pgErr != nil {
		return result, fmt.Errorf("PostgreSQL execution failed: %v", pgErr)
	}
	if ybErr != nil {
		return result, fmt.Errorf("YugabyteDB execution failed: %v", ybErr)
	}

	return result, nil
}

// chooseExplainClause selects the appropriate EXPLAIN clause based on query type and config
func chooseExplainClause(cfg *Config, query string) string {
	s := strings.TrimSpace(strings.ToLower(query))
	// Determine defaults if config not provided or fields empty
	selectClause := "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)"
	otherClause := "EXPLAIN (FORMAT TEXT)"
	if cfg != nil {
		if cfg.Comparison.ExplainSelect != "" {
			selectClause = cfg.Comparison.ExplainSelect
		}
		if cfg.Comparison.ExplainOther != "" {
			otherClause = cfg.Comparison.ExplainOther
		}
	}
	if strings.HasPrefix(s, "select") || strings.HasPrefix(s, "with") {
		return selectClause
	}
	return otherClause
}

// getExplainPlan gets EXPLAIN/EXPLAIN ANALYZE output for a query using config
func (s *SlowQueryAnalyzer) getExplainPlan(db *sql.DB, query string) (string, error) {
	clause := chooseExplainClause(s.config, query)
	explainQuery := fmt.Sprintf("%s %s", clause, query)
	rows, err := db.Query(explainQuery)
	if err != nil {
		return "", fmt.Errorf("failed to execute %s: %v", clause, err)
	}
	defer rows.Close()

	var plan string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return "", fmt.Errorf("failed to scan EXPLAIN result: %v", err)
		}
		plan += line + "\n"
	}

	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("error reading EXPLAIN results: %v", err)
	}

	return plan, nil
}

// GetSlowQuerySummary returns a summary of slow query analysis
func (s *SlowQueryAnalyzer) GetSlowQuerySummary() string {
	return fmt.Sprintf("Slow Query Analysis - Threshold: %.2fx, Fail on slow: %v",
		s.slowQueryThreshold, s.failOnSlowQueries)
}

// isDDLOrUtility returns true for statements that should not be EXPLAIN ANALYZE'd
func isDDLOrUtility(query string) bool {
	q := strings.TrimSpace(strings.ToLower(query))
	// Common DDL and utility statements that aren't suitable for EXPLAIN ANALYZE
	prefixes := []string{
		"create",
		"alter",
		"drop",
		"truncate",
		"grant",
		"revoke",
		"comment",
		"vacuum",
		"analyze", // stats analyze (not EXPLAIN ANALYZE)
		"explain", // user-issued EXPLAIN
		"set",
		"show",
		"begin",
		"start transaction",
		"commit",
		"rollback",
		"savepoint",
		"release savepoint",
		"prepare",
		"deallocate",
		"discard",
		"lock",
		"checkpoint",
		"cluster",
		"refresh materialized view",
		"do",
		"copy",
		"listen",
		"unlisten",
		"notify",
		"reset",
		"declare",
		"close",
		"fetch",
		"move",
		"call",
	}
	for _, p := range prefixes {
		if strings.HasPrefix(q, p) {
			return true
		}
	}
	return false
}
