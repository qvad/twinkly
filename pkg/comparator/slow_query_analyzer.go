package comparator

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/qvad/twinkly/pkg/ai"
	"github.com/qvad/twinkly/pkg/config"
)

// SlowQueryAnalyzer analyzes query performance differences between databases
type SlowQueryAnalyzer struct {
	config             *config.Config
	pgPool             *sql.DB
	ybPool             *sql.DB
	aiAgent            ai.AIAgent
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
	AIAnalysis     *ai.AnalysisResponse // Result from AI agent
}

// NewSlowQueryAnalyzer creates a new slow query analyzer
func NewSlowQueryAnalyzer(config *config.Config, pgPool, ybPool *sql.DB) *SlowQueryAnalyzer {
	threshold := 4.0 // Default: fail if YB is 4x slower than PG
	if config.Comparison.SlowQueryRatio > 0 {
		threshold = config.Comparison.SlowQueryRatio
	}

	// Initialize AI agent based on config (currently supports mock)
	var agent ai.AIAgent
	if config.Comparison.AI.Enabled {
		// TODO: Switch on provider type when real implementations exist
		agent = ai.NewMockAIAgent()
	}

	return &SlowQueryAnalyzer{
		config:             config,
		pgPool:             pgPool,
		ybPool:             ybPool,
		aiAgent:            agent,
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

	var wg sync.WaitGroup
	var pgPlan, ybPlan string
	var pgErr, ybErr error
	var pgDuration, ybDuration time.Duration

	wg.Add(2)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Execute on PostgreSQL with timing
	go func() {
		defer wg.Done()
		start := time.Now()
		pgPlan, pgErr = s.getExplainPlan(ctx, s.pgPool, query, "postgresql")
		pgDuration = time.Since(start)
	}()

	// Execute on YugabyteDB with timing
	go func() {
		defer wg.Done()
		start := time.Now()
		ybPlan, ybErr = s.getExplainPlan(ctx, s.ybPool, query, "yugabytedb")
		ybDuration = time.Since(start)
	}()

	wg.Wait()

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

	// AI Analysis Trigger
	if result.IsSlowQuery && s.aiAgent != nil {
		// Run AI analysis asynchronously to avoid blocking the response
		go func() {
			ctxAI, cancelAI := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancelAI()

			req := ai.AnalysisRequest{
				Query:           query,
				DiscrepancyType: "Performance",
				PostgreSQL: ai.BackendData{
					ExecutionTime: pgDuration,
					Plan:          pgPlan,
				},
				YugabyteDB: ai.BackendData{
					ExecutionTime: ybDuration,
					Plan:          ybPlan,
				},
				AdditionalInfo: []string{fmt.Sprintf("Slow Ratio: %.2fx", result.SlowRatio)},
			}

			resp, err := s.aiAgent.AnalyzeDiscrepancy(ctxAI, req)
			if err != nil {
				log.Printf("❌ AI Analysis failed: %v", err)
			} else {
				log.Printf("🤖 AI Analysis for slow query:\n%s\nRecommendation: %s", resp.Analysis, resp.Recommendation)
			}
		}()
	}

	// Log slow queries
	if result.IsSlowQuery {
		log.Printf("🐌 SLOW QUERY DETECTED: YugabyteDB is %.2fx slower than PostgreSQL", result.SlowRatio)
		log.Printf("Query: %s", query)
		log.Printf("PostgreSQL: %v, YugabyteDB: %v", pgDuration, ybDuration)
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

// chooseExplainClause selects the appropriate EXPLAIN clause based on DB type and query type
func chooseExplainClause(cfg *config.Config, query string, dbType string) string {
	s := strings.TrimSpace(strings.ToLower(query))
	isSelect := strings.HasPrefix(s, "select") || strings.HasPrefix(s, "with")

	// Default clauses
	pgClause := "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)"
	// ybClause is currently unused as we have specific logic below
	// ybClause := "EXPLAIN (ANALYZE, DIST, FORMAT TEXT)"

	if cfg != nil {
		// Use configured generic clauses if specific ones aren't fully implemented yet
		// For now, we hardcode the YB distinction requested by the user
		if dbType == "yugabytedb" {
			if isSelect {
				return "EXPLAIN (ANALYZE, DIST)"
			}
			return "EXPLAIN (DIST)"
		} else {
			// PostgreSQL
			if cfg.Comparison.ExplainSelect != "" && isSelect {
				return cfg.Comparison.ExplainSelect
			}
			if cfg.Comparison.ExplainOther != "" && !isSelect {
				return cfg.Comparison.ExplainOther
			}
			return pgClause
		}
	}
	return pgClause
}

// getExplainPlan gets EXPLAIN/EXPLAIN ANALYZE output for a query using config
func (s *SlowQueryAnalyzer) getExplainPlan(ctx context.Context, db *sql.DB, query string, dbType string) (string, error) {
	clause := chooseExplainClause(s.config, query, dbType)
	explainQuery := fmt.Sprintf("%s %s", clause, query)

	rows, err := db.QueryContext(ctx, explainQuery)
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
