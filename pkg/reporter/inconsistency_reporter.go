package reporter

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/qvad/twinkly/pkg/config"
)

// InconsistencyType represents different types of inconsistencies
type InconsistencyType string

const (
	RowCountMismatch          InconsistencyType = "ROW_COUNT_MISMATCH"
	DataValueMismatch         InconsistencyType = "DATA_VALUE_MISMATCH"
	ConstraintDivergence      InconsistencyType = "CONSTRAINT_DIVERGENCE"
	ErrorDivergence           InconsistencyType = "ERROR_DIVERGENCE"
	SequenceDivergence        InconsistencyType = "SEQUENCE_DIVERGENCE"
	TransactionDivergence     InconsistencyType = "TRANSACTION_DIVERGENCE"
	ConnectionErrorDivergence InconsistencyType = "CONNECTION_ERROR_DIVERGENCE"
	PerformanceDegradation    InconsistencyType = "PERFORMANCE_DEGRADATION"
)

// InconsistencyReport represents a detected inconsistency
type InconsistencyReport struct {
	ID               string            `json:"id"`
	Timestamp        time.Time         `json:"timestamp"`
	Type             InconsistencyType `json:"type"`
	Severity         string            `json:"severity"` // CRITICAL, HIGH, MEDIUM, LOW
	Query            string            `json:"query"`
	PostgreSQLResult ResultSummary     `json:"postgresql_result"`
	YugabyteDBResult ResultSummary     `json:"yugabytedb_result"`
	PostgreSQLInfo   *DatabaseInfo     `json:"postgresql_info,omitempty"`
	YugabyteDBInfo   *DatabaseInfo     `json:"yugabytedb_info,omitempty"`
	Differences      []string          `json:"differences"`
	Impact           string            `json:"impact"`
	Recommendation   string            `json:"recommendation"`
	StackTrace       string            `json:"stack_trace,omitempty"`
}

// ResultSummary summarizes database operation results
type ResultSummary struct {
	Success     bool     `json:"success"`
	RowCount    int      `json:"row_count"`
	Error       string   `json:"error,omitempty"`
	ExecutionMs int64    `json:"execution_ms"`
	SampleData  []string `json:"sample_data,omitempty"`
}

// DatabaseInfo provides metadata about a database endpoint included in the report
type DatabaseInfo struct {
	Name     string `json:"name"` // "PostgreSQL" or "YugabyteDB"
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	User     string `json:"user"`
}

// InconsistencyReporter handles reporting of inconsistencies
type InconsistencyReporter struct {
	mu              sync.Mutex
	reports         []InconsistencyReport
	reportDir       string
	criticalCount   int
	highCount       int
	mediumCount     int
	lowCount        int
	reportToConsole bool
	reportToFile    bool
	reportToWebhook bool
	webhookURL      string
	cfg             *config.Config
}

// NewInconsistencyReporter creates a new reporter
func NewInconsistencyReporter() *InconsistencyReporter {
	reportDir := "inconsistency_reports"
	os.MkdirAll(reportDir, 0755)

	return &InconsistencyReporter{
		reports:         make([]InconsistencyReport, 0),
		reportDir:       reportDir,
		reportToConsole: true,
		reportToFile:    true,
		reportToWebhook: false, // Can be configured
	}
}

// AttachConfig provides configuration to enrich reports with database metadata.
func (r *InconsistencyReporter) AttachConfig(cfg *config.Config) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cfg = cfg
}

// ReportInconsistency reports a new inconsistency
func (r *InconsistencyReporter) ReportInconsistency(
	incType InconsistencyType,
	severity string,
	query string,
	pgResult ResultSummary,
	ybResult ResultSummary,
	differences []string,
) {
	r.mu.Lock()
	defer r.mu.Unlock()

	report := InconsistencyReport{
		ID:               fmt.Sprintf("INC-%d-%s", time.Now().Unix(), incType),
		Timestamp:        time.Now(),
		Type:             incType,
		Severity:         severity,
		Query:            query,
		PostgreSQLResult: pgResult,
		YugabyteDBResult: ybResult,
		Differences:      differences,
		Impact:           r.assessImpact(incType, severity),
		Recommendation:   r.getRecommendation(incType),
	}

	// Enrich with database metadata if configuration is available
	if r.cfg != nil {
		report.PostgreSQLInfo = &DatabaseInfo{
			Name:     "PostgreSQL",
			Host:     r.cfg.Proxy.PostgreSQL.Host,
			Port:     r.cfg.Proxy.PostgreSQL.Port,
			Database: r.cfg.Proxy.PostgreSQL.Database,
			User:     r.cfg.Proxy.PostgreSQL.User,
		}
		report.YugabyteDBInfo = &DatabaseInfo{
			Name:     "YugabyteDB",
			Host:     r.cfg.Proxy.YugabyteDB.Host,
			Port:     r.cfg.Proxy.YugabyteDB.Port,
			Database: r.cfg.Proxy.YugabyteDB.Database,
			User:     r.cfg.Proxy.YugabyteDB.User,
		}
	}

	r.reports = append(r.reports, report)

	// Update counters
	switch severity {
	case "CRITICAL":
		r.criticalCount++
	case "HIGH":
		r.highCount++
	case "MEDIUM":
		r.mediumCount++
	case "LOW":
		r.lowCount++
	}

	// Report through various channels
	if r.reportToConsole {
		r.logToConsole(report)
	}

	if r.reportToFile {
		r.saveToFile(report)
	}

	if r.reportToWebhook && r.webhookURL != "" {
		r.sendToWebhook(report)
	}

	// For CRITICAL issues, also trigger immediate alert
	if severity == "CRITICAL" {
		r.triggerCriticalAlert(report)
	}
}

// logToConsole logs the inconsistency to console with formatting
func (r *InconsistencyReporter) logToConsole(report InconsistencyReport) {
	// Use different symbols based on severity
	symbol := ""
	switch report.Severity {
	case "CRITICAL":
		symbol = "🚨🚨🚨"
	case "HIGH":
		symbol = "⚠️⚠️"
	case "MEDIUM":
		symbol = "⚠️"
	case "LOW":
		symbol = "ℹ️"
	}

	log.Printf("\n%s INCONSISTENCY DETECTED %s", symbol, symbol)
	log.Printf("==================================================")
	log.Printf("ID: %s", report.ID)
	log.Printf("Type: %s", report.Type)
	log.Printf("Severity: %s", report.Severity)
	log.Printf("Timestamp: %s", report.Timestamp.Format(time.RFC3339))
	log.Printf("Query: %s", report.Query)
	log.Printf("")
	log.Printf("PostgreSQL Result:")
	log.Printf("  Success: %v", report.PostgreSQLResult.Success)
	log.Printf("  Row Count: %d", report.PostgreSQLResult.RowCount)
	if report.PostgreSQLResult.Error != "" {
		log.Printf("  Error: %s", report.PostgreSQLResult.Error)
	}
	log.Printf("")
	log.Printf("YugabyteDB Result:")
	log.Printf("  Success: %v", report.YugabyteDBResult.Success)
	log.Printf("  Row Count: %d", report.YugabyteDBResult.RowCount)
	if report.YugabyteDBResult.Error != "" {
		log.Printf("  Error: %s", report.YugabyteDBResult.Error)
	}
	log.Printf("")
	log.Printf("Differences:")
	for _, diff := range report.Differences {
		log.Printf("  - %s", diff)
	}
	log.Printf("")
	log.Printf("Impact: %s", report.Impact)
	log.Printf("Recommendation: %s", report.Recommendation)
	log.Printf("==================================================")
}

// saveToFile saves the report to a file
func (r *InconsistencyReporter) saveToFile(report InconsistencyReport) {
	// Create filename with timestamp
	filename := fmt.Sprintf("%s_%s_%s.json",
		report.Timestamp.Format("20060102_150405"),
		report.Type,
		report.ID,
	)

	filepath := filepath.Join(r.reportDir, filename)

	// Marshal report to JSON
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		log.Printf("Failed to marshal report: %v", err)
		return
	}

	// Write to file
	err = os.WriteFile(filepath, data, 0644)
	if err != nil {
		log.Printf("Failed to save report to file: %v", err)
		return
	}

	log.Printf("Report saved to: %s", filepath)
}

// sendToWebhook sends the report to a configured webhook
func (r *InconsistencyReporter) sendToWebhook(report InconsistencyReport) {
	// TODO: Implement webhook sending
	// This would send to Slack, PagerDuty, or custom endpoints
	log.Printf("Webhook notification would be sent for: %s", report.ID)
}

// triggerCriticalAlert triggers immediate alerts for critical issues
func (r *InconsistencyReporter) triggerCriticalAlert(report InconsistencyReport) {
	log.Printf("\n🚨🚨🚨 CRITICAL ALERT 🚨🚨🚨")
	log.Printf("IMMEDIATE ACTION REQUIRED!")
	log.Printf("Inconsistency ID: %s", report.ID)
	log.Printf("Type: %s", report.Type)
	log.Printf("This could lead to: %s", report.Impact)
	log.Printf("Action: %s", report.Recommendation)
	log.Printf("🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨🚨")

	// Also create a critical alert file
	alertFile := filepath.Join(r.reportDir, fmt.Sprintf("CRITICAL_ALERT_%s.txt", report.ID))
	alertContent := fmt.Sprintf(`CRITICAL INCONSISTENCY ALERT
========================
ID: %s
Time: %s
Type: %s
Query: %s

DIFFERENCES:
%s

IMPACT: %s

IMMEDIATE ACTION REQUIRED: %s

This alert indicates a critical compatibility issue between PostgreSQL and YugabyteDB.
Data integrity may be at risk. Please investigate immediately.
`, report.ID, report.Timestamp, report.Type, report.Query,
		formatDifferences(report.Differences), report.Impact, report.Recommendation)

	os.WriteFile(alertFile, []byte(alertContent), 0644)
}

// GenerateSummaryReport generates a summary of all inconsistencies
func (r *InconsistencyReporter) GenerateSummaryReport() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	summary := fmt.Sprintf(`
INCONSISTENCY SUMMARY REPORT
===========================
Generated: %s
Total Inconsistencies: %d

By Severity:
- CRITICAL: %d
- HIGH: %d
- MEDIUM: %d
- LOW: %d

By Type:
`, time.Now().Format(time.RFC3339), len(r.reports),
		r.criticalCount, r.highCount, r.mediumCount, r.lowCount)

	// Count by type
	typeCounts := make(map[InconsistencyType]int)
	for _, report := range r.reports {
		typeCounts[report.Type]++
	}

	for incType, count := range typeCounts {
		summary += fmt.Sprintf("- %s: %d\n", incType, count)
	}

	// Add critical issues details
	if r.criticalCount > 0 {
		summary += "\nCRITICAL ISSUES REQUIRING IMMEDIATE ATTENTION:\n"
		for _, report := range r.reports {
			if report.Severity == "CRITICAL" {
				summary += fmt.Sprintf("\n[%s] %s\nQuery: %s\nImpact: %s\n",
					report.ID, report.Type, report.Query, report.Impact)
			}
		}
	}

	// Save summary
	summaryFile := filepath.Join(r.reportDir, fmt.Sprintf("SUMMARY_%s.txt", time.Now().Format("20060102_150405")))
	os.WriteFile(summaryFile, []byte(summary), 0644)

	return summary
}

// Helper functions

func (r *InconsistencyReporter) assessImpact(incType InconsistencyType, severity string) string {
	impacts := map[InconsistencyType]string{
		RowCountMismatch:          "Different number of rows affected - could lead to data inconsistency",
		DataValueMismatch:         "Different data values returned - application logic may fail",
		ConstraintDivergence:      "Constraint enforcement differs - data integrity at risk",
		ErrorDivergence:           "Different error handling - application error handling may fail",
		SequenceDivergence:        "Sequence values differ - primary key conflicts possible",
		TransactionDivergence:     "Transaction behavior differs - ACID properties not guaranteed",
		ConnectionErrorDivergence: "Backend connection/transport error - results unreliable, consistency cannot be assessed",
		PerformanceDegradation:    "Query performance differs significantly - potential SLO breach and user impact",
	}

	if impact, ok := impacts[incType]; ok {
		if severity == "CRITICAL" {
			return "CRITICAL: " + impact + ". Production deployment would cause data corruption!"
		}
		return impact
	}
	return "Unknown impact"
}

func (r *InconsistencyReporter) getRecommendation(incType InconsistencyType) string {
	recommendations := map[InconsistencyType]string{
		RowCountMismatch:          "Investigate query execution differences. Check for timing issues or isolation level differences.",
		DataValueMismatch:         "Compare data types and collations. Check for precision differences.",
		ConstraintDivergence:      "Review constraint definitions. File YugabyteDB compatibility bug report.",
		ErrorDivergence:           "Update error handling code to handle both error types.",
		SequenceDivergence:        "Use explicit sequence values or UUIDs instead of relying on auto-increment.",
		TransactionDivergence:     "Review transaction isolation levels. May need to adjust application logic.",
		ConnectionErrorDivergence: "Investigate backend connectivity (network and server health). Treat the transaction as failed and retry after reconnection.",
		PerformanceDegradation:    "Investigate query plans and indexes on YugabyteDB; optimize schema or queries to reduce performance gap.",
	}

	if rec, ok := recommendations[incType]; ok {
		return rec
	}
	return "Investigate the difference and adjust application code or file a compatibility report"
}

func formatDifferences(differences []string) string {
	result := ""
	for _, diff := range differences {
		result += fmt.Sprintf("- %s\n", diff)
	}
	return result
}
