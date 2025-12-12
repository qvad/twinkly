package comparator

import (
	"log"
	"strings"
)

// ConstraintViolationType represents different types of constraint violations
type ConstraintViolationType int

const (
	UnknownConstraint ConstraintViolationType = iota
	UniqueViolation
	CheckViolation
	ForeignKeyViolation
	NotNullViolation
	PrimaryKeyViolation
)

// ConstraintViolationError represents a constraint violation with details
type ConstraintViolationError struct {
	Type           ConstraintViolationType
	ConstraintName string
	TableName      string
	ColumnName     string
	Value          string
	ErrorMessage   string
}

// DatabaseBehaviorDivergence represents when databases behave differently
type DatabaseBehaviorDivergence struct {
	Query             string
	PostgreSQLResult  *QueryExecutionResult
	YugabyteDBResult  *QueryExecutionResult
	DivergenceType    string
	CriticalityLevel  string
	RecommendedAction string
}

// QueryExecutionResult represents the result of executing a query on a database
type QueryExecutionResult struct {
	Success         bool
	Error           error
	ConstraintError *ConstraintViolationError
	AffectedRows    int64
	DatabaseName    string
}

// ConstraintDivergenceDetector detects when databases handle constraints differently
type ConstraintDivergenceDetector struct {
	detectedDivergences []DatabaseBehaviorDivergence
}

// NewConstraintDivergenceDetector creates a new detector
func NewConstraintDivergenceDetector() *ConstraintDivergenceDetector {
	return &ConstraintDivergenceDetector{
		detectedDivergences: make([]DatabaseBehaviorDivergence, 0),
	}
}

// AnalyzeConstraintError analyzes an error to determine if it's a constraint violation
func (d *ConstraintDivergenceDetector) AnalyzeConstraintError(err error) *ConstraintViolationError {
	if err == nil {
		return nil
	}

	errorMsg := strings.ToLower(err.Error())
	violation := &ConstraintViolationError{
		ErrorMessage: err.Error(),
	}

	// Analyze error patterns to identify constraint types
	switch {
	case strings.Contains(errorMsg, "unique constraint") || strings.Contains(errorMsg, "duplicate key"):
		violation.Type = UniqueViolation
		violation.ConstraintName = d.extractConstraintName(errorMsg, "unique constraint")

	case strings.Contains(errorMsg, "check constraint"):
		violation.Type = CheckViolation
		violation.ConstraintName = d.extractConstraintName(errorMsg, "check constraint")

	case strings.Contains(errorMsg, "foreign key constraint") || strings.Contains(errorMsg, "violates foreign key"):
		violation.Type = ForeignKeyViolation
		violation.ConstraintName = d.extractConstraintName(errorMsg, "foreign key")

	case strings.Contains(errorMsg, "not null constraint") || strings.Contains(errorMsg, "null value"):
		violation.Type = NotNullViolation
		violation.ColumnName = d.extractColumnName(errorMsg)

	case strings.Contains(errorMsg, "primary key") || strings.Contains(errorMsg, "duplicate key"):
		violation.Type = PrimaryKeyViolation
		violation.ConstraintName = d.extractConstraintName(errorMsg, "primary key")

	default:
		violation.Type = UnknownConstraint
	}

	// Extract table name
	violation.TableName = d.extractTableName(errorMsg)

	return violation
}

// DetectDivergence checks if two query results show divergent behavior
func (d *ConstraintDivergenceDetector) DetectDivergence(query string, pgResult, ybResult *QueryExecutionResult) *DatabaseBehaviorDivergence {
	// Case 1: One succeeded, one failed
	if pgResult.Success != ybResult.Success {
		divergence := &DatabaseBehaviorDivergence{
			Query:             query,
			PostgreSQLResult:  pgResult,
			YugabyteDBResult:  ybResult,
			DivergenceType:    "SUCCESS_FAILURE_MISMATCH",
			CriticalityLevel:  "CRITICAL",
			RecommendedAction: "REPORT_TO_YUGABYTEDB_TEAM",
		}

		// Determine which database behaved differently
		if pgResult.Success && !ybResult.Success {
			divergence.DivergenceType = "POSTGRESQL_SUCCEEDED_YUGABYTE_FAILED"
		} else {
			divergence.DivergenceType = "YUGABYTE_SUCCEEDED_POSTGRESQL_FAILED"
		}

		d.detectedDivergences = append(d.detectedDivergences, *divergence)
		return divergence
	}

	// Case 2: Both failed but with different constraint violations
	if !pgResult.Success && !ybResult.Success {
		if pgResult.ConstraintError != nil && ybResult.ConstraintError != nil {
			if pgResult.ConstraintError.Type != ybResult.ConstraintError.Type {
				divergence := &DatabaseBehaviorDivergence{
					Query:             query,
					PostgreSQLResult:  pgResult,
					YugabyteDBResult:  ybResult,
					DivergenceType:    "DIFFERENT_CONSTRAINT_VIOLATIONS",
					CriticalityLevel:  "HIGH",
					RecommendedAction: "INVESTIGATE_CONSTRAINT_DEFINITIONS",
				}

				d.detectedDivergences = append(d.detectedDivergences, *divergence)
				return divergence
			}
		}
	}

	// Case 3: Both succeeded but affected different number of rows
	if pgResult.Success && ybResult.Success {
		if pgResult.AffectedRows != ybResult.AffectedRows {
			divergence := &DatabaseBehaviorDivergence{
				Query:             query,
				PostgreSQLResult:  pgResult,
				YugabyteDBResult:  ybResult,
				DivergenceType:    "DIFFERENT_AFFECTED_ROWS",
				CriticalityLevel:  "MEDIUM",
				RecommendedAction: "CHECK_DATA_CONSISTENCY",
			}

			d.detectedDivergences = append(d.detectedDivergences, *divergence)
			return divergence
		}
	}

	return nil
}

// LogCriticalDivergence logs a critical database behavior divergence
func (d *ConstraintDivergenceDetector) LogCriticalDivergence(divergence *DatabaseBehaviorDivergence) {
	log.Printf("🚨🚨🚨 CRITICAL YUGABYTEDB COMPATIBILITY ISSUE DETECTED 🚨🚨🚨")
	log.Printf("==================================================================")
	log.Printf("Query: %s", divergence.Query)
	log.Printf("Divergence Type: %s", divergence.DivergenceType)
	log.Printf("Criticality: %s", divergence.CriticalityLevel)
	log.Printf("")

	if divergence.PostgreSQLResult != nil {
		log.Printf("PostgreSQL Result:")
		log.Printf("  Success: %v", divergence.PostgreSQLResult.Success)
		if divergence.PostgreSQLResult.Error != nil {
			log.Printf("  Error: %v", divergence.PostgreSQLResult.Error)
		}
		if divergence.PostgreSQLResult.ConstraintError != nil {
			log.Printf("  Constraint Type: %v", d.constraintTypeToString(divergence.PostgreSQLResult.ConstraintError.Type))
			log.Printf("  Constraint Name: %s", divergence.PostgreSQLResult.ConstraintError.ConstraintName)
		}
		log.Printf("  Affected Rows: %d", divergence.PostgreSQLResult.AffectedRows)
	}

	if divergence.YugabyteDBResult != nil {
		log.Printf("YugabyteDB Result:")
		log.Printf("  Success: %v", divergence.YugabyteDBResult.Success)
		if divergence.YugabyteDBResult.Error != nil {
			log.Printf("  Error: %v", divergence.YugabyteDBResult.Error)
		}
		if divergence.YugabyteDBResult.ConstraintError != nil {
			log.Printf("  Constraint Type: %v", d.constraintTypeToString(divergence.YugabyteDBResult.ConstraintError.Type))
			log.Printf("  Constraint Name: %s", divergence.YugabyteDBResult.ConstraintError.ConstraintName)
		}
		log.Printf("  Affected Rows: %d", divergence.YugabyteDBResult.AffectedRows)
	}

	log.Printf("")
	log.Printf("RECOMMENDED ACTION: %s", divergence.RecommendedAction)
	log.Printf("==================================================================")
	log.Printf("🚨 THIS INDICATES A CRITICAL DIFFERENCE IN DATABASE BEHAVIOR! 🚨")
	log.Printf("🚨 TRANSACTION WILL BE ROLLED BACK FOR DATA CONSISTENCY! 🚨")
	log.Printf("🚨 PLEASE REPORT THIS TO THE YUGABYTEDB TEAM! 🚨")
	log.Printf("==================================================================")
}

// GetDetectedDivergences returns all detected divergences
func (d *ConstraintDivergenceDetector) GetDetectedDivergences() []DatabaseBehaviorDivergence {
	return d.detectedDivergences
}

// GetCriticalDivergenceCount returns the count of critical divergences
func (d *ConstraintDivergenceDetector) GetCriticalDivergenceCount() int {
	count := 0
	for _, div := range d.detectedDivergences {
		if div.CriticalityLevel == "CRITICAL" {
			count++
		}
	}
	return count
}

// Helper methods for parsing error messages

func (d *ConstraintDivergenceDetector) extractConstraintName(errorMsg, constraintType string) string {
	// Try to extract constraint name from error message
	// This is a simplified implementation - real implementation would need
	// more sophisticated parsing for different PostgreSQL/YugabyteDB error formats

	// This is a simplified regex - in real implementation, use proper regex
	if strings.Contains(errorMsg, `"`) {
		start := strings.Index(errorMsg, `"`)
		if start != -1 {
			end := strings.Index(errorMsg[start+1:], `"`)
			if end != -1 {
				return errorMsg[start+1 : start+1+end]
			}
		}
	}

	return "unknown_constraint"
}

func (d *ConstraintDivergenceDetector) extractTableName(errorMsg string) string {
	// Extract table name from error message
	// Simplified implementation
	if strings.Contains(errorMsg, "table") {
		words := strings.Fields(errorMsg)
		for i, word := range words {
			if strings.ToLower(word) == "table" && i+1 < len(words) {
				return strings.Trim(words[i+1], `"'`)
			}
		}
	}
	return "unknown_table"
}

func (d *ConstraintDivergenceDetector) extractColumnName(errorMsg string) string {
	// Extract column name from error message
	// Simplified implementation
	if strings.Contains(errorMsg, "column") {
		words := strings.Fields(errorMsg)
		for i, word := range words {
			if strings.ToLower(word) == "column" && i+1 < len(words) {
				return strings.Trim(words[i+1], `"'`)
			}
		}
	}
	return "unknown_column"
}

func (d *ConstraintDivergenceDetector) constraintTypeToString(cType ConstraintViolationType) string {
	switch cType {
	case UniqueViolation:
		return "UNIQUE_VIOLATION"
	case CheckViolation:
		return "CHECK_VIOLATION"
	case ForeignKeyViolation:
		return "FOREIGN_KEY_VIOLATION"
	case NotNullViolation:
		return "NOT_NULL_VIOLATION"
	case PrimaryKeyViolation:
		return "PRIMARY_KEY_VIOLATION"
	default:
		return "UNKNOWN_CONSTRAINT"
	}
}
