package main

import (
	"fmt"
	"log"
	"reflect"
	"strings"
)

// ResultValidator validates and compares query results between databases
type ResultValidator struct {
	FailOnDifferences bool
}

// ValidationResult contains the result of comparing two database results
type ValidationResult struct {
	AreEqual      bool
	Differences   []string
	ShouldFail    bool
	ErrorMessage  string
}

// NewResultValidator creates a new result validator
func NewResultValidator(failOnDifferences bool) *ResultValidator {
	return &ResultValidator{
		FailOnDifferences: failOnDifferences,
	}
}

// ValidateResults compares results from PostgreSQL and YugabyteDB
func (rv *ResultValidator) ValidateResults(pgResults, ybResults []*PGMessage) (*ValidationResult, error) {
	result := &ValidationResult{
		AreEqual:   true,
		ShouldFail: false,
	}
	
	// Extract data rows from both results
	pgDataRows := extractDataRows(pgResults)
	ybDataRows := extractDataRows(ybResults)
	
	// Compare row counts
	if len(pgDataRows) != len(ybDataRows) {
		result.AreEqual = false
		diff := fmt.Sprintf("Row count mismatch: PostgreSQL returned %d rows, YugabyteDB returned %d rows", 
			len(pgDataRows), len(ybDataRows))
		result.Differences = append(result.Differences, diff)
		
		log.Printf("❌ RESULT DIFFERENCE: %s", diff)
	}
	
	// Compare individual rows (up to the minimum count)
	minRows := len(pgDataRows)
	if len(ybDataRows) < minRows {
		minRows = len(ybDataRows)
	}
	
	for i := 0; i < minRows; i++ {
		if !rv.compareDataRows(pgDataRows[i], ybDataRows[i]) {
			result.AreEqual = false
			diff := fmt.Sprintf("Row %d differs between databases", i+1)
			result.Differences = append(result.Differences, diff)
			
			log.Printf("❌ RESULT DIFFERENCE: %s", diff)
		}
	}
	
	// Check if we should fail on differences
	if !result.AreEqual && rv.FailOnDifferences {
		result.ShouldFail = true
		result.ErrorMessage = fmt.Sprintf("Query results differ between PostgreSQL and YugabyteDB:\n%s", 
			strings.Join(result.Differences, "\n"))
	}
	
	return result, nil
}

// compareDataRows compares two data row messages
func (rv *ResultValidator) compareDataRows(pgRow, ybRow *PGMessage) bool {
	if pgRow == nil && ybRow == nil {
		return true
	}
	if pgRow == nil || ybRow == nil {
		return false
	}
	
	// Compare the raw data
	return reflect.DeepEqual(pgRow.Data, ybRow.Data)
}

// CreateFailureError creates an SQL error for result differences
func (rv *ResultValidator) CreateFailureError(differences []string) error {
	errorMsg := "Query results differ between PostgreSQL and YugabyteDB:\n"
	for _, diff := range differences {
		errorMsg += fmt.Sprintf("  • %s\n", diff)
	}
	errorMsg += "\nThis indicates a compatibility issue that must be resolved before migration."
	
	return fmt.Errorf(errorMsg)
}