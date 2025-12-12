package proxy

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/qvad/twinkly/pkg/config"
)

// ErrorHandler manages error mapping and retry logic
type ErrorHandler struct {
	config *config.Config
}

// NewErrorHandler creates a new error handler
func NewErrorHandler(config *config.Config) *ErrorHandler {
	return &ErrorHandler{config: config}
}

// PostgreSQLError represents a PostgreSQL error with SQLSTATE
type PostgreSQLError struct {
	Code     string
	Message  string
	Detail   string
	Hint     string
	Position string
}

// ParsePostgreSQLError extracts error details from a PostgreSQL error
func ParsePostgreSQLError(err error) (*PostgreSQLError, bool) {
	// This is a simplified version - real implementation would parse the error protocol message
	errStr := err.Error()

	// Look for SQLSTATE in error message
	if strings.Contains(errStr, "SQLSTATE") {
		// Extract SQLSTATE code (simplified)
		codeStart := strings.Index(errStr, "SQLSTATE") + 9
		if codeStart+5 <= len(errStr) {
			code := errStr[codeStart : codeStart+5]
			return &PostgreSQLError{
				Code:    code,
				Message: errStr,
			}, true
		}
	}

	return nil, false
}

// HandleError processes an error according to configuration
func (eh *ErrorHandler) HandleError(err error, fromDB string, operation func() error) error {
	pgErr, isPgErr := ParsePostgreSQLError(err)
	if !isPgErr {
		// Not a PostgreSQL error, pass through
		return err
	}

	// Map the error
	mappedCode, action, found := eh.config.MapError(pgErr.Code, fromDB)
	if !found {
		// No mapping found, pass through
		return err
	}

	switch action {
	case "retry":
		return eh.handleRetry(pgErr, operation)
	case "transform":
		return eh.handleTransform(pgErr, mappedCode)
	case "ignore":
		return nil
	case "pass-through":
		return err
	default:
		return err
	}
}

// handleRetry implements retry logic for errors
func (eh *ErrorHandler) handleRetry(pgErr *PostgreSQLError, operation func() error) error {
	mapping := eh.findErrorMapping(pgErr.Code)
	if mapping == nil {
		return fmt.Errorf("SQLSTATE %s: %s", pgErr.Code, pgErr.Message)
	}

	maxRetries := mapping.MaxRetries
	if maxRetries <= 0 {
		maxRetries = 3 // default
	}

	retryDelay := mapping.RetryDelay
	if retryDelay == 0 {
		retryDelay = 100 * time.Millisecond // default
	}

	var lastErr error
	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			if eh.config.Monitoring.LogErrors {
				log.Printf("Retrying operation after %s error (attempt %d/%d)", pgErr.Code, i+1, maxRetries)
			}
			time.Sleep(retryDelay)
		}

		lastErr = operation()
		if lastErr == nil {
			// Success
			return nil
		}

		// Check if new error is also retryable
		newPgErr, isPgErr := ParsePostgreSQLError(lastErr)
		if !isPgErr || newPgErr.Code != pgErr.Code {
			// Different error, don't retry
			return lastErr
		}
	}

	return fmt.Errorf("operation failed after %d retries: %w", maxRetries, lastErr)
}

// handleTransform transforms an error according to configuration
func (eh *ErrorHandler) handleTransform(pgErr *PostgreSQLError, mappedCode string) error {
	mapping := eh.findErrorMapping(pgErr.Code)
	if mapping == nil {
		return fmt.Errorf("SQLSTATE %s: %s", pgErr.Code, pgErr.Message)
	}

	message := mapping.TransformMessage
	if message == "" {
		message = pgErr.Message
	}

	// Create transformed error
	return fmt.Errorf("SQLSTATE %s: %s", mappedCode, message)
}

// findErrorMapping finds the error mapping for a given code
func (eh *ErrorHandler) findErrorMapping(errorCode string) *config.ErrorMapping {
	// Check all categories
	categories := []map[string]config.ErrorMapping{
		eh.config.ErrorMappings.TransactionErrors,
		eh.config.ErrorMappings.ConstraintErrors,
		eh.config.ErrorMappings.FeatureErrors,
		eh.config.ErrorMappings.ConnectionErrors,
		eh.config.ErrorMappings.DataErrors,
	}

	for _, category := range categories {
		for _, mapping := range category {
			for _, code := range mapping.PostgreSQLCodes {
				if code == errorCode {
					return &mapping
				}
			}
			for _, code := range mapping.YugabyteCodes {
				if code == errorCode {
					return &mapping
				}
			}
		}
	}

	return nil
}

// TransactionManager handles transaction retry logic
type TransactionManager struct {
	errorHandler *ErrorHandler
	db           *sql.DB
}

// NewTransactionManager creates a new transaction manager
func NewTransactionManager(errorHandler *ErrorHandler, db *sql.DB) *TransactionManager {
	return &TransactionManager{
		errorHandler: errorHandler,
		db:           db,
	}
}

// ExecuteInTransaction executes a function within a transaction with retry logic
func (tm *TransactionManager) ExecuteInTransaction(ctx context.Context, fn func(*sql.Tx) error) error {
	operation := func() error {
		tx, err := tm.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}

		defer func() {
			if p := recover(); p != nil {
				tx.Rollback()
				panic(p)
			}
		}()

		err = fn(tx)
		if err != nil {
			tx.Rollback()
			return err
		}

		return tx.Commit()
	}

	err := operation()
	if err != nil {
		// Handle error with retry logic if applicable
		return tm.errorHandler.HandleError(err, "postgresql", operation)
	}

	return nil
}

// Example usage of error handling in practice
func ExampleErrorHandling(config *config.Config) {
	// Create error handler
	errorHandler := NewErrorHandler(config)

	// Database connection
	db, err := sql.Open("postgres", "host=localhost port=5432 user=postgres dbname=test sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Transaction manager with retry logic
	txManager := NewTransactionManager(errorHandler, db)

	// Execute a transaction that might encounter deadlocks
	err = txManager.ExecuteInTransaction(context.Background(), func(tx *sql.Tx) error {
		// Simulate operations that might deadlock
		_, err := tx.Exec("UPDATE accounts SET balance = balance - 100 WHERE id = 1")
		if err != nil {
			return err
		}

		_, err = tx.Exec("UPDATE accounts SET balance = balance + 100 WHERE id = 2")
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		log.Printf("Transaction failed: %v", err)
	} else {
		log.Println("Transaction completed successfully")
	}
}
