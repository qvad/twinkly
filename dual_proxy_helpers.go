package main

import (
	"fmt"
	"strings"
)

// collectQueryResults collects all messages until ReadyForQuery.
// It also detects SQL ErrorResponse frames and returns a synthesized error
// that includes severity, SQLSTATE code, and message for higher-level handling.
func (p *DualExecutionProxy) collectQueryResults(reader *PGProtocolReader) ([]*PGMessage, error) {
	var results []*PGMessage
	var firstErr error

	for {
		msg, err := reader.ReadMessage()
		if err != nil {
			return nil, err
		}

		results = append(results, msg)

		// Capture SQL errors so callers can detect failure vs success
		if msg.Type == msgTypeErrorResponse && firstErr == nil {
			sev, code, text := parseBackendError(msg.Data)
			// Build a readable error; include fields when available
			parts := make([]string, 0, 3)
			if sev != "" {
				parts = append(parts, sev)
			}
			if code != "" {
				parts = append(parts, code)
			}
			if text != "" {
				parts = append(parts, text)
			}
			if len(parts) == 0 {
				firstErr = fmt.Errorf("backend returned ErrorResponse")
			} else {
				firstErr = fmt.Errorf(strings.Join(parts, " "))
			}
		}

		// Check if this is the end of results
		if msg.Type == msgTypeReadyForQuery {
			break
		}
	}

	return results, firstErr
}

// extractDataRows extracts DataRow messages from a list of protocol messages
func extractDataRows(messages []*PGMessage) []*PGMessage {
	var dataRows []*PGMessage
	for _, msg := range messages {
		if msg.Type == msgTypeDataRow {
			dataRows = append(dataRows, msg)
		}
	}
	return dataRows
}

// GetReporter returns the inconsistency reporter for summary generation
func (p *DualExecutionProxy) GetReporter() *InconsistencyReporter {
	return p.reporter
}

// isTransportError returns true if the error indicates a connection/transport failure rather than a SQL error.
// isSyntaxError returns true if error indicates a SQL syntax error (SQLSTATE 42601 or contains 'syntax error').
func isSyntaxError(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	if strings.Contains(s, "42601") {
		return true
	}
	if strings.Contains(s, "syntax error") {
		return true
	}
	return false
}

func isTransportError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	// Common transport-related substrings observed in logs
	transportHints := []string{
		"connection reset by peer",
		"broken pipe",
		"write: broken pipe",
		"read: connection reset",
		"read: eof",
		"unexpected eof",
		"eof",
		"i/o timeout",
		"timeout exceeded",
		"deadline exceeded",
		"connection refused",
		"network is unreachable",
		"no route to host",
	}
	for _, hint := range transportHints {
		if strings.Contains(msg, hint) {
			return true
		}
	}
	return false
}
