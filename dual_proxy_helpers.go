package main

import "strings"

// collectQueryResults collects all messages until ReadyForQuery
func (p *DualExecutionProxy) collectQueryResults(reader *PGProtocolReader) ([]*PGMessage, error) {
	var results []*PGMessage

	for {
		msg, err := reader.ReadMessage()
		if err != nil {
			return nil, err
		}

		results = append(results, msg)

		// Check if this is the end of results
		if msg.Type == msgTypeReadyForQuery {
			break
		}
	}

	return results, nil
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
