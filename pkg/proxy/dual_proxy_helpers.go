package proxy

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/qvad/twinkly/pkg/protocol"
	"github.com/qvad/twinkly/pkg/reporter"
)

// BackendMessage holds a message read from a backend or an error
type BackendMessage struct {
	Msg *protocol.PGMessage
	Err error
}

// collectQueryResults collects all messages until ReadyForQuery from a channel.
// It also detects SQL ErrorResponse frames and returns a synthesized error
// that includes severity, SQLSTATE code, and message for higher-level handling.
func (s *sessionState) collectQueryResults(ctx context.Context, ch <-chan BackendMessage) ([]*protocol.PGMessage, error) {
	var results []*protocol.PGMessage
	var firstErr error

	for {
		select {
		case <-ctx.Done():
			return results, fmt.Errorf("timeout waiting for backend response")
		case bm, ok := <-ch:
			if !ok {
				return results, fmt.Errorf("backend connection closed unexpectedly")
			}
			if bm.Err != nil {
				return results, bm.Err
			}
			msg := bm.Msg

			results = append(results, msg)

			// Capture SQL errors so callers can detect failure vs success
			if msg.Type == protocol.MsgTypeErrorResponse && firstErr == nil {
				errResp, err := protocol.ParseErrorResponse(msg)
				if err != nil {
					firstErr = fmt.Errorf("failed to parse backend error: %v", err)
				} else {
					firstErr = fmt.Errorf("%s", errResp.String())
				}
			}

			// Check if this is the end of results
			if msg.Type == protocol.MsgTypeReadyForQuery {
				// Update transaction state based on ReadyForQuery status indicator
				// 'I' = Idle, 'T' = In transaction block, 'E' = In failed transaction block
				if len(msg.Data) > 0 {
					status := msg.Data[0]
					s.inTransaction = (status == 'T' || status == 'E')
				}
				return results, firstErr
			}
		}
	}
}

// extractDataRows extracts DataRow messages from a list of protocol messages
func extractDataRows(messages []*protocol.PGMessage) []*protocol.PGMessage {
	var dataRows []*protocol.PGMessage
	for _, msg := range messages {
		if msg.Type == protocol.MsgTypeDataRow {
			dataRows = append(dataRows, msg)
		}
	}
	return dataRows
}

// extractSampleData extracts a sample or checksum of the result set
func extractSampleData(messages []*protocol.PGMessage) []string {
	dataRows := extractDataRows(messages)
	count := len(dataRows)
	var sample []string

	if count < 20 {
		for i, row := range dataRows {
			// Convert raw bytes to hex string for display since we don't know the encoding/types easily here
			// without deep protocol parsing context (RowDescription is in separate message).
			// A simple hex dump is safe.
			hexData := hex.EncodeToString(row.Data)
			sample = append(sample, fmt.Sprintf("Row %d: %s", i+1, hexData))
		}
	} else {
		hasher := md5.New()
		for _, row := range dataRows {
			hasher.Write(row.Data)
		}
		checksum := hex.EncodeToString(hasher.Sum(nil))
		sample = append(sample, fmt.Sprintf("MD5 Checksum: %s (Total rows: %d)", checksum, count))
	}
	return sample
}

// GetReporter returns the inconsistency reporter for summary generation
func (p *DualExecutionProxy) GetReporter() *reporter.InconsistencyReporter {
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
