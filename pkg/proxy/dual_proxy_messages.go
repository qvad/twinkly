package proxy

import (
	"context"
	"log"
	"time"

	"github.com/qvad/twinkly/pkg/protocol"
	"github.com/qvad/twinkly/pkg/reporter"
)

// proxyMessage forwards a non-query message to a specific backend using PostgreSQL Extended Query Protocol.
//
// Extended Query Protocol Flow:
//
//	Parse -> Bind -> Execute -> Sync (repeat as needed)
//
// Key deadlock prevention strategy:
//   - Most messages (Parse, Bind, Execute, etc.) are sent without reading responses
//   - Only Sync triggers response draining to avoid deadlocks
//   - PostgreSQL servers buffer responses until Sync, so premature reads can block
//
// Parameters:
//   - msg: The protocol message to forward
//   - clientWriter: Writer to send responses back to the client
//   - dbWriter: Writer to send the message to the database
//   - dbCh: Channel receiving asynchronous responses from the database
//   - dbName: Database name for logging purposes
func (p *DualExecutionProxy) proxyMessage(msg *protocol.PGMessage, clientWriter *protocol.PGProtocolWriter,
	dbWriter *protocol.PGProtocolWriter, dbCh <-chan BackendMessage, dbName string) {
	// Optional protocol/query logging for extended protocol
	if p.config != nil && (p.config.Debug.LogAllQueries || p.config.Debug.LogProtocol) {
		switch msg.Type {
		case protocol.MsgTypeParse:
			if sql, err := protocol.ExtractParseQuery(msg); err == nil && sql != "" {
				p.lastParsedSQL = sql
				log.Printf("QUERY (Parse)->%s: %s", dbName, sql)
			} else {
				log.Printf("Protocol->%s: Parse", dbName)
			}
		case protocol.MsgTypeBind:
			log.Printf("Protocol->%s: Bind", dbName)
		case protocol.MsgTypeExecute:
			log.Printf("Protocol->%s: Execute", dbName)
		case protocol.MsgTypeDescribe:
			log.Printf("Protocol->%s: Describe", dbName)
		case protocol.MsgTypeClose:
			log.Printf("Protocol->%s: Close", dbName)
		case protocol.MsgTypeSync:
			log.Printf("Protocol->%s: Sync", dbName)
		case protocol.MsgTypeFlush:
			log.Printf("Protocol->%s: Flush", dbName)
		default:
			log.Printf("Protocol->%s: message type %c (%d)", dbName, msg.Type, msg.Type)
		}
	}
	// Always forward the message to the selected backend first
	if err := dbWriter.WriteMessage(msg); err != nil {
		log.Printf("Failed to send message to %s: %v", dbName, err)
		return
	}

	switch msg.Type {
	case protocol.MsgTypeSync:
		// Sync Message Handling (Critical for Extended Query Protocol):
		//
		// After receiving Sync, PostgreSQL flushes all buffered responses from previous
		// Parse/Bind/Execute messages and sends ReadyForQuery to signal completion.
		//
		// We must drain ALL responses until ReadyForQuery to:
		//   1. Prevent response buffer overflow in the backend
		//   2. Allow the client to know when the extended query sequence is complete
		//   3. Maintain proper protocol state synchronization
		//
		// Drain and forward everything until protocol.ReadyForQuery so the client can progress.
		timeout := time.After(30 * time.Second)
		for {
			select {
			case <-timeout:
				log.Printf("Timeout waiting for response after Sync from %s", dbName)
				return
			case bm, ok := <-dbCh:
				if !ok {
					log.Printf("Failed to read response after Sync from %s: connection closed", dbName)
					// Log completion status for extended protocol visibility
					if p.config != nil && (p.config.Debug.LogAllQueries || p.config.Debug.LogProtocol) {
						log.Printf("%s - error", dbName)
					}
					return
				}
				if bm.Err != nil {
					log.Printf("Failed to read response after Sync from %s: %v", dbName, bm.Err)
					if p.config != nil && (p.config.Debug.LogAllQueries || p.config.Debug.LogProtocol) {
						log.Printf("%s - error", dbName)
					}
					return
				}
				response := bm.Msg

				if err := clientWriter.WriteMessage(response); err != nil {
					log.Printf("Failed to send response to client: %v", err)
					return
				}
				if response.Type == protocol.MsgTypeReadyForQuery {
					// Successful completion of this extended-protocol unit
					if p.config != nil && (p.config.Debug.LogAllQueries || p.config.Debug.LogProtocol) {
						log.Printf("%s - ok", dbName)
					}
					return
				}
			}
		}
	case protocol.MsgTypeFlush:
		// Flush Message Handling:
		//
		// Flush requests the server to send any pending responses immediately,
		// but does NOT indicate end of extended query sequence (unlike Sync).
		//
		// We deliberately avoid blocking reads here because:
		//   1. Server may or may not have responses ready
		//   2. Responses will be drained during the next Sync operation
		//   3. Blocking here can cause deadlocks if server hasn't prepared responses yet
		return
	default:
		// Extended Query Protocol Non-Sync Messages:
		//
		// For Parse/Bind/Execute/Describe/Close messages, we send them to the backend
		// but do NOT attempt to read responses immediately. This is crucial because:
		//
		//   1. PostgreSQL buffers responses until Sync (optimization)
		//   2. Reading prematurely can cause deadlocks when server isn't ready
		//   3. Some servers require the complete message sequence before responding
		//   4. Response collection happens only during Sync processing
		//
		// This pattern maintains protocol compliance and prevents proxy deadlocks.
		return
	}
}

// proxyExtendedDualMessage implements dual-execution for Extended Query Protocol messages.
//
// Dual Execution Strategy:
//  1. Forward the message to both PostgreSQL and YugabyteDB
//  2. Only drain responses after Sync (following Extended Query Protocol rules)
//  3. Collect and compare results from both backends
//  4. Report any discrepancies (errors, row counts, data differences)
//  5. Forward results from the configured "source of truth" database
//
// This function handles the complex logic of:
//   - Concurrent response collection from both databases
//   - Error divergence detection and reporting
//   - Result validation and comparison
//   - Proper Extended Query Protocol state management
//
// The dual execution provides real-time compatibility testing between PostgreSQL
// and YugabyteDB while maintaining transparent proxy behavior for the client.
func (p *DualExecutionProxy) proxyExtendedDualMessage(msg *protocol.PGMessage, clientWriter *protocol.PGProtocolWriter,
	pgWriter *protocol.PGProtocolWriter, pgCh <-chan BackendMessage,
	ybWriter *protocol.PGProtocolWriter, ybCh <-chan BackendMessage) {
	// Log intent per backend
	if p.config != nil && (p.config.Debug.LogAllQueries || p.config.Debug.LogProtocol) {
		switch msg.Type {
		case protocol.MsgTypeParse:
			if sql, err := protocol.ExtractParseQuery(msg); err == nil && sql != "" {
				p.lastParsedSQL = sql
				log.Printf("QUERY (Parse)->PostgreSQL: %s", sql)
				log.Printf("QUERY (Parse)->YugabyteDB: %s", sql)
			} else {
				log.Printf("Protocol->PostgreSQL: Parse")
				log.Printf("Protocol->YugabyteDB: Parse")
			}
		case protocol.MsgTypeBind:
			log.Printf("Protocol->PostgreSQL: Bind")
			log.Printf("Protocol->YugabyteDB: Bind")
		case protocol.MsgTypeExecute:
			log.Printf("Protocol->PostgreSQL: Execute")
			log.Printf("Protocol->YugabyteDB: Execute")
		case protocol.MsgTypeDescribe:
			log.Printf("Protocol->PostgreSQL: Describe")
			log.Printf("Protocol->YugabyteDB: Describe")
		case protocol.MsgTypeClose:
			log.Printf("Protocol->PostgreSQL: Close")
			log.Printf("Protocol->YugabyteDB: Close")
		case protocol.MsgTypeSync:
			log.Printf("Protocol->PostgreSQL: Sync")
			log.Printf("Protocol->YugabyteDB: Sync")
		case protocol.MsgTypeFlush:
			log.Printf("Protocol->PostgreSQL: Flush")
			log.Printf("Protocol->YugabyteDB: Flush")
		default:
			log.Printf("Protocol->PostgreSQL: message type %c (%d)", msg.Type, msg.Type)
			log.Printf("Protocol->YugabyteDB: message type %c (%d)", msg.Type, msg.Type)
		}
	}

	// Forward to both backends
	if err := pgWriter.WriteMessage(msg); err != nil {
		log.Printf("Failed to send message to PostgreSQL: %v", err)
	}
	if err := ybWriter.WriteMessage(msg); err != nil {
		log.Printf("Failed to send message to YugabyteDB: %v", err)
	}

	// Only read after Sync
	if msg.Type != protocol.MsgTypeSync {
		return
	}

	// Drain and collect both sides concurrently
	var pgResults, ybResults []*protocol.PGMessage
	var pgErr, ybErr error
	done := make(chan struct{}, 2)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	go func() { pgResults, pgErr = p.collectQueryResults(ctx, pgCh); done <- struct{}{} }()
	go func() { ybResults, ybErr = p.collectQueryResults(ctx, ybCh); done <- struct{}{} }()
	<-done
	<-done

	// Outcome logs
	if p.config != nil && (p.config.Debug.LogAllQueries || p.config.Debug.LogProtocol) {
		if pgErr != nil {
			log.Printf("PostgreSQL - error: %v", pgErr)
		} else {
			log.Printf("PostgreSQL - ok")
		}
		if ybErr != nil {
			log.Printf("YugabyteDB - error: %v", ybErr)
		} else {
			log.Printf("YugabyteDB - ok")
		}
	}

	// Handle one-sided failures (divergence)
	if pgErr != nil || ybErr != nil {
		if (pgErr == nil) != (ybErr == nil) {
			// Optional suppression for catalog queries
			ignoreCatalog := p.config.Comparison.IgnoreCatalogDifferences && p.config.IsCatalogQuery(p.lastParsedSQL)
			// Check for syntax error divergence and apply special policy
			isSyntaxDiv := (pgErr != nil && isSyntaxError(pgErr) && ybErr == nil) || (ybErr != nil && isSyntaxError(ybErr) && pgErr == nil)
			if isSyntaxDiv {
				// Optionally fail-and-rollback (no report)
				if p.config.Comparison.SyntaxErrorDivergenceFailAndRollback {
					// Send a clear syntax error to client and mark TX state accordingly
					var errText string
					if pgErr != nil {
						errText = pgErr.Error()
					} else {
						errText = ybErr.Error()
					}
					errMsg := protocol.CreateErrorMessage("ERROR", "42601", "Syntax error detected on one backend; aborting transaction to avoid undefined behavior: "+errText)
					_ = clientWriter.WriteMessage(errMsg)
					status := byte('I')
					if p.inTransaction {
						status = 'E'
					}
					_ = clientWriter.WriteMessage(&protocol.PGMessage{Type: protocol.MsgTypeReadyForQuery, Data: []byte{status}})
					// Do not report unless explicitly enabled
					if p.config.Comparison.SyntaxErrorDivergenceReport {
						pgSummary := reporter.ResultSummary{Success: pgErr == nil}
						if pgErr != nil {
							pgSummary.Error = pgErr.Error()
						}
						ybSummary := reporter.ResultSummary{Success: ybErr == nil}
						if ybErr != nil {
							ybSummary.Error = ybErr.Error()
						}
						diffs := []string{"One database succeeded while the other failed (syntax error)"}
						p.reporter.ReportInconsistency(reporter.ErrorDivergence, "LOW", "(extended protocol unit)", pgSummary, ybSummary, diffs)
					}
					return
				}
				// If not failing transaction, optionally report per config
				if p.config.Comparison.SyntaxErrorDivergenceReport {
					pgSummary := reporter.ResultSummary{
						Success:    pgErr == nil,
						SampleData: extractSampleData(pgResults),
					}
					if pgErr != nil {
						pgSummary.Error = pgErr.Error()
					}
					ybSummary := reporter.ResultSummary{
						Success:    ybErr == nil,
						SampleData: extractSampleData(ybResults),
					}
					if ybErr != nil {
						ybSummary.Error = ybErr.Error()
					}
					diffs := []string{"One database succeeded while the other failed (syntax error)"}
					p.reporter.ReportInconsistency(reporter.ErrorDivergence, "LOW", "(extended protocol unit)", pgSummary, ybSummary, diffs)
				}
				// Continue to forward from source-of-truth (no failure) if not failing
			} else {
				// Non-syntax divergence: optionally report as CRITICAL
				if !ignoreCatalog {
					pgSummary := reporter.ResultSummary{
						Success:    pgErr == nil,
						SampleData: extractSampleData(pgResults),
					}
					if pgErr != nil {
						pgSummary.Error = pgErr.Error()
					}
					ybSummary := reporter.ResultSummary{
						Success:    ybErr == nil,
						SampleData: extractSampleData(ybResults),
					}
					if ybErr != nil {
						ybSummary.Error = ybErr.Error()
					}
					diffs := []string{"One database succeeded while the other failed"}
					p.reporter.ReportInconsistency(reporter.ErrorDivergence, "CRITICAL", "(extended protocol unit)", pgSummary, ybSummary, diffs)
				}
			}
		}
	}

	// Compare results when both succeeded
	if pgErr == nil && ybErr == nil && len(pgResults) > 0 && len(ybResults) > 0 {
		pgData := extractDataRows(pgResults)
		ybData := extractDataRows(ybResults)
		if p.config != nil && (p.config.Comparison.LogComparisons || (p.config.Comparison.LogDifferencesOnly && len(pgData) != len(ybData))) {
			log.Printf("Query result comparison (extended) - PostgreSQL: %d rows, YugabyteDB: %d rows", len(pgData), len(ybData))
		}
		if len(pgData) != len(ybData) {
			pgSummary := reporter.ResultSummary{
				Success:    true,
				RowCount:   len(pgData),
				SampleData: extractSampleData(pgResults),
			}
			ybSummary := reporter.ResultSummary{
				Success:    true,
				RowCount:   len(ybData),
				SampleData: extractSampleData(ybResults),
			}
			diffs := []string{
				"Row count differs",
			}
			severity := "HIGH"
			if p.config.Comparison.FailOnDifferences {
				severity = "CRITICAL"
			}
			p.reporter.ReportInconsistency(reporter.RowCountMismatch, severity, "(extended protocol unit)", pgSummary, ybSummary, diffs)
		}
		// Optional deep validation
		if p.resultValidator != nil {
			validation, err := p.resultValidator.ValidateResults(pgResults, ybResults)
			if err == nil && validation.ShouldFail {
				severity := "HIGH"
				if p.config.Comparison.FailOnDifferences {
					severity = "CRITICAL"
				}
				pgDataRows := extractDataRows(pgResults)
				ybDataRows := extractDataRows(ybResults)
				p.reporter.ReportInconsistency(
					reporter.DataValueMismatch,
					severity,
					"(extended protocol unit)",
					reporter.ResultSummary{
						Success:    true,
						RowCount:   len(pgDataRows),
						SampleData: extractSampleData(pgResults),
					},
					reporter.ResultSummary{
						Success:    true,
						RowCount:   len(ybDataRows),
						SampleData: extractSampleData(ybResults),
					},
					validation.Differences,
				)
			}
		}
	}

	// Forward results from source of truth
	var resultsToForward []*protocol.PGMessage
	if p.config.Comparison.SourceOfTruth == "yugabytedb" {
		resultsToForward = ybResults
	} else {
		resultsToForward = pgResults
	}
	for _, m := range resultsToForward {
		if err := clientWriter.WriteMessage(m); err != nil {
			log.Printf("Failed to forward result to client: %v", err)
			return
		}
	}
}
