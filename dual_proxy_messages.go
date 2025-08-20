package main

import "log"

// proxyMessage forwards a non-query message to a specific backend (dbName).
// For extended protocol, avoid blocking reads until Sync to prevent deadlocks
// where the server waits for Sync before replying.
func (p *DualExecutionProxy) proxyMessage(msg *PGMessage, clientWriter *PGProtocolWriter,
	dbWriter *PGProtocolWriter, dbReader *PGProtocolReader, dbName string) {
	// Optional protocol/query logging for extended protocol
	if p.config != nil && (p.config.Debug.LogAllQueries || p.config.Debug.LogProtocol) {
		switch msg.Type {
		case msgTypeParse:
			if sql, err := ExtractParseQuery(msg); err == nil && sql != "" {
				p.lastParsedSQL = sql
				log.Printf("QUERY (Parse)->%s: %s", dbName, sql)
			} else {
				log.Printf("Protocol->%s: Parse", dbName)
			}
		case msgTypeBind:
			log.Printf("Protocol->%s: Bind", dbName)
		case msgTypeExecute:
			log.Printf("Protocol->%s: Execute", dbName)
		case msgTypeDescribe:
			log.Printf("Protocol->%s: Describe", dbName)
		case msgTypeClose:
			log.Printf("Protocol->%s: Close", dbName)
		case msgTypeSync:
			log.Printf("Protocol->%s: Sync", dbName)
		case msgTypeFlush:
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
	case msgTypeSync:
		// After Sync, the backend will flush all outstanding responses and end with ReadyForQuery.
		// Drain and forward everything until ReadyForQuery so the client can progress.
		for {
			response, err := dbReader.ReadMessage()
			if err != nil {
				log.Printf("Failed to read response after Sync from %s: %v", dbName, err)
				// Log completion status for extended protocol visibility
				if p.config != nil && (p.config.Debug.LogAllQueries || p.config.Debug.LogProtocol) {
					log.Printf("%s - error", dbName)
				}
				return
			}
			if err := clientWriter.WriteMessage(response); err != nil {
				log.Printf("Failed to send response to client: %v", err)
				return
			}
			if response.Type == msgTypeReadyForQuery {
				// Successful completion of this extended-protocol unit
				if p.config != nil && (p.config.Debug.LogAllQueries || p.config.Debug.LogProtocol) {
					log.Printf("%s - ok", dbName)
				}
				break
			}
		}
	case msgTypeFlush:
		// For Flush, do not block here. Server may send pending responses,
		// which will be drained on subsequent Sync. Avoid reading here to prevent stalls.
		return
	default:
		// For Parse/Bind/Execute/Describe/Close/etc., do not attempt to read now.
		// Many servers may not respond until Sync; reading here can deadlock the proxy.
		return
	}
}

// proxyExtendedDualMessage mirrors an extended-protocol message to both backends.
// It only performs blocking reads after Sync, draining both sides to ReadyForQuery,
// comparing results, and forwarding from the configured source of truth.
func (p *DualExecutionProxy) proxyExtendedDualMessage(msg *PGMessage, clientWriter *PGProtocolWriter,
	pgWriter *PGProtocolWriter, pgReader *PGProtocolReader,
	ybWriter *PGProtocolWriter, ybReader *PGProtocolReader) {
	// Log intent per backend
	if p.config != nil && (p.config.Debug.LogAllQueries || p.config.Debug.LogProtocol) {
		switch msg.Type {
		case msgTypeParse:
			if sql, err := ExtractParseQuery(msg); err == nil && sql != "" {
				p.lastParsedSQL = sql
				log.Printf("QUERY (Parse)->PostgreSQL: %s", sql)
				log.Printf("QUERY (Parse)->YugabyteDB: %s", sql)
			} else {
				log.Printf("Protocol->PostgreSQL: Parse")
				log.Printf("Protocol->YugabyteDB: Parse")
			}
		case msgTypeBind:
			log.Printf("Protocol->PostgreSQL: Bind")
			log.Printf("Protocol->YugabyteDB: Bind")
		case msgTypeExecute:
			log.Printf("Protocol->PostgreSQL: Execute")
			log.Printf("Protocol->YugabyteDB: Execute")
		case msgTypeDescribe:
			log.Printf("Protocol->PostgreSQL: Describe")
			log.Printf("Protocol->YugabyteDB: Describe")
		case msgTypeClose:
			log.Printf("Protocol->PostgreSQL: Close")
			log.Printf("Protocol->YugabyteDB: Close")
		case msgTypeSync:
			log.Printf("Protocol->PostgreSQL: Sync")
			log.Printf("Protocol->YugabyteDB: Sync")
		case msgTypeFlush:
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
	if msg.Type != msgTypeSync {
		return
	}

	// Drain and collect both sides concurrently
	var pgResults, ybResults []*PGMessage
	var pgErr, ybErr error
	done := make(chan struct{}, 2)
	go func() { pgResults, pgErr = p.collectQueryResults(pgReader); done <- struct{}{} }()
	go func() { ybResults, ybErr = p.collectQueryResults(ybReader); done <- struct{}{} }()
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
					errMsg := CreateErrorMessage("ERROR", "42601", "Syntax error detected on one backend; aborting transaction to avoid undefined behavior: "+errText)
					_ = clientWriter.WriteMessage(errMsg)
					status := byte('I')
					if p.inTransaction {
						status = 'E'
					}
					_ = clientWriter.WriteMessage(&PGMessage{Type: msgTypeReadyForQuery, Data: []byte{status}})
					// Do not report unless explicitly enabled
					if p.config.Comparison.SyntaxErrorDivergenceReport {
						pgSummary := ResultSummary{Success: pgErr == nil}
						if pgErr != nil {
							pgSummary.Error = pgErr.Error()
						}
						ybSummary := ResultSummary{Success: ybErr == nil}
						if ybErr != nil {
							ybSummary.Error = ybErr.Error()
						}
						diffs := []string{"One database succeeded while the other failed (syntax error)"}
						p.reporter.ReportInconsistency(ErrorDivergence, "LOW", "(extended protocol unit)", pgSummary, ybSummary, diffs)
					}
					return
				}
				// If not failing transaction, optionally report per config
				if p.config.Comparison.SyntaxErrorDivergenceReport {
					pgSummary := ResultSummary{Success: pgErr == nil}
					if pgErr != nil {
						pgSummary.Error = pgErr.Error()
					}
					ybSummary := ResultSummary{Success: ybErr == nil}
					if ybErr != nil {
						ybSummary.Error = ybErr.Error()
					}
					diffs := []string{"One database succeeded while the other failed (syntax error)"}
					p.reporter.ReportInconsistency(ErrorDivergence, "LOW", "(extended protocol unit)", pgSummary, ybSummary, diffs)
				}
				// Continue to forward from source-of-truth (no failure) if not failing
			} else {
				// Non-syntax divergence: optionally report as CRITICAL
				if !ignoreCatalog {
					pgSummary := ResultSummary{Success: pgErr == nil}
					if pgErr != nil {
						pgSummary.Error = pgErr.Error()
					}
					ybSummary := ResultSummary{Success: ybErr == nil}
					if ybErr != nil {
						ybSummary.Error = ybErr.Error()
					}
					diffs := []string{"One database succeeded while the other failed"}
					p.reporter.ReportInconsistency(ErrorDivergence, "CRITICAL", "(extended protocol unit)", pgSummary, ybSummary, diffs)
				}
			}
		}
	}

	// Compare results when both succeeded
	if pgErr == nil && ybErr == nil && len(pgResults) > 0 && len(ybResults) > 0 {
		pgData := extractDataRows(pgResults)
		ybData := extractDataRows(ybResults)
		if p.config.Comparison.LogComparisons || (p.config.Comparison.LogDifferencesOnly && len(pgData) != len(ybData)) {
			log.Printf("Query result comparison (extended) - PostgreSQL: %d rows, YugabyteDB: %d rows", len(pgData), len(ybData))
		}
		if len(pgData) != len(ybData) {
			pgSummary := ResultSummary{Success: true, RowCount: len(pgData)}
			ybSummary := ResultSummary{Success: true, RowCount: len(ybData)}
			diffs := []string{
				"Row count differs",
			}
			severity := "HIGH"
			if p.config.Comparison.FailOnDifferences {
				severity = "CRITICAL"
			}
			p.reporter.ReportInconsistency(RowCountMismatch, severity, "(extended protocol unit)", pgSummary, ybSummary, diffs)
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
					DataValueMismatch,
					severity,
					"(extended protocol unit)",
					ResultSummary{Success: true, RowCount: len(pgDataRows)},
					ResultSummary{Success: true, RowCount: len(ybDataRows)},
					validation.Differences,
				)
			}
		}
	}

	// Forward results from source of truth
	var resultsToForward []*PGMessage
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
