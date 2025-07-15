package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

// DualExecutionProxy implements the actual dual-execution logic
type DualExecutionProxy struct {
	config              *Config
	resolver            *DualDatabaseResolver
	pgAddr              string
	ybAddr              string
	security            *SecurityConfig
	constraintDetector  *ConstraintDivergenceDetector
	reporter            *InconsistencyReporter
	slowQueryAnalyzer   *SlowQueryAnalyzer
	resultValidator     *ResultValidator
}

// NewDualExecutionProxy creates a new dual execution proxy
func NewDualExecutionProxy(config *Config) *DualExecutionProxy {
	pgAddr := fmt.Sprintf("%s:%d", config.Proxy.PostgreSQL.Host, config.Proxy.PostgreSQL.Port)
	ybAddr := fmt.Sprintf("%s:%d", config.Proxy.YugabyteDB.Host, config.Proxy.YugabyteDB.Port)
	
	return &DualExecutionProxy{
		config:             config,
		pgAddr:             pgAddr,
		ybAddr:             ybAddr,
		security:           NewSecurityConfig(),
		constraintDetector: NewConstraintDivergenceDetector(),
		reporter:           NewInconsistencyReporter(),
		resultValidator:    NewResultValidator(config.Comparison.FailOnDifferences),
	}
}

// NewDualExecutionProxyWithReporter creates a new dual execution proxy with a shared reporter
func NewDualExecutionProxyWithReporter(config *Config, reporter *InconsistencyReporter) *DualExecutionProxy {
	pgAddr := fmt.Sprintf("%s:%d", config.Proxy.PostgreSQL.Host, config.Proxy.PostgreSQL.Port)
	ybAddr := fmt.Sprintf("%s:%d", config.Proxy.YugabyteDB.Host, config.Proxy.YugabyteDB.Port)
	
	return &DualExecutionProxy{
		config:             config,
		pgAddr:             pgAddr,
		ybAddr:             ybAddr,
		security:           NewSecurityConfig(),
		constraintDetector: NewConstraintDivergenceDetector(),
		reporter:           reporter,
		resultValidator:    NewResultValidator(config.Comparison.FailOnDifferences),
	}
}

// HandleConnection handles a client connection with dual execution
func (p *DualExecutionProxy) HandleConnection(clientConn net.Conn) {
	defer clientConn.Close()
	
	clientAddr := clientConn.RemoteAddr().String()
	log.Printf("New connection from %s", clientAddr)
	
	// Create connections to both databases
	pgConn, err := net.DialTimeout("tcp", p.pgAddr, 10*time.Second)
	if err != nil {
		log.Printf("Failed to connect to PostgreSQL: %v", err)
		return
	}
	defer pgConn.Close()
	
	ybConn, err := net.DialTimeout("tcp", p.ybAddr, 10*time.Second)
	if err != nil {
		log.Printf("Failed to connect to YugabyteDB: %v", err)
		return
	}
	defer ybConn.Close()
	
	// Initialize resolver for this connection
	p.resolver = NewDualDatabaseResolver(p.config, p.pgAddr, p.ybAddr)
	defer p.resolver.Close()
	
	// Initialize slow query analyzer if pools are available
	if p.config.Comparison.ReportSlowQueries {
		pgPool, ybPool := p.resolver.GetPools()
		if pgPool != nil && ybPool != nil {
			p.slowQueryAnalyzer = NewSlowQueryAnalyzer(p.config, pgPool, ybPool)
			log.Printf("✓ Slow query analyzer initialized (threshold: %.2fx)", p.config.Comparison.SlowQueryRatio)
		}
	}
	
	// Handle startup phase (authentication, parameter negotiation)
	if err := p.handleStartupPhase(clientConn, pgConn, ybConn); err != nil {
		log.Printf("Startup phase failed: %v", err)
		return
	}
	
	// Create protocol readers/writers
	clientReader := NewPGProtocolReader(clientConn)
	clientWriter := NewPGProtocolWriter(clientConn)
	
	pgReader := NewPGProtocolReader(pgConn)
	pgWriter := NewPGProtocolWriter(pgConn)
	
	ybReader := NewPGProtocolReader(ybConn)
	ybWriter := NewPGProtocolWriter(ybConn)
	
	// Main query processing loop
	for {
		// Read message from client
		msg, err := clientReader.ReadMessage()
		if err != nil {
			if err != io.EOF {
				log.Printf("Error reading from client: %v", err)
			}
			break
		}
		
		// Check for termination
		if IsTerminateMessage(msg) {
			log.Printf("Client requested termination")
			pgWriter.WriteMessage(msg)
			ybWriter.WriteMessage(msg)
			break
		}
		
		// Handle query messages
		if IsQueryMessage(msg) {
			query, err := ParseQuery(msg)
			if err != nil {
				log.Printf("Failed to parse query: %v", err)
				continue
			}
			
			log.Printf("Query: %s", query)
			
			// Security check
			if err := p.security.ValidateQuery(query); err != nil {
				log.Printf("Security validation failed: %v", err)
				errorMsg := CreateErrorMessage("ERROR", "42501", fmt.Sprintf("Security validation failed: %v", err))
				clientWriter.WriteMessage(errorMsg)
				readyMsg := &PGMessage{Type: msgTypeReadyForQuery, Data: []byte{'I'}}
				clientWriter.WriteMessage(readyMsg)
				continue
			}
			
			// Check if we should compare this query
			if p.config.Comparison.Enabled {
				// CRITICAL FEATURE 2: Analyze query performance first if enabled
				if p.slowQueryAnalyzer != nil && p.config.Comparison.FailOnSlowQueries {
					slowResult, slowErr := p.slowQueryAnalyzer.AnalyzeQuery(query)
					if slowErr != nil {
						// Return error with EXPLAIN ANALYZE plans
						log.Printf("🚨 SLOW QUERY REJECTED: %v", slowErr)
						errorMsg := CreateErrorMessage("ERROR", "XX000", slowErr.Error())
						clientWriter.WriteMessage(errorMsg)
						readyMsg := &PGMessage{Type: msgTypeReadyForQuery, Data: []byte{'I'}}
						clientWriter.WriteMessage(readyMsg)
						continue
					}
					if slowResult != nil && slowResult.IsSlowQuery {
						log.Printf("⚠️  Slow query detected but not failing (ratio: %.2fx)", slowResult.SlowRatio)
					}
				}
				
				// Execute on both databases and compare
				if err := p.executeDualQuery(msg, query, clientWriter, pgWriter, pgReader, ybWriter, ybReader); err != nil {
					log.Printf("Dual execution failed: %v", err)
				}
			} else {
				// Default routing based on source of truth
				if p.config.Comparison.SourceOfTruth == "yugabytedb" {
					p.proxySingleQuery(msg, clientWriter, ybWriter, ybReader, "YugabyteDB")
				} else {
					p.proxySingleQuery(msg, clientWriter, pgWriter, pgReader, "PostgreSQL")
				}
			}
		} else {
			// For non-query messages, forward to source of truth
			if p.config.Comparison.SourceOfTruth == "yugabytedb" {
				p.proxyMessage(msg, clientWriter, ybWriter, ybReader)
			} else {
				p.proxyMessage(msg, clientWriter, pgWriter, pgReader)
			}
		}
	}
}

// handleStartupPhase handles the connection startup phase
func (p *DualExecutionProxy) handleStartupPhase(client, pg, yb net.Conn) error {
	log.Printf("Starting connection startup phase")
	
	// Buffer to accumulate startup message
	startupBuf := make([]byte, 8192)
	
	// Read the first message (could be SSL request or startup message)
	// First 4 bytes are the message length
	if _, err := io.ReadFull(client, startupBuf[:4]); err != nil {
		return fmt.Errorf("failed to read message length: %w", err)
	}
	
	// Get message length
	msgLen := int(binary.BigEndian.Uint32(startupBuf[:4]))
	if msgLen > len(startupBuf) {
		return fmt.Errorf("message too large: %d bytes", msgLen)
	}
	
	// Read the rest of the message
	if _, err := io.ReadFull(client, startupBuf[4:msgLen]); err != nil {
		return fmt.Errorf("failed to read message body: %w", err)
	}
	
	log.Printf("Received message: %d bytes", msgLen)
	
	// Check if this is an SSL request (8 bytes total, protocol version 80877103)
	if msgLen == 8 {
		protocolVersion := binary.BigEndian.Uint32(startupBuf[4:8])
		if protocolVersion == 80877103 { // SSL request magic number
			log.Printf("SSL request received, rejecting SSL")
			// Send SSL rejection (single byte 'N')
			if _, err := client.Write([]byte{'N'}); err != nil {
				return fmt.Errorf("failed to send SSL rejection: %w", err)
			}
			
			// Now read the actual startup message
			if _, err := io.ReadFull(client, startupBuf[:4]); err != nil {
				return fmt.Errorf("failed to read startup message length after SSL: %w", err)
			}
			
			msgLen = int(binary.BigEndian.Uint32(startupBuf[:4]))
			if msgLen > len(startupBuf) {
				return fmt.Errorf("startup message too large: %d bytes", msgLen)
			}
			
			if _, err := io.ReadFull(client, startupBuf[4:msgLen]); err != nil {
				return fmt.Errorf("failed to read startup message body: %w", err)
			}
			
			log.Printf("Received actual startup message: %d bytes", msgLen)
		}
	}
	
	totalRead := msgLen
	
	// Send startup message to both backends
	if _, err := pg.Write(startupBuf[:totalRead]); err != nil {
		return fmt.Errorf("failed to send startup to PostgreSQL: %w", err)
	}
	if _, err := yb.Write(startupBuf[:totalRead]); err != nil {
		return fmt.Errorf("failed to send startup to YugabyteDB: %w", err)
	}
	
	// Determine primary connection based on source of truth
	var primaryConn, secondaryConn net.Conn
	var primaryName, secondaryName string
	
	if p.config.Comparison.SourceOfTruth == "yugabytedb" {
		primaryConn = yb
		secondaryConn = pg
		primaryName = "YugabyteDB"
		secondaryName = "PostgreSQL"
	} else {
		primaryConn = pg
		secondaryConn = yb
		primaryName = "PostgreSQL"
		secondaryName = "YugabyteDB"
	}
	
	// Create protocol readers for proper message parsing
	primaryReader := NewPGProtocolReader(primaryConn)
	
	// Forward authentication and startup messages from primary
	for {
		msg, err := primaryReader.ReadMessage()
		if err != nil {
			return fmt.Errorf("failed to read startup response from %s: %w", primaryName, err)
		}
		
		// Forward to client
		writer := NewPGProtocolWriter(client)
		if err := writer.WriteMessage(msg); err != nil {
			return fmt.Errorf("failed to send startup response to client: %w", err)
		}
		
		// Check if startup is complete
		if msg.Type == msgTypeReadyForQuery {
			log.Printf("Startup phase completed with %s", primaryName)
			break
		}
		
		// Log authentication messages
		if msg.Type == msgTypeAuthentication {
			log.Printf("Authentication message from %s", primaryName)
		}
	}
	
	// Consume and discard startup responses from secondary database
	go func() {
		secondaryReader := NewPGProtocolReader(secondaryConn)
		for {
			msg, err := secondaryReader.ReadMessage()
			if err != nil {
				log.Printf("Secondary %s startup reader stopped: %v", secondaryName, err)
				break
			}
			if msg.Type == msgTypeReadyForQuery {
				log.Printf("Secondary %s ready", secondaryName)
				break
			}
		}
	}()
	
	return nil
}

// executeDualQuery executes a query on both databases and compares results
func (p *DualExecutionProxy) executeDualQuery(msg *PGMessage, query string, clientWriter *PGProtocolWriter, 
	pgWriter *PGProtocolWriter, pgReader *PGProtocolReader,
	ybWriter *PGProtocolWriter, ybReader *PGProtocolReader) error {
	
	log.Printf("Executing dual query: %s", query)
	
	// Send query to both databases
	if err := pgWriter.WriteMessage(msg); err != nil {
		return fmt.Errorf("failed to send query to PostgreSQL: %w", err)
	}
	if err := ybWriter.WriteMessage(msg); err != nil {
		return fmt.Errorf("failed to send query to YugabyteDB: %w", err)
	}
	
	// Collect results from both databases
	var pgResults, ybResults []*PGMessage
	var pgErr, ybErr error
	var wg sync.WaitGroup
	wg.Add(2)
	
	// Collect PostgreSQL results
	go func() {
		defer wg.Done()
		pgResults, pgErr = p.collectQueryResults(pgReader)
	}()
	
	// Collect YugabyteDB results
	go func() {
		defer wg.Done()
		ybResults, ybErr = p.collectQueryResults(ybReader)
	}()
	
	wg.Wait()
	
	// CRITICAL: Analyze for constraint violation divergence
	pgResult := &QueryExecutionResult{
		Success:      pgErr == nil,
		Error:        pgErr,
		DatabaseName: "PostgreSQL",
	}
	if pgErr != nil {
		pgResult.ConstraintError = p.constraintDetector.AnalyzeConstraintError(pgErr)
	}
	
	ybResult := &QueryExecutionResult{
		Success:      ybErr == nil,
		Error:        ybErr,
		DatabaseName: "YugabyteDB",
	}
	if ybErr != nil {
		ybResult.ConstraintError = p.constraintDetector.AnalyzeConstraintError(ybErr)
	}
	
	// Detect critical divergence in constraint handling
	divergence := p.constraintDetector.DetectDivergence(query, pgResult, ybResult)
	if divergence != nil {
		p.constraintDetector.LogCriticalDivergence(divergence)
		
		// Report the constraint divergence
		differences := []string{
			fmt.Sprintf("PostgreSQL: %s", pgResult.Error),
			fmt.Sprintf("YugabyteDB: %s", ybResult.Error),
			fmt.Sprintf("Divergence Type: %s", divergence.DivergenceType),
		}
		
		pgSummary := ResultSummary{
			Success: pgResult.Success,
			Error:   fmt.Sprintf("%v", pgResult.Error),
		}
		
		ybSummary := ResultSummary{
			Success: ybResult.Success,
			Error:   fmt.Sprintf("%v", ybResult.Error),
		}
		
		p.reporter.ReportInconsistency(
			ConstraintDivergence,
			divergence.CriticalityLevel,
			query,
			pgSummary,
			ybSummary,
			differences,
		)
		
		// If this is a critical divergence, we should fail the transaction
		if divergence.CriticalityLevel == "CRITICAL" {
			log.Printf("🚨 CRITICAL CONSTRAINT DIVERGENCE - FAILING TRANSACTION FOR CONSISTENCY 🚨")
			// Return error to client about the divergence
			errorMsg := CreateErrorMessage("ERROR", "XX000", 
				fmt.Sprintf("CRITICAL YugabyteDB compatibility issue detected: %s. Transaction aborted for data consistency.", 
					divergence.DivergenceType))
			clientWriter.WriteMessage(errorMsg)
			readyMsg := &PGMessage{Type: msgTypeReadyForQuery, Data: []byte{'I'}}
			clientWriter.WriteMessage(readyMsg)
			return fmt.Errorf("critical constraint divergence detected")
		}
	}
	
	// Check for errors and report error divergence
	if pgErr != nil || ybErr != nil {
		if pgErr != nil {
			log.Printf("PostgreSQL error: %v", pgErr)
		}
		if ybErr != nil {
			log.Printf("YugabyteDB error: %v", ybErr)
		}
		
		// Report error divergence if one succeeded and one failed
		if (pgErr == nil) != (ybErr == nil) {
			differences := []string{
				fmt.Sprintf("PostgreSQL: %v", pgErr),
				fmt.Sprintf("YugabyteDB: %v", ybErr),
				"One database succeeded while the other failed",
			}
			
			pgSummary := ResultSummary{
				Success: pgErr == nil,
			}
			if pgErr != nil {
				pgSummary.Error = pgErr.Error()
			}
			
			ybSummary := ResultSummary{
				Success: ybErr == nil,
			}
			if ybErr != nil {
				ybSummary.Error = ybErr.Error()
			}
			
			p.reporter.ReportInconsistency(
				ErrorDivergence,
				"CRITICAL",
				query,
				pgSummary,
				ybSummary,
				differences,
			)
		}
	}
	
	// Compare results if both succeeded
	if pgErr == nil && ybErr == nil && len(pgResults) > 0 && len(ybResults) > 0 {
		// Extract data rows for comparison
		pgDataRows := extractDataRows(pgResults)
		ybDataRows := extractDataRows(ybResults)
		
		if len(pgDataRows) > 0 || len(ybDataRows) > 0 {
			// Log comparison
			if p.config.Comparison.LogComparisons || 
			   (p.config.Comparison.LogDifferencesOnly && len(pgDataRows) != len(ybDataRows)) {
				log.Printf("Query result comparison - PostgreSQL: %d rows, YugabyteDB: %d rows", 
					len(pgDataRows), len(ybDataRows))
				
				if len(pgDataRows) != len(ybDataRows) {
					log.Printf("WARNING: Row count mismatch for query: %s", query)
					
					// Report the row count mismatch
					differences := []string{
						fmt.Sprintf("PostgreSQL returned %d rows", len(pgDataRows)),
						fmt.Sprintf("YugabyteDB returned %d rows", len(ybDataRows)),
						fmt.Sprintf("Difference: %d rows", len(pgDataRows) - len(ybDataRows)),
					}
					
					pgSummary := ResultSummary{
						Success:  true,
						RowCount: len(pgDataRows),
					}
					
					ybSummary := ResultSummary{
						Success:  true,
						RowCount: len(ybDataRows),
					}
					
					severity := "HIGH"
					if p.config.Comparison.FailOnDifferences {
						severity = "CRITICAL"
					}
					
					p.reporter.ReportInconsistency(
						RowCountMismatch,
						severity,
						query,
						pgSummary,
						ybSummary,
						differences,
					)
					
					if p.config.Comparison.FailOnDifferences {
						// Still return results from source of truth, but log the error
						log.Printf("ERROR: Failing due to result differences (fail-on-differences=true)")
					}
				}
			}
		}
	}
	
	// CRITICAL FEATURE 1: Validate results and fail if different
	if pgErr == nil && ybErr == nil && p.resultValidator != nil {
		validation, err := p.resultValidator.ValidateResults(pgResults, ybResults)
		if err != nil {
			return fmt.Errorf("result validation failed: %w", err)
		}
		
		if validation.ShouldFail {
			// Return SQL error to user about result differences
			log.Printf("🚨 CRITICAL: Query results differ - returning error to user")
			errorMsg := CreateErrorMessage("ERROR", "XX000", validation.ErrorMessage)
			clientWriter.WriteMessage(errorMsg)
			readyMsg := &PGMessage{Type: msgTypeReadyForQuery, Data: []byte{'I'}}
			clientWriter.WriteMessage(readyMsg)
			return fmt.Errorf("query results differ between databases")
		}
	}
	
	// Forward results from source of truth (YugabyteDB)
	var resultsToForward []*PGMessage
	if p.config.Comparison.SourceOfTruth == "yugabytedb" {
		resultsToForward = ybResults
	} else {
		resultsToForward = pgResults
	}
	
	// Send results to client
	for _, result := range resultsToForward {
		if err := clientWriter.WriteMessage(result); err != nil {
			return fmt.Errorf("failed to send result to client: %w", err)
		}
	}
	
	return nil
}

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

// proxySingleQuery forwards a query to a single database
func (p *DualExecutionProxy) proxySingleQuery(msg *PGMessage, clientWriter *PGProtocolWriter,
	dbWriter *PGProtocolWriter, dbReader *PGProtocolReader, dbName string) {
	
	log.Printf("Routing query to %s", dbName)
	
	// Send query
	if err := dbWriter.WriteMessage(msg); err != nil {
		log.Printf("Failed to send query to %s: %v", dbName, err)
		errorMsg := CreateErrorMessage("ERROR", "08006", fmt.Sprintf("Failed to send query to %s", dbName))
		clientWriter.WriteMessage(errorMsg)
		return
	}
	
	// Forward all response messages
	for {
		response, err := dbReader.ReadMessage()
		if err != nil {
			log.Printf("Failed to read response from %s: %v", dbName, err)
			break
		}
		
		if err := clientWriter.WriteMessage(response); err != nil {
			log.Printf("Failed to send response to client: %v", err)
			break
		}
		
		// Check if this is the end of results
		if response.Type == msgTypeReadyForQuery {
			break
		}
	}
}

// proxyMessage forwards a non-query message
func (p *DualExecutionProxy) proxyMessage(msg *PGMessage, clientWriter *PGProtocolWriter,
	dbWriter *PGProtocolWriter, dbReader *PGProtocolReader) {
	
	// Send message
	if err := dbWriter.WriteMessage(msg); err != nil {
		log.Printf("Failed to send message: %v", err)
		return
	}
	
	// Forward response
	response, err := dbReader.ReadMessage()
	if err != nil {
		log.Printf("Failed to read response: %v", err)
		return
	}
	
	if err := clientWriter.WriteMessage(response); err != nil {
		log.Printf("Failed to send response to client: %v", err)
	}
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