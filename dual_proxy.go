package main

import (
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

// DualExecutionProxy implements the actual dual-execution logic
type DualExecutionProxy struct {
	config                  *Config
	resolver                *DualDatabaseResolver
	pgAddr                  string
	ybAddr                  string
	security                *SecurityConfig
	constraintDetector      *ConstraintDivergenceDetector
	reporter                *InconsistencyReporter
	slowQueryAnalyzer       *SlowQueryAnalyzer
	resultValidator         *ResultValidator
	secondaryReady          bool   // true if secondary backend completed startup for this connection
	inTransaction           bool   // simple tracker for explicit client transactions
	secondaryDisabledLogged bool   // ensures we log the DISABLED warning only once per connection
	secondaryBlocked        bool   // once set due to inconsistency, stop sending further operations to secondary for this connection
	secondaryReason         string // last known reason why secondary is disabled/not ready
	lastParsedSQL           string // last SQL seen in extended protocol Parse for this connection
	requireSecondary        bool   // per-connection enforcement; initialized from config and may be relaxed on specific startup errors
}

// NewDualExecutionProxy creates a new dual execution proxy
func NewDualExecutionProxy(config *Config) *DualExecutionProxy {
	pgAddr := fmt.Sprintf("%s:%d", config.Proxy.PostgreSQL.Host, config.Proxy.PostgreSQL.Port)
	ybAddr := fmt.Sprintf("%s:%d", config.Proxy.YugabyteDB.Host, config.Proxy.YugabyteDB.Port)

	rep := NewInconsistencyReporter()
	rep.AttachConfig(config)
	return &DualExecutionProxy{
		config:             config,
		pgAddr:             pgAddr,
		ybAddr:             ybAddr,
		security:           NewSecurityConfig(),
		constraintDetector: NewConstraintDivergenceDetector(),
		reporter:           rep,
		resultValidator:    NewResultValidator(config.Comparison.FailOnDifferences, config.Comparison.SortBeforeCompare),
		requireSecondary:   config.Comparison.RequireSecondary,
	}
}

// NewDualExecutionProxyWithReporter creates a new dual execution proxy with a shared reporter
func NewDualExecutionProxyWithReporter(config *Config, reporter *InconsistencyReporter) *DualExecutionProxy {
	pgAddr := fmt.Sprintf("%s:%d", config.Proxy.PostgreSQL.Host, config.Proxy.PostgreSQL.Port)
	ybAddr := fmt.Sprintf("%s:%d", config.Proxy.YugabyteDB.Host, config.Proxy.YugabyteDB.Port)

	reporter.AttachConfig(config)
	return &DualExecutionProxy{
		config:             config,
		pgAddr:             pgAddr,
		ybAddr:             ybAddr,
		security:           NewSecurityConfig(),
		constraintDetector: NewConstraintDivergenceDetector(),
		reporter:           reporter,
		resultValidator:    NewResultValidator(config.Comparison.FailOnDifferences, config.Comparison.SortBeforeCompare),
		requireSecondary:   config.Comparison.RequireSecondary,
	}
}

// HandleConnection handles a client connection with dual execution
// logSecondaryDisabled prints a standardized, one-time warning when secondary (PostgreSQL when SoT is YB, or vice versa) is disabled for this connection.
func (p *DualExecutionProxy) logSecondaryDisabled(reason string) {
	// Preserve the most informative reason for surfacing to the client.
	// Avoid overwriting a detailed prior reason (e.g., startup failed / timeout) with a generic one.
	lowerNew := strings.ToLower(strings.TrimSpace(reason))
	// Determine if new reason is generic
	isGeneric := lowerNew == "secondary not ready for this connection" || lowerNew == "unknown reason" || lowerNew == "not ready"
	// Allow upgrade to inconsistency-specific reasons or if previous reason is empty
	if p.secondaryReason == "" || (!isGeneric && (len(reason) >= len(p.secondaryReason) || strings.Contains(lowerNew, "inconsistency"))) {
		p.secondaryReason = reason
	}
	if p.secondaryDisabledLogged {
		return
	}
	p.secondaryDisabledLogged = true
	sot := strings.ToLower(p.config.Comparison.SourceOfTruth)
	secondaryName := "PostgreSQL"
	if sot == "postgresql" {
		secondaryName = "YugabyteDB"
	}
	useReason := p.secondaryReason
	if strings.TrimSpace(useReason) == "" {
		useReason = reason
	}
	if p.requireSecondary {
		log.Printf("⚠ %s secondary DISABLED for this connection: %s. Dual comparison is REQUIRED; queries will be rejected until secondary is healthy.", secondaryName, useReason)
	} else {
		log.Printf("⚠ %s secondary DISABLED for this connection: %s. Operating in DEGRADED MODE: routing only to %s; comparisons disabled for this connection.", secondaryName, useReason, strings.Title(p.config.Comparison.SourceOfTruth))
	}
}

func (p *DualExecutionProxy) HandleConnection(clientConn net.Conn) {
	defer clientConn.Close()

	clientAddr := clientConn.RemoteAddr().String()
	log.Printf("New connection from %s", clientAddr)
	// Wrap client connection to dump bytes we send back to client when enabled
	clientConn = wrapConn(clientConn, "→Client", p.config)

	// Create connections to both databases
	pgConn, err := net.DialTimeout("tcp", p.pgAddr, 10*time.Second)
	if err != nil {
		log.Printf("Failed to connect to PostgreSQL: %v", err)
		return
	}
	// Wrap for TX logging before TLS negotiation to capture SSLRequest too
	pgConn = wrapConn(pgConn, "→PostgreSQL", p.config)
	// Attempt to enable TLS if backend supports it
	if secured, serr := p.maybeEnableBackendTLS(pgConn, p.config.Proxy.PostgreSQL.Host, "PostgreSQL"); serr == nil {
		pgConn = wrapConn(secured, "→PostgreSQL", p.config)
	} else {
		log.Printf("Failed to negotiate TLS with PostgreSQL: %v", serr)
		pgConn.Close()
		return
	}
	defer pgConn.Close()

	ybConn, err := net.DialTimeout("tcp", p.ybAddr, 10*time.Second)
	if err != nil {
		log.Printf("Failed to connect to YugabyteDB: %v", err)
		return
	}
	// Wrap for TX logging before TLS negotiation to capture SSLRequest too
	ybConn = wrapConn(ybConn, "→YugabyteDB", p.config)
	// Attempt to enable TLS if backend supports it
	if secured, serr := p.maybeEnableBackendTLS(ybConn, p.config.Proxy.YugabyteDB.Host, "YugabyteDB"); serr == nil {
		ybConn = wrapConn(secured, "→YugabyteDB", p.config)
	} else {
		log.Printf("Failed to negotiate TLS with YugabyteDB: %v", serr)
		ybConn.Close()
		return
	}
	defer ybConn.Close()

	// Initialize resolver for this connection only if not already provided
	createdLocalResolver := false
	if p.resolver == nil {
		p.resolver = NewDualDatabaseResolver(p.config, p.pgAddr, p.ybAddr)
		createdLocalResolver = true
	}
	defer func() {
		if createdLocalResolver && p.resolver != nil {
			p.resolver.Close()
		}
	}()

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

			// Track simple transaction state for mirroring safety
			if isTxnBegin(query) {
				p.inTransaction = true
			}
			if isTxnEnd(query) {
				p.inTransaction = false
			}

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
				if p.slowQueryAnalyzer != nil && p.config.Comparison.ReportSlowQueries {
					slowResult, slowErr := p.slowQueryAnalyzer.AnalyzeQuery(query)
					if slowErr != nil {
						log.Printf("Slow query analyzer error: %v (continuing)", slowErr)
					} else if slowResult != nil && slowResult.IsSlowQuery {
						log.Printf("🐌 Slow query detected (ratio: %.2fx) — reporting and continuing", slowResult.SlowRatio)
						// Report performance degradation to consistency reports, do not block execution
						pgMs := int64(slowResult.PostgreSQLTime / time.Millisecond)
						ybMs := int64(slowResult.YugabyteDBTime / time.Millisecond)
						p.reporter.ReportInconsistency(
							PerformanceDegradation,
							"MEDIUM",
							query,
							ResultSummary{Success: true, ExecutionMs: pgMs},
							ResultSummary{Success: true, ExecutionMs: ybMs},
							[]string{fmt.Sprintf("YugabyteDB is %.2fx slower than PostgreSQL", slowResult.SlowRatio)},
						)
					}
				}

				// Execute on both databases and compare only if secondary is ready
				// If secondary has been blocked due to inconsistency, treat as not ready
				if p.secondaryBlocked {
					p.secondaryReady = false
				}
				if p.secondaryReady {
					if err := p.executeDualQuery(msg, query, clientWriter, pgWriter, pgReader, ybWriter, ybReader); err != nil {
						log.Printf("Dual execution failed: %v", err)
					}
				} else {
					// Secondary not ready for this connection. Allow narrowly scoped BOOTSTRAP DDL to proceed
					// so the operator can create missing roles/databases on the secondary.
					if isBootstrapDDL(query) && p.resolver != nil {
						p.logSecondaryDisabled("secondary not ready for this connection (bootstrap mode)")
						// Mirror to secondary via pools using admin credentials
						if err := p.resolver.MirrorDDLToSecondary(query); err != nil {
							log.Printf("Warning: bootstrap mirroring to secondary failed: %v", err)
						} else {
							log.Printf("✓ Bootstrap DDL mirrored to secondary. You may need to reconnect to finalize secondary startup.")
						}
						// Route user-visible execution to source of truth
						if p.config.Comparison.SourceOfTruth == "yugabytedb" {
							p.proxySingleQuery(msg, clientWriter, ybWriter, ybReader, "YugabyteDB")
						} else {
							p.proxySingleQuery(msg, clientWriter, pgWriter, pgReader, "PostgreSQL")
						}
						continue
					}
					// For all other queries
					if p.requireSecondary {
						// Reject to enforce dual execution policy
						p.logSecondaryDisabled("secondary not ready for this connection")
						p.secondaryBlocked = true // block further operations on this connection until reconnect
						// Return clear SQL error to client and do not route to any backend. Include concrete reason if known.
						reason := p.secondaryReason
						if reason == "" {
							reason = "unknown reason"
						}
						errorText := fmt.Sprintf("Dual comparison is required but secondary is not available (%s); query aborted. Please retry after secondary recovers or reconnect.", reason)
						errorMsg := CreateErrorMessage("ERROR", "XX000", errorText)
						clientWriter.WriteMessage(errorMsg)
						readyMsg := &PGMessage{Type: msgTypeReadyForQuery, Data: []byte{'I'}}
						clientWriter.WriteMessage(readyMsg)
						continue
					} else {
						// Degraded mode: route to source of truth only, no comparison
						p.logSecondaryDisabled("secondary not ready for this connection")
						if p.config.Comparison.SourceOfTruth == "yugabytedb" {
							p.proxySingleQuery(msg, clientWriter, ybWriter, ybReader, "YugabyteDB")
						} else {
							p.proxySingleQuery(msg, clientWriter, pgWriter, pgReader, "PostgreSQL")
						}
						continue
					}
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
			// For non-query (extended protocol) messages
			if p.config.Comparison.Enabled && p.secondaryReady && p.config.Comparison.DualExtendedProtocol {
				// Mirror to both backends; only read/drain on Sync
				p.proxyExtendedDualMessage(msg, clientWriter, pgWriter, pgReader, ybWriter, ybReader)
			} else {
				// Forward to source of truth only
				if p.config.Comparison.SourceOfTruth == "yugabytedb" {
					p.proxyMessage(msg, clientWriter, ybWriter, ybReader, "YugabyteDB")
				} else {
					p.proxyMessage(msg, clientWriter, pgWriter, pgReader, "PostgreSQL")
				}
			}
		}
	}
}

// handleStartupPhase handles the connection startup phase
func (p *DualExecutionProxy) handleStartupPhase(client, pg, yb net.Conn) error {
	log.Printf("Starting connection startup phase")
	// Assume secondary is not ready until confirmed
	p.secondaryReady = false

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

	// Create protocol readers/writers for proper message parsing
	primaryReader := NewPGProtocolReader(primaryConn)
	clientReader := NewPGProtocolReader(client)
	clientWriter := NewPGProtocolWriter(client)
	primaryWriter := NewPGProtocolWriter(primaryConn)
	secondaryWriter := NewPGProtocolWriter(secondaryConn)

	// Channel to provide client auth messages to secondary when it requests them
	clientAuthCh := make(chan *PGMessage, 4)

	// Prepare a waiter for the secondary reaching ReadyForQuery and handle its auth requests
	readyCh := make(chan error, 1)
	go func() {
		secondaryReader := NewPGProtocolReader(secondaryConn)
		var lastErrDetail string
		for {
			msg, err := secondaryReader.ReadMessage()
			if err != nil {
				// If we previously saw an ErrorResponse, include it in the reason
				if lastErrDetail != "" {
					readyCh <- fmt.Errorf("secondary %s startup reader stopped after error: %s (%v)", secondaryName, lastErrDetail, err)
				} else {
					readyCh <- fmt.Errorf("secondary %s startup reader stopped: %w", secondaryName, err)
				}
				return
			}
			// Capture server ErrorResponse to surface meaningful reason (e.g., FATAL 3D000 database does not exist)
			if msg.Type == msgTypeErrorResponse {
				sev, code, text := parseBackendError(msg.Data)
				if code != "" || text != "" || sev != "" {
					if sev == "" {
						sev = "ERROR"
					}
					lastErrDetail = strings.TrimSpace(fmt.Sprintf("%s %s %s", sev, code, text))
				} else {
					lastErrDetail = "server returned ErrorResponse"
				}
				// continue reading; backend may close shortly after
				continue
			}
			if msg.Type == msgTypeAuthentication {
				if len(msg.Data) >= 4 {
					code := int(binary.BigEndian.Uint32(msg.Data[0:4]))
					if code != 0 {
						// Secondary requests client auth. Wait for the next client auth message captured from primary.
						select {
						case authMsg := <-clientAuthCh:
							if err := secondaryWriter.WriteMessage(authMsg); err != nil {
								readyCh <- fmt.Errorf("failed to forward client auth to %s: %w", secondaryName, err)
								return
							}
						case <-time.After(10 * time.Second):
							readyCh <- fmt.Errorf("timeout waiting for client auth response for %s", secondaryName)
							return
						}
					}
				}
			}
			if msg.Type == msgTypeReadyForQuery {
				log.Printf("Secondary %s ready", secondaryName)
				readyCh <- nil
				return
			}
		}
	}()

	// Forward authentication and startup messages from primary
	for {
		msg, err := primaryReader.ReadMessage()
		if err != nil {
			return fmt.Errorf("failed to read startup response from %s: %w", primaryName, err)
		}

		// Forward primary->client
		if err := clientWriter.WriteMessage(msg); err != nil {
			return fmt.Errorf("failed to send startup response to client: %w", err)
		}

		// If primary requests authentication (non-OK), read client's response and forward to PRIMARY only.
		// Also make the auth message available to the secondary if it asked for it.
		if msg.Type == msgTypeAuthentication {
			// AuthenticationOk has code 0 in the first 4 bytes
			if len(msg.Data) >= 4 {
				code := int(binary.BigEndian.Uint32(msg.Data[0:4]))
				if code != 0 { // needs client response (Password/SASL)
					clientMsg, cerr := clientReader.ReadMessage()
					if cerr != nil {
						return fmt.Errorf("failed to read client auth response: %w", cerr)
					}
					// Send client auth response to primary backend only
					if err := primaryWriter.WriteMessage(clientMsg); err != nil {
						return fmt.Errorf("failed to forward client auth to %s: %w", primaryName, err)
					}
					// Provide the auth message to secondary if it needs it
					select {
					case clientAuthCh <- clientMsg:
						// queued for secondary if requested
					default:
						// channel full or not requested; skip without blocking
					}
				}
			}
		}

		// Check if startup is complete on primary
		if msg.Type == msgTypeReadyForQuery {
			log.Printf("Startup phase completed with %s", primaryName)
			break
		}
	}

	// Ensure secondary also completed startup before proceeding (with timeout)
	select {
	case err := <-readyCh:
		if err != nil {
			// Non-fatal for startup: establish connection to primary.
			// Suppress noisy warnings for benign EOF/transport errors.
			lower := strings.ToLower(err.Error())
			// Special case: missing database on secondary (SQLSTATE 3D000). Optionally fail-open for this connection.
			if p.config.Comparison.FailOpenOnMissingDatabase && (strings.Contains(lower, "3d000") || strings.Contains(lower, "database \"") && strings.Contains(lower, "does not exist") || strings.Contains(lower, "database does not exist")) {
				p.requireSecondary = false
				log.Printf("Warning: secondary %s missing database during startup (%v). Operating in DEGRADED MODE for this connection: comparisons disabled until secondary is healthy.", secondaryName, err)
			}
			if p.requireSecondary {
				if isTransportError(err) && strings.Contains(lower, "eof") {
					log.Printf("Info: secondary %s not available during startup (EOF). Dual comparison is REQUIRED; queries will be rejected for this connection until secondary is healthy.", secondaryName)
				} else {
					log.Printf("Warning: secondary %s failed during startup (%v). Startup will continue, but dual comparison is REQUIRED; queries will be rejected for this connection until secondary is healthy.", secondaryName, err)
				}
			} else {
				if isTransportError(err) && strings.Contains(lower, "eof") {
					log.Printf("Info: secondary %s not available during startup (EOF). Operating in DEGRADED MODE: comparisons disabled for this connection.", secondaryName)
				} else {
					log.Printf("Warning: secondary %s failed during startup (%v). Operating in DEGRADED MODE: comparisons disabled for this connection.", secondaryName, err)
				}
			}
			p.secondaryReady = false
			p.logSecondaryDisabled("startup failed: " + err.Error())
		} else {
			p.secondaryReady = true
		}
	case <-time.After(10 * time.Second):
		if p.requireSecondary {
			log.Printf("Warning: secondary %s did not complete startup within timeout. Startup will continue, but dual comparison is REQUIRED; queries will be rejected for this connection until secondary is healthy.", secondaryName)
		} else {
			log.Printf("Warning: secondary %s did not complete startup within timeout. Operating in DEGRADED MODE: comparisons disabled for this connection.", secondaryName)
		}
		p.secondaryReady = false
		p.logSecondaryDisabled("startup timeout")
	}

	return nil
}

// executeDualQuery executes a query on both databases and compares results
func (p *DualExecutionProxy) executeDualQuery(msg *PGMessage, query string, clientWriter *PGProtocolWriter,
	pgWriter *PGProtocolWriter, pgReader *PGProtocolReader,
	ybWriter *PGProtocolWriter, ybReader *PGProtocolReader) error {

	// High-level query log
	if p.config != nil && p.config.Debug.LogAllQueries {
		log.Printf("QUERY: %s", query)
	} else {
		log.Printf("Executing dual query: %s", query)
	}

	// Send query to both databases
	if err := pgWriter.WriteMessage(msg); err != nil {
		// Prevent client hang: inform client of backend write failure
		errorMsg := CreateErrorMessage("ERROR", "08006", fmt.Sprintf("failed to send query to PostgreSQL: %v", err))
		clientWriter.WriteMessage(errorMsg)
		readyMsg := &PGMessage{Type: msgTypeReadyForQuery, Data: []byte{'I'}}
		clientWriter.WriteMessage(readyMsg)
		return fmt.Errorf("failed to send query to PostgreSQL: %w", err)
	}
	if err := ybWriter.WriteMessage(msg); err != nil {
		// Prevent client hang: inform client of backend write failure
		errorMsg := CreateErrorMessage("ERROR", "08006", fmt.Sprintf("failed to send query to YugabyteDB: %v", err))
		clientWriter.WriteMessage(errorMsg)
		readyMsg := &PGMessage{Type: msgTypeReadyForQuery, Data: []byte{'I'}}
		clientWriter.WriteMessage(readyMsg)
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

	// Per-backend outcome logging
	if p.config != nil && p.config.Debug.LogAllQueries {
		if pgErr != nil {
			log.Printf("PG - error: %v", pgErr)
		} else {
			log.Printf("PG - ok")
		}
		if ybErr != nil {
			log.Printf("YB - error: %v", ybErr)
		} else {
			log.Printf("YB - ok")
		}
	}

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

		// Choose appropriate inconsistency type based on error nature
		incType := ConstraintDivergence
		mismatch := (pgErr == nil) != (ybErr == nil)
		if mismatch {
			if isTransportError(pgErr) || isTransportError(ybErr) {
				incType = ConnectionErrorDivergence
			} else if pgResult.ConstraintError == nil && ybResult.ConstraintError == nil {
				incType = ErrorDivergence
			}
		}
		p.reporter.ReportInconsistency(
			incType,
			divergence.CriticalityLevel,
			query,
			pgSummary,
			ybSummary,
			differences,
		)

		// If this is a critical divergence, we should fail the transaction
		if divergence.CriticalityLevel == "CRITICAL" {
			log.Printf("🚨 CRITICAL CONSTRAINT DIVERGENCE - FAILING TRANSACTION FOR CONSISTENCY 🚨")
			// Block secondary for this connection to prevent further operations
			p.secondaryBlocked = true
			p.secondaryReady = false
			p.logSecondaryDisabled("inconsistency detected (constraint divergence)")
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
			// Syntax error divergence special case
			if (pgErr != nil && isSyntaxError(pgErr) && ybErr == nil) || (ybErr != nil && isSyntaxError(ybErr) && pgErr == nil) {
				// Optionally fail transaction (no report)
				if p.config.Comparison.SyntaxErrorDivergenceFailAndRollback {
					var errText string
					if pgErr != nil {
						errText = pgErr.Error()
					} else {
						errText = ybErr.Error()
					}
					errMsg := CreateErrorMessage("ERROR", "42601", "Syntax error detected on one backend; aborting transaction to avoid undefined behavior: "+errText)
					clientWriter.WriteMessage(errMsg)
					status := byte('I')
					if p.inTransaction {
						status = 'E'
					}
					clientWriter.WriteMessage(&PGMessage{Type: msgTypeReadyForQuery, Data: []byte{status}})
					if p.config.Comparison.SyntaxErrorDivergenceReport {
						pgSummary := ResultSummary{Success: pgErr == nil}
						if pgErr != nil {
							pgSummary.Error = pgErr.Error()
						}
						ybSummary := ResultSummary{Success: ybErr == nil}
						if ybErr != nil {
							ybSummary.Error = ybErr.Error()
						}
						differences := []string{"One database succeeded while the other failed (syntax error)"}
						p.reporter.ReportInconsistency(ErrorDivergence, "LOW", query, pgSummary, ybSummary, differences)
					}
					return fmt.Errorf("syntax error divergence")
				}
				// Not failing: optionally report, else proceed to forward successful DB
				if p.config.Comparison.SyntaxErrorDivergenceReport {
					pgSummary := ResultSummary{Success: pgErr == nil}
					if pgErr != nil {
						pgSummary.Error = pgErr.Error()
					}
					ybSummary := ResultSummary{Success: ybErr == nil}
					if ybErr != nil {
						ybSummary.Error = ybErr.Error()
					}
					differences := []string{"One database succeeded while the other failed (syntax error)"}
					p.reporter.ReportInconsistency(ErrorDivergence, "LOW", query, pgSummary, ybSummary, differences)
				}
				// Fall through to forwarding
			}

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

			// Optionally suppress divergence reporting for catalog queries
			ignoreCatalog := p.config.Comparison.IgnoreCatalogDifferences && p.config.IsCatalogQuery(query)
			if !ignoreCatalog {
				p.reporter.ReportInconsistency(
					ErrorDivergence,
					"CRITICAL",
					query,
					pgSummary,
					ybSummary,
					differences,
				)
			}
			// Prevent client hang: either fail fast or forward the successful DB
			if p.config.Comparison.FailOnDifferences && !ignoreCatalog {
				// Block secondary for this connection due to inconsistency
				p.secondaryBlocked = true
				p.secondaryReady = false
				p.logSecondaryDisabled("inconsistency detected (error divergence)")
				errorMsg := CreateErrorMessage("ERROR", "XX000", "Query execution diverged: one database succeeded while the other failed")
				clientWriter.WriteMessage(errorMsg)
				readyMsg := &PGMessage{Type: msgTypeReadyForQuery, Data: []byte{'I'}}
				clientWriter.WriteMessage(readyMsg)
				return fmt.Errorf("error divergence between databases")
			}
			// If not failing on differences, forward from the successful database to keep client responsive
			if ybErr == nil {
				for _, m := range ybResults {
					if err := clientWriter.WriteMessage(m); err != nil {
						return fmt.Errorf("failed to forward YB result: %w", err)
					}
				}
			} else if pgErr == nil {
				for _, m := range pgResults {
					if err := clientWriter.WriteMessage(m); err != nil {
						return fmt.Errorf("failed to forward PG result: %w", err)
					}
				}
			} else {
				// Both failed: send a generic error with ReadyForQuery
				errorMsg := CreateErrorMessage("ERROR", "XX000", "Both databases failed to execute the query")
				clientWriter.WriteMessage(errorMsg)
				readyMsg := &PGMessage{Type: msgTypeReadyForQuery, Data: []byte{'I'}}
				clientWriter.WriteMessage(readyMsg)
			}
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
					ignoreCatalog := p.config.Comparison.IgnoreCatalogDifferences && p.config.IsCatalogQuery(query)
					if !ignoreCatalog {
						differences := []string{
							fmt.Sprintf("PostgreSQL returned %d rows", len(pgDataRows)),
							fmt.Sprintf("YugabyteDB returned %d rows", len(ybDataRows)),
							fmt.Sprintf("Difference: %d rows", len(pgDataRows)-len(ybDataRows)),
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
	}

	// CRITICAL FEATURE 1: Validate results and fail if different
	if pgErr == nil && ybErr == nil && p.resultValidator != nil {
		validation, err := p.resultValidator.ValidateResults(pgResults, ybResults)
		if err != nil {
			return fmt.Errorf("result validation failed: %w", err)
		}

		if validation.ShouldFail {
			ignoreCatalog := p.config.Comparison.IgnoreCatalogDifferences && p.config.IsCatalogQuery(query)
			if !ignoreCatalog {
				// Report inconsistency and block secondary for this connection
				severity := "HIGH"
				if p.config.Comparison.FailOnDifferences {
					severity = "CRITICAL"
				}
				pgDataRows := extractDataRows(pgResults)
				ybDataRows := extractDataRows(ybResults)
				p.reporter.ReportInconsistency(
					DataValueMismatch,
					severity,
					query,
					ResultSummary{Success: true, RowCount: len(pgDataRows)},
					ResultSummary{Success: true, RowCount: len(ybDataRows)},
					validation.Differences,
				)
				p.secondaryBlocked = true
				p.secondaryReady = false
				p.logSecondaryDisabled("inconsistency detected (result mismatch)")
				// Return SQL error to user about result differences
				log.Printf("🚨 CRITICAL: Query results differ - returning error to user")
				errorMsg := CreateErrorMessage("ERROR", "XX000", validation.ErrorMessage)
				clientWriter.WriteMessage(errorMsg)
				readyMsg := &PGMessage{Type: msgTypeReadyForQuery, Data: []byte{'I'}}
				clientWriter.WriteMessage(readyMsg)
				return fmt.Errorf("query results differ between databases")
			}
		}
	}

	// Forward results from the configured source of truth (note: main.go enforces YugabyteDB at runtime)
	var resultsToForward []*PGMessage
	var forwardFrom string
	if p.config.Comparison.SourceOfTruth == "yugabytedb" {
		resultsToForward = ybResults
		forwardFrom = "YB"
	} else {
		resultsToForward = pgResults
		forwardFrom = "PG"
	}
	if p.config != nil && p.config.Debug.LogAllQueries {
		log.Printf("Forwarding results from %s", forwardFrom)
	}

	// Send results to client
	for _, result := range resultsToForward {
		if err := clientWriter.WriteMessage(result); err != nil {
			return fmt.Errorf("failed to send result to client: %w", err)
		}
	}

	return nil
}

// proxySingleQuery forwards a query to a single database
func (p *DualExecutionProxy) proxySingleQuery(msg *PGMessage, clientWriter *PGProtocolWriter, dbWriter *PGProtocolWriter, dbReader *PGProtocolReader, dbName string) {
	// Log query text if available
	if p.config != nil && p.config.Debug.LogAllQueries {
		if q, err := ParseQuery(msg); err == nil {
			log.Printf("QUERY: %s", q)
		}
	}
	log.Printf("Routing query to %s", dbName)

	// Send query
	if err := dbWriter.WriteMessage(msg); err != nil {
		log.Printf("Failed to send query to %s: %v", dbName, err)
		errorMsg := CreateErrorMessage("ERROR", "08006", fmt.Sprintf("Failed to send query to %s", dbName))
		clientWriter.WriteMessage(errorMsg)
		// Ensure client is released from wait state
		readyMsg := &PGMessage{Type: msgTypeReadyForQuery, Data: []byte{'I'}}
		clientWriter.WriteMessage(readyMsg)
		return
	}

	// Forward all response messages
	completedOK := false
	for {
		response, err := dbReader.ReadMessage()
		if err != nil {
			log.Printf("Failed to read response from %s: %v", dbName, err)
			// Send error to client and ReadyForQuery to prevent hang
			errorMsg := CreateErrorMessage("ERROR", "08006", fmt.Sprintf("Failed to read response from %s", dbName))
			clientWriter.WriteMessage(errorMsg)
			readyMsg := &PGMessage{Type: msgTypeReadyForQuery, Data: []byte{'I'}}
			clientWriter.WriteMessage(readyMsg)
			break
		}

		if err := clientWriter.WriteMessage(response); err != nil {
			log.Printf("Failed to send response to client: %v", err)
			break
		}

		// Check if this is the end of results
		if response.Type == msgTypeReadyForQuery {
			completedOK = true
			break
		}
	}
	if p.config != nil && p.config.Debug.LogAllQueries {
		status := "ok"
		if !completedOK {
			status = "error"
		}
		log.Printf("%s - %s", dbName, status)
	}
}

// isReadOnlyQuery returns true for simple read-only statements we can safely compare using pools.
func isReadOnlyQuery(query string) bool {
	// Very lightweight check: handle common read-only prefixes.
	s := strings.TrimSpace(strings.ToLower(query))
	if strings.HasPrefix(s, "select") || strings.HasPrefix(s, "with") {
		return true
	}
	return false
}

// isSchemaChangingDDL returns true for DDL statements that modify schema
func isSchemaChangingDDL(query string) bool {
	s := strings.TrimSpace(strings.ToLower(query))
	prefixes := []string{
		"create",
		"alter",
		"drop",
		"truncate",
		"rename",
		"reindex",
		"cluster", // on table
	}
	for _, pfx := range prefixes {
		if strings.HasPrefix(s, pfx) {
			return true
		}
	}
	return false
}

// isMutatingDML returns true for DML that changes data
func isMutatingDML(query string) bool {
	s := strings.TrimSpace(strings.ToLower(query))
	prefixes := []string{
		"insert",
		"update",
		"delete",
		"merge",
	}
	for _, pfx := range prefixes {
		if strings.HasPrefix(s, pfx) {
			return true
		}
	}
	return false
}

// isTxnBegin detects start of an explicit transaction
func isTxnBegin(query string) bool {
	s := strings.TrimSpace(strings.ToLower(query))
	return strings.HasPrefix(s, "begin") || strings.HasPrefix(s, "start transaction")
}

// isTxnEnd detects end of an explicit transaction
func isTxnEnd(query string) bool {
	s := strings.TrimSpace(strings.ToLower(query))
	return strings.HasPrefix(s, "commit") || strings.HasPrefix(s, "rollback") || strings.HasPrefix(s, "end") || strings.HasPrefix(s, "abort")
}

// isBootstrapDDL returns true for narrowly scoped DDL needed to bootstrap the secondary
// so that dual-execution can begin (e.g., creating missing roles or databases).
func isBootstrapDDL(query string) bool {
	s := strings.TrimSpace(strings.ToLower(query))
	if strings.HasPrefix(s, "create database") {
		return true
	}
	if strings.HasPrefix(s, "create role") {
		return true
	}
	return false
}

// maybeEnableBackendTLS attempts to negotiate TLS with a backend using PostgreSQL SSLRequest.
// If backend responds 'S', it wraps the connection with TLS. If 'N', it returns the original conn.
// Returns error only when negotiation or handshake fails unexpectedly.
func (p *DualExecutionProxy) maybeEnableBackendTLS(conn net.Conn, serverName string, backendName string) (net.Conn, error) {
	// Build SSLRequest: length=8, code=80877103
	sslReq := make([]byte, 8)
	binary.BigEndian.PutUint32(sslReq[0:4], 8)
	binary.BigEndian.PutUint32(sslReq[4:8], 80877103)

	if _, err := conn.Write(sslReq); err != nil {
		return conn, fmt.Errorf("failed to send SSLRequest to %s: %w", backendName, err)
	}

	// Read single-byte response: 'S' or 'N'
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		// Non-fatal; continue without deadline
	}
	resp := make([]byte, 1)
	if _, err := io.ReadFull(conn, resp); err != nil {
		// Clear deadline and fail; backend closed or not speaking protocol
		_ = conn.SetReadDeadline(time.Time{})
		return conn, fmt.Errorf("failed to read SSL negotiation response from %s: %w", backendName, err)
	}
	_ = conn.SetReadDeadline(time.Time{})

	switch resp[0] {
	case 'S':
		// Backend accepts TLS; perform handshake
		cfg := &tls.Config{
			ServerName:         serverName,
			InsecureSkipVerify: true, // TODO: make configurable; minimal viable fix for now
		}
		tlsConn := tls.Client(conn, cfg)
		if err := tlsConn.Handshake(); err != nil {
			return conn, fmt.Errorf("TLS handshake with %s failed: %w", backendName, err)
		}
		log.Printf("%s backend TLS enabled (SNI=%s)", backendName, serverName)
		return tlsConn, nil
	case 'N':
		log.Printf("%s backend does not support TLS; continuing in plaintext", backendName)
		return conn, nil
	default:
		return conn, fmt.Errorf("unexpected SSL negotiation response from %s: 0x%X", backendName, resp[0])
	}
}

// parseBackendError extracts Severity (S/V), Code (C), and Message (M) from a backend ErrorResponse payload.
// Returns empty strings if fields are not present.
func parseBackendError(data []byte) (severity, code, message string) {
	// ErrorResponse is a sequence of fields: <type byte><cstring> ... ending with a terminator 0x00
	i := 0
	for i < len(data) {
		if data[i] == 0 { // terminator
			break
		}
		fieldType := data[i]
		i++
		// read cstring
		start := i
		for i < len(data) && data[i] != 0 {
			i++
		}
		val := ""
		if start <= len(data) {
			val = string(data[start:i])
		}
		// skip null terminator if present
		if i < len(data) && data[i] == 0 {
			i++
		}
		switch fieldType {
		case 'S': // Severity (localized)
			if severity == "" {
				severity = val
			}
		case 'V': // Severity (non-localized)
			if severity == "" {
				severity = val
			}
		case 'C':
			code = val
		case 'M':
			message = val
		}
	}
	return
}
