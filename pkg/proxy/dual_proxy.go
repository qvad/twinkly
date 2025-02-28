package proxy

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/qvad/twinkly/pkg/comparator"
	"github.com/qvad/twinkly/pkg/config"
	logger "github.com/qvad/twinkly/pkg/log"
	"github.com/qvad/twinkly/pkg/protocol"
	"github.com/qvad/twinkly/pkg/reporter"
)

// DualExecutionProxy implements the actual dual-execution logic
type DualExecutionProxy struct {
	config             *config.Config
	resolver           *SideChannelManager
	pgAddr             string
	ybAddr             string
	security           *SecurityConfig
	constraintDetector *comparator.ConstraintDivergenceDetector
	reporter           *reporter.InconsistencyReporter
	resultValidator    *comparator.ResultValidator
	requireSecondary   bool
}

// sessionState holds per-connection state to ensure isolation between client sessions.
// Each client connection gets its own sessionState instance to prevent state bleeding.
type sessionState struct {
	proxy                   *DualExecutionProxy
	slowQueryAnalyzer       *comparator.SlowQueryAnalyzer
	secondaryReady          bool
	inTransaction           bool
	secondaryDisabledLogged bool
	secondaryBlocked        bool
	secondaryReason         string
	lastParsedSQL           string
}

// NewDualExecutionProxy creates a new dual execution proxy
func NewDualExecutionProxy(config *config.Config) *DualExecutionProxy {
	pgAddr := fmt.Sprintf("%s:%d", config.Proxy.PostgreSQL.Host, config.Proxy.PostgreSQL.Port)
	ybAddr := fmt.Sprintf("%s:%d", config.Proxy.YugabyteDB.Host, config.Proxy.YugabyteDB.Port)

	rep := reporter.NewInconsistencyReporter()
	rep.AttachConfig(config)
	return &DualExecutionProxy{
		config:             config,
		pgAddr:             pgAddr,
		ybAddr:             ybAddr,
		security:           NewSecurityConfig(),
		constraintDetector: comparator.NewConstraintDivergenceDetector(),
		reporter:           rep,
		resultValidator:    comparator.NewResultValidator(config.Comparison.FailOnDifferences, config.Comparison.SortBeforeCompare),
		requireSecondary:   config.Comparison.RequireSecondary,
	}
}

// NewDualExecutionProxyWithReporter creates a new dual execution proxy with a shared reporter
func NewDualExecutionProxyWithReporter(config *config.Config, rep *reporter.InconsistencyReporter) *DualExecutionProxy {
	pgAddr := fmt.Sprintf("%s:%d", config.Proxy.PostgreSQL.Host, config.Proxy.PostgreSQL.Port)
	ybAddr := fmt.Sprintf("%s:%d", config.Proxy.YugabyteDB.Host, config.Proxy.YugabyteDB.Port)

	rep.AttachConfig(config)
	return &DualExecutionProxy{
		config:             config,
		pgAddr:             pgAddr,
		ybAddr:             ybAddr,
		security:           NewSecurityConfig(),
		constraintDetector: comparator.NewConstraintDivergenceDetector(),
		reporter:           rep,
		resultValidator:    comparator.NewResultValidator(config.Comparison.FailOnDifferences, config.Comparison.SortBeforeCompare),
		requireSecondary:   config.Comparison.RequireSecondary,
	}
}

// newSessionState creates a new per-connection session state
func (p *DualExecutionProxy) newSessionState() *sessionState {
	return &sessionState{
		proxy: p,
	}
}

func (p *DualExecutionProxy) Start() error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", p.config.Proxy.ListenPort))
	if err != nil {
		return err
	}
	defer listener.Close()
	log.Printf("Proxy listening on %d", p.config.Proxy.ListenPort)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue
		}
		go p.HandleConnection(conn)
	}
}

func (s *sessionState) logSecondaryDisabled(reason string) {
	if !s.secondaryDisabledLogged {
		log.Printf("Secondary disabled: %s", reason)
		s.secondaryDisabledLogged = true
	}
}

func (p *DualExecutionProxy) HandleConnection(clientConn net.Conn) {
	defer clientConn.Close()
	clientConn = logger.WrapConn(clientConn, "→Client", p.config)

	// Create per-session state to ensure isolation between client connections
	session := p.newSessionState()

	pgConn, err := net.DialTimeout("tcp", p.pgAddr, 10*time.Second)
	if err != nil {
		log.Printf("Failed to dial PostgreSQL (%s): %v", p.pgAddr, err)
		return
	}
	// Disable backend TLS for now to avoid SCRAM-SHA-256-PLUS issues with non-SSL clients
	// pgConn, _ = p.maybeEnableBackendTLS(pgConn, p.config.Proxy.PostgreSQL.Host, "PostgreSQL")
	defer pgConn.Close()

	ybConn, err := net.DialTimeout("tcp", p.ybAddr, 10*time.Second)
	if err != nil {
		log.Printf("Failed to dial YugabyteDB (%s): %v", p.ybAddr, err)
		return
	}
	// ybConn, _ = p.maybeEnableBackendTLS(ybConn, p.config.Proxy.YugabyteDB.Host, "YugabyteDB")
	defer ybConn.Close()

	if p.resolver == nil {
		p.resolver = NewSideChannelManager(p.config, p.pgAddr, p.ybAddr)
	}
	if p.config.Comparison.ReportSlowQueries {
		pgPool, ybPool := p.resolver.GetPools()
		if pgPool != nil && ybPool != nil {
			session.slowQueryAnalyzer = comparator.NewSlowQueryAnalyzer(p.config, pgPool, ybPool)
		}
	}

	if err := p.handleStartupPhase(clientConn, pgConn, ybConn); err != nil {
		log.Printf("Startup failed: %v", err)
		return
	}

	clientReader := protocol.NewPGProtocolReader(clientConn)
	clientWriter := protocol.NewPGProtocolWriter(clientConn)
	pgWriter := protocol.NewPGProtocolWriter(pgConn)
	ybWriter := protocol.NewPGProtocolWriter(ybConn)

	pgCh := make(chan BackendMessage, 1000)
	ybCh := make(chan BackendMessage, 1000)
	go p.runBackendReader(protocol.NewPGProtocolReader(pgConn), pgCh, "PostgreSQL")
	go p.runBackendReader(protocol.NewPGProtocolReader(ybConn), ybCh, "YugabyteDB")

	for {
		msg, err := clientReader.ReadMessage()
		if err != nil {
			break
		}
		if protocol.IsTerminateMessage(msg) {
			pgWriter.WriteMessage(msg)
			ybWriter.WriteMessage(msg)
			break
		}
		if protocol.IsQueryMessage(msg) {
			query, _ := protocol.ParseQuery(msg)
			if p.config.Comparison.Enabled {
				session.executeDualQuery(msg, query, clientWriter, pgWriter, pgCh, ybWriter, ybCh)
			} else {
				session.proxySingleQuery(msg, clientWriter, pgWriter, pgCh, "PostgreSQL")
			}
		} else {
			session.proxyExtendedDualMessage(msg, clientWriter, pgWriter, pgCh, ybWriter, ybCh)
		}
	}
}

func (p *DualExecutionProxy) handleStartupPhase(client, pg, yb net.Conn) error {
	// Read first 8 bytes to check for SSLRequest or GSSENCRequest
	header := make([]byte, 8)
	if _, err := io.ReadFull(client, header); err != nil {
		log.Printf("[STARTUP] Failed to read initial 8 bytes: %v", err)
		return err
	}

	// Parse code from the second int32 (bytes 4-8)
	code := int32(binary.BigEndian.Uint32(header[4:8]))
	length := int(binary.BigEndian.Uint32(header[0:4]))
	log.Printf("[STARTUP] Received initial packet: length=%d, code=%d (0x%08X), header=%x", length, code, code, header)

	if code == protocol.SSLRequestCode {
		log.Printf("[STARTUP] SSLRequest detected, rejecting with 'N'")
		// Reject SSLRequest with 'N'
		if _, err := client.Write([]byte{'N'}); err != nil {
			return err
		}

		// Now read the real startup message (Length + Body)
		// First read length (4 bytes)
		lengthBuf := make([]byte, 4)
		if _, err := io.ReadFull(client, lengthBuf); err != nil {
			return err
		}
		length := int(binary.BigEndian.Uint32(lengthBuf))

		// Read the rest of the message
		if length < 4 {
			return fmt.Errorf("invalid startup message length: %d", length)
		}
		body := make([]byte, length-4)
		if _, err := io.ReadFull(client, body); err != nil {
			return err
		}

		// Reconstruct full packet
		fullPacket := make([]byte, length)
		copy(fullPacket[0:4], lengthBuf)
		copy(fullPacket[4:], body)

		// Forward to backends
		if _, err := pg.Write(fullPacket); err != nil {
			return err
		}
		if _, err := yb.Write(fullPacket); err != nil {
			return err
		}

	} else {
		log.Printf("[STARTUP] Not SSLRequest, treating as StartupMessage")
		// Not SSLRequest, so it's likely the StartupMessage header we just read
		length := int(binary.BigEndian.Uint32(header[0:4]))
		if length < 8 {
			return fmt.Errorf("invalid message length: %d", length)
		}
		log.Printf("[STARTUP] StartupMessage length=%d, protocol version=0x%08X", length, code)

		// Read the rest of the message
		remaining := length - 8
		rest := make([]byte, remaining)
		if _, err := io.ReadFull(client, rest); err != nil {
			return err
		}

		// Reconstruct full packet
		fullPacket := make([]byte, length)
		copy(fullPacket[0:8], header)
		copy(fullPacket[8:], rest)

		// Forward to backends
		if _, err := pg.Write(fullPacket); err != nil {
			return err
		}
		if _, err := yb.Write(fullPacket); err != nil {
			return err
		}
	}

	log.Printf("[STARTUP] Forwarded startup message to backends, now handling auth phase")

	// Handle auth for PG and YB independently in parallel
	// PG auth is forwarded to client, YB auth is handled silently
	var wg sync.WaitGroup
	var pgErr, ybErr error

	pgReader := protocol.NewPGProtocolReader(pg)
	ybReader := protocol.NewPGProtocolReader(yb)
	clientReader := protocol.NewPGProtocolReader(client)
	clientWriter := protocol.NewPGProtocolWriter(client)
	pgWriter := protocol.NewPGProtocolWriter(pg)

	// Handle YugabyteDB auth in background (it likely uses trust auth)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			msg, err := ybReader.ReadMessage()
			if err != nil {
				log.Printf("[STARTUP] Error reading from YB during auth: %v", err)
				ybErr = fmt.Errorf("YugabyteDB auth failed: %w", err)
				return
			}
			log.Printf("[STARTUP] YB auth message: type=%c (0x%02X), len=%d", msg.Type, msg.Type, len(msg.Data))
			if msg.Type == protocol.MsgTypeErrorResponse {
				log.Printf("[STARTUP] ERROR from YB: %s", parseErrorFields(msg.Data))
				ybErr = fmt.Errorf("YugabyteDB returned error during startup")
				return
			}
			if msg.Type == protocol.MsgTypeReadyForQuery {
				log.Printf("[STARTUP] YugabyteDB ready")
				return
			}
			// YB auth messages are not forwarded to client - we only use PG's auth flow
		}
	}()

	// Handle PostgreSQL auth - this is the "source of truth" for client auth
	for {
		msg, err := pgReader.ReadMessage()
		if err != nil {
			log.Printf("[STARTUP] Error reading from PG: %v", err)
			pgErr = err
			break
		}
		log.Printf("[STARTUP] PG -> Client: type=%c (0x%02X), len=%d", msg.Type, msg.Type, len(msg.Data))

		// Log error messages in detail
		if msg.Type == protocol.MsgTypeErrorResponse {
			log.Printf("[STARTUP] ERROR from PG: %s", parseErrorFields(msg.Data))
		}

		clientWriter.WriteMessage(msg)

		if msg.Type == protocol.MsgTypeReadyForQuery {
			log.Printf("[STARTUP] PostgreSQL ready, startup complete")
			break
		}

		// If PG requests auth, get response from client and forward ONLY to PG
		if msg.Type == protocol.MsgTypeAuthentication && len(msg.Data) >= 4 {
			authType := binary.BigEndian.Uint32(msg.Data[0:4])
			log.Printf("[STARTUP] PG Auth type: %d", authType)

			// Auth types requiring client response:
			// 0 = AuthenticationOk (no response needed)
			// 3 = CleartextPassword
			// 5 = MD5Password
			// 10 = SASL
			// 11 = SASLContinue
			// 12 = SASLFinal (no response needed)
			if authType == 3 || authType == 5 || authType == 10 || authType == 11 {
				log.Printf("[STARTUP] Waiting for client auth response...")
				clientMsg, err := clientReader.ReadMessage()
				if err != nil {
					log.Printf("[STARTUP] Error reading client auth response: %v", err)
					pgErr = err
					break
				}
				log.Printf("[STARTUP] Client -> PG: type=%c (0x%02X), len=%d", clientMsg.Type, clientMsg.Type, len(clientMsg.Data))
				// Forward ONLY to PG, not to YB (YB has different auth)
				if err := pgWriter.WriteMessage(clientMsg); err != nil {
					pgErr = err
					break
				}
			}
		}
	}

	// Wait for YB auth to complete
	wg.Wait()

	if pgErr != nil {
		return pgErr
	}
	if ybErr != nil {
		return ybErr
	}

	return nil
}

// executeDualQuery executes a query on both databases and compares results
// When PrimaryDatabase is "yugabytedb" (default), executes on YugabyteDB first.
// If YugabyteDB fails with an ignored error code (e.g., 0A000 for unsupported feature),
// PostgreSQL execution is skipped entirely and the YugabyteDB error is returned to the client.
func (s *sessionState) executeDualQuery(msg *protocol.PGMessage, query string, clientWriter *protocol.PGProtocolWriter,
	pgWriter *protocol.PGProtocolWriter, pgCh <-chan BackendMessage,
	ybWriter *protocol.PGProtocolWriter, ybCh <-chan BackendMessage) error {

	p := s.proxy
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var pgResults, ybResults []*protocol.PGMessage
	var pgErr, ybErr error

	// Determine execution order based on primary database config
	primaryIsYugabyte := p.config.Comparison.PrimaryDatabase == "yugabytedb"

	if primaryIsYugabyte {
		// Execute on YugabyteDB first (primary)
		if err := ybWriter.WriteMessage(msg); err != nil {
			clientWriter.WriteMessage(protocol.CreateErrorMessage("ERROR", "08006", "YB write failed"))
			return err
		}
		ybResults, ybErr = s.collectQueryResults(ctx, ybCh)

		// Check if YugabyteDB failed with an ignored error code
		if ybErr != nil {
			errorCode := extractErrorCodeFromResults(ybResults)
			if errorCode != "" && p.config.IsIgnoredErrorCode(errorCode) {
				if p.config.Debug.LogAllQueries || p.config.Debug.LogProtocol {
					log.Printf("YugabyteDB failed with ignored error code %s, skipping PostgreSQL execution: %s", errorCode, query)
				}
				// Forward YugabyteDB results (error) to client and skip PostgreSQL
				for _, r := range ybResults {
					clientWriter.WriteMessage(r)
				}
				return nil
			}
		}

		// YugabyteDB didn't fail with ignored error, execute on PostgreSQL too
		if err := pgWriter.WriteMessage(msg); err != nil {
			clientWriter.WriteMessage(protocol.CreateErrorMessage("ERROR", "08006", "PG write failed"))
			return err
		}
		pgResults, pgErr = s.collectQueryResults(ctx, pgCh)

	} else {
		// Execute on PostgreSQL first (primary)
		if err := pgWriter.WriteMessage(msg); err != nil {
			clientWriter.WriteMessage(protocol.CreateErrorMessage("ERROR", "08006", "PG write failed"))
			return err
		}
		pgResults, pgErr = s.collectQueryResults(ctx, pgCh)

		// Check if PostgreSQL failed with an ignored error code
		if pgErr != nil {
			errorCode := extractErrorCodeFromResults(pgResults)
			if errorCode != "" && p.config.IsIgnoredErrorCode(errorCode) {
				if p.config.Debug.LogAllQueries || p.config.Debug.LogProtocol {
					log.Printf("PostgreSQL failed with ignored error code %s, skipping YugabyteDB execution: %s", errorCode, query)
				}
				// Forward PostgreSQL results (error) to client and skip YugabyteDB
				for _, r := range pgResults {
					clientWriter.WriteMessage(r)
				}
				return nil
			}
		}

		// PostgreSQL didn't fail with ignored error, execute on YugabyteDB too
		if err := ybWriter.WriteMessage(msg); err != nil {
			clientWriter.WriteMessage(protocol.CreateErrorMessage("ERROR", "08006", "YB write failed"))
			return err
		}
		ybResults, ybErr = s.collectQueryResults(ctx, ybCh)
	}

	// Comparison logic
	mismatch := false
	var differences []string

	if (pgErr == nil) != (ybErr == nil) {
		// Only flag as mismatch if error divergence reporting is enabled
		if p.config.Comparison.ReportErrorDivergence {
			mismatch = true
			differences = append(differences, "Error divergence")
		}
	} else if pgErr == nil {
		if p.resultValidator != nil {
			res, _ := p.resultValidator.ValidateResults(pgResults, ybResults)
			if res.ShouldFail {
				mismatch = true
				differences = res.Differences
			}
		}
	}

	if mismatch && p.config.Comparison.Enabled {
		s.secondaryBlocked = true
		s.logSecondaryDisabled("result mismatch")

		// Report inconsistency
		pgSummary := reporter.ResultSummary{
			Success:    pgErr == nil,
			RowCount:   len(extractDataRows(pgResults)),
			SampleData: extractSampleData(pgResults),
		}
		if pgErr != nil {
			pgSummary.Error = pgErr.Error()
		}
		ybSummary := reporter.ResultSummary{
			Success:    ybErr == nil,
			RowCount:   len(extractDataRows(ybResults)),
			SampleData: extractSampleData(ybResults),
		}
		if ybErr != nil {
			ybSummary.Error = ybErr.Error()
		}
		p.reporter.ReportInconsistency(reporter.DataValueMismatch, "HIGH", query, pgSummary, ybSummary, differences)

		if s.slowQueryAnalyzer != nil && p.config.Comparison.AI.Enabled {
			go func() {
				if isReadOnlyQuery(query) {
					s.slowQueryAnalyzer.AnalyzeQuery(query)
				}
			}()
		}

		// Always fail for mutating queries (DDL/DML), or if fail-on-differences is enabled
		if p.config.Comparison.FailOnDifferences || isMutatingQuery(query) {
			clientWriter.WriteMessage(protocol.CreateErrorMessage("ERROR", "TW001", "TWINKLY_VALIDATION_FAILED"))
			clientWriter.WriteMessage(&protocol.PGMessage{Type: protocol.MsgTypeReadyForQuery, Data: []byte{'I'}})
			return fmt.Errorf("TWINKLY_VALIDATION_FAILED: mismatch detected")
		}
	}

	// Forward results from source of truth
	var results []*protocol.PGMessage
	if p.config.Comparison.SourceOfTruth == "yugabytedb" {
		results = ybResults
	} else {
		results = pgResults
	}
	for _, r := range results {
		clientWriter.WriteMessage(r)
	}
	return nil
}

// extractErrorCodeFromResults extracts SQLSTATE error code from result messages
func extractErrorCodeFromResults(results []*protocol.PGMessage) string {
	for _, msg := range results {
		if msg.Type == protocol.MsgTypeErrorResponse {
			return protocol.ExtractErrorCode(msg)
		}
	}
	return ""
}

func (s *sessionState) proxySingleQuery(msg *protocol.PGMessage, clientWriter *protocol.PGProtocolWriter, dbWriter *protocol.PGProtocolWriter, dbCh <-chan BackendMessage, dbName string) {
	dbWriter.WriteMessage(msg)
	timeout := time.After(30 * time.Second)
	for {
		select {
		case <-timeout:
			// Just break loop on timeout to avoid hanging, upstream might eventually close conn
			log.Printf("Timeout waiting for response from %s", dbName)
			return
		case bm, ok := <-dbCh:
			if !ok {
				return
			}
			if bm.Err != nil {
				return
			}
			clientWriter.WriteMessage(bm.Msg)
			if bm.Msg.Type == protocol.MsgTypeReadyForQuery {
				return
			}
		}
	}
}

// runBackendReader implements the critical async reader pattern for deadlock prevention.
//
// Deadlock Prevention Strategy:
//
//	This goroutine continuously reads from a backend database connection and forwards
//	messages to a buffered channel. This prevents the main proxy logic from blocking
//	on backend reads, which is essential for the Extended Query Protocol.
//
// Why This Pattern is Necessary:
//  1. PostgreSQL Extended Query Protocol can buffer responses until Sync
//  2. If proxy blocks reading responses before sending Sync, deadlock occurs
//  3. Backend may be waiting for more messages while proxy waits for responses
//  4. Async readers ensure messages flow continuously without blocking
//
// Channel Management:
//   - Uses buffered channels (typically 1000 capacity) to prevent blocking
//   - Closes channel on connection error to signal end of stream
//   - Each backend connection gets its own dedicated reader goroutine
//
// This pattern is fundamental to the proxy's ability to handle concurrent
// dual-execution without deadlocking either PostgreSQL or YugabyteDB.
func (p *DualExecutionProxy) runBackendReader(reader *protocol.PGProtocolReader, ch chan<- BackendMessage, name string) {
	defer close(ch)
	for {
		msg, err := reader.ReadMessage()
		// Forward both successful messages and errors to the channel
		// The receiving code will handle error cases appropriately
		ch <- BackendMessage{Msg: msg, Err: err}
		if err != nil {
			// Connection error: terminate this reader goroutine
			// Channel closure signals downstream that no more messages will arrive
			return
		}
	}
}

func (p *DualExecutionProxy) maybeEnableBackendTLS(conn net.Conn, host, name string) (net.Conn, error) {
	sslReq := make([]byte, 8)
	binary.BigEndian.PutUint32(sslReq[0:4], 8)
	binary.BigEndian.PutUint32(sslReq[4:8], protocol.SSLRequestCode)
	conn.Write(sslReq)

	resp := make([]byte, 1)
	conn.Read(resp)
	if resp[0] == 'S' {
		cfg := &tls.Config{ServerName: host, InsecureSkipVerify: true}
		tlsConn := tls.Client(conn, cfg)
		if err := tlsConn.Handshake(); err != nil {
			return nil, err
		}
		return tlsConn, nil
	}
	return conn, nil
}

// Helpers
func isReadOnlyQuery(q string) bool {
	s := strings.ToLower(q)
	return strings.HasPrefix(s, "select") || strings.HasPrefix(s, "with")
}

// isMutatingQuery returns true if the query modifies data (DDL or DML)
func isMutatingQuery(q string) bool {
	s := strings.TrimSpace(strings.ToLower(q))
	// DDL operations
	if strings.HasPrefix(s, "create") || strings.HasPrefix(s, "alter") ||
		strings.HasPrefix(s, "drop") || strings.HasPrefix(s, "truncate") {
		return true
	}
	// DML operations
	if strings.HasPrefix(s, "insert") || strings.HasPrefix(s, "update") ||
		strings.HasPrefix(s, "delete") {
		return true
	}
	return false
}

func isBootstrapDDL(q string) bool {
	s := strings.TrimSpace(strings.ToLower(q))
	if strings.HasPrefix(s, "create database") {
		return true
	}
	if strings.HasPrefix(s, "create role") {
		return true
	}
	return false
}

// parseErrorFields parses PostgreSQL error response fields for logging
func parseErrorFields(data []byte) string {
	var result strings.Builder
	i := 0
	for i < len(data) {
		fieldType := data[i]
		if fieldType == 0 {
			break
		}
		i++
		// Find null terminator
		end := i
		for end < len(data) && data[end] != 0 {
			end++
		}
		value := string(data[i:end])
		i = end + 1

		switch fieldType {
		case 'S':
			result.WriteString(fmt.Sprintf("Severity=%s ", value))
		case 'C':
			result.WriteString(fmt.Sprintf("Code=%s ", value))
		case 'M':
			result.WriteString(fmt.Sprintf("Message=%s ", value))
		case 'D':
			result.WriteString(fmt.Sprintf("Detail=%s ", value))
		case 'H':
			result.WriteString(fmt.Sprintf("Hint=%s ", value))
		}
	}
	return result.String()
}
