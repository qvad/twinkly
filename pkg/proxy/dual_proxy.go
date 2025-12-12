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
	config                  *config.Config
	resolver                *DualDatabaseResolver
	pgAddr                  string
	ybAddr                  string
	security                *SecurityConfig
	constraintDetector      *comparator.ConstraintDivergenceDetector
	reporter                *reporter.InconsistencyReporter
	slowQueryAnalyzer       *comparator.SlowQueryAnalyzer
	resultValidator         *comparator.ResultValidator
	secondaryReady          bool
	inTransaction           bool
	secondaryDisabledLogged bool
	secondaryBlocked        bool
	secondaryReason         string
	lastParsedSQL           string
	requireSecondary        bool
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
func NewDualExecutionProxyWithReporter(config *config.Config, reporter *reporter.InconsistencyReporter) *DualExecutionProxy {
	pgAddr := fmt.Sprintf("%s:%d", config.Proxy.PostgreSQL.Host, config.Proxy.PostgreSQL.Port)
	ybAddr := fmt.Sprintf("%s:%d", config.Proxy.YugabyteDB.Host, config.Proxy.YugabyteDB.Port)

	reporter.AttachConfig(config)
	return &DualExecutionProxy{
		config:             config,
		pgAddr:             pgAddr,
		ybAddr:             ybAddr,
		security:           NewSecurityConfig(),
		constraintDetector: comparator.NewConstraintDivergenceDetector(),
		reporter:           reporter,
		resultValidator:    comparator.NewResultValidator(config.Comparison.FailOnDifferences, config.Comparison.SortBeforeCompare),
		requireSecondary:   config.Comparison.RequireSecondary,
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

func (p *DualExecutionProxy) logSecondaryDisabled(reason string) {
	if !p.secondaryDisabledLogged {
		log.Printf("Secondary disabled: %s", reason)
		p.secondaryDisabledLogged = true
	}
}

func (p *DualExecutionProxy) HandleConnection(clientConn net.Conn) {
	defer clientConn.Close()
	clientConn = logger.WrapConn(clientConn, "→Client", p.config)

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
		p.resolver = NewDualDatabaseResolver(p.config, p.pgAddr, p.ybAddr)
	}
	if p.config.Comparison.ReportSlowQueries {
		pgPool, ybPool := p.resolver.GetPools()
		if pgPool != nil && ybPool != nil {
			p.slowQueryAnalyzer = comparator.NewSlowQueryAnalyzer(p.config, pgPool, ybPool)
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
				p.executeDualQuery(msg, query, clientWriter, pgWriter, pgCh, ybWriter, ybCh)
			} else {
				p.proxySingleQuery(msg, clientWriter, pgWriter, pgCh, "PostgreSQL")
			}
		} else {
			p.proxyExtendedDualMessage(msg, clientWriter, pgWriter, pgCh, ybWriter, ybCh)
		}
	}
}

func (p *DualExecutionProxy) handleStartupPhase(client, pg, yb net.Conn) error {
	// Read first 8 bytes to check for SSLRequest or GSSENCRequest
	header := make([]byte, 8)
	if _, err := io.ReadFull(client, header); err != nil {
		return err
	}

	// Parse code from the second int32 (bytes 4-8)
	code := int32(binary.BigEndian.Uint32(header[4:8]))

	if code == protocol.SSLRequestCode {
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
		// Not SSLRequest, so it's likely the StartupMessage header we just read
		length := int(binary.BigEndian.Uint32(header[0:4]))
		if length < 8 {
			return fmt.Errorf("invalid message length: %d", length)
		}

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

	pgReader := protocol.NewPGProtocolReader(pg)
	clientWriter := protocol.NewPGProtocolWriter(client)

	for {
		msg, err := pgReader.ReadMessage()
		if err != nil {
			return err
		}
		clientWriter.WriteMessage(msg)
		if msg.Type == protocol.MsgTypeReadyForQuery {
			break
		}
	}

	go func() {
		ybReader := protocol.NewPGProtocolReader(yb)
		for {
			msg, err := ybReader.ReadMessage()
			if err != nil || msg.Type == protocol.MsgTypeReadyForQuery {
				break
			}
		}
	}()

	return nil
}

// executeDualQuery executes a query on both databases and compares results
func (p *DualExecutionProxy) executeDualQuery(msg *protocol.PGMessage, query string, clientWriter *protocol.PGProtocolWriter,
	pgWriter *protocol.PGProtocolWriter, pgCh <-chan BackendMessage,
	ybWriter *protocol.PGProtocolWriter, ybCh <-chan BackendMessage) error {

	if err := pgWriter.WriteMessage(msg); err != nil {
		clientWriter.WriteMessage(protocol.CreateErrorMessage("ERROR", "08006", "PG write failed"))
		return err
	}
	if err := ybWriter.WriteMessage(msg); err != nil {
		clientWriter.WriteMessage(protocol.CreateErrorMessage("ERROR", "08006", "YB write failed"))
		return err
	}

	var pgResults, ybResults []*protocol.PGMessage
	var pgErr, ybErr error
	var wg sync.WaitGroup
	wg.Add(2)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	go func() { defer wg.Done(); pgResults, pgErr = p.collectQueryResults(ctx, pgCh) }()
	go func() { defer wg.Done(); ybResults, ybErr = p.collectQueryResults(ctx, ybCh) }()
	wg.Wait()

	// Comparison logic
	mismatch := false
	var differences []string

	if (pgErr == nil) != (ybErr == nil) {
		mismatch = true
		differences = append(differences, "Error divergence")
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
		p.secondaryBlocked = true
		p.logSecondaryDisabled("result mismatch")

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

		if p.slowQueryAnalyzer != nil && p.config.Comparison.AI.Enabled {
			go func() {
				if isReadOnlyQuery(query) {
					p.slowQueryAnalyzer.AnalyzeQuery(query)
				}
			}()
		}

		if p.config.Comparison.FailOnDifferences {
			clientWriter.WriteMessage(protocol.CreateErrorMessage("ERROR", "XX000", "Result mismatch detected"))
			clientWriter.WriteMessage(&protocol.PGMessage{Type: protocol.MsgTypeReadyForQuery, Data: []byte{'I'}})
			return fmt.Errorf("mismatch detected")
		}
	}

	// Forward results
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

func (p *DualExecutionProxy) proxySingleQuery(msg *protocol.PGMessage, clientWriter *protocol.PGProtocolWriter, dbWriter *protocol.PGProtocolWriter, dbCh <-chan BackendMessage, dbName string) {
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

// Helpers
func isReadOnlyQuery(q string) bool {
	s := strings.ToLower(q)
	return strings.HasPrefix(s, "select") || strings.HasPrefix(s, "with")
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
