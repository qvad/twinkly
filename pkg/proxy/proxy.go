package proxy

import (
	"fmt"
	"log"
	"net"
	"strings"
	"sync"

	"github.com/qvad/twinkly/pkg/config"
	"github.com/qvad/twinkly/pkg/reporter"
)

// SimpleProxy implements a basic PostgreSQL proxy without pgbroker dependency
type SimpleProxy struct {
	config   *config.Config
	resolver *DualDatabaseResolver
	listener net.Listener
	shutdown chan struct{}
	wg       sync.WaitGroup
	reporter *reporter.InconsistencyReporter
	mu       sync.Mutex
}

// NewSimpleProxy creates a new proxy instance
func NewSimpleProxy(config *config.Config) *SimpleProxy {
	rep := reporter.NewInconsistencyReporter()
	rep.AttachConfig(config)
	return &SimpleProxy{
		config:   config,
		shutdown: make(chan struct{}),
		reporter: rep,
	}
}

// Start starts the proxy server
func (p *SimpleProxy) Start(listenAddr string) error {
	var err error
	p.listener, err = net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", listenAddr, err)
	}

	// Create resolver
	postgresAddr := fmt.Sprintf("%s:%d", p.config.Proxy.PostgreSQL.Host, p.config.Proxy.PostgreSQL.Port)
	yugabyteAddr := fmt.Sprintf("%s:%d", p.config.Proxy.YugabyteDB.Host, p.config.Proxy.YugabyteDB.Port)

	p.resolver = NewDualDatabaseResolver(p.config, postgresAddr, yugabyteAddr)

	// Initialize comparison pools if enabled
	if p.config.Comparison.Enabled {
		if err := p.resolver.InitializePools(); err != nil {
			return fmt.Errorf("failed to initialize comparison pools: %w", err)
		}
		log.Printf("✓ Comparison mode enabled with source of truth: %s", p.config.Comparison.SourceOfTruth)
	}

	log.Printf("✓ Proxy server started successfully")
	log.Printf("Starting dual-database proxy on %s", listenAddr)
	log.Printf("PostgreSQL backend: %s", postgresAddr)
	log.Printf("YugabyteDB backend: %s", yugabyteAddr)

	go p.acceptConnections()
	return nil
}

// acceptConnections handles incoming client connections
func (p *SimpleProxy) acceptConnections() {
	for {
		select {
		case <-p.shutdown:
			return
		default:
			conn, err := p.listener.Accept()
			if err != nil {
				if !strings.Contains(err.Error(), "use of closed network connection") {
					log.Printf("Accept error: %v", err)
				}
				continue
			}

			p.wg.Add(1)
			go p.handleConnection(conn)
		}
	}
}

// handleConnection handles a single client connection
func (p *SimpleProxy) handleConnection(clientConn net.Conn) {
	defer p.wg.Done()

	// Use the dual execution proxy with shared reporter for actual query handling
	dualProxy := NewDualExecutionProxyWithReporter(p.config, p.reporter)
	// Reuse the initialized resolver (with pools) for background comparisons
	dualProxy.resolver = p.resolver
	dualProxy.HandleConnection(clientConn)
}

// Shutdown gracefully shuts down the proxy
func (p *SimpleProxy) Shutdown() {
	close(p.shutdown)

	if p.listener != nil {
		p.listener.Close()
	}

	// Wait for all connections to finish
	p.wg.Wait()

	// Generate summary report from all dual proxies
	p.generateInconsistencyReport()

	// Clean up resolver
	if p.resolver != nil {
		if err := p.resolver.Close(); err != nil {
			log.Printf("Error closing resolver: %v", err)
		}
	}
}

// generateInconsistencyReport generates and displays the final inconsistency report
func (p *SimpleProxy) generateInconsistencyReport() {
	if p.reporter != nil {
		summary := p.reporter.GenerateSummaryReport()
		log.Printf("\n%s", summary)
	}
}
