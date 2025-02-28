package integration

import (
	"database/sql"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/qvad/twinkly/pkg/config"
	"github.com/qvad/twinkly/pkg/proxy"
)

// Global proxy instance to ensure we only start it once if needed,
// or we can start/stop per test. For simplicity, we'll start it once per test suite if possible,
// but Go testing runs independent tests. We'll verify if port is listening.

func startProxy(t *testing.T) *proxy.DualExecutionProxy {
	// Check if port 5431 is already taken (proxy running)
	conn, err := net.DialTimeout("tcp", "localhost:5431", 100*time.Millisecond)
	if err == nil {
		conn.Close()
		t.Log("Proxy already running on port 5431")
		return nil // Assume already running
	}

	// Load config
	// We need to resolve env vars manually or let the config loader do it if it supports it.
	// The provided config uses ${VAR} syntax which hocon supports if enabled,
	// or we might need to rely on the config loader's implementation.
	// Looking at pkg/config/config.go would be wise, but let's assume standard behavior for now.
	// actually, let's verify pkg/config/config.go behavior.

	// Quick fix: Set env vars if not set, though Docker sets them.

	cfg, err := config.LoadConfig("test/config/integration-test.conf")
	if err != nil {
		// Fallback for running from root
		cfg, err = config.LoadConfig("../config/integration-test.conf")
		if err != nil {
			t.Fatalf("Failed to load config: %v", err)
		}
	}

	// Override hosts to match docker service names if running in docker
	if host := os.Getenv("POSTGRES_HOST"); host != "" {
		cfg.Proxy.PostgreSQL.Host = host
	}
	if host := os.Getenv("YUGABYTE_HOST"); host != "" {
		cfg.Proxy.YugabyteDB.Host = host
	}

	p := proxy.NewDualExecutionProxy(cfg)

	go func() {
		if err := p.Start(); err != nil {
			// It might fail if address already in use, which is fine if we raced
			t.Logf("Proxy server stopped: %v", err)
		}
	}()

	// Wait for port 5431
	t.Log("Waiting for Proxy to start on port 5431...")
	for i := 0; i < 50; i++ {
		conn, err := net.DialTimeout("tcp", "localhost:5431", 100*time.Millisecond)
		if err == nil {
			conn.Close()
			t.Log("✅ Proxy is ready")
			return p
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("Proxy failed to start on port 5431")
	return nil
}

func connectToProxy(t *testing.T) *sql.DB {
	// connect to localhost:5431
	// user/pass/db should match what the backend expects because the proxy forwards startup packet?
	// The config says postgres-only-patterns, etc.
	// We use the same creds as backend.

	connStr := "host=localhost port=5431 user=testuser password=testpass dbname=testdb sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("Failed to open Proxy connection: %v", err)
	}
	return db
}

func TestProxy_DualWrite(t *testing.T) {
	startProxy(t)

	// Give proxy a moment to be fully ready
	time.Sleep(1 * time.Second)

	dbProxy := connectToProxy(t)
	defer dbProxy.Close()

	// Create a unique table for this test to avoid conflicts
	tableName := fmt.Sprintf("proxy_test_%d", time.Now().UnixNano())

	// We need to create table. DDL might be forwarded to both?
	// The architecture says "Write: Executed on both".
	// Let's assume DDL is also forwarded.

	_, err := dbProxy.Exec(fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, value TEXT)", tableName))
	if err != nil {
		t.Fatalf("Failed to create table via proxy: %v", err)
	}

	// Insert via Proxy
	testValue := "dual_write_test"
	_, err = dbProxy.Exec(fmt.Sprintf("INSERT INTO %s (id, value) VALUES (1, $1)", tableName), testValue)
	if err != nil {
		t.Fatalf("Failed to insert via proxy: %v", err)
	}

	// Verify in Postgres Direct
	pgDB := connectToPostgreSQL(t)
	defer pgDB.Close()

	var pgVal string
	err = pgDB.QueryRow(fmt.Sprintf("SELECT value FROM %s WHERE id=1", tableName)).Scan(&pgVal)
	if err != nil {
		t.Fatalf("Failed to find row in PostgreSQL: %v", err)
	}
	if pgVal != testValue {
		t.Errorf("PostgreSQL value mismatch: got %s, want %s", pgVal, testValue)
	}

	// Verify in YugabyteDB Direct
	ybDB := connectToYugabyteDB(t)
	defer ybDB.Close()

	var ybVal string
	err = ybDB.QueryRow(fmt.Sprintf("SELECT value FROM %s WHERE id=1", tableName)).Scan(&ybVal)
	if err != nil {
		t.Fatalf("Failed to find row in YugabyteDB: %v", err)
	}
	if ybVal != testValue {
		t.Errorf("YugabyteDB value mismatch: got %s, want %s", ybVal, testValue)
	}

	t.Log("✅ Dual write verified successfully")

	// Cleanup
	dbProxy.Exec(fmt.Sprintf("DROP TABLE %s", tableName))
}

func TestProxy_InconsistencyDetection(t *testing.T) {
	startProxy(t)
	time.Sleep(1 * time.Second)

	dbProxy := connectToProxy(t)
	defer dbProxy.Close()

	tableName := fmt.Sprintf("inconsistency_test_%d", time.Now().UnixNano())

	// Create table via direct connections to ensure state
	pgDB := connectToPostgreSQL(t)
	defer pgDB.Close()
	ybDB := connectToYugabyteDB(t)
	defer ybDB.Close()

	_, err := pgDB.Exec(fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, value TEXT)", tableName))
	if err != nil {
		t.Fatal(err)
	}
	_, err = ybDB.Exec(fmt.Sprintf("CREATE TABLE %s (id INT PRIMARY KEY, value TEXT)", tableName))
	if err != nil {
		t.Fatal(err)
	}

	// Insert DIFFERENT data directly
	_, err = pgDB.Exec(fmt.Sprintf("INSERT INTO %s (id, value) VALUES (1, 'apple')", tableName))
	if err != nil {
		t.Fatal(err)
	}
	_, err = ybDB.Exec(fmt.Sprintf("INSERT INTO %s (id, value) VALUES (1, 'banana')", tableName))
	if err != nil {
		t.Fatal(err)
	}

	// Read via Proxy
	// Config has "fail-on-differences = true", so this should return an error
	var result string
	err = dbProxy.QueryRow(fmt.Sprintf("SELECT value FROM %s WHERE id=1", tableName)).Scan(&result)

	if err == nil {
		t.Errorf("Expected error due to inconsistency, but got result: %s", result)
	} else {
		t.Logf("✅ Successfully caught inconsistency error: %v", err)
	}

	// Cleanup
	pgDB.Exec(fmt.Sprintf("DROP TABLE %s", tableName))
	ybDB.Exec(fmt.Sprintf("DROP TABLE %s", tableName))
}
