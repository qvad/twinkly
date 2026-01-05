package proxy

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

const (
	// Test configuration
	proxyPort    = 5431
	postgresPort = 5432
	yugabytePort = 5433
	testDatabase = "test"
	testUser     = "postgres"
	testPassword = ""
)

// TestConfig holds test database configuration
type TestConfig struct {
	Name   string
	Port   int
	Direct bool // true = direct connection, false = through proxy
}

// getConnectionString builds a PostgreSQL connection string
func getConnectionString(config TestConfig) string {
	port := config.Port
	if !config.Direct {
		port = proxyPort
	}

	connStr := fmt.Sprintf("host=localhost port=%d user=%s dbname=%s sslmode=disable",
		port, testUser, testDatabase)

	if testPassword != "" {
		connStr += fmt.Sprintf(" password=%s", testPassword)
	}

	return connStr
}

// TestBasicConnectivity tests basic connection to the database
func TestBasicConnectivity(t *testing.T) {
	configs := []TestConfig{
		{Name: "PostgreSQL-Direct", Port: postgresPort, Direct: true},
		{Name: "PostgreSQL-Proxy", Port: postgresPort, Direct: false},
		// Uncomment when YugabyteDB is available
		// {Name: "YugabyteDB-Direct", Port: yugabytePort, Direct: true},
		// {Name: "YugabyteDB-Proxy", Port: yugabytePort, Direct: false},
	}

	for _, config := range configs {
		t.Run(config.Name, func(t *testing.T) {
			connStr := getConnectionString(config)
			db, err := sql.Open("postgres", connStr)
			if err != nil {
				t.Skipf("Skipping %s: %v", config.Name, err)
				return
			}
			defer db.Close()

			// Test ping
			err = db.Ping()
			if err != nil {
				t.Skipf("Database not available for %s: %v", config.Name, err)
				return
			}

			// Test simple query
			var result int
			err = db.QueryRow("SELECT 1").Scan(&result)
			if err != nil {
				t.Errorf("Failed to execute simple query: %v", err)
				return
			}

			if result != 1 {
				t.Errorf("Expected 1, got %d", result)
			}

			t.Logf("%s: Connection successful", config.Name)
		})
	}
}

// TestQueryExecution tests various query types
func TestQueryExecution(t *testing.T) {
	configs := []TestConfig{
		{Name: "PostgreSQL-Proxy", Port: postgresPort, Direct: false},
	}

	queries := []struct {
		name  string
		query string
		check func(*testing.T, *sql.Rows)
	}{
		{
			name:  "Simple SELECT",
			query: "SELECT 42 as answer",
			check: func(t *testing.T, rows *sql.Rows) {
				var answer int
				if rows.Next() {
					err := rows.Scan(&answer)
					if err != nil {
						t.Errorf("Scan failed: %v", err)
						return
					}
					if answer != 42 {
						t.Errorf("Expected 42, got %d", answer)
					}
				} else {
					t.Error("No rows returned")
				}
			},
		},
		{
			name:  "System Catalog Query",
			query: "SELECT count(*) FROM pg_class",
			check: func(t *testing.T, rows *sql.Rows) {
				var count int
				if rows.Next() {
					err := rows.Scan(&count)
					if err != nil {
						t.Errorf("Scan failed: %v", err)
						return
					}
					if count <= 0 {
						t.Errorf("Expected positive count, got %d", count)
					}
					t.Logf("pg_class has %d entries", count)
				}
			},
		},
		{
			name:  "Information Schema Query",
			query: "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' LIMIT 5",
			check: func(t *testing.T, rows *sql.Rows) {
				count := 0
				for rows.Next() {
					var tableName string
					err := rows.Scan(&tableName)
					if err != nil {
						t.Errorf("Scan failed: %v", err)
						return
					}
					t.Logf("Found table: %s", tableName)
					count++
				}
				t.Logf("Found %d public tables", count)
			},
		},
	}

	for _, config := range configs {
		t.Run(config.Name, func(t *testing.T) {
			connStr := getConnectionString(config)
			db, err := sql.Open("postgres", connStr)
			if err != nil {
				t.Skipf("Skipping %s: %v", config.Name, err)
				return
			}
			defer db.Close()

			if err := db.Ping(); err != nil {
				t.Skipf("Database not available: %v", err)
				return
			}

			for _, q := range queries {
				t.Run(q.name, func(t *testing.T) {
					rows, err := db.Query(q.query)
					if err != nil {
						t.Errorf("Query failed: %v", err)
						return
					}
					defer rows.Close()

					q.check(t, rows)
				})
			}
		})
	}
}

// TestPreparedStatements tests prepared statement handling
func TestPreparedStatements(t *testing.T) {
	configs := []TestConfig{
		{Name: "PostgreSQL-Proxy", Port: postgresPort, Direct: false},
	}

	for _, config := range configs {
		t.Run(config.Name, func(t *testing.T) {
			connStr := getConnectionString(config)
			db, err := sql.Open("postgres", connStr)
			if err != nil {
				t.Skipf("Skipping %s: %v", config.Name, err)
				return
			}
			defer db.Close()

			if err := db.Ping(); err != nil {
				t.Skipf("Database not available: %v", err)
				return
			}

			// Test simple prepared statement
			stmt, err := db.Prepare("SELECT $1::int + $2::int")
			if err != nil {
				t.Errorf("Prepare failed: %v", err)
				return
			}
			defer stmt.Close()

			var result int
			err = stmt.QueryRow(10, 32).Scan(&result)
			if err != nil {
				t.Errorf("QueryRow failed: %v", err)
				return
			}

			if result != 42 {
				t.Errorf("Expected 42, got %d", result)
			}

			// Test catalog query as prepared statement (this is what SQLancer does)
			stmt2, err := db.Prepare("SELECT opcname FROM pg_opclass WHERE opcname = $1")
			if err != nil {
				t.Errorf("Prepare catalog query failed: %v", err)
				return
			}
			defer stmt2.Close()

			var opcname string
			err = stmt2.QueryRow("array_ops").Scan(&opcname)
			if err != nil {
				if err == sql.ErrNoRows {
					t.Log("No array_ops found (might be normal)")
				} else {
					t.Errorf("QueryRow catalog query failed: %v", err)
				}
				return
			}

			if opcname != "array_ops" {
				t.Errorf("Expected 'array_ops', got '%s'", opcname)
			}
		})
	}
}

// TestTransactions tests transaction handling
func TestTransactions(t *testing.T) {
	configs := []TestConfig{
		{Name: "PostgreSQL-Proxy", Port: postgresPort, Direct: false},
	}

	for _, config := range configs {
		t.Run(config.Name, func(t *testing.T) {
			connStr := getConnectionString(config)
			db, err := sql.Open("postgres", connStr)
			if err != nil {
				t.Skipf("Skipping %s: %v", config.Name, err)
				return
			}
			defer db.Close()

			if err := db.Ping(); err != nil {
				t.Skipf("Database not available: %v", err)
				return
			}

			// Start transaction
			tx, err := db.Begin()
			if err != nil {
				t.Errorf("Begin transaction failed: %v", err)
				return
			}

			// Execute query in transaction
			var result int
			err = tx.QueryRow("SELECT 1").Scan(&result)
			if err != nil {
				t.Errorf("Query in transaction failed: %v", err)
				tx.Rollback()
				return
			}

			// Commit transaction
			err = tx.Commit()
			if err != nil {
				t.Errorf("Commit failed: %v", err)
				return
			}

			t.Log("Transaction completed successfully")
		})
	}
}

// TestConcurrentConnections tests multiple concurrent connections
func TestConcurrentConnections(t *testing.T) {
	configs := []TestConfig{
		{Name: "PostgreSQL-Proxy", Port: postgresPort, Direct: false},
	}

	for _, config := range configs {
		t.Run(config.Name, func(t *testing.T) {
			connStr := getConnectionString(config)

			// Test opening multiple connections
			connections := make([]*sql.DB, 5)
			for i := range connections {
				db, err := sql.Open("postgres", connStr)
				if err != nil {
					t.Skipf("Skipping %s: %v", config.Name, err)
					return
				}
				connections[i] = db
				defer db.Close()

				if err := db.Ping(); err != nil {
					t.Skipf("Database not available: %v", err)
					return
				}
			}

			// Execute queries on all connections concurrently
			done := make(chan bool, len(connections))
			for i, db := range connections {
				go func(idx int, database *sql.DB) {
					var result int
					err := database.QueryRow("SELECT $1::int", idx).Scan(&result)
					if err != nil {
						t.Errorf("Connection %d query failed: %v", idx, err)
					} else if result != idx {
						t.Errorf("Connection %d: expected %d, got %d", idx, idx, result)
					} else {
						t.Logf("Connection %d: success", idx)
					}
					done <- true
				}(i, db)
			}

			// Wait for all goroutines
			for i := 0; i < len(connections); i++ {
				<-done
			}
		})
	}
}

// Helper function to check if proxy is running
func isProxyRunning() bool {
	conn, err := sql.Open("postgres", fmt.Sprintf("host=localhost port=%d user=%s dbname=%s sslmode=disable",
		proxyPort, testUser, testDatabase))
	if err != nil {
		return false
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = conn.PingContext(ctx)
	return err == nil
}
