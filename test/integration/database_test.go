package integration

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// TestDatabaseConnectivity tests basic database connections
func TestDatabaseConnectivity(t *testing.T) {
	// Test PostgreSQL connection
	t.Run("PostgreSQL", func(t *testing.T) {
		db := connectToPostgreSQL(t)
		defer db.Close()

		var result int
		err := db.QueryRow("SELECT 1").Scan(&result)
		if err != nil {
			t.Fatalf("PostgreSQL query failed: %v", err)
		}
		if result != 1 {
			t.Errorf("Expected 1, got %d", result)
		}
	})

	// Test YugabyteDB connection
	t.Run("YugabyteDB", func(t *testing.T) {
		db := connectToYugabyteDB(t)
		defer db.Close()

		var result int
		err := db.QueryRow("SELECT 1").Scan(&result)
		if err != nil {
			t.Fatalf("YugabyteDB query failed: %v", err)
		}
		if result != 1 {
			t.Errorf("Expected 1, got %d", result)
		}
	})
}

// TestDatabaseSchemas verifies that both databases have the expected schema
func TestDatabaseSchemas(t *testing.T) {
	pgDB := connectToPostgreSQL(t)
	defer pgDB.Close()

	ybDB := connectToYugabyteDB(t)
	defer ybDB.Close()

	tables := []string{"users", "products", "orders"}

	for _, table := range tables {
		t.Run(fmt.Sprintf("Table_%s", table), func(t *testing.T) {
			// Check PostgreSQL
			var pgExists bool
			err := pgDB.QueryRow(`
				SELECT EXISTS (
					SELECT FROM information_schema.tables 
					WHERE table_name = $1
				)
			`, table).Scan(&pgExists)
			if err != nil {
				t.Fatalf("Failed to check PostgreSQL table %s: %v", table, err)
			}
			if !pgExists {
				t.Errorf("Table %s does not exist in PostgreSQL", table)
			}

			// Check YugabyteDB
			var ybExists bool
			err = ybDB.QueryRow(`
				SELECT EXISTS (
					SELECT FROM information_schema.tables 
					WHERE table_name = $1
				)
			`, table).Scan(&ybExists)
			if err != nil {
				t.Fatalf("Failed to check YugabyteDB table %s: %v", table, err)
			}
			if !ybExists {
				t.Errorf("Table %s does not exist in YugabyteDB", table)
			}
		})
	}
}

// TestDataConsistency verifies data consistency between databases
func TestDataConsistency(t *testing.T) {
	pgDB := connectToPostgreSQL(t)
	defer pgDB.Close()

	ybDB := connectToYugabyteDB(t)
	defer ybDB.Close()

	queries := map[string]string{
		"user_count":    "SELECT COUNT(*) FROM users",
		"product_count": "SELECT COUNT(*) FROM products",
		"order_count":   "SELECT COUNT(*) FROM orders",
		"total_revenue": "SELECT COALESCE(SUM(p.price * o.quantity), 0) FROM orders o JOIN products p ON o.product_id = p.id",
	}

	for name, query := range queries {
		t.Run(name, func(t *testing.T) {
			var pgResult, ybResult interface{}

			err := pgDB.QueryRow(query).Scan(&pgResult)
			if err != nil {
				t.Fatalf("PostgreSQL query failed: %v", err)
			}

			err = ybDB.QueryRow(query).Scan(&ybResult)
			if err != nil {
				t.Fatalf("YugabyteDB query failed: %v", err)
			}

			if fmt.Sprintf("%v", pgResult) != fmt.Sprintf("%v", ybResult) {
				t.Errorf("Data mismatch for %s: PostgreSQL=%v, YugabyteDB=%v",
					name, pgResult, ybResult)
			} else {
				t.Logf("%s: Both databases return %v", name, pgResult)
			}
		})
	}
}

// TestComplexQueries tests complex queries that might behave differently
func TestComplexQueries(t *testing.T) {
	pgDB := connectToPostgreSQL(t)
	defer pgDB.Close()

	ybDB := connectToYugabyteDB(t)
	defer ybDB.Close()

	// Test complex JOIN with aggregation
	t.Run("ComplexJoinAggregation", func(t *testing.T) {
		query := `
			SELECT 
				u.username,
				COUNT(o.id) as order_count,
				COALESCE(SUM(p.price * o.quantity), 0) as total_spent
			FROM users u
			LEFT JOIN orders o ON u.id = o.user_id
			LEFT JOIN products p ON o.product_id = p.id
			GROUP BY u.id, u.username
			ORDER BY total_spent DESC
		`

		pgRows := executeAndCountRows(t, pgDB, query)
		ybRows := executeAndCountRows(t, ybDB, query)

		if pgRows != ybRows {
			t.Errorf("Row count mismatch: PostgreSQL=%d, YugabyteDB=%d", pgRows, ybRows)
		} else {
			t.Logf("Complex join query returned %d rows from both databases", pgRows)
		}
	})

	// Test window functions
	t.Run("WindowFunctions", func(t *testing.T) {
		query := `
			SELECT 
				username,
				ROW_NUMBER() OVER (ORDER BY id) as row_num,
				RANK() OVER (ORDER BY created_at) as rank
			FROM users
			ORDER BY id
		`

		pgRows := executeAndCountRows(t, pgDB, query)
		ybRows := executeAndCountRows(t, ybDB, query)

		if pgRows != ybRows {
			t.Errorf("Window function row count mismatch: PostgreSQL=%d, YugabyteDB=%d", pgRows, ybRows)
		}
	})
}

// TestTransactionBehavior tests transaction consistency
func TestTransactionBehavior(t *testing.T) {
	pgDB := connectToPostgreSQL(t)
	defer pgDB.Close()

	ybDB := connectToYugabyteDB(t)
	defer ybDB.Close()

	t.Run("RollbackConsistency", func(t *testing.T) {
		// Test rollback in PostgreSQL
		pgTx, err := pgDB.Begin()
		if err != nil {
			t.Fatalf("Failed to begin PostgreSQL transaction: %v", err)
		}

		_, err = pgTx.Exec("INSERT INTO users (username, email) VALUES ($1, $2)",
			"test_rollback_pg", "rollback@pg.test")
		if err != nil {
			pgTx.Rollback()
			t.Fatalf("Failed to insert in PostgreSQL transaction: %v", err)
		}

		pgTx.Rollback()

		// Test rollback in YugabyteDB
		ybTx, err := ybDB.Begin()
		if err != nil {
			t.Fatalf("Failed to begin YugabyteDB transaction: %v", err)
		}

		_, err = ybTx.Exec("INSERT INTO users (username, email) VALUES ($1, $2)",
			"test_rollback_yb", "rollback@yb.test")
		if err != nil {
			ybTx.Rollback()
			t.Fatalf("Failed to insert in YugabyteDB transaction: %v", err)
		}

		ybTx.Rollback()

		// Verify both rollbacks worked
		var pgExists, ybExists bool
		pgDB.QueryRow("SELECT EXISTS(SELECT 1 FROM users WHERE username = $1)",
			"test_rollback_pg").Scan(&pgExists)
		ybDB.QueryRow("SELECT EXISTS(SELECT 1 FROM users WHERE username = $1)",
			"test_rollback_yb").Scan(&ybExists)

		if pgExists {
			t.Error("PostgreSQL rollback failed - user still exists")
		}
		if ybExists {
			t.Error("YugabyteDB rollback failed - user still exists")
		}
	})
}

// Helper functions

func connectToPostgreSQL(t *testing.T) *sql.DB {
	host := getEnv("POSTGRES_HOST", "postgres")
	port := getEnv("POSTGRES_PORT", "5432")
	user := getEnv("POSTGRES_USER", "testuser")
	password := getEnv("POSTGRES_PASSWORD", "testpass")
	dbname := getEnv("POSTGRES_DB", "testdb")

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	t.Logf("Connecting to PostgreSQL: host=%s port=%s user=%s dbname=%s", host, port, user, dbname)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("Failed to open PostgreSQL connection: %v", err)
	}

	// Wait for connection with logging
	t.Log("Waiting for PostgreSQL to be ready...")
	for i := 0; i < 30; i++ {
		if err := db.Ping(); err == nil {
			t.Logf("✅ PostgreSQL ready after %d attempts", i+1)
			return db
		} else {
			if i%5 == 0 {
				t.Logf("Attempt %d/30: PostgreSQL not ready yet: %v", i+1, err)
			}
		}
		time.Sleep(1 * time.Second)
	}

	finalErr := db.Ping()
	t.Fatalf("PostgreSQL did not become available after 30 attempts. Final error: %v", finalErr)
	return nil
}

func connectToYugabyteDB(t *testing.T) *sql.DB {
	host := getEnv("YUGABYTE_HOST", "yugabytedb")
	port := getEnv("YUGABYTE_PORT", "5433")
	user := getEnv("YUGABYTE_USER", "yugabyte")
	dbname := getEnv("YUGABYTE_DB", "yugabyte")

	connStr := fmt.Sprintf("host=%s port=%s user=%s dbname=%s sslmode=disable",
		host, port, user, dbname)

	t.Logf("Connecting to YugabyteDB: host=%s port=%s user=%s dbname=%s", host, port, user, dbname)
	t.Logf("Connection string: %s", connStr)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("Failed to open YugabyteDB connection: %v", err)
	}

	// Wait for connection with detailed logging
	t.Log("Waiting for YugabyteDB to be ready...")
	for i := 0; i < 60; i++ { // YugabyteDB takes longer to start
		if err := db.Ping(); err == nil {
			t.Logf("✅ YugabyteDB ready after %d attempts", i+1)
			return db
		} else {
			if i%5 == 0 {
				t.Logf("Attempt %d/60: YugabyteDB not ready yet: %v", i+1, err)
			}
		}
		time.Sleep(1 * time.Second)
	}

	// Get the final error for better diagnostics
	finalErr := db.Ping()
	t.Fatalf("YugabyteDB did not become available after 60 attempts. Final error: %v", finalErr)
	return nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func executeAndCountRows(t *testing.T, db *sql.DB, query string) int {
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
	}
	return count
}
