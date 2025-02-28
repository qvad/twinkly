package integration

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// TestPostgreSQLOnly tests just PostgreSQL connectivity and schema
func TestPostgreSQLOnly(t *testing.T) {
	db := connectToPostgreSQLOnly(t)
	defer db.Close()

	t.Run("BasicConnectivity", func(t *testing.T) {
		var result int
		err := db.QueryRow("SELECT 1").Scan(&result)
		if err != nil {
			t.Fatalf("Basic query failed: %v", err)
		}
		if result != 1 {
			t.Errorf("Expected 1, got %d", result)
		}
		t.Logf("✅ PostgreSQL connectivity test passed")
	})

	t.Run("SchemaExists", func(t *testing.T) {
		tables := []string{"users", "products", "orders"}
		for _, table := range tables {
			var exists bool
			err := db.QueryRow(`
				SELECT EXISTS (
					SELECT FROM information_schema.tables 
					WHERE table_name = $1
				)
			`, table).Scan(&exists)
			if err != nil {
				t.Fatalf("Failed to check table %s: %v", table, err)
			}
			if !exists {
				t.Errorf("Table %s does not exist", table)
			} else {
				t.Logf("✅ Table %s exists", table)
			}
		}
	})

	t.Run("DataExists", func(t *testing.T) {
		queries := map[string]string{
			"users":    "SELECT COUNT(*) FROM users",
			"products": "SELECT COUNT(*) FROM products",
			"orders":   "SELECT COUNT(*) FROM orders",
		}

		for name, query := range queries {
			var count int
			err := db.QueryRow(query).Scan(&count)
			if err != nil {
				t.Fatalf("Query %s failed: %v", name, err)
			}
			if count == 0 {
				t.Errorf("Table %s has no data", name)
			} else {
				t.Logf("✅ Table %s has %d rows", name, count)
			}
		}
	})

	t.Run("ComplexQuery", func(t *testing.T) {
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

		rows, err := db.Query(query)
		if err != nil {
			t.Fatalf("Complex query failed: %v", err)
		}
		defer rows.Close()

		userCount := 0
		for rows.Next() {
			var username string
			var orderCount int
			var totalSpent float64
			if err := rows.Scan(&username, &orderCount, &totalSpent); err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			userCount++
			t.Logf("User %s: %d orders, $%.2f total", username, orderCount, totalSpent)
		}

		if userCount == 0 {
			t.Error("Complex query returned no results")
		} else {
			t.Logf("✅ Complex query returned %d users", userCount)
		}
	})
}

func connectToPostgreSQLOnly(t *testing.T) *sql.DB {
	host := getEnvVar("POSTGRES_HOST", "localhost")
	port := getEnvVar("POSTGRES_PORT", "5432")
	user := getEnvVar("POSTGRES_USER", "testuser")
	password := getEnvVar("POSTGRES_PASSWORD", "testpass")
	dbname := getEnvVar("POSTGRES_DB", "testdb")

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	t.Logf("Connecting to PostgreSQL: host=%s port=%s user=%s dbname=%s", host, port, user, dbname)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatalf("Failed to open PostgreSQL connection: %v", err)
	}

	// Wait for connection with detailed logging
	t.Log("Waiting for PostgreSQL to be ready...")
	for i := 0; i < 30; i++ {
		if err := db.Ping(); err == nil {
			t.Logf("✅ PostgreSQL ready after %d attempts", i+1)
			return db
		}
		if i%5 == 0 {
			t.Logf("Attempt %d/30: PostgreSQL not ready yet, waiting...", i+1)
		}
		time.Sleep(1 * time.Second)
	}

	t.Fatal("PostgreSQL did not become available within 30 seconds")
	return nil
}

func getEnvVar(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
