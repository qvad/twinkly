# Integration Testing for Twinkly

This directory contains comprehensive integration tests for the Twinkly dual-execution PostgreSQL proxy.

## Overview

The integration tests verify that Twinkly correctly handles:
- Dual database execution and result comparison
- PostgreSQL protocol compliance
- Transaction management
- Error handling and divergence reporting
- Performance analysis

## Test Structure

```
test/
├── integration/           # Go integration tests
│   ├── proxy_test.go     # Basic proxy functionality tests
│   └── full_proxy_test.go # Full end-to-end proxy tests
├── sql/                  # Database initialization scripts
│   ├── init-postgres.sql # PostgreSQL test data
│   └── init-yugabyte.sql # YugabyteDB test data
├── config/               # Test configurations
│   └── integration-test.conf # Integration test config
├── Dockerfile.integration # Docker image for running tests
└── README.md            # This file
```

## Running Integration Tests

### Prerequisites

- Docker and Docker Compose
- Go 1.21+
- PostgreSQL client tools (for manual testing)

### Quick Start

Run the complete integration test suite with Docker:

```bash
make docker-test
```

This command:
1. Starts PostgreSQL and YugabyteDB containers
2. Initializes test data in both databases
3. Builds and runs the integration tests
4. Cleans up containers

### Manual Testing

For development and debugging, you can run components separately:

```bash
# Start test databases
make test-db-up

# Wait for databases to be ready (check health)
docker-compose -f docker-compose.test.yml ps

# Run integration tests
make test-integration

# Stop databases
make test-db-down
```

### Development Workflow

For rapid iteration during development:

```bash
# Full development test cycle
make dev-test
```

This runs the databases, waits for readiness, executes tests, and cleans up.

## Test Configuration

The integration tests use `test/config/integration-test.conf` which:
- Uses environment variables for database connections
- Enables result comparison and validation
- Configures appropriate timeouts for testing
- Enables detailed logging for debugging

Key configuration differences from production:
- `fail-on-differences = true` - Tests fail if databases return different results
- `require-secondary = true` - Tests require both databases to be available
- `source-of-truth = "postgresql"` - PostgreSQL is the authoritative source
- Reduced connection pools and timeouts for faster test execution

## Test Database Schema

Both PostgreSQL and YugabyteDB are initialized with identical schemas:

- `users` table - User accounts with unique constraints
- `products` table - Product catalog with pricing
- `orders` table - Order records with foreign keys
- Indexes on commonly queried columns
- A view (`user_order_summary`) for complex query testing

This schema tests:
- Basic CRUD operations
- JOIN queries across multiple tables
- Constraint enforcement (unique, foreign key)
- Index usage and performance
- View handling

## Test Coverage

### Basic Functionality (`proxy_test.go`)
- Database connectivity and health checks
- Basic SELECT queries
- JOIN operations
- Aggregate queries with GROUP BY
- Data consistency verification
- Transaction commit/rollback
- Error handling (syntax errors, constraints, missing tables)

### Full Proxy Integration (`full_proxy_test.go`)
- End-to-end proxy startup and connection handling
- Query execution through proxy
- Transaction management through proxy
- Extended Query Protocol (prepared statements)
- Error propagation through proxy
- Performance benchmarking

### Performance Testing
- Query execution time comparison between databases
- Throughput benchmarking
- Connection handling under load
- Memory usage monitoring

## Environment Variables

The tests use these environment variables (with defaults):

```bash
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=testuser
POSTGRES_PASSWORD=testpass
POSTGRES_DB=testdb

YUGABYTE_HOST=localhost
YUGABYTE_PORT=5433
YUGABYTE_USER=yugabyte
YUGABYTE_DB=yugabyte
```

## Troubleshooting

### Database Connection Issues
```bash
# Check if databases are running
docker-compose -f docker-compose.test.yml ps

# Check database logs
docker-compose -f docker-compose.test.yml logs postgres
docker-compose -f docker-compose.test.yml logs yugabytedb

# Test direct connections
psql -h localhost -p 5432 -U testuser -d testdb
psql -h localhost -p 5433 -U yugabyte -d yugabyte
```

### Test Failures
- Check that both databases have identical test data
- Verify proxy configuration is correct
- Review test logs for specific error messages
- Ensure no other services are using the test ports

### Performance Issues
- YugabyteDB takes longer to start than PostgreSQL
- Increase health check timeouts if needed
- Use `docker-compose logs` to monitor startup progress

## Adding New Tests

When adding integration tests:

1. Add test functions to `proxy_test.go` or `full_proxy_test.go`
2. Ensure tests clean up any data they create
3. Use transactions for data modification tests
4. Test both success and failure scenarios
5. Include performance assertions where appropriate

Example test structure:
```go
func TestNewFeature(t *testing.T) {
    env := setupTestEnvironment(t)
    defer env.teardown()
    
    // Test logic here
    
    t.Run("SubTest", func(t *testing.T) {
        // Subtest logic
    })
}
```

## CI/CD Integration

For continuous integration:

```yaml
# Example GitHub Actions integration
- name: Run Integration Tests
  run: make docker-test
  
- name: Upload Test Results
  uses: actions/upload-artifact@v2
  with:
    name: test-results
    path: test-results.xml
```

The Docker-based tests are self-contained and don't require external database setup in CI environments.