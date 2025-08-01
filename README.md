# Twinkly - PostgreSQL to YugabyteDB Compatibility Testing Proxy

<div align="center">

![Go Version](https://img.shields.io/badge/Go-1.21%2B-blue)
[![Test Coverage](https://img.shields.io/badge/coverage-39.4%25-yellow)](https://github.com/qvad/twinkly)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

**A dual-execution proxy for testing PostgreSQL to YugabyteDB compatibility**

</div>

## Overview

Twinkly is a specialized QA tool that acts as a proxy between your application and both PostgreSQL and YugabyteDB databases. It executes every query on both databases simultaneously, compares the results in real-time, and returns SQL errors when differences are detected - making incompatibilities immediately visible during testing.

### Key Features

- 🔄 **Dual Execution**: Every query runs on both PostgreSQL and YugabyteDB in parallel
- 🔍 **Real-time Comparison**: Results are compared before returning to the client
- ❌ **Fail-Fast Testing**: Returns SQL errors on mismatches, causing tests to fail immediately
- ⚡ **Performance Analysis**: Detects when YugabyteDB is slower than PostgreSQL
- 📊 **Detailed Reporting**: Logs all inconsistencies with recommendations
- 🔧 **Configurable Behavior**: Fine-tune comparison rules and exclusions
- 🚀 **Wire Protocol Support**: Full PostgreSQL wire protocol implementation

## Quick Start

### Prerequisites

- Go 1.21 or higher
- PostgreSQL instance
- YugabyteDB instance
- `golangci-lint` (optional, for linting)

### Installation

```bash
# Clone the repository
git clone https://github.com/qvad/twinkly.git
cd twinkly

# Install dependencies
make install

# Build the proxy
make build
```

### Basic Usage

1. **Configure your databases** in `config/twinkly.conf`:

```hocon
proxy {
  listen-port = 5431
  
  postgresql {
    host = "localhost"
    port = 5432
    user = "postgres"
    password = "postgres"
    database = "testdb"
  }
  
  yugabytedb {
    host = "localhost"
    port = 5433
    user = "yugabyte"
    password = "yugabyte"
    database = "testdb"
  }
}
```

2. **Start the proxy**:

```bash
make run
# Or directly:
./twinkly
```

3. **Connect your application** to Twinkly on port 5431 instead of directly to the database.

## How It Works

```mermaid
graph LR
    A[Test Application] -->|Query| B[Twinkly Proxy<br/>:5431]
    B -->|Parallel Execution| C[PostgreSQL<br/>:5432]
    B -->|Parallel Execution| D[YugabyteDB<br/>:5433]
    C -->|Results| B
    D -->|Results| B
    B -->|Compare Results| E{Match?}
    E -->|Yes| F[Return YB Results]
    E -->|No| G[Return SQL Error]
```

1. Your test application connects to Twinkly (default port 5431)
2. Twinkly forwards each query to both databases simultaneously
3. Results are compared in-flight
4. If results match: YugabyteDB results are returned
5. If results differ: SQL error is returned, failing the test

## Configuration

### Core Configuration Options

```hocon
comparison {
    # Which database's results to return when they match
    source-of-truth = "yugabytedb"
    
    # Generate SQL errors on result mismatch
    fail-on-differences = true
    
    # Performance monitoring
    fail-on-slow-queries = true
    slow-query-ratio = 4.0  # Fail if YB is 4x slower
    
    # Queries to exclude from comparison
    exclude-patterns = [
        "SHOW.*",
        "SELECT.*version\\(\\).*",
        "SELECT.*pg_catalog.*"
    ]
    
    # EXPLAIN ANALYZE options
    explain-options {
        yugabytedb {
            dist = true  # Show distributed query plan
        }
    }
}
```

### Advanced Features

#### Slow Query Detection

When YugabyteDB performs slower than PostgreSQL by the configured ratio:

```hocon
comparison {
    slow-query-ratio = 4.0        # Threshold multiplier
    fail-on-slow-queries = true   # Return error with EXPLAIN plans
}
```

#### Query Transformations

Automatically add ORDER BY for deterministic comparisons:

```hocon
comparison {
    force-order-by-compare = true
    order-by-patterns = [
        {
            pattern = "SELECT.*FROM users.*"
            order-by = "id ASC"
        }
    ]
}
```

#### Error Mapping

Map PostgreSQL-specific errors to YugabyteDB equivalents:

```hocon
errors {
    postgresql-to-yugabytedb = {
        "23505" = "23505"  # unique_violation
        "23503" = "23503"  # foreign_key_violation
    }
}
```

## Development

### Building and Testing

```bash
# Build the proxy
make build

# Run all tests
make test

# Run specific test suites
make test-config      # Configuration tests
make test-validator   # Result validator tests
make test-security    # Security tests

# Run with verbose output
make test-verbose

# Format code
make fmt

# Run linter
make lint

# Build for multiple platforms
make build-all
```

### Running Individual Tests

```bash
# Run the demo test
go test -v -run TestRunDemo

# Run a specific test
go test -v -run TestSpecificName
```

## Architecture

### Core Components

- **`main.go`**: Entry point and server initialization
- **`dual_proxy.go`**: Core dual-execution logic
- **`result_validator.go`**: Result comparison engine
- **`pgproto.go`**: PostgreSQL wire protocol implementation
- **`config.go`**: Configuration management
- **`slow_query_analyzer.go`**: Performance analysis with EXPLAIN
- **`inconsistency_reporter.go`**: Detailed mismatch reporting

### Query Flow

1. **Receive Query**: `dual_proxy.go:handleQueryPhase()`
2. **Check Exclusions**: `config.go:ShouldCompareQuery()`
3. **Dual Execution**: `dual_proxy.go:executeDualQuery()`
4. **Compare Results**: `result_validator.go:ValidateResults()`
5. **Return Response**: Either results or SQL error

## Reporting

### Inconsistency Reports

Mismatches are logged to `inconsistency_reports/` directory as JSON files:

```json
{
  "timestamp": "2024-01-15T10:30:45Z",
  "type": "ROW_COUNT_MISMATCH",
  "severity": "HIGH",
  "query": "SELECT * FROM users WHERE active = true",
  "postgresql_result": {
    "row_count": 150,
    "sample_rows": [...]
  },
  "yugabytedb_result": {
    "row_count": 148,
    "sample_rows": [...]
  },
  "recommendation": "Check data synchronization between databases"
}
```

### Performance Reports

When YugabyteDB is slower, detailed EXPLAIN ANALYZE output is provided:

```
Slow query detected: YugabyteDB 4.5x slower than PostgreSQL

PostgreSQL Plan:
Seq Scan on users (cost=0.00..10.50 rows=500 width=104)
  Planning Time: 0.123 ms
  Execution Time: 2.456 ms

YugabyteDB Plan:
Seq Scan on users (cost=0.00..100.00 rows=1000 width=104)
  Storage Table Read Requests: 3
  Storage Table Rows Scanned: 500
  Planning Time: 0.543 ms
  Execution Time: 11.052 ms
```

## Use Cases

### 1. Migration Testing

Test your application's compatibility before migrating:

```bash
# Point your test suite at Twinkly
DATABASE_URL=postgresql://user:pass@localhost:5431/testdb npm test
```

### 2. Regression Testing

Ensure new YugabyteDB versions maintain compatibility:

```bash
# Run your existing PostgreSQL test suite through Twinkly
pytest --database-url postgresql://localhost:5431/testdb
```

### 3. Performance Validation

Identify queries that need optimization for YugabyteDB:

```hocon
comparison {
    fail-on-slow-queries = true
    slow-query-ratio = 2.0  # Strict performance requirements
}
```

## Limitations

- **Test Environment Only**: Not designed for production use
- **Connection Pooling**: Each client connection creates two backend connections
- **Webhook Notifications**: Not yet implemented (logs TODO)
- **Transaction Handling**: Limited support for complex transaction scenarios

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Built with the [HOCON](https://github.com/gurkankaymak/hocon) configuration library
- PostgreSQL wire protocol implementation inspired by community standards
- Designed specifically for YugabyteDB compatibility testing

---

<div align="center">
Made with ❤️ for the PostgreSQL and YugabyteDB communities
</div>