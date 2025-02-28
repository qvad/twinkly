# Twinkly: High-Performance Dual-Database Proxy

Twinkly is an advanced, protocol-aware PostgreSQL proxy designed for seamless migration and compatibility testing between **PostgreSQL** and **YugabyteDB**. It sits between your client application and the databases, executing queries against both backends in parallel to validate data consistency, protocol compliance, and performance in real-time.

## 🚀 Key Features

*   **Dual-Database Execution:** Mirrors every query to both PostgreSQL and YugabyteDB concurrently.
*   **Real-Time Consistency Checks:** Compares result sets (row counts, data values) and reports mismatches immediately.
*   **Deadlock-Free Architecture:** Implements a robust asynchronous reader pattern to prevent TCP window exhaustion and deadlocks common in simple proxies.
*   **Protocol Compliance:** Fully supports the PostgreSQL wire protocol, including the complex Extended Query Protocol (Parse/Bind/Execute/Sync).
*   **Performance Analysis:** Detects performance regressions by comparing execution times. Can trigger `EXPLAIN ANALYZE` on slow queries.
*   **AI-Powered Insights (Experimental):** Interface for LLM integration to automatically analyze root causes of performance discrepancies (e.g., "Missing index on YugabyteDB").
*   **Configurable Source of Truth:** Choose which database's response is returned to the client (default: PostgreSQL).

## 🛠️ Architecture

Twinkly follows a clean, modular Go architecture. For a deep dive into the system design, see [ARCHITECTURE.md](ARCHITECTURE.md).

*   **`pkg/proxy`**: Core proxy logic, connection handling, and async message forwarding.
*   **`pkg/protocol`**: Zero-dependency PostgreSQL wire protocol parser/writer.
*   **`pkg/comparator`**: Logic for validating results and analyzing performance divergence.
*   **`pkg/reporter`**: Generates detailed JSON/Text reports on detected inconsistencies.

## 📦 Prerequisites

*   **Go 1.21+**
*   **PostgreSQL 11+**
*   **YugabyteDB 2.14+**
*   Network access to both database instances.

## ⚡ Quick Start

### 1. Build
```bash
make build
# Output binary: ./twinkly
```

### 2. Configure
Edit `config/twinkly.conf` to point to your databases. The configuration uses HOCON format and supports environment variable expansion (e.g., `${POSTGRES_HOST}`).

**Example `config/twinkly.conf`:**
```hocon
proxy {
  listen-port = 5431
  
  postgresql {
    host = "localhost"
    port = 5432
    user = "postgres"
    database = "testdb"
  }
  
  yugabytedb {
    host = "localhost"
    port = 5433
    user = "yugabyte"
    database = "testdb"
  }
}

comparison {
  enabled = true
  source-of-truth = "postgresql"
  fail-on-differences = true
}
```

### 3. Run
```bash
./twinkly -config config/twinkly.conf
```

### 4. Connect
Point your application or `psql` to the proxy:
```bash
psql -h localhost -p 5431 -U postgres -d testdb
```

## 🧪 Testing

Twinkly includes a comprehensive integration test suite that runs in Docker.

### Run Integration Tests
```bash
make docker-test
```
This command spins up fresh PostgreSQL and YugabyteDB containers, executes a suite of tests (dual writes, inconsistency detection, complex queries), and tears down the environment.

### Development Commands
*   `make build`: Compile the binary.
*   `make run`: Run with default config.
*   `make test`: Run unit tests.
*   `make fmt`: Format code.
*   `make lint`: Run linters.

## ⚙️ Configuration Reference

| Section | Setting | Description |
| :--- | :--- | :--- |
| **proxy** | `listen-port` | Port the proxy listens on (default 5431). |
| | `routing.postgres-only-patterns` | Regex patterns for queries sent ONLY to Postgres. |
| **comparison** | `enabled` | Enable/disable result comparison. |
| | `fail-on-differences` | If true, returns an error to the client on mismatch. |
| | `log-differences-only` | Reduce log noise by only logging mismatches. |
| | `report-slow-queries` | Enable performance regression analysis. |
| **monitoring** | `log-errors` | Log all SQL errors. |

## 🤝 Contributing

1.  **Conventions:** All library code resides in `pkg/`. Use `protocol.PGMessage` for typed message handling.
2.  **Concurrency:** Use `sync.WaitGroup` for parallel operations and Channels for backend communication.
3.  **Safety:** Wrap connections with `logger.WrapConn` for debugging.

## License
MIT
