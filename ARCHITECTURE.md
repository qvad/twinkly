# Twinkly Architecture

Twinkly is designed as a transparent, high-performance proxy that enables safe migration and verification between PostgreSQL and YugabyteDB.

## System Overview

```
[Client App] <---> [Twinkly Proxy] <---> [PostgreSQL (Source of Truth)]
                                   \
                                    \--> [YugabyteDB (Verification Target)]
```

The proxy intercepts PostgreSQL wire protocol messages and forwards them to both backends. It then collects responses, compares them, and returns the response from the "Source of Truth" (usually PostgreSQL during migration) to the client.

## Core Components

### 1. Proxy Layer (`pkg/proxy`)
*   **`DualExecutionProxy`**: The central orchestrator. It manages the lifecycle of client connections and the corresponding backend connections.
*   **Async Reader Pattern**: To prevent deadlocks, Twinkly spawns dedicated goroutines (`runBackendReader`) for each backend connection. These goroutines drain messages into buffered channels, ensuring that a slow backend or a large result set never blocks the proxy from processing the other backend or the client.
*   **`dual_proxy_messages.go`**: Implements the state machine for the PostgreSQL Extended Query Protocol (Parse, Bind, Execute, Sync). It handles the complexity of synchronizing state across two databases that might respond at different speeds.

### 2. Protocol Layer (`pkg/protocol`)
*   **`pgproto.go`**: A zero-dependency parser and writer for the PostgreSQL wire protocol (v3.0). It handles packet framing, type marshaling, and message parsing, ensuring byte-perfect communication.

### 3. Comparator Engine (`pkg/comparator`)
*   **`ResultValidator`**: Performs deep comparison of result sets. It checks:
    *   Row counts.
    *   Command completion tags (e.g., `INSERT 0 1`).
    *   Binary data values (row by row).
    *   Error messages.
*   **`SlowQueryAnalyzer`**: Monitors execution time. If a query is significantly slower on YugabyteDB, it triggers a parallel `EXPLAIN ANALYZE` on both DBs to capture execution plans for debugging.

### 4. Reporting & Analysis (`pkg/reporter`, `pkg/ai`)
*   **Inconsistency Reporter**: When a mismatch is detected, it generates a structured JSON report containing the query, the differing results (including sample data), and impact analysis.
*   **AI Agent**: An experimental module that can send performance regression data (query plans) to an LLM to get actionable optimization advice (e.g., "Add index on column X").

### 5. Configuration (`pkg/config`)
*   Uses HOCON format for flexible configuration.
*   Supports environment variable expansion (e.g., `${DB_HOST}`).
*   Allows fine-grained control over routing patterns (e.g., send `pg_catalog` queries only to Postgres).

## Key Workflows

### Query Execution Flow
1.  **Receive**: Proxy reads a message (e.g., `Query`, `Parse`) from the Client.
2.  **Forward**: Proxy writes the message to both PostgreSQL and YugabyteDB connections.
3.  **Collect**: Dedicated reader goroutines read responses from backends into channels.
4.  **Synchronize**: The main proxy loop waits for responses from both (with timeouts).
5.  **Compare**: Results are compared for consistency.
    *   If `fail-on-differences` is true and a mismatch occurs, an error is returned to the client.
    *   Otherwise, the difference is logged/reported asynchronously.
6.  **Respond**: The response from the configured "Source of Truth" is written back to the Client.

### Transaction Management
Twinkly tracks transaction state (`Idle`, `InTransaction`, `Failed`) by inspecting `ReadyForQuery` messages. This ensures that both connections remain in sync regarding transaction boundaries (`BEGIN`, `COMMIT`, `ROLLBACK`).

## Directory Structure

```
/
├── cmd/twinkly/       # Entry point (main.go)
├── config/            # Default configuration files
├── pkg/               # Library code
│   ├── proxy/         # Connection handling & logic
│   ├── protocol/      # Wire protocol parser
│   ├── comparator/    # Result & performance comparison
│   ├── reporter/      # Inconsistency reporting
│   ├── config/        # Config loading
│   └── ai/            # AI analysis interface
└── test/              # Integration tests & Docker setup
```
