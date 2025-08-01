# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Twinkly is a QA tool for testing PostgreSQL-to-YugabyteDB compatibility**. It acts as a proxy that executes every query on both databases simultaneously, compares results, and returns SQL errors when they differ - causing tests to fail immediately on incompatibilities.

**Key Point**: This is NOT a production proxy. It's specifically designed for controlled testing environments where security is less critical.

## Commands

### Building and Running
```bash
make build              # Build the proxy binary
make build-all          # Build for multiple platforms
make run                # Build and run the proxy (listens on port 5431)
make clean              # Clean build artifacts
```

### Testing
```bash
make test               # Run all tests (39.4% coverage currently)
make test-verbose       # Run tests with verbose output
make test-config        # Run configuration tests only
make test-validator     # Run result validator tests only
make test-security      # Run security tests only
go test -v -run TestRunDemo      # Run the tool demonstration
go test -v -run TestSpecificName # Run a specific test by name
```

### Development
```bash
make fmt                # Format Go code
make lint               # Run golangci-lint (requires installation)
make install            # Install dependencies (go mod tidy + download)
```

## Architecture

### How It Works

1. **Client Connection**: Test client connects to Twinkly on port 5431 (configurable)
2. **Dual Execution**: Twinkly forwards each query to BOTH PostgreSQL and YugabyteDB simultaneously
3. **Result Comparison**: Results are compared in-flight before returning to client
4. **Decision Logic**:
   - **If results match**: Returns YugabyteDB results to client (configurable source of truth)
   - **If results differ**: Returns SQL error to client, causing test to fail immediately
   - **If query matches exclude pattern**: Skips comparison, returns results normally

### Core Flow Components

**Entry Points**:
- `main.go` → `SimpleProxy` → `DualExecutionProxy.HandleConnection()`

**Query Execution Path**:
1. `dual_proxy.go:handleQueryPhase()` - Receives query from client
2. `dual_proxy.go:executeDualQuery()` - Executes on both databases in parallel
3. `result_validator.go:ValidateResults()` - Compares results
4. Returns either results or error based on comparison

**Key Decision Points**:
- `config.go:ShouldCompareQuery()` - Checks if query should be compared
- `config.go:AddOrderByToQuery()` - Adds ORDER BY for deterministic results
- `main.go:45-47` - **WARNING**: Forcibly overrides source of truth to YugabyteDB

### Critical Configuration

The main configuration file `config/twinkly.conf` controls behavior:

```hocon
comparison {
    source-of-truth = "yugabytedb"    # Which DB's results to return to client
    fail-on-differences = true        # Generate SQL errors on mismatch
    fail-on-slow-queries = true       # Fail if YB is slower than threshold
    slow-query-ratio = 4.0            # Fail if YB is 4x slower than PG
    
    exclude-patterns = [              # Queries to skip comparison
        "SHOW.*",
        "SELECT.*version\\(\\).*"
    ]
    
    explain-options {
        yugabytedb {
            dist = true               # YB-specific: distributed query plan
        }
    }
}

```

### Additional Components

**Reporting & Analysis**:
- `inconsistency_reporter.go` - Logs mismatches to console/files (webhook TODO)
- `slow_query_analyzer.go` - Runs EXPLAIN ANALYZE when YB is slower
- `constraint_divergence_detector.go` - Detects different constraint behaviors
- `error_handler.go` - Maps error codes between databases

**Infrastructure**:
- `pgproto.go` - PostgreSQL wire protocol implementation
- `dual_resolver.go` - Connection pool management with health checks
- `security.go` - Query validation (less critical for QA tool)

## Key Implementation Details

### Source of Truth Behavior
The code **forcibly overrides** the configured source-of-truth to YugabyteDB in `main.go:45-47`. This means results from YugabyteDB are always returned to the client when queries match, regardless of configuration.

### Slow Query Detection
When YugabyteDB is slower than PostgreSQL by the configured ratio (default 4x):
1. Runs EXPLAIN ANALYZE on both databases
2. YugabyteDB EXPLAIN includes DIST option for distributed query details
3. Returns SQL error with both execution plans if `fail-on-slow-queries = true`

### Test Coverage
- **Current Coverage**: 39.4% 
- **Well Tested**: Core requirements, result validation, protocol handling
- **Gaps**: Database-dependent tests skip without real DBs, limited mocking

### Known Limitations
1. Webhook notifications not implemented (just logs TODO)
2. Many integration tests require real PostgreSQL/YugabyteDB instances
3. `ApplyValidationResult` method is deprecated
4. No connection retry logic for initial database connections