# Twinkly Dual Database Proxy Features

## Core Functionality

### 1. Dual Database Connections
- **Purpose**: Connects to both PostgreSQL and YugabyteDB simultaneously
- **Implementation**: Each client connection creates two backend connections
- **Location**: `dual_proxy.go:66-78`

```go
// Creates connections to both databases
pgConn, err := net.DialTimeout("tcp", p.pgAddr, 10*time.Second)
ybConn, err := net.DialTimeout("tcp", p.ybAddr, 10*time.Second)
```

### 2. Result Comparison
- **Purpose**: Compares query results between PostgreSQL and YugabyteDB
- **Implementation**: Executes queries on both databases and validates results
- **Location**: `dual_proxy.go:317-547`

```go
// Execute on both databases and compare
pgResults, pgErr := p.collectQueryResults(pgReader)
ybResults, ybErr := p.collectQueryResults(ybReader)
```

### 3. Error on Mismatch
- **Purpose**: Returns SQL error to client when results don't match
- **Implementation**: Uses ResultValidator to compare and generate errors
- **Location**: `dual_proxy.go:514-530`

```go
if validation.ShouldFail {
    // Return SQL error to user about result differences
    errorMsg := CreateErrorMessage("ERROR", "XX000", validation.ErrorMessage)
    clientWriter.WriteMessage(errorMsg)
    return fmt.Errorf("query results differ between databases")
}
```

### 4. Allowed Results Configuration
- **Purpose**: Configure patterns for acceptable result differences
- **Implementation**: Exclude patterns in configuration
- **Location**: `config/twinkly.conf:80-86`

```hocon
comparison {
    # Queries to exclude from comparison
    exclude-patterns = [
        "SHOW.*",
        "SELECT.*version\\(\\).*",
        "SELECT.*current_timestamp.*",
        "SELECT.*now\\(\\).*"
    ]
}
```

## Configuration

### Source of Truth
- **Default**: YugabyteDB is the source of truth
- **Purpose**: Determines which database results are returned to client
- **Configuration**: `comparison.source-of-truth = "yugabytedb"`

### Failure Modes
- **Fail on Differences**: `comparison.fail-on-differences = true`
- **Fail on Slow Queries**: `comparison.fail-on-slow-queries = true`

## Testing Integration

### Stopping Tests on Mismatch
When result validation fails:
1. **Error Generation**: SQL error with code "XX000" is returned
2. **Connection Termination**: Client connection receives error and terminates
3. **Test Failure**: Test framework receives database error and stops execution
4. **Issue Documentation**: Error message describes the specific difference found

### Example Error Message
```
ERROR: Query results differ between PostgreSQL and YugabyteDB:
Row count mismatch: PostgreSQL returned 5 rows, YugabyteDB returned 3 rows
```

## Usage Flow

1. **Client Connection**: Client connects to proxy port (5431)
2. **Dual Connection**: Proxy creates connections to both databases
3. **Query Execution**: Each query runs on both databases
4. **Result Comparison**: Results are validated for differences
5. **Error or Forward**: Either return error on mismatch or forward results
6. **Test Termination**: Test stops immediately on validation failure

## Benefits

- **Early Detection**: Compatibility issues found before production
- **Automated Testing**: No manual comparison required
- **Comprehensive Coverage**: Every query is tested against both databases
- **Detailed Reporting**: Specific differences are documented in error messages