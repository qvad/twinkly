# Twinkly: Dual-Execution PostgreSQL/YugabyteDB Proxy

Twinkly is a lightweight PostgreSQL protocol proxy that connects to both PostgreSQL and YugabyteDB simultaneously, executes client queries against both backends, and compares results to detect incompatibilities. It can optionally fail fast on differences and/or report slow queries with EXPLAIN plans.

- Module: `github.com/qvad/twinkly`
- Language: Go 1.21+

## Why Twinkly?
- Catch incompatibilities between PostgreSQL and YugabyteDB early
- Automate validation in CI using normal SQL clients/tests
- Get detailed inconsistency reports to speed up root-cause analysis

## Key Features
- Dual execution: run queries on PostgreSQL and YugabyteDB in parallel
- Result comparison with configurable source of truth
- Strict error on mismatch mode for automated testing
- Slow query analyzer with configurable EXPLAIN clauses
- Inconsistency reporting with endpoint metadata and severity

See FEATURES.md for a detailed, code-referenced feature list.

## Architecture Overview
The proxy implements core PostgreSQL protocol handling and a dual-execution engine with result validation. The codebase is organized by concern (proxy, resolver, validators, helpers, analyzers).

- High-level overview: ARCHITECTURE.md
- Requirements and test mapping: REQUIREMENTS.md

## Quick Start

### 1) Install
```bash
# Go 1.21+
go install github.com/qvad/twinkly@latest
```

Or clone the repo and build locally:
```bash
git clone https://github.com/qvad/twinkly.git
cd twinkly
make build  # or: go build ./...
```

### 2) Configure
Copy or edit the sample configuration:
```bash
cp config/twinkly.conf my-twinkly.conf
$EDITOR my-twinkly.conf
```
Important settings include PostgreSQL/YugabyteDB endpoints and comparison policies (source-of-truth, exclude patterns, slow query thresholds). See comments inside `config/twinkly.conf`.

Resolution order for configuration file:
1. -config path (or -c)
2. $TWINKLY_CONFIG environment variable
3. ./config/twinkly.conf
4. <binary_dir>/config/twinkly.conf

### 3) Run the proxy
```bash
# from repository root or your GOPATH/pkg/mod checkout
./twinkly -config my-twinkly.conf
# or shorthand
./twinkly -c my-twinkly.conf
# or using environment variable
TWINKLY_CONFIG=my-twinkly.conf ./twinkly
# or if built with `go build` as main package
go run . -config my-twinkly.conf
```
The proxy logs which config file it uses. If it cannot find a config, it prints the locations it tried and a tip on how to pass one.
The proxy listens on the configured port (default from config). Point your SQL client or test suite to the proxy address.

Note: At runtime, the proxy enforces YugabyteDB as the source of truth for client-visible results. If your config sets a different value, it will be overridden with a warning.

## Usage Notes
- By default, YugabyteDB is enforced as the source of truth for client-visible results.
- On mismatches, the proxy can return a SQL error (configurable), allowing test suites to fail fast.
- Slow queries can be analyzed using EXPLAIN/EXPLAIN ANALYZE; thresholds and clauses are configurable.

## Running Tests
```bash
go test -v ./...
```
All tests should pass; see REQUIREMENTS.md for coverage mapping to the code.

## Project Layout (selected)
- main.go, tool_demo.go: Entrypoints and demo wiring
- dual_proxy.go: Core dual-execution proxy and protocol flow
- dual_proxy_helpers.go, dual_proxy_messages.go: Helpers and extended-protocol handling
- dual_resolver.go: Backend connection management/utilities
- result_validator.go: Result comparison logic
- slow_query_analyzer.go: Performance/plan analysis
- config.go, config/twinkly.conf: Configuration model and sample config
- ARCHITECTURE.md, FEATURES.md, REQUIREMENTS.md: Documentation

## Security
The proxy includes basic query/security checks and TLS handling for backend connections. Review `security.go` and configuration before production use.

## License
This project is licensed under the terms of the LICENSE file included in this repository.
