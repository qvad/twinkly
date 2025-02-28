// Package main implements yb-twinkly, a dual-database PostgreSQL protocol proxy
// that can route and mirror queries to PostgreSQL and YugabyteDB, compare results,
// and report incompatibilities. The code is organized into focused files:
//
//   - main.go, tool_demo.go: entry points and simple CLI/demo wiring
//   - config.go: configuration loading and typed config
//   - proxy.go: SimpleProxy server lifecycle (listen/accept/shutdown)
//   - dual_proxy.go: core DualExecutionProxy (connection startup, query loop, routing)
//   - dual_proxy_messages.go: non-query message routing for extended protocol
//   - dual_proxy_helpers.go: helper methods used by DualExecutionProxy
//   - dual_resolver.go: background pools and admin operations for comparisons/mirroring
//   - result_validator.go, slow_query_analyzer*.go: comparison and performance analysis
//   - constraint_divergence_detector.go: constraint mismatch detection/reporting
//   - error_handler.go, security.go, netlog.go, pgproto.go: ancillary utilities
//
// The package remains `main` to avoid import churn in tests. The refactoring here
// splits large files into smaller, focused units without changing behavior.
package main
