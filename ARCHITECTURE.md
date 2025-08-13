# yb-twinkly Architecture Overview

This project is a dual-execution PostgreSQL protocol proxy that connects to both PostgreSQL and YugabyteDB, routes client traffic, and can compare results to detect incompatibilities.

This refactor introduces a light-weight structure without changing behavior, by splitting large files into cohesive units within the same package.

## Package layout (package main)

- main.go, tool_demo.go
  - Entrypoints and CLI/demo wiring.
- config.go
  - Configuration parsing and typed configuration model.
- proxy.go
  - SimpleProxy server lifecycle (listen/accept/shutdown) and shared reporter.
- dual_proxy.go
  - Core DualExecutionProxy: connection startup, query loop, routing, TLS negotiation, utility predicates (isReadOnlyQuery, DDL/DML detection, txn markers) and error parsing.
- dual_proxy_messages.go
  - Extended-protocol message handling for non-query messages (Parse/Bind/Execute/Flush/Sync). Avoids blocking reads except after Sync.
- dual_proxy_helpers.go
  - Small helpers used by DualExecutionProxy (collecting results, extracting rows, transport error detector, reporter accessor).
- dual_resolver.go
  - Backend connection pools and admin ops used for background comparisons and DDL mirroring.
- result_validator.go
  - Row/command-result comparison logic and summary structs.
- slow_query_analyzer*.go
  - Performance comparison helpers and tests.
- constraint_divergence_detector.go
  - Detects and reports constraint handling divergences.
- error_handler.go, security.go, netlog.go, pgproto.go
  - Supporting utilities (error mapping, security checks, network logging, PG protocol bits).

## Design guidelines

- Keep DualExecutionProxy minimal: prefer extracting narrowly-scoped helpers to dual_proxy_helpers.go.
- Don’t block on backend reads for extended protocol until Sync to avoid deadlocks.
- Keep package `main` until a broader module refactor is warranted; tests depend on current imports.
- Prefer small files per concern; functions should be easy to navigate via file names.

## Future improvements (non-breaking)

- Consider moving configuration load/validation into a subpackage (config/) if tests allow.
- Extract protocol types and helpers (pgproto.go) into a subpackage.
- Introduce context-aware logging and structured fields.
- Add integration diagram to this doc illustrating data flow through proxy and comparators.


## Inconsistency report format update

In addition to query and result summaries, each report now includes endpoint metadata for both databases:
- postgresql_info: { name, host, port, database, user }
- yugabytedb_info: { name, host, port, database, user }

These fields help operators quickly identify which endpoints produced the divergent results.
