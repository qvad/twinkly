# Twinkly: Dual-Execution PostgreSQL/YugabyteDB Proxy

Twinkly is a high-performance, protocol-aware proxy that sits between your application and two databases (PostgreSQL and YugabyteDB). It mirrors traffic to both, validating compatibility and performance in real-time.

## 🚀 New Features (v2.0 Refactor)

*   **Deadlock-Free Architecture:** Uses asynchronous background readers for all backend connections.
*   **Parallel Analysis:** Runs `EXPLAIN ANALYZE` on both databases concurrently when analyzing slow queries.
*   **AI Integration:** Hooks for LLM-based root cause analysis of performance regressions (experimental).
*   **Protocol Compliance:** Strict PostgreSQL wire-protocol parser (`pkg/protocol`).

## Quick Start

1.  **Configure:** Edit `config/twinkly.conf` (set DB hosts/ports).
2.  **Build:** `make build`
3.  **Run:** `./twinkly`
4.  **Connect:** Point your app or `psql` to `localhost:5431`.

## Documentation

See [GEMINI.md](GEMINI.md) for a comprehensive developer guide and architectural overview.

## License
MIT