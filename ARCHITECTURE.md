# Architecture

This document outlines the proposed architecture for the refactored twinkly project.

## Project Structure

The project will be restructured into the following directory layout:

```
/
├── cmd/
│   └── twinkly/
│       └── main.go
├── pkg/
│   ├── proxy/
│   │   ├── dual_proxy.go
│   │   ├── proxy.go
│   │   └── ...
│   ├── config/
│   │   ├── config.go
│   │   └── ...
│   ├── comparator/
│   │   ├── comparator.go
│   │   └── ...
│   └── ...
├── internal/
│   └── ...
├── go.mod
├── go.sum
└── ...
```

- **`cmd/`**: This directory will contain the entry point of the application.
- **`pkg/`**: This directory will contain reusable packages that can be shared with other applications.
- **`internal/`**: This directory will contain packages that are specific to the twinkly project and should not be imported by other applications.

## Components

The application will be composed of the following main components:

- **Proxy**: The proxy component will be responsible for listening for incoming connections and forwarding them to the appropriate database.
- **Config**: The config component will be responsible for loading and validating the application's configuration.
- **Comparator**: The comparator component will be responsible for comparing the results from the two databases.
- **Reporter**: The reporter component will be responsible for reporting any inconsistencies found between the two databases.
- **Cache**: The cache component will be responsible for caching the results of frequently executed queries.
- **Connection Pool**: The connection pool component will be responsible for managing database connections.

## Workflow

The following diagram illustrates the workflow of the application:

```
[Client] -> [Proxy] -> [Connection Pool] -> [PostgreSQL]
                  |
                  -> [Connection Pool] -> [YugabyteDB]
                  |
                  -> [Comparator] -> [Reporter]
                  |
                  -> [Cache]
```

1. The client connects to the proxy.
2. The proxy gets two connections from the connection pool, one for PostgreSQL and one for YugabyteDB.
3. The proxy sends the client's query to both databases.
4. The proxy receives the results from both databases.
5. The proxy sends the results to the comparator.
6. The comparator compares the results and sends a report to the reporter if there are any inconsistencies.
7. The proxy sends the results from the source of truth database to the client.
8. The proxy caches the results if caching is enabled.
9. The proxy returns the connections to the connection pool.