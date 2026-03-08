# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0-beta.1] - Unreleased

### Added

- `CeleriantClient` — single-connection TCP client with full protocol v3 support
- `CeleriantPool` — topology-aware connection pool with leader routing, failover, and round-robin read distribution
- Read, Write, Delete, TrimStart, AggregateDetails, RegisterSchema operations
- Watch API for real-time aggregate change notifications (single-shard and multi-shard)
- Streaming pagination via `IAsyncEnumerable` — `ReadAllAsync`, `ListOrgsAsync`, `ListAggregateTypesAsync`, `ListAggregatesAsync`
- TLS and mTLS support including KMS/HSM-backed private keys
- API key and RSA signing authentication
- Zstd, Snappy, Brotli, and Gzip compression with auto-compression threshold
- Dependency injection integration via `AddCeleriantPool()`
- JSON event serializer
- Targets net8.0, net9.0, and net10.0
