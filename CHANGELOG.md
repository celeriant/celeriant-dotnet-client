# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.0] - 2026-06-11

### Added

- `TrimReplicationBackpressure` (3006) and `DeleteReplicationBackpressure` (4007) error codes. Both throw `ServerBusyException` (same handling as `WriteReplicationBackpressure`), so the pool auto-retries them.

## [0.4.1] - 2026-06-10

### Fixed

- Watch now subscribes once and reads server-pushed responses; previously each `NextAsync` re-sent the request as a long-poll (watch was always push — the server ignored the extra bytes). No API change.
- Watch probe falls back to multi-shard on error 9002 (`ShardRoutingIncompatibleFilters`) as well as 9001; 9002 previously threw `CeleriantErrorException`.
- Guide and `Celeriant.Reference`: on idempotency violation, point-read the contested `ClientSeq` and compare `EventId` before treating it as success — with a shared `ClientId` the seq may belong to a sibling's write.

## [0.4.0] - 2026-06-10

### Changed

- **Breaking:** `WriteAsync` now returns `WriteResponse` instead of `SuccessResponse`. `WriteResponse` exposes `MaxAggregateVersion` (the highest aggregate version committed, populated only for single-aggregate writes) and `CorrelationId`.
- **Breaking:** The `clientId` parameter on the single-aggregate `WriteAsync` overloads (`CeleriantClient` and `CeleriantPool`) is now a required `Guid` instead of an optional `Guid?` that defaulted to a fresh random GUID. A per-call random ID silently disabled client-seq idempotency; callers must now pass a stable ID per logical writer.
- **Breaking:** `ClientId` is now `required` on `WriteRequest`, `DeleteRequest`, `TrimStartRequest`, and `RegisterSchemaRequest`.

## [0.3.0] - 2026-06-04

### Added

- `WatchErrorException` and the `WatchTooManySubscribers` error — surfaced when the server rejects a watch subscription because the per-aggregate subscriber limit is exceeded.

## [0.2.0] - 2026-03-25

### Added

- `ServerBusyException` — thrown when server returns error 11000 (shard channel full)
- Pool auto-retries on `ServerBusyException` by trying the next available node

## [0.1.0-beta.1] - 2026-03-17

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
