# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.7.0] - 2026-08-29

### Fixed

- **`RegisterSchemaAsync` did not fail over to the leader.** A follower rejects a schema registration with error 2027 (`RegisterSchemaCannotAcceptWrites`), which carries the leader address exactly like the write/trim/delete not-leader errors — but the client did not treat it as a not-leader condition, so it surfaced as a fatal `ServerInternalErrorException` instead of failing over. On a cluster where schemas are registered at startup and the pool's configured address is a follower, registration failed hard. 2027 is now a not-leader condition and the pool fails over, verified live.
- **A pre-epoch or empty write is now rejected client-side.** The wire encodes `EventTimestamp` as unsigned epoch milliseconds and cannot represent a time before 1970, so a default/unset `DateTimeOffset` wrapped to a far-*future* timestamp and silently corrupted timestamp-bounded reads (it appeared in `MinEventTimestamp` filters and vanished from `MaxEventTimestamp` filters). Such a timestamp, and an events-less write, now throw `ArgumentException` before anything is sent — consistent with the existing null-`EventValue` guard.
- **RSA client identity was derived with the wrong byte order, so writes were rejected under `require_client_identity`.** `CeleriantCrypto.GenerateClientIdentity` built the Guid with a plain `new Guid(bytes)` (mixed-endian) while the server reads `SHA-256(DER)[0..16]` as a little-endian u128 — the two disagreed, and the guide tells you to use the derived value as your write `ClientId`. Confirmed against a live `--require-client-identity` server: the documented id was rejected (`error 10003 client_id mismatch`) while the value from `IdentifyAsync` was accepted. The derivation now matches the server (and `IdentifyAsync`), verified end-to-end. Only affected identity-enforcing deployments; found by driving a real enforcing server, not visible to hermetic or no-auth tests.
- **A null `AggregateEvent.EventValue` desynchronised the connection.** It serialized to a msgpack nil the server could not decode, dropping the connection and surfacing as an opaque `ConnectionFailedException` a retry loop could not tell from a real network fault. A null is now rejected client-side with `ArgumentException` (naming the offending event index) before anything is sent, so one bad field can no longer poison a live connection.
- **`TrimIndexOutOfRangeException.CurrentMaxBatchIndex` was always 0.** The client read the wrong error-detail key (`max_event_batch_index`; the server sends `max_aggregate_version`), so a caller clamping a retry to it would trim to 0. Corrected the key.
- **A default or zero `ReadFilters.FromAggregateVersion` was rejected instead of reading from the start.** The doc promised "0 is treated as 1" but only the `From(n)` factory clamped; a `new ReadFilters { … }` that left the field at its `long` default of 0 was rejected with `BatchIndexUnavailableException`. The property now clamps values below 1 to 1, so the default is safe.
- **A disposed `CeleriantPool` operation threw `ConnectionFailedException` instead of the documented `ObjectDisposedException`** — the same type a genuinely-down cluster throws, silently reclassifying a lifecycle bug as a retryable fault. The read, write, list and watch paths now throw `ObjectDisposedException` as documented (already listed under Added). `CeleriantClient` use-after-dispose now names `CeleriantClient` rather than an internal `SemaphoreSlim`.

- **A multi-shard watch could go silent instead of erroring.** A watch subscription never reconnects and never catches up a gap, so the guarantee it rests on is that a caller is either receiving every matching event or being told it is not. On the multi-shard path a background shard reader could stop without completing the channel, leaving `NextAsync` waiting forever on a subscription nobody was reading — subscribed, receiving nothing, and never told. Every reader exit now terminally ends the subscription. Three related fixes:
  - The `CancellationToken` passed to `WatchConnection.ConnectAsync` / `CeleriantPool.WatchAsync` now scopes **connecting only**. It was linked into the background readers, so cancelling it after connect silently retired a live subscription the caller still held. The subscription now lives until the `WatchConnection` is disposed.
  - A shard failure surfaces as the same exception type as the identical single-shard failure. It previously arrived as `System.Threading.Channels.ChannelClosedException`, which a `catch (CeleriantClientException)` written against the single-shard path did not catch. Repeated reads of a dead watch now replay the original failure instead of re-throwing one shared exception whose stack trace grew on every call.
  - Concurrent `DisposeAsync` calls, and a `NextAsync` racing a disposal, could surface `NullReferenceException`.
- **A watch could be handed back before every shard was subscribed.** Shard subscriptions were sent from the background readers, so a shard that could not subscribe surfaced its error later — through a connection the caller was already holding and already reading events from. Every shard is now subscribed and acknowledged before `ConnectAsync` returns; a shard that cannot subscribe fails the connect, and every connection opened by a failed connect is closed. A well-formed reply of the wrong type is no longer accepted as an acknowledgement on either path.
- **A watch connect could hang forever on a half-dead node.** `WatchOptions.ConnectionTimeout` bounded the dial and TLS handshake but not the subscription round trip, so a node that accepted the socket and never answered stalled the connect indefinitely — and `CeleriantPool.WatchAsync` can only fail over on a connect that returns. The acknowledgement is now bounded by the same timeout and raises `ConnectionTimeoutException`, which routing already treats as failover-class.
- **An empty or inverted shard range silently watched one shard.** `StartShard = 6` with `MaxShardHint = 3` subscribed shard 6 — a shard the caller never asked for and the server may not have — and reported itself healthy. The range is now rejected: `ArgumentOutOfRangeException` when the caller's own `MaxShardHint` is at fault, `ProtocolException` when the bound came from the server's `num_shards`, which is a runtime condition the same options would survive against a larger cluster. A negative `StartShard` is rejected too.
- **A poisoned multi-shard watch left its sibling readers parked on their sockets** until the caller happened to dispose, which a caller that reconnects on error never does. The first shard failure now stops every reader and releases every connection.
- **A watch that failed to dial the cached leader left the pool pinned to it**, so every later operation opened with the same doomed dial. The leader now reverts to the configured primary when it refuses a watch connect in leader-pinned routing.
- **Cancelled requests no longer poison the pool.** If a caller's `CancellationToken` fired between the request write and the response read, the undrained response stayed in the socket and the connection went back to the pool reporting healthy — the next borrower read the previous request's response and got a plausible answer to a question it never asked. `OperationCanceledException` is not a `CeleriantClientException`, so no `catch` in the pool saw it. A connection whose exchange did not complete now reports `IsPoisoned` and is discarded on return.
- **Responses are bound to their request by correlation id.** The transport fills `CorrelationId` when the caller leaves it null and verifies the echo before the response is interpreted — ahead of the point where a server error frame becomes a thrown exception, so a stale error belonging to a different request can no longer surface as this request's failure. A mismatch throws `ProtocolException` and retires the connection. A caller-supplied id is never overwritten, so the guarantee holds only while caller-supplied ids are unique per request.
- **A caller waiting at the connection cap could hang.** A lease returned as broken is disposed and releases the semaphore without writing to the idle channel, so a caller parked waiting for a connection never woke. It now waits on both.
- **A watch connect could still hang on a node that answered the dial and nothing else.** The identity handshake between the dial and the subscription had no timeout of its own, and watch clients never set a request timeout, so a node with `IdentityConfig` configured could park `ConnectAsync` indefinitely — the same failover-class stall the bounded acknowledgement above was meant to close. `WatchOptions.ConnectionTimeout` now bounds the handshake too and raises `ConnectionTimeoutException`.
- **The connection pool lost capacity under churn.** A caller entering connection creation holds a semaphore permit; if a connection was returned to the idle set while it queued behind the dial gate, it picked that one up and kept its own permit as well. One permit was lost per hit, and since the at-cap wakeup loop routes every retry through that path, a pool under churn walked to zero capacity and callers parked forever on slots that would never free. The surplus permit is now released on both the reuse and the evict-stale branches.
- **Disposing a connection pool could strand a caller forever, and could leak a connection.** Two defects in the same window. `SemaphoreSlim.Dispose()` does not complete a wait that is already pending — and once disposed, a cancellation can no longer reach it — so a caller parked at the connection cap when the pool was disposed never returned at all. The pool now cancels every parked waiter *before* it disposes anything, and those callers get the transport's own connection-failed error. Separately, a connection dialled while disposal was in progress was dropped with its socket still open: the dial gate's `Release()` in a `finally` threw `ObjectDisposedException`, and a throw from `finally` discards the value the `return` was carrying. Both releases are now guarded, and a dial that completes into a disposed pool closes its connection instead of leasing it. `GetConnectionAsync` on a disposed pool also reports the transport's error rather than a raw `ObjectDisposedException`.
- **A second caller on a shared connection could be told it was poisoned when it was not.** The entry check in the async send path tested `IsPoisoned`, which includes the "exchange in progress" dirty flag, and it ran *before* the send lock — so two tasks sharing one client intermittently saw a healthy connection mid-request as poisoned, and the pool retired it. The check now runs under the lock, where dirty can only mean the previous holder left without clearing it. The synchronous send path keeps its check where it is: it holds no lock, so an exchange in flight there really is a reason to refuse.
- **Application errors no longer discard healthy connections.** The pool retired a connection on any `CeleriantClientException`, so a `WriteOccException` in an OCC retry loop re-dialled on every attempt. A server-decoded error arrives as a complete response frame, which leaves the stream clean. Retirement is now limited to `ConnectionFailedException`, `CeleriantTimeoutException` and `ProtocolException` — the classes the transport raises when the exchange did not complete, matching `leaves_connection_dirty` in the Rust client. `NotLeader`, `ServerBusy` and `IdentityRequired` keep their connections.
- **A reply of the wrong kind was accepted as the answer.** Responses were bound to their request by correlation id only, and that check exempts `Watch` — whose frame carries no correlation field on the wire at all. So a `Watch` frame arriving in answer to a read passed every check the client had and was handed back as a successful read of a different shape; the `ProtocolException` a caller eventually saw came from the pool's response mapper, one layer above the connection pool and therefore after the desynchronised connection had already been returned for reuse. The response type is now checked against the request type before the response is interpreted, and a mismatch poisons the connection like a correlation mismatch does. Error frames stay exempt — the server answers any request with `GenericError`. This is the second of the two things the Rust client uses to mark a stream dirty; the client previously had only the first.
- **The client could not connect to a server that ships a compression dictionary.** `IdentifyResponse.compression_dict_bytes` arrives as a msgpack *array of integers*, not as `bin`: every other byte field in the protocol is annotated on the server to route through `serde_bytes`, and that one is not, so `rmp_serde`'s compact encoding writes the bare `Vec<u8>` as a sequence. Rust talking to Rust never notices — it is symmetric — but this client read `bin` and threw `Failed to deserialize IdentifyResponse`. The server ships the dictionary on the first Identify of every connection that supplies an identity, so any pooled client configured with `IdentityConfig` failed at connect. The field now reads either encoding.
- **A cancelled dial is no longer reported as a connect timeout.** If the caller's token fired just after the dial timer, `ConnectAsync` raised `ConnectionTimeoutException` — which routing reads as failover-class and answers by dialling the next node, work the caller had already said it did not want. The caller's token now takes precedence when both have fired.

### Added

- **Disposed pool and client operations now throw `ObjectDisposedException`.** Every `CeleriantPool` method already documented it, but the guard was only wired to `GetConnectionAsync`; the typed operations fell through to the node pool and surfaced `ConnectionFailedException` — the same type a genuinely-down cluster throws, so a disposed-pool programming bug was silently reclassified as a retryable network fault. The read, write, list and watch paths now throw `ObjectDisposedException` as documented. `CeleriantClient` gained the same guard: using it after `DisposeAsync` previously threw an `ObjectDisposedException` naming an internal `SemaphoreSlim`; it now names `CeleriantClient`, and a `<remarks>` documents the lifecycle.
- **`ConnectAsync` / `ConnectTlsAsync` now document their exceptions** — `ArgumentException` for a malformed address and `ConnectionFailedException` / `ConnectionTimeoutException` for an unreachable node — matching the taxonomy the pool methods already carry. The three `WithMaxRequestSize` / `WithMaxResponseSize` / `WithTimeout` doc-comments now state they mutate the receiver and return it (they are not immutable copies, despite the `With` name).
- **`WatchConnection.Address`** — the node a subscription is attached to. Because a watch never reconnects and never catches up, a caller that reconnects after a failure is starting a new subscription from whatever that node's tip is now; comparing this across reconnects is how it notices the subscription moved node, and therefore that notifications in between may have been missed. Mirrors `address()` on the Rust client.
- **A watch spans at most 1024 shards.** A server-reported `num_shards` was previously taken at face value: a value past `int` range wrapped to a small number and silently watched a handful of shards, and a large in-range value exhausted sockets or memory before the caller could catch anything. This is a client-side sanity bound, not a protocol limit, and the Rust client has no equivalent.

### Changed

- **`SchemaValidationException.FailedClientSeq` was renamed to `FailedEventIndex`.** It never held the event's `ClientSeq` — it is the zero-based position of the failing event within the request's events array (the server's `client_event_index`). The old name pointed recovery code at the wrong value. Source-breaking for anyone reading `FailedClientSeq`; the value and meaning are unchanged.
- **The redundant `CeleriantClient.ConnectAsync(string, CancellationToken)` overload was removed.** With both it and `ConnectAsync(string, TimeSpan?, ClientTlsConfig?, CancellationToken)` present, the natural one-argument call `ConnectAsync("host:port")` was a compile error (`CS0121`, ambiguous), which is why every example had to pass the unnatural `ct: default`. The single full overload covers every case — `ConnectAsync("host:port")`, `ConnectAsync("host:port", ct: token)`, and the timeout/TLS forms all compile — and the docs drop the `ct: default` wart. **Source-breaking only for a caller passing a `CancellationToken` positionally as the second argument** (`ConnectAsync(addr, token)`); pass it named (`ct: token`) or use the one-argument form. No call site in this repo used the positional form.
- **Bad shard ranges blame the caller whenever the caller named the bound.** The origin split introduced above now applies at *all three* rejection points in a multi-shard watch connect — the empty or inverted range, the shard-count cast overflow, and the 1024-shard client bound — not just the first. The last two previously raised `ProtocolException` blaming the server even when the number came from the caller's own `WatchOptions.MaxShardHint`. They now raise `ArgumentOutOfRangeException` in that case. The reported `ParamName` is `MaxShardHint` rather than `StartShard`, because when the bound is the caller's, `MaxShardHint` is the value at fault. **This changes the exception type** a caller sees for `MaxShardHint` values at or past 1024: `ArgumentOutOfRangeException` is not a `CeleriantClientException`, so a `catch (CeleriantClientException)` around connect no longer sweeps a programmer error up with the runtime failures. Note this does not change failover, in either direction — `CeleriantPool.WatchAsync` retries only on `ConnectionFailedException` and `ConnectionTimeoutException`, so neither type was ever retried across candidate nodes.

- **Request DTOs are `record` rather than `class`.** `ReadRequest`, `WriteRequest` and the other nine request types under `Celeriant.Client.Requests` changed from `sealed class` to `sealed record` so the transport can copy one to fill its correlation id without a hand-written field-by-field clone. The wire format is byte-identical — same `[Key]` ordering, same formatters — and no behaviour depends on the old semantics in this repo. Two consequences for downstream consumers on recompile:
  - `==` on two request instances now compares structurally instead of by reference. Note the comparison is shallow over collection members: two `WriteRequest`s with identical contents still compare unequal, because `Writes` is a `Dictionary`.
  - `ToString()` now dumps every property, including `RegisterSchemaRequest.Schema`. Check your logging if you log request objects directly.

  This is a source-compatible but semantically visible change. Decide whether it warrants a minor or major bump before publishing.

## [0.6.0] - 2026-08-16

First release published alongside `Celeriant.Transport` 0.1.0 (extracted shared wire
transport: framing, dictionary compression, connection pool). `Celeriant.Client` now
depends on it as a package.

### Changed

- **Read routing**: pool reads (read, aggregate details, list, watch, `GetConnectionAsync`) now go to the current leader by default, guaranteeing read-your-writes. `RouteReadsToFollowers` remains the explicit opt-in for follower reads (rotated, eventually consistent). Previously reads round-robined across all nodes and could return stale data or miss a just-written aggregate.
- **Follower routing HA**: with `RouteReadsToFollowers`, the leader is now the last-resort candidate: if every follower fails, reads and watches are served by the leader instead of throwing. `WatchOptions` gained `ConnectionTimeout` (the pool defaults it from its own connection timeout, so a black-holed node no longer stalls watch failover), and `WatchAsync` now always applies pool TLS/identity even when caller-supplied options are passed.
- **Breaking (source, pre-1.0)**: `ITransportExceptionFactory` gained a `ConnectTimeout` member. Connection-establishment timeouts (dial, TLS handshake) now throw the new `ConnectionTimeoutException` (subclass of `CeleriantTimeoutException`) instead of the base type; routing treats them as failover-class, unlike request timeouts. Existing `catch (CeleriantTimeoutException)` sites keep working.

## [0.5.0] - 2026-06-11

### Added

- `TrimReplicationBackpressure` (3006) and `DeleteReplicationBackpressure` (4007) error codes. Both throw `ServerBusyException` (same handling as `WriteReplicationBackpressure`), so the pool auto-retries them.

## [0.4.1] - 2026-06-10

### Fixed

- Watch now subscribes once and reads server-pushed responses; previously each `NextAsync` re-sent the request as a long-poll (watch was always push: the server ignored the extra bytes). No API change.
- Watch probe falls back to multi-shard on error 9002 (`ShardRoutingIncompatibleFilters`) as well as 9001; 9002 previously threw `CeleriantErrorException`.
- Guide and `Celeriant.Reference`: on idempotency violation, point-read the contested `ClientSeq` and compare `EventId` before treating it as success: with a shared `ClientId` the seq may belong to a sibling's write.

## [0.4.0] - 2026-06-10

### Changed

- **Breaking:** `WriteAsync` now returns `WriteResponse` instead of `SuccessResponse`. `WriteResponse` exposes `MaxAggregateVersion` (the highest aggregate version committed, populated only for single-aggregate writes) and `CorrelationId`.
- **Breaking:** The `clientId` parameter on the single-aggregate `WriteAsync` overloads (`CeleriantClient` and `CeleriantPool`) is now a required `Guid` instead of an optional `Guid?` that defaulted to a fresh random GUID. A per-call random ID silently disabled client-seq idempotency; callers must now pass a stable ID per logical writer.
- **Breaking:** `ClientId` is now `required` on `WriteRequest`, `DeleteRequest`, `TrimStartRequest`, and `RegisterSchemaRequest`.

## [0.3.0] - 2026-06-04

### Added

- `WatchErrorException` and the `WatchTooManySubscribers` error: surfaced when the server rejects a watch subscription because the per-aggregate subscriber limit is exceeded.

## [0.2.0] - 2026-03-25

### Added

- `ServerBusyException`: thrown when server returns error 11000 (shard channel full)
- Pool auto-retries on `ServerBusyException` by trying the next available node

## [0.1.0-beta.1] - 2026-03-17

### Added

- `CeleriantClient`: single-connection TCP client with full protocol v3 support
- `CeleriantPool`: topology-aware connection pool with leader routing, failover, and round-robin read distribution
- Read, Write, Delete, TrimStart, AggregateDetails, RegisterSchema operations
- Watch API for real-time aggregate change notifications (single-shard and multi-shard)
- Streaming pagination via `IAsyncEnumerable`: `ReadAllAsync`, `ListOrgsAsync`, `ListAggregateTypesAsync`, `ListAggregatesAsync`
- TLS and mTLS support including KMS/HSM-backed private keys
- API key and RSA signing authentication
- Zstd, Snappy, Brotli, and Gzip compression with auto-compression threshold
- Dependency injection integration via `AddCeleriantPool()`
- JSON event serializer
- Targets net8.0, net9.0, and net10.0
