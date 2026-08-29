# Reference-app parity: .NET vs Rust

Feature-by-feature diff between this .NET reference (`Celeriant.Reference/`) and the
Rust reference (`celeriant-db/celeriant_reference/`), plus what this pass closed.
Aye, this doc be LLM-written, so it carries a bit o' pirate in its prose by house rule.

## The gap that mattered

The Rust reference ships two projection backends and picks one at boot from the
`PROJECTION` env var (`postgres` default, `memory` alternative). The .NET reference
had only the Postgres one, wired straight into DI with no seam to swap it. That was
the whole point of this task.

Both backends run the identical write loop and the identical dedup guarantees. They
differ in one thing only: where the projection cursor lives, and therefore where the
request-response cache lives.

- Postgres backend: cursor and cache are shared across replicas, moved together
  atomically in one SQL statement. A retry landing on any replica reads the shared
  response row.
- In-memory backend: cursor and cache are per process. Each replica folds the stream
  itself and rebuilds its own dedup index as a side effect of replay. A retry landing
  on a different replica than the original is still caught, but by that replica
  re-folding the stream rather than reading a shared row.

Correctness in both rests on Celeriant's `ClientSeq` / `expectedVersion` guards, which
are server-side and shared. The projection cache is only a fast path, never the safety
boundary.

## What was closed

### 1. In-memory account service (the big one)

New file `AccountServiceMem.cs`, ported from Rust `account_service_mem.rs`. It holds a
`Dictionary<Guid, MemAccount>` behind a plain `lock`, never held across an `await`
(lock, mutate, unlock — same discipline as the Rust `Mutex`). Each `MemAccount` carries
balance, cursor (`LastBatchIndex`), `MaxClientSeq`, and a `Recent` dedup index of
`eventId -> RecentWrite` maintained mid-fold and evicted by expiry.

Semantics matched to Rust line for line:

- Lazy catch-up reads new events from the replica's own cursor, folds under the lock,
  and returns the fresh projection plus a cache hit if the `eventId` already landed.
- Dedup entries born from replay get the window minus the batch's server-time age
  (batch-vs-tip, not the local clock, so skew cannot misjudge them). Entries born from
  our own write get the full window on the monotonic clock. Monotonic time is
  `Environment.TickCount64`, the .NET stand-in for Rust's `Instant`.
- `RecordWrite` bumps the projection conditionally so it never goes backwards if a
  sibling fold got there first.
- `ResolveTransferHits` reconstructs a missing transfer leg from current state, because
  a hit on either leg proves the all-or-nothing write landed.
- Same OCC-retry ladder as the Postgres service: OCC conflict re-derives `ClientSeq`,
  timeout holds it constant, inflight-duplicate retries, idempotency violation is
  resolved against the stream via `Verify.WhoOwnsSeqAsync`.

The per-process-not-shared guarantee is called out in a class-level comment, mirroring
the note in the Rust mem module.

### 2. Selection wiring

`Program.cs` now reads the `Projection` config key (`Postgres` default, `InMemory`
alternative), mirroring Rust `main.rs`. An unknown value throws at startup rather than
booting a half-configured app.

- `InMemory` registers `MemAccountService` as a **singleton** (the projection must
  survive across requests) and never touches Postgres: no `NpgsqlDataSource`, no
  `InitDatabase`, no projection-row seed. Celeriant aggregate seeding still runs.
- `Postgres` keeps the existing behaviour: `AccountService` scoped, schema init, and
  both the Postgres row seed and the Celeriant aggregate seed.

`IAccountService` is the new seam. Both services implement it; every endpoint handler
injects the interface, not the concrete class.

### 3. `IAccountService` abstraction

New file `IAccountService.cs`. `TransferResult` was lifted out of the nested
`AccountService.TransferResult` to a top-level record so both backends and the interface
share it. `AccountProjection`, `WriteResult`, and `CatchUpResult` were already top-level.

### 4. Shared fold helpers (de-duplication of code, not of events)

`ReplayEvent` and `FormatEvent` moved from `AccountService`'s private methods into a
shared `AccountEvents` static class (mirrors Rust `events.rs`); `Backoff` moved into
`Verify` (mirrors Rust `verify.rs`). Both backends now call the same helpers, so they
replay and render identically instead of drifting.

### 5. History output: `toAccountId` / `fromAccountId`

Rust `format_event` emits `toAccountId` on a `TransferredOut` and `fromAccountId` on a
`TransferredIn`. The .NET `FormatEvent` dropped both. The shared `AccountEvents.FormatEvent`
now emits them. Confirmed live: a Bob history read after a transfer shows
`"type":"TransferredIn","amountCents":2500,"fromAccountId":"..."`.

## Known divergences left open (deliberate)

These are intentional .NET-side conventions, not bugs. Changing them would ripple into
the SPA and buy nothing.

1. **`batchIndex` vs `aggregateVersion` in the wire JSON and query params.** The .NET
   reference names the cursor `batchIndex` everywhere the API surface touches it
   (`balanceCents`/`batchIndex` responses, `minBatchIndex`/`fromBatchIndex` query params,
   the `last_batch_index` / `last_client_event_index` Postgres columns). Rust uses
   `aggregateVersion`, `minVersion`, `fromVersion`, `last_version`, `last_client_seq`.
   The .NET SPA in `wwwroot/app.js` consumes `batchIndex` throughout, so this is a
   self-consistent naming choice on the .NET side. Left as-is.

2. **Listen port and SSE plumbing.** Rust binds `0.0.0.0:5001` and serves SSE with an
   axum broadcast channel; .NET uses the default Kestrel binding and a bounded
   `System.Threading.Channels` broadcaster with `DropOldest`. Functionally equivalent,
   framework-idiomatic on each side. Left as-is.

3. **Postgres driver detail.** Rust uses `tokio_postgres` with hand-rolled SQL; .NET
   uses `Npgsql`. The SQL statements are the same shape (single-statement atomic persist,
   opportunistic expiry cleanup). No behavioural gap.

## Build and smoke

`dotnet build Celeriant.Reference -c Release` succeeds, zero warnings.

In-memory variant boot-smoked against the live Celeriant at `localhost:10000`
(4-shard standalone), with no Postgres running:

- Boot log: `Projection backend: in-memory`, watch connection established, no Postgres
  connection attempted.
- Deposit `$12.34` on Alice: balance `50500 -> 51734`, batchIndex `2 -> 3`.
- Idempotent deposit (same `Idempotency-Key` twice): second call returned the cached
  response, no double-apply.
- Transfer Alice -> Bob `$25.00`: both legs landed, `from` and `to` balances and cursors
  correct.
- History fold correct, including the new `fromAccountId` on the transfer-in.
- Negative deposit -> 422 `VALIDATION_ERROR`; over-balance withdraw -> 422
  `INSUFFICIENT_FUNDS` with the balance echoed.

The Postgres variant was not re-smoked (no task requirement, and the code path is
untouched except for the shared-helper extraction and the `IAccountService` seam, both
compile-checked). Nothing in the Postgres path changed behaviourally.
