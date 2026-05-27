# Celeriant Reference API — Design Document

## Overview

A production-pattern web API demonstrating how to build a backend service on top of Celeriant's event sourcing. Same banking domain as the simple demo (Alice, Bob, Charlie), but with server-side validation, a PostgreSQL read projection, exactly-once write semantics via `ClientSeq`, and lazy catch-up projection (no background services).

This is the "how you'd actually build it" companion to the browser-side demo.

---

## Core Principles

1. **Celeriant is the source of truth.** PostgreSQL holds a derived projection — a cache that can always be rebuilt from the event stream.
2. **Lazy catch-up, not background sync.** The projection is updated on-demand when a read or write needs it. No background `IHostedService`, no polling, no `WatchAsync` consumer. The API servers are stateless.
3. **Server-side validation.** Business rules (balance >= 0) are enforced on the backend against the projection, not trusted from the client.
4. **Exactly-once writes** via `EnforceClientIdempotency` + `ClientSeq`. Celeriant is the deduplication layer — no outbox pattern needed.
5. **No locks held across network calls.** Postgres row locks are never held while talking to Celeriant. The projection is updated optimistically after a successful Celeriant write.

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│              Client (any HTTP consumer)               │
│                                                       │
│  GET  /api/accounts/{id}/balance                      │
│  POST /api/accounts/{id}/deposit                      │
│  POST /api/accounts/{id}/withdraw                     │
│  POST /api/transfers                                  │
└─────────────────────┬─────────────────────────────────┘
                      │ HTTP
┌─────────────────────┴─────────────────────────────────┐
│      ASP.NET Minimal API (net8.0) — STATELESS          │
│                                                        │
│  • Validates commands against Postgres projection      │
│  • Catches up projection lazily before reads/writes    │
│  • Writes events to Celeriant with OCC + idempotency   │
│  • Optimistically updates projection after write       │
│                                                        │
│  ICeleriantPool (singleton)    NpgsqlDataSource (DI)   │
└───────────┬──────────────────────────┬─────────────────┘
            │ TCP :10000               │ TCP :5432
┌───────────┴───────────┐  ┌───────────┴──────────────┐
│   Celeriant Server     │  │   PostgreSQL              │
│   (source of truth)    │  │   (read projection cache) │
└────────────────────────┘  └───────────────────────────┘
```

Key distinction from the simple demo: the API is no longer a dumb proxy. It reads, validates, and manages projection state. But it remains **stateless** — all state lives in Postgres and Celeriant.

---

## Lazy Catch-Up Projection

The central pattern. Every read and every write starts by ensuring the projection is current.

### How it works

```
CatchUp(accountId):
  1. SELECT balance_cents, last_batch_index FROM account_balances WHERE account_id = @id
  2. ReadAsync from Celeriant with FromAggregateVersion = last_batch_index + 1
  3. If no new events → return current projection (already up to date)
  4. Replay new events to compute updated balance
  5. UPSERT into account_balances with WHERE last_batch_index < @newBatchIndex
     (prevents going backwards if a concurrent request already caught up further)
  6. Return updated projection
```

### Why no background projector?

- **Stateless servers**: No `IHostedService` to manage, no graceful shutdown concerns, no singleton coordination across instances. Horizontal scaling is trivial.
- **No wasted work**: Only accounts that are actually being accessed get caught up. An account untouched for a month doesn't consume any resources.
- **Natural consistency**: The projection is always fresh *at the moment you need it*. No "how stale is acceptable?" tuning.
- **Simpler deployment**: Just the API, Postgres, and Celeriant. No separate projector process to monitor and restart.

### When you WOULD want a background projector

- **Reporting/analytics** queries that scan all accounts (can't lazily catch up thousands of aggregates per request)
- **Reactive side-effects** — sending emails, triggering webhooks when events occur
- **Pre-warming** projections for latency-sensitive hot paths

These are valid use cases but orthogonal to the core CQRS pattern. They can be layered on later using `WatchAsync`.

### Concurrency during catch-up

Multiple requests may try to catch up the same account simultaneously. This is fine:

- Both read the same stale projection from Postgres
- Both read the same new events from Celeriant
- Both compute the same new balance
- Both attempt the UPSERT — the `WHERE last_batch_index < @new` means one wins and one is a no-op
- Both proceed with the correct balance

No row locks needed. The catch-up is naturally idempotent.

---

## Write Path

### Single-account write (deposit/withdraw)

```
1. CatchUp(accountId)                              → gets fresh balance + last_batch_index + max ClientSeq for our ClientId
2. Validate invariant (balance - amount >= 0)       → reject if insufficient funds
3. WriteAsync to Celeriant:
     - key = accountId
     - events = [Deposited/Withdrawn]
     - clientId = service's client ID
     - clientSeq = max + 1 from catch-up      (exactly-once)
     - expectedVersion = last_batch_index   (OCC guard)
     - enforceClientIdempotency = true
4. On success:
     - Optimistically update projection:
       UPDATE account_balances
       SET balance_cents = @newBalance, last_batch_index = last_batch_index + 1
       WHERE account_id = @id AND last_batch_index = @expectedBatchIndex
     - If UPDATE matches 0 rows → fine, next read will catch up
     - Return success to caller
5. On WriteOccException:
     - Another writer modified this account between our read and write
     - Go back to step 1 (catch up will pick up their events, re-validate, retry)
     - After N retries, return 409 to caller
6. On IdempotencyViolationException:
     - Our exact write already landed (crash recovery scenario)
     - Treat as success, catch up projection
```

### Transfer (multi-aggregate write)

```
1. CatchUp(fromAccountId)                          → source balance + batch index + max ClientSeq
2. CatchUp(toAccountId)                            → dest balance + batch index + max ClientSeq
3. Validate source balance >= transfer amount
4. WriteAsync to Celeriant using WriteRequest:
     - Writes dictionary with two entries:
       fromAccount → TransferredOut, ExpectedVersion = source batch index
       toAccount   → TransferredIn, ExpectedVersion = dest batch index
     - clientSeq = max + 1 from each aggregate's catch-up
     - enforceClientIdempotency = true on both
     - Single atomic write — both succeed or neither does
5. On success:
     - Optimistically update both projections
     - Return success
6. On WriteOccException:
     - One or both accounts were modified since our read
     - Retry from step 1
```

The multi-aggregate write is atomic at Celeriant. The OCC guards on both aggregates ensure our projections were current when we validated. If either is stale, the whole write is rejected and we retry with fresh state.

### No locks held across the network

The critical design choice: steps 1-2 (Postgres reads) are separate from step 3 (Celeriant write). We don't hold a `FOR UPDATE` lock on the Postgres rows while calling Celeriant. Instead:

- We read the projection (no lock)
- We write to Celeriant (OCC protects us from stale reads)
- We optimistically update the projection (conditional UPDATE)

If someone modifies the account between our Postgres read and Celeriant write, the OCC check at Celeriant catches it. If someone catches up the projection between our Celeriant write and our Postgres update, the conditional UPDATE is a no-op and the projection is already correct.

---

## Client Event Index Management

### What is it?

`ClientSeq` is a client-controlled monotonic integer on each event. Combined with `ClientId` and `EnforceClientIdempotency`, Celeriant guarantees that the same `(ClientId, ClientSeq)` pair for a given aggregate is never written twice. On duplicate, it throws `IdempotencyViolationException` with `LastAcceptedClientSeq`.

### Why it matters

The classic failure mode:

```
1. Service writes event to Celeriant → succeeds
2. Service crashes before updating its own state
3. Service restarts and retries the write
4. Without idempotency → duplicate event in the stream
```

With `EnforceClientIdempotency`, step 4 returns `IdempotencyViolationException` instead of creating a duplicate. The service knows the write already landed and can proceed.

### Deriving the index

During catch-up (step C of every write), each `AggregateEventBatch` includes the `ClientId` that wrote it. Scan for batches matching your `ClientId` and track the highest `ClientSeq`. Use `max + 1` for your next write.

No external sequence generator needed. No database table. The index is derived from the data you already read.

### Concurrent writers

Two service instances catching up the same aggregate at the same time will derive the same `max + 1`. This is fine — OCC handles it. One writer's `ExpectedVersion` won't match after the other's write lands. The loser retries with fresh state, gets a new `max + 1`, and proceeds.

### Single ClientId per service

All API instances share one `ClientId` (configured, deterministic). Since `ClientSeq` is per `(AggregateKey, ClientId)` and OCC serializes concurrent writes to the same aggregate, there's no collision risk. One client identity for the service, no coordination needed.

Alternative (per-instance ClientId) would make the idempotency tracking per-instance but adds operational complexity. Not worth it unless you're at massive scale.

### Crash recovery

If the process crashes between the Celeriant write and the Postgres projection update:

1. The next request for that account triggers a lazy catch-up
2. The catch-up reads the event from Celeriant (it was committed)
3. The projection is updated with the event's effects
4. The `max + 1` derivation naturally advances past the committed event
5. The system is self-healing — no manual intervention, no recovery logic

If the caller retries the HTTP request after a timeout:

1. The catch-up finds the account already updated (previous write landed)
2. The new `max + 1` is derived from the committed event
3. Validation runs against the current state
4. Either the retry is still valid (and writes a new event) or it's rejected on its own merits

The HTTP-level retry produces a genuinely new command, not a duplicate. The event-level deduplication (ClientSeq) protects against the infrastructure-level crash, not the business-level retry.

---

## Read Path

### GET /api/accounts/{id}/balance

```
1. CatchUp(accountId)
2. Return { balance_cents, last_batch_index }
```

Simple. The caller gets a consistent view as of the moment of the request.

### Read-your-writes (optional)

A caller who just completed a write knows the resulting `newBatchIndex`. On their next read, they can pass it as a query parameter:

```
GET /api/accounts/{id}/balance?minBatchIndex=7
```

The API checks: if the projection's `last_batch_index < 7`, it catches up before returning. This guarantees the caller sees at least their own write, even if a concurrent request hasn't caught up yet.

Without this parameter, the caller might see stale data if another server instance serves the read and hasn't caught up. In practice this is rare (catch-up is fast), but the parameter provides a strong consistency guarantee when needed.

---

## Data Access — Raw Npgsql

Using `NpgsqlDataSource` directly. No ORM — the schema is one table and every query is a conditional write. EF's change tracker would add complexity with no benefit.

### Schema

```sql
CREATE TABLE IF NOT EXISTS account_balances (
    account_id       UUID PRIMARY KEY,
    account_name     TEXT NOT NULL,
    balance_cents    BIGINT NOT NULL DEFAULT 0,
    last_batch_index BIGINT NOT NULL DEFAULT 0,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Intentionally minimal:

- **No event log table** — Celeriant IS the event log. Duplicating it in Postgres defeats the purpose.
- **No checkpoint table** — the checkpoint (`last_batch_index`) is embedded in the projection row. They're always atomically consistent.
- **No outbox table** — `EnforceClientIdempotency` eliminates the need.
- **No sequence table** — `ClientSeq` is derived from the catch-up read, not an external sequence.
- **No migrations** — the API runs `CREATE TABLE IF NOT EXISTS` on startup. One table, no schema evolution to manage.

### Catch-up read

```csharp
await using var cmd = dataSource.CreateCommand(
    "SELECT balance_cents, last_batch_index FROM account_balances WHERE account_id = @id");
cmd.Parameters.AddWithValue("id", accountId);
```

### Catch-up UPSERT (after replaying new events)

```csharp
await using var cmd = dataSource.CreateCommand(@"
    INSERT INTO account_balances (account_id, account_name, balance_cents, last_batch_index, updated_at)
    VALUES (@id, @name, @balance, @batchIndex, now())
    ON CONFLICT (account_id) DO UPDATE
    SET balance_cents = @balance, last_batch_index = @batchIndex, updated_at = now()
    WHERE account_balances.last_batch_index < @batchIndex");
cmd.Parameters.AddWithValue("id", accountId);
cmd.Parameters.AddWithValue("name", accountName);
cmd.Parameters.AddWithValue("balance", newBalanceCents);
cmd.Parameters.AddWithValue("batchIndex", newBatchIndex);
```

The `WHERE last_batch_index < @batchIndex` prevents going backwards if a concurrent request already caught up further.

### Optimistic projection update (after successful Celeriant write)

```csharp
await using var cmd = dataSource.CreateCommand(@"
    UPDATE account_balances
    SET balance_cents = @balance, last_batch_index = @batchIndex, updated_at = now()
    WHERE account_id = @id AND last_batch_index = @expectedBatchIndex");
cmd.Parameters.AddWithValue("id", accountId);
cmd.Parameters.AddWithValue("balance", newBalanceCents);
cmd.Parameters.AddWithValue("batchIndex", newBatchIndex);
cmd.Parameters.AddWithValue("expectedBatchIndex", expectedBatchIndex);
```

0 rows affected → fine, next read will catch up.

### Seeding

On startup, the API ensures the projection rows exist:

```csharp
foreach (var account in Constants.Accounts)
{
    await using var cmd = dataSource.CreateCommand(@"
        INSERT INTO account_balances (account_id, account_name, balance_cents, last_batch_index, updated_at)
        VALUES (@id, @name, 0, 0, now())
        ON CONFLICT (account_id) DO NOTHING");
    cmd.Parameters.AddWithValue("id", account.Id);
    cmd.Parameters.AddWithValue("name", account.Name);
    await cmd.ExecuteNonQueryAsync();
}
```

The actual balance comes from catching up against Celeriant. The seed just ensures the rows exist for the catch-up to update.

The Celeriant seed events (initial deposits) are written the same way as in the simple demo — check if the aggregate exists, if not, write the seed deposit.

---

## REST API

All endpoints validate on the server. The client sends commands, not events.

### GET /api/accounts

Returns account metadata. No projection state — just IDs and names.

```json
{
  "accounts": [
    { "id": "...", "name": "Alice" },
    { "id": "...", "name": "Bob" },
    { "id": "...", "name": "Charlie" }
  ]
}
```

### GET /api/accounts/{accountId}/balance?minBatchIndex={n}

Catches up projection, returns balance. Optional `minBatchIndex` for read-your-writes.

```json
{
  "balanceCents": 50000,
  "batchIndex": 5
}
```

The caller should hold onto `batchIndex` for subsequent writes (OCC) and reads (read-your-writes).

### GET /api/accounts/{accountId}/history?fromBatchIndex={n}

Returns event history for display purposes. Catches up projection first (so the projection is current), then reads events from Celeriant.

```json
{
  "events": [
    {
      "batchIndex": 1,
      "type": "Deposited",
      "amountCents": 50000,
      "timestamp": "2026-03-16T10:00:00Z"
    },
    {
      "batchIndex": 2,
      "type": "Withdrawn",
      "amountCents": 5000,
      "timestamp": "2026-03-16T10:05:00Z"
    }
  ],
  "currentBatchIndex": 2,
  "balanceCents": 45000
}
```

### POST /api/accounts/{accountId}/deposit

```json
{ "amountCents": 10000 }
```

Server-side logic:
1. Validate `amountCents > 0`
2. CatchUp projection
3. Write `Deposited` event to Celeriant with OCC + idempotency
4. Update projection optimistically
5. Return new state

**Response (success):**
```json
{
  "balanceCents": 60000,
  "batchIndex": 6
}
```

**Response (OCC conflict after retries — 409):**
```json
{
  "error": "CONFLICT",
  "message": "Account was modified concurrently. Please retry."
}
```

### POST /api/accounts/{accountId}/withdraw

```json
{ "amountCents": 5000 }
```

Server-side logic:
1. Validate `amountCents > 0`
2. CatchUp projection
3. **Validate `balance - amountCents >= 0`** — this is the key difference from the simple demo
4. Write `Withdrawn` event with OCC + idempotency
5. Update projection
6. Return new state

**Response (insufficient funds — 422):**
```json
{
  "error": "INSUFFICIENT_FUNDS",
  "balanceCents": 3000,
  "message": "Cannot withdraw $50.00 — balance is $30.00"
}
```

### POST /api/transfers

```json
{
  "fromAccountId": "...",
  "toAccountId": "...",
  "amountCents": 7500
}
```

Server-side logic:
1. Validate `amountCents > 0`, `fromAccountId != toAccountId`
2. CatchUp both accounts
3. **Validate source `balance - amountCents >= 0`**
4. Atomic multi-aggregate write to Celeriant (TransferredOut + TransferredIn)
5. Update both projections
6. Return new state for both accounts

**Response (success):**
```json
{
  "from": { "balanceCents": 42500, "batchIndex": 6 },
  "to": { "balanceCents": 32500, "batchIndex": 4 }
}
```

### No clientId in the API

Unlike the simple demo, the caller doesn't provide a `clientId`. The service owns its Celeriant client identity. The caller just sends domain commands. This is the production pattern — services own their event store identity, external consumers don't know or care.

---

## Retry Strategy

### Connection failure vs request timeout

Two distinct failure modes, handled differently:

- **Connection failure** (connection refused, DNS failure, TLS handshake failure): The request never reached the server. No side effects possible. **Fail immediately** — return 503 to the caller. No retry.
- **Request timeout** (connection established, request sent, no response within deadline): The server may or may not have processed the request. **Retry with exponential backoff** — the operation may have succeeded, and the retry must be safe.

This distinction applies to every network call in the chain — Celeriant reads, Celeriant writes, and Postgres operations.

### Catch-up retries

Every step in `CatchUp` is naturally idempotent, so request timeouts are retried directly:

```
CatchUp(accountId):
  1. SELECT from Postgres          — idempotent read, retry on request timeout
  2. ReadAsync from Celeriant      — idempotent read, retry on request timeout
  3. Replay events (local)         — no network
  4. UPSERT into Postgres          — idempotent (WHERE last_batch_index < @new), retry on request timeout
  5. Return updated projection
```

Each step retries independently with exponential backoff (e.g. 100ms, 200ms, 400ms) up to a small limit (e.g. 3 attempts). Connection failures fail the whole request immediately.

### Write path retries

Writes can fail due to OCC (another writer modified the aggregate) or ambiguous request timeouts (the Celeriant write may or may not have landed). The API retries both cases internally:

```
projection = CatchUp(accountId)                              // has its own retry-on-timeout
clientSeq = max(our ClientId) + 1 from catch-up       // derived ONCE per request

for attempt in 1..MAX_RETRIES:
    if attempt > 1:
        projection = CatchUp(accountId)                      // fresh projection for retry
    if not valid(projection, command):
        return 422 (validation failure against current state)
    try:
        write to Celeriant with OCC + clientSeq
        update projection                                    // Postgres UPDATE, retries on timeout
        return 200
    catch WriteOccException:
        backoff(attempt)
        continue  // retry with fresh projection, BUT same clientSeq
    catch RequestTimeoutException:
        backoff(attempt)
        continue  // ambiguous — retry with fresh projection, BUT same clientSeq
    catch IdempotencyViolationException:
        // our prior attempt within this request actually landed
        CatchUp(accountId)  // absorb the landed event into projection
        return 200
    catch ConnectionFailure:
        return 503  // server unreachable, fail immediately
return 409  // exhausted retries
```

**Critical: `clientSeq` is derived once from the first catch-up and held constant across all retry attempts within the same request.** On retry after a timeout, the catch-up may reveal our own previously-landed event. If we re-derived `max + 1`, the new index would be higher and Celeriant would accept a duplicate. By holding it constant, `IdempotencyViolationException` fires and we know the write already landed.

**Backoff:** Exponential with jitter — e.g. 100ms × 2^attempt + random jitter. Keeps retry storms manageable under contention.

This keeps the retry loop inside the API. The caller sees either success, a validation error (with current state), a 503 (Celeriant unreachable), or a 409 after retries are exhausted. Suggested `MAX_RETRIES = 3`.

For transfers, OCC or timeout failure on either account triggers a full retry (catch up both, re-validate, re-attempt the atomic write with the same `clientSeq` values).

---

## HTTP-Level Idempotency

Celeriant's `ClientSeq` protects against infrastructure-level duplicates (crash between write and ack). But it doesn't protect against application-level retries — a user clicking "Transfer" twice, or a browser retrying after a timeout, produces two separate catch-ups with two separate `ClientSeq` values. From Celeriant's perspective, both are legitimate new writes.

This is solved with a short-lived in-memory cache at the API boundary. It's a generic API concern, not specific to Celeriant.

### Design

- Caller sends an `Idempotency-Key` header (UUID) with every write request
- API checks a `ConcurrentDictionary<Guid, (Result, DateTimeOffset)>` before processing
- Cache hit → return the stored result immediately, no write attempted
- Cache miss → process normally, store the result on success
- **90-second TTL**, lazily evicted — no background thread

### Lazy eviction

On every cache lookup, scan and remove entries older than 90 seconds. With a short TTL and low entry count (only in-flight and recent writes), the scan is negligible.

```csharp
public class IdempotencyCache
{
    private readonly ConcurrentDictionary<Guid, (object Result, DateTimeOffset ExpiresAt)> _cache = new();

    public bool TryGet(Guid key, out object result)
    {
        Evict();
        if (_cache.TryGetValue(key, out var entry) && entry.ExpiresAt > DateTimeOffset.UtcNow)
        {
            result = entry.Result;
            return true;
        }
        result = null;
        return false;
    }

    public void Set(Guid key, object result)
    {
        _cache[key] = (result, DateTimeOffset.UtcNow.AddSeconds(90));
    }

    private void Evict()
    {
        var now = DateTimeOffset.UtcNow;
        foreach (var kvp in _cache)
        {
            if (kvp.Value.ExpiresAt <= now)
                _cache.TryRemove(kvp.Key, out _);
        }
    }
}
```

### What this is NOT

- Not a correctness requirement — if the cache is lost (process restart, different instance), the system is still safe. Catch-up + OCC + ClientSeq handle it.
- Not shared across instances — a retry hitting a different instance goes through the full write path, which is safe.
- Not durable — 90 seconds covers button smashing and fast HTTP retries. Anything longer is a new user action.

---

## Project Structure

```
Celeriant.Reference/
├── Program.cs                # Endpoints, DI, seed logic
├── AccountService.cs         # Catch-up, validate, write, retry loop, SQL — the whole pattern in one file
├── AccountEvents.cs          # Event record types + hard-coded IDs + deterministic GUID helper
├── IdempotencyCache.cs       # ConcurrentDictionary with 90s lazy eviction
├── Celeriant.Reference.csproj # net8.0, refs Celeriant.Client, Npgsql
└── docker-compose.yml         # Celeriant server + Postgres + API
```

Intentionally flat. `AccountService` is one class with `CatchUpAsync`, `WriteDepositAsync`, `WriteWithdrawAsync`, `WriteTransferAsync`, and the event replay logic. No repository abstractions, no generic projector framework — just the pattern, clearly expressed.

---

## Docker Compose

```yaml
services:
  celeriant-server:
    image: celeriant-server:latest
    ports:
      - "10000:10000"
    security_opt:
      - seccomp=unconfined
    ulimits:
      memlock:
        soft: -1
        hard: -1
    command:
      - "--standalone"
      - "--data-root"
      - "/var/lib/celeriant"
      - "--client-port"
      - "10000"
      - "--num-shards"
      - "1"
      - "--log-level"
      - "warn"
    volumes:
      - celeriant-data:/var/lib/celeriant

  postgres:
    image: postgres:16
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: celeriant_reference
      POSTGRES_USER: demo
      POSTGRES_PASSWORD: demo
    volumes:
      - pg-data:/var/lib/postgresql/data

  reference-api:
    build: .
    ports:
      - "5001:8080"
    environment:
      - Celeriant__Address=celeriant-server:10000
      - ConnectionStrings__Postgres=Host=postgres;Database=celeriant_reference;Username=demo;Password=demo
    depends_on:
      - celeriant-server
      - postgres

volumes:
  celeriant-data:
  pg-data:
```

---

## Key Differences from Simple Demo

| Aspect | Simple Demo | Reference API |
|--------|------------|---------------|
| Validation | Browser-side | Server-side |
| Projection | Browser accumulates events | PostgreSQL read model |
| Projection updates | Manual "Refresh" button | Lazy catch-up on every request |
| API role | Dumb stateless proxy | Validates, projects, manages consistency |
| Idempotency | None | ClientSeq + EnforceClientIdempotency |
| Client identity | Caller provides clientId | Service owns its clientId |
| OCC handling | Caller retries | Internal retry loop |
| Background services | None | None (both are stateless) |
| Read model | In-memory (browser) | PostgreSQL |
| Infrastructure | Celeriant only | Celeriant + PostgreSQL |

---

## What This Demonstrates

1. **CQRS with lazy projection** — write model (Celeriant events) separated from read model (Postgres), updated on demand
2. **Exactly-once writes** — `ClientSeq` + `EnforceClientIdempotency` without an outbox table
3. **Self-healing after crashes** — lazy catch-up naturally recovers from any failure between Celeriant write and Postgres update
4. **Server-side invariant enforcement** — balance checked on the backend, not trusted from the client
5. **Optimistic concurrency with internal retry** — OCC conflicts handled transparently, caller just sees success or failure
6. **Atomic multi-aggregate writes** — transfers span two aggregates in one write, with OCC on both
7. **Read-your-writes consistency** — `minBatchIndex` parameter for strong reads when needed
8. **Stateless API servers** — no background threads, no singleton coordination, trivial horizontal scaling
