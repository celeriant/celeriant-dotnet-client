# Exactly-Once Failure Analysis

Full chain-of-operations analysis for a single write request (e.g. withdraw), examining every failure point and the controls that maintain the exactly-once invariant. Updated to reflect the actual implementation in `AccountService.cs`.

## Defence-in-Depth Layers

| Layer | Mechanism | Protects Against |
|-------|-----------|-----------------|
| Browser | Idempotency key (UUID) per action | Button smashing, UI-level retries |
| API in-memory cache | `IdempotencyCache` (`ConcurrentDictionary<Guid, Result>`) with 90s TTL | Fast HTTP retries hitting the same instance |
| Celeriant write path | `ClientSeq` (max + 1) + `EnforceClientIdempotency` | Infrastructure-level duplicates (crash between write and ack) |
| Celeriant write path | `ExpectedVersion` (OCC) | Stale reads, concurrent writers |
| Lazy catch-up | Projection rebuilt from Celeriant on next read | Self-healing after any crash between Celeriant write and Postgres update |
| Postgres projection | `last_client_event_index` persisted alongside balance | Avoids full Celeriant scan; self-heals via replay of new batches |

## Implementation: ClientSeq Derivation

Unlike the original design which proposed a separate Celeriant read to derive `max(ClientSeq)`, the implementation stores `last_client_event_index` in the Postgres `account_balances` row. This is updated in two places:

1. **During catch-up replay** — when new batches are read from Celeriant, any batch with `ClientId == ServiceClientId` has its events scanned for the highest `ClientSeq`. The running max is persisted in the UPSERT.

2. **During optimistic projection update** — after a successful write, `UpdateProjectionOptimistically` writes the new `clientSeq` alongside the updated balance and batch index.

**Self-healing property:** If the Postgres `last_client_event_index` is stale (due to a failed optimistic update or process crash), the next catch-up replays new batches from Celeriant and picks up any events from our `ClientId` in those batches, correcting the max. Since a stale `last_client_event_index` is always paired with a stale `last_batch_index` (both are written atomically in the same row), the catch-up always reads the missing batches and self-corrects.

**Invariant:** `last_client_event_index` in Postgres is always <= the true max in Celeriant. It may lag behind temporarily, but the catch-up replay brings it current before any write uses it.

## Implementation: OCC vs Timeout Retry Distinction

The retry loop distinguishes two failure types:

- **`WriteOccException`** — Celeriant definitively rejected the write. Our event was never accepted. On retry, `clientSeq` is **re-derived** from the fresh catch-up (`projection.MaxClientSeq + 1`). This prevents a false `IdempotencyViolation` when a concurrent writer used the same index.

- **`CeleriantTimeoutException`** — Ambiguous. The write may or may not have landed. On retry, `clientSeq` is **held constant** from the initial derivation. If the write did land, `IdempotencyViolation` catches the duplicate. If it didn't, the retry proceeds normally.

This distinction is critical. Holding constant on OCC retry would cause silent write drops when concurrent requests derive the same `clientSeq` (see E-EDGE).

## Full Chain of Operations

```
A. Browser generates idempotency key (UUID), sends HTTP request
B. API receives request, checks in-memory idempotency cache
C. API catches up projection (Postgres read + Celeriant ReadAsync + replay)
D. API validates business rules against projection
E. API derives ClientSeq = last_client_event_index + 1 from projection
F. API sends WriteAsync to Celeriant (OCC + EnforceClientIdempotency)
G. Celeriant validates OCC (ExpectedVersion)
H. Celeriant validates client idempotency (ClientSeq)
I. Celeriant fsyncs to disk
J. Celeriant replicates to follower
K. Celeriant returns success response to API
L. API stores result in in-memory idempotency cache
M. API optimistically updates Postgres projection (balance + batch index + client event index)
N. API returns HTTP response to browser
```

---

## Step A — Browser generates idempotency key, sends HTTP request

### A-PASS

UUID generated via `crypto.randomUUID()`, request dispatched with `Idempotency-Key` header. Proceed to B.

### A-FAIL: Network failure before request reaches API

**Scenario:** Browser has no network connectivity, DNS failure, or TLS handshake failure.

**Impact:** Request never left the browser. No side effects anywhere.

**Exactly-once maintained?** Yes. Nothing happened. Browser can retry with the same idempotency key.

---

## Step B — API checks in-memory idempotency cache

### B-PASS (cache miss)

Idempotency key not found in cache. This is a new request. Proceed to C.

### B-PASS (cache hit)

Idempotency key found in `IdempotencyCache` with a stored result.

**Action:** Return the stored result immediately. No write attempted.

**Exactly-once maintained?** Yes. This is a duplicate request (button smash or fast retry). The original result is returned.

### B-FAIL: API instance crashed/restarted (cache lost)

**Scenario:** The in-memory cache is empty because the process restarted between the original request and the retry.

**Impact:** Cache miss. Request proceeds as new through steps C-N. However, if the original request completed step F (Celeriant write), the catch-up in step C will reveal the event. If it didn't complete step F, there's nothing to deduplicate — the original request had no side effect.

**Exactly-once maintained?** Not guaranteed if the original write landed and is visible on the read path.
- If original write landed and is visible: catch-up absorbs it, `last_client_event_index` advances, derives new `max + 1` and new `ExpectedVersion`. Neither OCC nor ClientSeq will reject the retry — both values are fresh. **The retry is accepted as a genuinely new event. This is a business-level duplicate.** If the retry happens to fail validation (e.g. insufficient funds after the first withdrawal), the duplicate is prevented by business rules, not by infrastructure.
- If original write landed but is NOT yet visible (not replicated to read path): OCC rejects the retry (write-path cache knows the batch index advanced). Safe.
- If original write did NOT land: no side effect from original, retry is the first real attempt. Safe.

### B-FAIL: Retry hits different API instance (load balancer)

**Scenario:** In-memory cache is per-instance. Retry routed to a different instance.

**Impact:** Same as cache lost — proceeds as new request. Same analysis as above.

**Exactly-once maintained?** Not guaranteed — same gap as above. The in-memory cache is the only layer that prevents HTTP-level duplicates once the event is visible on the read path. Celeriant's ClientSeq protects against infrastructure duplicates (same derived values reused), not against retries that catch up and derive new values.

---

## Step C — API catches up projection (CatchUpAsync)

### C-PASS

CatchUp runs:
1. SELECT from Postgres → `balance_cents`, `last_batch_index`, `last_client_event_index`
2. ReadAsync from Celeriant with `FromAggregateVersion = last_batch_index + 1`
3. If no new events → return current projection (already up to date)
4. Replay new events to compute updated balance; scan new batches for our `ClientId` to update `maxClientSeq`
5. UPSERT into Postgres with `WHERE last_batch_index < @newBatchIndex` (prevents going backwards)
6. Return updated projection with `MaxClientSeq`

Proceed to D.

### C-FAIL: Connection failure (Celeriant or Postgres unreachable)

**Scenario:** Celeriant server or Postgres is down or unreachable. Connection never established.

**Impact:** Request never reached the service. No state changed anywhere.

**Action:** Fail immediately. Return 503 to browser. No retry — if we can't connect, retrying won't help.

**Exactly-once maintained?** Yes. Nothing happened.

### C-FAIL: Request timeout (connection established, no response)

**Scenario:** Connection to Celeriant (or Postgres) was established and the read request was sent, but no response arrived within the deadline.

**Impact:** The read is idempotent — safe to retry regardless of whether the server processed it. The Postgres UPSERT has `WHERE last_batch_index < @newBatchIndex`, making it idempotent too.

**Action:** Exception propagates up. API returns 500 to browser. Browser retries.

**Exactly-once maintained?** Yes. Reads and conditional UPSERTs have no side effects worth worrying about.

### C-EDGE: Catch-up sees an event from a previous request we thought failed

**Scenario:** A prior request completed step F (Celeriant write) but failed at step K-N (ack lost, API crashed). Now the catch-up reveals that event.

**Impact:** The projection updates to include the previous write's effects. The `maxClientSeq` advances past the value that previous request used. The Postgres `last_client_event_index` is updated by the replay.

**Exactly-once maintained?** Yes. This is the self-healing property. The "lost" write is recovered through catch-up. The current request proceeds with accurate state.

### C-EDGE: Postgres has stale `last_client_event_index`

**Scenario:** A previous write succeeded at Celeriant but `UpdateProjectionOptimistically` failed (M-FAIL). Postgres has `last_batch_index = N` and `last_client_event_index = X`, but the true values are `N+1` and `X+1`.

**Impact:** CatchUp reads Postgres (`N`, `X`), reads new events from Celeriant starting at `N+1`. Finds our event with `ClientSeq = X+1`. Replay updates `maxClientSeq` to `X+1`. UPSERTs corrected values.

**Exactly-once maintained?** Yes. Stale `last_client_event_index` always pairs with stale `last_batch_index` (same row, atomic write). The catch-up replay naturally corrects both.

---

## Step D — API validates business rules against projection

### D-PASS

Validation succeeds (e.g. balance >= withdrawal amount). Proceed to E.

### D-FAIL: Validation rejects the command

**Scenario:** Insufficient funds, invalid amount, etc.

**Impact:** No write attempted. Return 422 to browser with current state (`InsufficientFundsException` includes `BalanceCents`).

**Exactly-once maintained?** Yes. No side effects.

### D-EDGE: Validation passes against stale projection

**Scenario:** Another writer committed an event between our catch-up (C) and now. Our projection is stale. We validate against outdated balance.

**Impact:** We proceed with an incorrect validation result. However, Celeriant's OCC check (step G) will catch this — our `ExpectedVersion` won't match.

**Exactly-once maintained?** Yes. OCC is the safety net for stale reads.

---

## Step E — API derives ClientSeq = max + 1

### E-PASS

`clientSeq = projection.MaxClientSeq + 1`. This is `last_client_event_index + 1` from Postgres (possibly updated by catch-up replay). Proceed to F.

### E-EDGE: Two concurrent API instances derive the same index

**Scenario:** Both instances catch up at the same time, see the same `last_client_event_index`, both compute `max + 1`.

**Impact:** Both will attempt to write with the same `ClientSeq` and the same `ExpectedVersion`. One will succeed and advance the batch index. The other gets `WriteOccException` (OCC fails because batch index advanced).

**On OCC retry, the loser re-derives `clientSeq`:** CatchUp picks up the winner's event (which advances `last_client_event_index`). The loser derives a new `max + 1` that is higher than the winner's index. The retry succeeds with its own event.

**Why re-derive on OCC is safe:** OCC means our write was definitively rejected — the event never landed. There is no ambiguity (unlike a timeout). Re-deriving produces a fresh, non-colliding index.

**Why holding constant on OCC would be wrong:** After the loser catches up, its `ExpectedVersion` is fresh (passes OCC), but its held `ClientSeq` matches the winner's committed event. `IdempotencyViolation` fires — the loser falsely believes its own prior attempt landed. The loser's write is **silently dropped**. This was a bug in the initial implementation.

**Exactly-once maintained?** Yes, because OCC retries re-derive `clientSeq`.

### E-EDGE: `last_client_event_index` not yet in Postgres (first write after seed)

**Scenario:** Postgres has `last_client_event_index = 0` because the seed event's `clientSeq` hasn't been absorbed yet.

**Impact:** CatchUp reads Postgres (`last_batch_index = 0`, `last_client_event_index = 0`), reads events from Celeriant starting at batch 1. Finds the seed batch from our `ServiceClientId` with `clientSeq = 1`. Replay updates `maxClientSeq` to 1. UPSERTs corrected values. Write derives `clientSeq = 2`.

**Exactly-once maintained?** Yes. The catch-up replay always corrects stale values.

---

## Step F — API sends WriteAsync to Celeriant

### F-PASS

Request reaches Celeriant. Proceed to G.

### F-FAIL: Connection failure (connection refused, connection dropped before send)

**Scenario:** Cannot establish connection to Celeriant, or connection drops before the write request is sent.

**Impact:** Write never reached Celeriant. No side effects.

**Action:** `ConnectionFailedException` propagates. Return 503 to browser. No retry.

**Exactly-once maintained?** Yes. Nothing happened.

### F-FAIL: Request timeout (connection established, request sent, no response)

**Scenario:** Connection was established and the write request was sent, but no response arrived. This covers: Celeriant received but hasn't responded, TCP dropped mid-processing, slow response. From the API's perspective, this is ambiguous — indistinguishable from K-FAIL.

**Impact:** The write may or may not have been processed by Celeriant.

**Action:** `CeleriantTimeoutException` caught. The retry loop holds `clientSeq` constant, catches up with fresh `ExpectedVersion`, and retries. See K-FAIL for the full analysis.

**Exactly-once maintained?** Yes — see K-FAIL analysis.

---

## Step G — Celeriant validates OCC (ExpectedVersion)

### G-PASS

`ExpectedVersion` matches the aggregate's current batch index in the write-path mem cache. No concurrent modification since our catch-up. Proceed to H.

### G-FAIL: OptimisticConcurrencyViolation

**Scenario:** Another writer committed an event to this aggregate between our catch-up (step C) and our write (step F). The batch index advanced.

**Impact:** Write rejected. No side effects at Celeriant.

**Action:** `WriteOccException` caught. API re-derives `clientSeq` from fresh catch-up and retries. After N retries, return 409 to browser.

**Exactly-once maintained?** Yes. No event was written. Re-derive is safe (write was definitively rejected).

### G-CRITICAL: Why OCC must be checked before idempotency

If idempotency were checked first, a concurrent writer using the same derived `ClientSeq` (same `max + 1` from same catch-up state) would get `IdempotencyViolationException`. The API would treat this as "my write already landed" — but it wasn't our write. The concurrent writer's different event would be treated as ours.

With OCC first: the stale `ExpectedVersion` catches the concurrent modification, and we correctly retry with fresh state and a re-derived `clientSeq`.

---

## Step H — Celeriant validates client idempotency (ClientSeq)

### H-PASS

`ClientSeq` is greater than the last accepted index for our `(AggregateKey, ClientId)` pair. Proceed to I.

### H-FAIL: ClientIdempotencyViolation

**Scenario:** OCC passed (step G), meaning the batch index matched — no concurrent writer modified this aggregate. But our `ClientSeq` is <= the last accepted index for our ClientId.

**Since OCC passed, this means:**
- The aggregate state hasn't changed since our catch-up
- Yet our ClientId has a higher recorded index than we derived from catch-up

**This can only happen in the timeout retry path:** A previous attempt within this request wrote to Celeriant, the write was accepted (index recorded in mem cache), but the response was lost (timeout). Our retry caught up but the event wasn't yet visible on the read path. OCC passes (write-path and read-path may have different visibility windows), but the held-constant `clientSeq` triggers the idempotency guard.

**Action:** Treat as success. The prior write with this index already landed. Catch up the projection, return success to browser.

**Exactly-once maintained?** Yes. This is the designed deduplication. The event exists exactly once in Celeriant.

**Note:** This exception should never fire after an OCC retry, because OCC retries re-derive `clientSeq`. If it does fire after an OCC retry, it indicates a logic error.

---

## Step I — Celeriant fsyncs to disk

### I-PASS

Event durably written to the leader's WAL. Proceed to J.

### I-FAIL: Fsync failure (disk full, I/O error)

**Scenario:** The write was validated but couldn't be persisted.

**Impact:** Celeriant performs fsync rollback — clears queue positions, aggregate write snapshots, and client snapshots from mem cache. The write is as if it never happened.

**Action:** Celeriant returns error to API. API returns 500 to browser. Browser retries with same idempotency key.

**On retry:** Catch-up shows no new event (write was rolled back). Same `max + 1` derived. Same write attempted. If disk issue resolved, succeeds normally.

**Exactly-once maintained?** Yes. The rolled-back write left no trace.

---

## Step J — Celeriant replicates to follower

### J-PASS

Event replicated to follower node. Now durable on both nodes. Event becomes visible to read path. Proceed to K.

### J-FAIL: Replication timeout/failure

**Scenario:** Event is fsynced on leader but follower is unreachable or slow.

**Impact depends on Celeriant's durability configuration:**
- **If configured for leader-only durability:** Write already succeeded at step I. Replication is async. Proceed to K.
- **If configured for replicated durability:** Write blocks until replication succeeds or times out. On timeout, Celeriant may return error.

**If error returned to API:** API returns 500 to browser. The event IS on the leader's disk but may or may not survive a leader failure.

**On retry (leader survived):** Catch-up may or may not see the event (read visibility depends on replication). If not visible, same `max + 1` derived, write attempted, `IdempotencyViolationException` (index already in leader's mem cache) — treated as success. If visible, catch-up absorbs it, proceeds normally.

**On retry (leader failed, follower promoted):** The event may be lost if it wasn't replicated. Retry writes it fresh. This is a data loss scenario inherent to the replication config, not an exactly-once violation.

**Exactly-once maintained?** Yes, within the durability guarantee configured. If the event survives, it exists exactly once. If it's lost due to leader failure before replication, the retry is the first successful write.

---

## Step K — Celeriant returns success response to API

### K-PASS

API receives confirmation that the write succeeded. Proceed to L.

### K-FAIL: Response lost (TCP reset, timeout on read)

**Scenario:** Celeriant processed the write successfully (steps G-J all passed), but the response never reached the API. The API sees a `CeleriantTimeoutException`.

**This is the critical ambiguous failure.** The event IS durably stored in Celeriant. The API doesn't know this. The API cannot distinguish this from F-FAIL (write never reached Celeriant).

**Action:** The retry loop holds `clientSeq` constant and catches up for a fresh `ExpectedVersion`.

**Internal retry (same ClientSeq, fresh catch-up):**
- **If event is visible (replicated):** Catch-up absorbs the event. Projection updates. `last_client_event_index` advances. Fresh `ExpectedVersion` derived. API retries WriteAsync with the same `clientSeq` but updated `ExpectedVersion`. Celeriant checks:
  1. OCC passes (fresh `ExpectedVersion` is correct)
  2. Idempotency check: `ClientSeq` <= last accepted for our `ClientId` → `IdempotencyViolationException`
  3. API treats as success — the original write landed. Catches up projection and returns success.
- **If event is NOT yet visible (not replicated to read path):** Catch-up doesn't see the event. Same state as before. Same `ExpectedVersion`. Write sent to Celeriant. OCC rejects (write-path cache knows the batch index advanced). **This is treated as an OCC retry**, so `clientSeq` is re-derived. But re-derivation produces the same value (catch-up didn't see new events). Eventually replication completes, catch-up sees the event, and the idempotency path above resolves it.

**Why holding ClientSeq constant on timeout matters:** If the API re-derived `max + 1` from the catch-up, and the event was visible, the new `max + 1` would be higher than the original. Both OCC and idempotency checks would pass — Celeriant would accept a duplicate event. By holding `clientSeq` constant across timeout retries, the idempotency guard catches the already-landed write.

**If the API process crashes before it can retry:** The browser retries with the same idempotency key. The in-memory cache is lost. The browser's retry goes through the full chain with a fresh catch-up and a fresh `max + 1` derivation. This is the B-FAIL/N-FAIL gap — see those sections.

**Exactly-once maintained?** Yes, as long as the API process stays alive to execute the internal retry with the preserved `clientSeq`. If the process crashes, the in-memory idempotency cache becomes the only guard (see B-FAIL, N-FAIL).

---

## Step L — API stores result in in-memory idempotency cache

### L-PASS

Result stored in `IdempotencyCache` with 90s TTL. Any duplicate request with the same idempotency key hitting this instance within the TTL window gets the cached result. Proceed to M.

### L-FAIL: Cache insertion fails (OOM, process about to crash)

**Scenario:** The Celeriant write succeeded (we have the result) but we can't store it in the cache.

**Impact:** If the process crashes before returning a response, this degrades to the K-FAIL scenario (ack lost + process crash). If the process stays alive but the cache write failed, and also N fails (response lost), then on retry:

- Step B: Cache miss (insertion failed)
- Step C: Catch-up from Celeriant
  - **If event is visible (replicated):** Projection updates. `last_client_event_index` advances. New `max + 1` derived. New `ExpectedVersion` derived. **Neither OCC nor ClientSeq will reject the retry** — both values are fresh and valid. Celeriant accepts it as a genuinely new event. **This is a business-level duplicate.**
  - **If event is NOT yet visible:** Same `max + 1` and `ExpectedVersion` as before. OCC rejects it (write-path cache knows the batch index advanced). Safe.

**Exactly-once maintained?** Not guaranteed. Same gap as B-FAIL.

---

## Step M — API optimistically updates Postgres projection

### M-PASS

```sql
UPDATE account_balances
SET balance_cents = @new, last_batch_index = @newBatchIndex,
    last_client_event_index = @clientSeq
WHERE account_id = @id AND last_batch_index = @expectedBatchIndex
```

Rows affected = 1. Projection is current, including `last_client_event_index`. Proceed to N.

### M-PASS (0 rows affected)

**Scenario:** Another request already caught up the projection past our batch index (concurrent catch-up from a parallel request).

**Impact:** The projection is already at least as current as our write. No action needed. The `last_client_event_index` may be slightly stale in Postgres, but the next catch-up will correct it.

**Exactly-once maintained?** Yes. The projection is correct (possibly more current than our write).

### M-FAIL: Postgres connection failure

**Scenario:** Postgres is down or connection pool exhausted. Cannot establish connection.

**Impact:** The Celeriant write succeeded. The event is durable. The projection is stale. `last_client_event_index` in Postgres is stale.

**Action:** Log the error. Return success to browser anyway (the write DID succeed — Celeriant is the source of truth, not Postgres). See `UpdateProjectionOptimistically` — the catch block logs and continues.

**Self-healing:** The next request for this account triggers catch-up (step C), which reads from Celeriant, replays new events (updating `maxClientSeq` from our batch), and UPSERTs corrected values into Postgres. Both `balance_cents` and `last_client_event_index` are restored.

**Exactly-once maintained?** Yes. The event exists once in Celeriant. The projection catches up lazily.

### M-FAIL: Postgres request timeout

**Scenario:** Connection established, UPDATE sent, no response within deadline. The UPDATE may or may not have been applied.

**Impact:** The Celeriant write succeeded. The projection may or may not be current.

**Action:** The `UpdateProjectionOptimistically` catch block logs and returns success. The UPDATE is idempotent — `WHERE last_batch_index = @expectedBatchIndex` means applying it twice is a no-op. Self-heals on next catch-up.

**Exactly-once maintained?** Yes.

### M-FAIL: API crashes between Celeriant write (K) and Postgres update (M)

**Scenario:** Process killed, hardware failure, OOM.

**Impact:** Event is in Celeriant. Projection is stale (both balance and `last_client_event_index`). In-memory cache is lost.

**Self-healing:** Next request catches up, replays the missing event(s), corrects both `balance_cents` and `last_client_event_index` in Postgres.

**Exactly-once maintained?** Yes (for the event layer). The browser retry issue (B-FAIL/N-FAIL gap) applies at the HTTP layer.

---

## Step N — API returns HTTP response to browser

### N-PASS

Browser receives 200 with new balance and batch index. Operation complete.

### N-FAIL: Response lost (network failure, browser timeout)

**Scenario:** API sent the response but browser didn't receive it.

**Impact:** From browser's perspective, the request failed. The write succeeded. Projection is updated. In-memory cache has the result.

**On browser retry (same idempotency key):**
- Same API instance: Step B cache hit. Returns stored result. No duplicate.
- Different API instance: Step B cache miss. Step C catch-up sees the event. Derives new `max + 1` and new `ExpectedVersion`. **Neither OCC nor ClientSeq will reject the retry** — the retry is accepted as a genuinely new event. Business-level duplicate unless validation rejects it (e.g. insufficient funds).

**Exactly-once maintained?** Only if the retry hits the same instance (cache hit). If it hits a different instance, same gap as B-FAIL.

---

## Summary: Failure Mode Coverage Matrix

| Failure Point | Event Written? | Duplicate Risk? | Control |
|--------------|---------------|-----------------|---------|
| A-FAIL: Network before API | No | None | No side effects |
| B-FAIL: Cache lost/wrong instance | Maybe | **Yes, if event visible on retry** | In-memory cache is the only HTTP-level dedup guard |
| C-FAIL: Celeriant/Postgres read fails | No | None | No side effects |
| D-FAIL: Validation rejects | No | None | No side effects |
| E-EDGE: Concurrent same index | No (OCC rejects loser) | None | OCC retry re-derives clientSeq |
| F-FAIL: Network before Celeriant | No | None | No side effects, return 503 |
| G-FAIL: OCC violation | No | None | Internal retry with re-derived clientSeq |
| H-FAIL: Idempotency violation | Yes (prior timeout attempt) | None | Treated as success (OCC passed first) |
| I-FAIL: Fsync failure | No | None | Rolled back, safe to retry |
| J-FAIL: Replication failure | Leader only | None* | Depends on durability config |
| K-FAIL: Ack lost (timeout) | Yes | None (if API retries inline) | Internal retry with preserved clientSeq → IdempotencyViolation catches landed write |
| L-FAIL: Cache insert fails | Yes | **Yes, if event visible on retry** | Same gap as B-FAIL |
| M-FAIL: Postgres unavailable | Yes | None | Lazy catch-up self-heals both balance and last_client_event_index |
| N-FAIL: Response lost to browser | Yes | **Yes, if retry hits different instance** | In-memory cache protects same-instance only |

## Critical Failure: K-FAIL (ack lost after successful write)

The API's internal retry loop handles this by **holding `clientSeq` constant** across timeout retries (but NOT OCC retries) within the same request:

1. Catch up with fresh `ExpectedVersion`, but **same** `clientSeq`
2. If event is visible: OCC passes, `IdempotencyViolationException` fires → treated as success
3. If event is not visible: OCC rejects (write-path cache advanced) → retry again. Since this is an OCC rejection, `clientSeq` would normally be re-derived, but the catch-up didn't see new events from our ClientId, so re-derivation produces the same value. Eventually replication completes and path (2) resolves it.

## Critical Distinction: OCC Retry vs Timeout Retry

| | OCC Retry | Timeout Retry |
|---|-----------|---------------|
| **Our write landed?** | Definitively NO | Ambiguous |
| **clientSeq** | Re-derived from fresh catch-up | Held constant |
| **Why** | Prevents false IdempotencyViolation from concurrent writer's index | Ensures IdempotencyViolation catches our own landed write |
| **Risk if wrong** | Silent write drop (held) / Duplicate (re-derived) | Duplicate (re-derived) / Correct (held) |

## Remaining Gap: B-FAIL/N-FAIL

When the API process crashes after a successful Celeriant write but before returning a response to the browser, and the browser retries against a different instance (or the same instance after restart), the `clientSeq` is re-derived from catch-up and advances past the landed event. The in-memory idempotency cache (per-instance, non-durable, 90s TTL) is the only guard against a business-level duplicate in this scenario.

**Impact:** The duplicate is a **new, valid event** — same business intent but with fresh OCC and idempotency values. It passes all Celeriant-level checks.

**Mitigation options (not implemented):**
- Durable idempotency store (e.g. Postgres-backed `Idempotency-Key → result` table) would close this gap completely
- Sticky sessions (route retries to the same instance) would make the in-memory cache effective across browser retries
- Shorter client timeouts with `AbortController` reduce the window where the browser doesn't know the outcome

**Practical risk:** The window is narrow. It requires: (1) the Celeriant write to succeed, (2) the API to crash or the response to be lost, (3) the browser to retry to a different instance, (4) all within the time it takes for the event to become visible on the read path. In most deployments this is acceptable; for financial-grade dedup, a durable idempotency store is recommended.

---

## Postgres Projection: Self-Healing Properties

The `account_balances` table stores `last_client_event_index` alongside `balance_cents` and `last_batch_index`. All three are written atomically in the same row, and all three self-heal via the same catch-up mechanism:

| Scenario | What's stale? | How it self-heals |
|----------|---------------|-------------------|
| M-FAIL: Postgres update fails after Celeriant write | All three (balance, batch index, client event index) | Next catch-up replays from `last_batch_index + 1`, picks up the missing event(s), UPSERTs corrected values |
| M-PASS 0 rows: concurrent catch-up already advanced | `last_client_event_index` may lag | Next catch-up that replays events from our ClientId corrects it |
| Process crash between Celeriant write and Postgres update | All three | Same as M-FAIL — catch-up replays and corrects |
| Postgres wiped (disaster recovery) | All three reset to 0 | Full replay from Celeriant batch 1 rebuilds everything |

**Key invariant:** `last_client_event_index` in Postgres is always <= the true max in Celeriant. It may lag, but it never leads. Catch-up replay only advances it — the UPSERT `WHERE last_batch_index < @batchIndex` prevents going backwards, and we only update `maxClientSeq` when we see a higher value during replay.

---

## Invariant Statement

The combination of OCC, ClientSeq idempotency, the internal retry loop with **distinct OCC vs timeout handling**, lazy catch-up, and Postgres-backed `last_client_event_index` provides exactly-once semantics **within a single API request lifetime**:

- **Concurrent writers** are serialised by OCC; the loser re-derives `clientSeq` and succeeds on retry
- **Lost acks (K-FAIL)** are handled by the internal retry loop preserving `clientSeq` → `IdempotencyViolationException` catches the landed write
- **Stale projections** (including `last_client_event_index`) self-heal through lazy catch-up replay on the next read or write

**Not fully covered:** If the API process crashes after a successful Celeriant write but before returning a response to the browser, and the browser retries against a different instance (or the same instance after restart), the `clientSeq` is re-derived from catch-up and advances past the landed event. The in-memory idempotency cache (per-instance, non-durable, 90s TTL) is the only guard against a business-level duplicate in this scenario. For stronger guarantees, a durable idempotency store (e.g. Postgres-backed) would be needed at the HTTP boundary.
