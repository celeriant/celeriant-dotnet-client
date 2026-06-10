# Celeriant.Reference

A production-grade reference API showing how to build an event-sourced system with [Celeriant](https://celeriant.io) and a Postgres read projection. Safe to run as a fleet of replicas sharing one client id.

Banking domain: deposits, withdrawals, and atomic multi-aggregate transfers with server-side balance validation.

## Run

```bash
docker compose up -d
```

Open http://localhost:5001.

## What it demonstrates

- **Lazy catch-up projection**: Postgres read model rebuilt on-demand from Celeriant, no background projection service
- **Exactly-once writes**: `ClientSeq` derived from catch-up + `EnforceClientIdempotency` on the server
- **OCC retry loops**: re-derive state on conflict, retry with fresh `expectedVersion`
- **Atomic multi-aggregate transfers**: debit and credit written in a single `WriteRequest` with OCC on both
- **HTTP idempotency via `Idempotency-Key`**: a request-response cache (`request_responses` table) lives next to the projection cursor and moves with it atomically, so a retried request gets its original response back on any replica without writing a duplicate
- **Stream-verified conflict resolution**: a `ClientIdempotencyViolation` is never taken at face value. `Verify.WhoOwnsSeqAsync` point-reads the contested `ClientSeq` from the stream to tell "my prior attempt landed" from "a sibling took my sequence"
- **Self-healing Postgres projection**: stale values auto-corrected by catch-up replay

The cache is not what prevents double-writes; the server's `(ClientId, ClientSeq)` check does that. The cache restores the lost response. The rule that makes it fleet-safe is colocation: the response cache lives wherever the projection cursor lives, written in the same atomic statement, because once the shared cursor moves past an event no replica will ever replay it. See DESIGN.md for the full write path and FAILURE-ANALYSIS.md for the failure-by-failure walkthrough.

## Running locally without Docker

Start the dependencies:

```bash
docker compose up -d celeriant-server postgres
```

Then run the app:

```bash
dotnet run
```

Open http://localhost:5001.
