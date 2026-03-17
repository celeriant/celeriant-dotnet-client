# Celeriant.Reference

A production-grade reference API showing how to build an event-sourced system with [Celeriant](https://celeriant.io) and a Postgres read projection.

Banking domain: deposits, withdrawals, and atomic multi-aggregate transfers with server-side balance validation.

## Run

```bash
docker compose up -d
```

Open http://localhost:5001.

## What it demonstrates

- **Lazy catch-up projection** — Postgres read model rebuilt on-demand from Celeriant, no background projection service
- **Exactly-once writes** — `ClientEventIndex` derived from catch-up + `EnforceClientIdempotency` on the server
- **OCC retry loops** — re-derive state on conflict, retry with fresh `expectedEventBatchIndex`
- **Atomic multi-aggregate transfers** — debit and credit written in a single `WriteRequest` with OCC on both
- **HTTP idempotency cache** — duplicate POST protection via `Idempotency-Key` header
- **Self-healing Postgres projection** — stale values auto-corrected by catch-up replay

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
