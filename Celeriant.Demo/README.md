# Celeriant.Demo

A simple browser-based banking demo that shows basic [Celeriant](https://celeriant.io) read/write patterns.

Three seeded accounts (Alice, Bob, Charlie) with deposits, withdrawals, and atomic multi-aggregate transfers. The UI lets you pick a client ID and see OCC conflicts in action. A live watch feed shows changes as they happen via SSE.

## Run

```bash
docker compose up -d
```

Open http://localhost:5000.

## What it demonstrates

- Writing events with `CeleriantPool` and `JsonEventSerializer`
- Optimistic concurrency control (`expectedVersion`)
- Atomic multi-aggregate writes (transfers across two accounts)
- Watch API with SSE broadcast to the browser
- DI registration via `AddCeleriantPool()`

## Running locally without Docker

If you want to run the API outside of Docker (e.g. for debugging), start just the Celeriant server:

```bash
docker compose up -d celeriant-server
```

Then run the app:

```bash
dotnet run
```

Open http://localhost:5000.
