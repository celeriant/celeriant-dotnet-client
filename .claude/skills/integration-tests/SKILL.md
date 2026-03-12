---
name: integration-tests
description: Run integration tests with automatic docker lifecycle management. Starts the required containers, waits for readiness, runs tests, and stops containers.
disable-model-invocation: true
argument-hint: "[all | standalone | cluster | tls]"
---

# Integration Tests

Run integration tests for the Celeriant .NET client. This skill manages the full docker lifecycle: start the required containers, wait for readiness, run the tests, and stop the containers.

## Argument

`$ARGUMENTS` selects which test suite(s) to run:

| Argument | Docker Compose | Tests |
|----------|---------------|-------|
| `standalone` | `docker-compose.yml` | StandaloneTests, StreamingTests, TypedMethodTests, WatchTests, BugVerificationTests, CompressionIntegrationTests, EventSerializerIntegrationTests, PoolTests |
| `cluster` | `docker-compose.cluster.yml` | ClusterTests |
| `tls` | `docker-compose.tls.yml` | MtlsTests |
| `all` (default) | All three, sequentially | All integration tests |

If `$ARGUMENTS` is empty, treat it as `all`.

## Execution Steps

For each suite being run:

### 1. Stop conflicting containers

Before starting any compose file, stop all three compose stacks to avoid port conflicts (standalone and cluster both use port 10000):

```bash
docker compose -f docker-compose.yml down 2>/dev/null
docker compose -f docker-compose.cluster.yml down 2>/dev/null
docker compose -f docker-compose.tls.yml down 2>/dev/null
```

### 2. Start containers and wait for readiness

**Standalone** (`docker-compose.yml`):
```bash
docker compose up -d
```
Wait ~3 seconds, then verify with a TCP check on port 10000.

**Cluster** (`docker-compose.cluster.yml`):
```bash
docker compose -f docker-compose.cluster.yml up -d
```
Wait ~8 seconds for leader election. The cluster needs MinIO + 2 Celeriant nodes. Verify ports 10000 and 10002 are accepting connections.

**TLS** (`docker-compose.tls.yml`):
```bash
docker compose -f docker-compose.tls.yml up -d
```
Wait ~3 seconds. Verify port 10010 is accepting connections. Requires `test-certs/` directory to exist (run `test-certs/generate.sh` if missing).

### 3. Run the tests

Use `dotnet test` with the appropriate filter:

| Suite | Filter |
|-------|--------|
| standalone | `dotnet test --filter "FullyQualifiedName~Celeriant.Client.IntegrationTests" --filter "FullyQualifiedName!~ClusterTests&FullyQualifiedName!~MtlsTests"` |
| cluster | `dotnet test --filter "FullyQualifiedName~Celeriant.Client.IntegrationTests.ClusterTests"` |
| tls | `dotnet test --filter "FullyQualifiedName~Celeriant.Client.IntegrationTests.MtlsTests"` |

For `all`, run each suite sequentially (standalone, then cluster, then tls), stopping and starting the appropriate containers between suites.

### 4. Stop containers after tests

After each suite completes, stop its containers:
```bash
docker compose -f <compose-file> down
```

### 5. Report results

After all suites have run, present a summary table showing passed/failed/skipped counts per suite and the overall result.

## Important Notes

- Port 10000 is shared between standalone and cluster - they cannot run simultaneously
- TLS uses port 10010 and can technically coexist, but stop it anyway to keep things clean
- If a container fails to start, check `docker compose -f <file> logs` for diagnostics
- The cluster needs time for leader election after startup - if cluster tests fail on first run with connection errors, the wait time may need increasing
- `docker-compose` (v1) is not available; use `docker compose` (v2)
