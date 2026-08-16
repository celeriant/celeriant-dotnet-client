# Celeriant .NET Client

Official .NET client for [Celeriant](https://celeriant.io): the distributed event store built for event sourcing at scale.

Celeriant is an event store that lets you enforce business invariants at write time across multiple streams, without distributed transactions. Optimistic concurrency control, strict event ordering, exactly-once writes, schema validation, and cluster-wide durability. PostgreSQL gives you correctness but not throughput. Kafka gives you throughput but not correctness. Celeriant gives you both.

- [Website](https://celeriant.io)
- [Documentation](https://docs.celeriant.io)
- [GitHub](https://github.com/celeriant/celeriant-db)

Targets `net8.0`, `net9.0`, and `net10.0`. Dependencies are `Celeriant.Transport` (shared wire framing, published alongside), `MessagePack` for serialisation, and `ZstdSharp` for compression.

For a deeper walkthrough: aggregate modelling, schemas, watch API, connection pool internals: see the [usage guide](https://github.com/celeriant/celeriant-dotnet-client/blob/main/docs/guide.md).

## Install

```bash
dotnet add package Celeriant.Client
```

## Quick start

### 1. Start the server

Celeriant uses io_uring, so the container needs `seccomp=unconfined`.

```bash
docker run -d --name celeriant \
  --security-opt seccomp=unconfined \
  -p 10000:10000 \
  ghcr.io/celeriant/celeriant \
  --standalone --data-root /var/lib/celeriant --num-shards 1
```

### 2. Connect and write an event

```csharp
await using var client = await CeleriantClient.ConnectAsync("localhost:10000");

var serializer = JsonEventSerializer.Default;
var key = new AggregateKey(orgId: myOrg, aggregateTypeId: myType, aggregateId: orderId);

await client.WriteAsync(key, [
    AggregateEventExtensions.Create(eventTypeMajor: 1, new OrderPlaced(orderId, 99.95m), serializer)
]);
```

### 3. Read it back

```csharp
var response = await client.ReadAsync(new ReadRequest
{
    AggregateKey = key,
    Filters = ReadFilters.From(1)
});

var order = response.EventBatches[0].Events[0].GetValue<OrderPlaced>(serializer);
```

## Connection pool

For production use, `CeleriantPool` manages connections, routes writes to the leader, and distributes reads across nodes. It implements `ICeleriantPool` for easy testing and DI.

```csharp
await using var pool = new CeleriantPool(new CeleriantPoolOptions
{
    Address = "localhost:10000",
    MaxConnections = 20,
});

await pool.WriteAsync(key, events);
var read = await pool.ReadAsync(readRequest);
```

The pool also supports `DeleteAsync`, `TrimStartAsync`, `AggregateDetailsAsync`, `RegisterSchemaAsync`, `WatchAsync`, `ReadAllAsync` (streaming), and listing operations (`ListOrgsAsync`, `ListAggregateTypesAsync`, `ListAggregatesAsync`).

With DI (registers `ICeleriantPool` as a singleton):

```csharp
builder.Services.AddCeleriantPool(options =>
{
    options.Address = "localhost:10000";
    options.MaxConnections = 20;
});
```

Then inject `ICeleriantPool` into your services:

```csharp
public class MyService(ICeleriantPool pool)
{
    public Task DoWork() => pool.ReadAsync(...);
}
```

## TLS / mTLS

```csharp
var tls = ClientTlsConfig.WithClientCertificateFromPem("localhost", "client.crt", "client.key");
await using var client = await CeleriantClient.ConnectTlsAsync("localhost:10010", tls);
```

To run the TLS integration tests locally:

```bash
test-certs/generate.sh
docker compose -f docker-compose.tls.yml up -d
dotnet test
```

## Examples

- **[Celeriant.Demo](https://github.com/celeriant/celeriant-dotnet-client/tree/main/Celeriant.Demo)**: simple browser-based banking demo. `cd Celeriant.Demo && docker compose up -d` to start everything. Shows basic read/write patterns with a minimal UI.
- **[Celeriant.Reference](https://github.com/celeriant/celeriant-dotnet-client/tree/main/Celeriant.Reference)**: production-grade reference API with Postgres read projections, exactly-once writes, OCC retry loops, and multi-aggregate transfers. `cd Celeriant.Reference && docker compose up -d` to start everything.

## Running tests

```bash
# unit tests only
dotnet test Celeriant.Client.Tests

# integration tests (requires docker compose up -d)
dotnet test Celeriant.Client.IntegrationTests
```

## License

Apache 2.0
