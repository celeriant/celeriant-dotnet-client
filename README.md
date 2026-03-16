# Celeriant .NET Client

Official .NET client for [Celeriant](https://github.com/celeriant/celeriant) — the event store built for event-sourcing at scale.

Targets `net8.0`. Zero external runtime dependencies beyond `MessagePack` for serialisation and `ZstdSharp`/`Snappier` for compression.

For a deeper walkthrough — aggregate modelling, schemas, watch API, connection pool internals — see the [usage guide](docs/guide.md).

## Quick start

### 1. Start the server

```bash
docker compose up -d
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

## Running tests

```bash
# unit tests only
dotnet test Celeriant.Client.Tests

# integration tests (requires docker compose up -d)
dotnet test Celeriant.Client.IntegrationTests
```

## License

Apache 2.0
