# Celeriant .NET Client

Official .NET client for [Celeriant](https://github.com/celeriant/celeriant) — the event store built for event-sourcing at scale.

Targets `net8.0`. Zero external runtime dependencies beyond `MessagePack` for serialisation and `ZstdSharp`/`Snappier` for compression.

## Quick start

### 1. Start the server

```bash
docker compose up -d
```

### 2. Connect and write an event

```csharp
await using var client = await CeleriantClient.ConnectAsync("localhost:10000");

var key = new AggregateKey(orgId: myOrg, aggregateTypeId: myType, aggregateId: orderId);

await client.WriteAsync(key, [new AggregateEvent
{
    ClientEventIndex = 1,
    EventTimestamp = DateTimeOffset.UtcNow,
    EventTypeMajor = 1,
    EventValue = payload,
}]);
```

### 3. Read it back

```csharp
var response = await client.ReadAsync(new ReadRequest
{
    AggregateKey = key,
    Filters = ReadFilters.From(1)
});
```

## Connection pool

For production use, `CeleriantPool` manages connections, routes writes to the leader, and distributes reads across nodes.

```csharp
await using var pool = new CeleriantPool(new CeleriantPoolOptions
{
    Address = "localhost:10000",
    MaxConnections = 20,
});

await pool.WriteAsync(key, events);
var read = await pool.ReadAsync(readRequest);
```

With DI:

```csharp
builder.Services.AddCeleriantPool(options =>
{
    options.Address = "localhost:10000";
    options.MaxConnections = 20;
});
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
