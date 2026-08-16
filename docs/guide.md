# Celeriant .NET Client Guide

This guide covers the concepts and patterns you need to build event-sourced systems with Celeriant. For installation and a minimal example, see the [README](../README.md).

## Aggregates and keys

Every event in Celeriant lives inside an aggregate, addressed by three IDs:

    org_id / aggregate_type_id / aggregate_id

Think of it as a hierarchy. Organisations at the top, aggregate types within orgs, and individual aggregates at the leaves.

```csharp
var key = new AggregateKey(
    orgId: tenantId,
    aggregateTypeId: orderTypeId,
    aggregateId: orderId
);
```

All three are `Guid`s. You define them. Celeriant doesn't care what they mean: it just guarantees ordering and isolation within each aggregate.

A few modelling examples:

- `Acme Corp / Orders / order-123`: classic DDD aggregate
- `Acme Corp / UserProfiles / user-456`: one stream per user
- `Acme Corp / Devices / device-789`: one stream per IoT device

There's no cardinality limit. Millions of aggregates, billions of events. Celeriant's storage engine uses bloom filters and bounded memory: it won't fall over like a PostgreSQL index would.

## Connections and the pool

### Connections are cheap

Celeriant connections are plain TCP sockets. Not like PostgreSQL where each connection spawns a server process. There's no session state, no connection overhead worth worrying about. Connect, send requests, dispose.

```csharp
await using var client = await CeleriantClient.ConnectAsync("localhost:10000", ct: default);
```

A single connection is fine for simple use cases, scripts, or admin tools. The client reuses the TCP connection across multiple requests.

### The pool

For production workloads, `CeleriantPool` is what you want. It manages a set of connections and routes operations to the right node:

- **Writes** always go to the leader. If the leader moves (failover), the pool detects this and reroutes automatically.
- **Reads** also go to the leader by default. This gives you read-your-writes: a read issued after a successful write sees that write.
- **Follower reads** are explicit. Set `RouteReadsToFollowers = true` to send reads to followers and keep the leader free for writes. Follower reads are eventually consistent: a lagging follower returns whatever it has, including "aggregate does not exist" for an aggregate you just wrote. Only opt in if your read path tolerates stale data. If every follower is unreachable, reads and watches fall back to the leader rather than failing: a follower outage costs leader load, not availability.

```csharp
await using var pool = new CeleriantPool(new CeleriantPoolOptions
{
    Address = "localhost:10000",
    MaxConnections = 20,
    RouteReadsToFollowers = true,
});
```

The pool is fully thread-safe. Share a single instance across your application. It handles connection lifecycle, failover, and node discovery.

Key pool options:

| Option | Default | Description |
|--------|---------|-------------|
| `MaxConnections` | 10 | Connection pool ceiling |
| `ConnectionTimeout` | 5s | TCP connect timeout |
| `RequestTimeout` | 30s | Per-request timeout |
| `IdleTimeout` | 25s | Close idle connections (must be below server's `slow_client_timeout`) |
| `RouteReadsToFollowers` | false | Keep the leader free for writes |

Wire compression is automatic and requires no configuration. When the cluster uses dictionary
compression it ships a zstd dictionary to the client during the Identify handshake (cached and
shared across pooled connections). The client then compresses large variable-size requests
(writes, schema registration) with that dictionary and transparently decompresses responses.
Clusters that don't use dictionary compression: and connections that never identify: send
everything uncompressed.

### DI registration

```csharp
builder.Services.AddCeleriantPool(options =>
{
    options.Address = "localhost:10000";
    options.MaxConnections = 20;
});
```

This registers `ICeleriantPool` as a singleton. Inject the interface into your services:

```csharp
public class OrderService(ICeleriantPool pool)
{
    private static readonly JsonEventSerializer Serializer = JsonEventSerializer.Default;
    private static readonly Guid ClientId = Guid.Parse("...");  // stable per service, see Client ID below

    public async Task PlaceOrder(Guid orderId, decimal total, string customer)
    {
        var key = new AggregateKey(orgId, orderTypeId, orderId);
        await pool.WriteAsync(key, [
            AggregateEventExtensions.Create(eventTypeMajor: 1, new OrderPlaced(orderId, total, customer), Serializer)
        ], ClientId);
    }
}
```

## TLS and mTLS

For mTLS with PEM files:

```csharp
var tls = ClientTlsConfig.WithClientCertificateFromPem("localhost", "client.crt", "client.key");
await using var client = await CeleriantClient.ConnectTlsAsync("localhost:10010", tls);
```

Server-only TLS (no client cert):

```csharp
var tls = ClientTlsConfig.Create("localhost");
```

The pool accepts TLS config via `CeleriantPoolOptions.TlsConfig`. Watch connections can also use their own TLS config via `WatchOptions.TlsConfig`.

## Client identity

When the server has `require_client_identity` enabled, the first message on a connection must be an Identify request. Three factory methods, all sent via the same Identify message:

### API key

Server stores four key slots (two ReadWrite, two ReadOnly) as SHA-256 hashes. The client sends the raw key in the Identify message; the server hashes and compares.

```csharp
var identity = ClientIdentityConfig.FromApiKey("base64-encoded-32-byte-key");
```

### Client ID (Guid)

Convenience for when you want to use a `Guid` as your identity. Converted to a base64-encoded key for the wire protocol. Useful when storing client IDs as UUID columns in PostgreSQL alongside event offsets.

```csharp
var identity = ClientIdentityConfig.FromClientId(myServiceGuid);
```

### RSA key pair

Generate an RSA-2048 keypair (DER-encoded, base64). Client identity is derived deterministically from the public key: `SHA-256(DER bytes)[0..16]` as a Guid (little-endian u128). Same keypair, same identity, on any server.

```csharp
// Generate once, persist the keypair
using var rsa = RSA.Create(2048);
var publicKeyBase64 = Convert.ToBase64String(rsa.ExportSubjectPublicKeyInfo());
var privateKeyBase64 = Convert.ToBase64String(rsa.ExportPkcs8PrivateKey());

var identity = ClientIdentityConfig.FromRsaKeyPair(publicKeyBase64, privateKeyBase64);

// Derive the client ID (Guid) from the public key if you need it
var clientId = CeleriantCrypto.GenerateClientIdentity(publicKeyBase64);
```

When the connection identifies, the client library generates a nonce (current epoch milliseconds), signs it with the private key (RSASSA-PKCS1-v1_5 SHA-256), and sends the public key, nonce, and signature. The server validates the signature and checks the nonce (2-minute expiry, 60-second clock skew tolerance). All automatic.

### Using identity

```csharp
// Direct connection
await using var client = await CeleriantClient.ConnectAsync("localhost:10000", ct: default);
await client.IdentifyAsync(identity);

// Or via pool (identifies automatically on each new connection)
await using var pool = new CeleriantPool(new CeleriantPoolOptions
{
    Address = "localhost:10000",
    IdentityConfig = identity,
});
```

Access levels are connection-scoped. ReadOnly blocks write/delete/trim/schema operations. The pool handles identity automatically on every new connection it creates.

## Serialization

Your events are domain objects: records, classes, whatever. You don't hand-serialize them to `byte[]`. The client has a built-in serialization layer that handles this.

```csharp
public record OrderPlaced(Guid OrderId, decimal Total, string Customer);
public record OrderShipped(Guid OrderId, DateTimeOffset ShippedAt);
```

`JsonEventSerializer` uses `System.Text.Json` out of the box, zero extra dependencies:

```csharp
var serializer = JsonEventSerializer.Default;
```

To create an event from a domain object:

```csharp
var evt = AggregateEventExtensions.Create(
    eventTypeMajor: 1,
    new OrderPlaced(orderId, 99.95m, "Alice"),
    serializer);
```

To read it back:

```csharp
var order = evt.GetValue<OrderPlaced>(serializer);
```

If you need custom JSON settings (camelCase, converters, etc.), pass your own `JsonSerializerOptions`:

```csharp
var serializer = new JsonEventSerializer(new JsonSerializerOptions
{
    PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
});
```

For other formats, implement `IEventSerializer`:

```csharp
public interface IEventSerializer
{
    byte[] Serialize<T>(T value);
    T Deserialize<T>(ReadOnlySpan<byte> data);
}
```

Plug in MessagePack, Protobuf, Avro: whatever your team uses. Different event types can use different serializers in the same batch if needed.

### Raw byte[] payloads

If you're already handling serialization yourself, or your payload is already bytes (images, protobuf wire format, etc.), skip the serializer and set `EventValue` directly:

```csharp
new AggregateEvent
{
    ClientSeq = 1,
    EventTimestamp = DateTimeOffset.UtcNow,
    EventTypeMajor = 1,
    EventValue = myBytes,
}
```

## Client ID

Every write carries a `ClientId` (Guid, mapped to u128 on the wire) that identifies the writing service. Celeriant uses it for exactly-once tracking: the highest `ClientSeq` is tracked per `(AggregateKey, ClientId)`.

When client identity is enabled on the server, the write `ClientId` must match the identity derived from your Identify handshake. The server enforces this on every write, delete, trim, and schema request. A mismatch is rejected. So your RSA-derived identity or API key identity IS your write ClientId. See the [RSA key pair](#rsa-key-pair) section for how to derive it with `CeleriantCrypto.GenerateClientIdentity`.

When identity is not enabled, `ClientId` is self-declared. Use a stable Guid for your service:

```csharp
// All instances of OrderService share this Guid
private static readonly Guid MyClientId = Guid.Parse("...");
```

All instances of the same service should share a `ClientId`. Different services writing to the same aggregates should use different IDs.

## Writing events

The simplest write pushes events into a single aggregate:

```csharp
var serializer = JsonEventSerializer.Default;

await client.WriteAsync(key, [
    AggregateEventExtensions.Create(eventTypeMajor: 1, new OrderPlaced(orderId, 99.95m, "Alice"), serializer)
], myClientId);
```

`EventTypeMajor` and `EventTypeMinor` identify the event's schema version. Use major for breaking changes, minor for backwards-compatible additions. These tie into the schema registry (more on that below).

### Optimistic concurrency control

Pass `expectedVersion` to guard a write. If another writer has appended to the aggregate since you last read it, the write is rejected with a `WriteOccException`. This is how you enforce business invariants at write time: no distributed locks needed.

```csharp
await client.WriteAsync(key, events, myClientId,
    expectedVersion: currentBatchIndex);
```

When a concurrency conflict happens, the exception tells you exactly what went wrong:

```csharp
try
{
    await pool.WriteAsync(key, events, myClientId, expectedVersion: staleIndex);
}
catch (WriteOccException ex)
{
    // ex.ExpectedVersion: what you passed in
    // ex.CurrentAggregateVersion: where the aggregate actually is
    // Re-read, re-validate, retry
}
```

There is no automatic retry on OCC failures. That's by design: only your domain logic knows whether a retry is safe. Catch up to the tip of the aggregate event stream, re-validate your business rules, and try again.

### Exactly-once writes

Set `EnforceClientIdempotency = true` and provide a `ClientSeq` on each event. Celeriant tracks the highest `ClientSeq` per `(AggregateKey, ClientId)`. If a write is retried due to a timeout and the original already landed, the server rejects the duplicate with an `IdempotencyViolationException` instead of writing it twice.

The retry behaviour depends on why the write failed:

- **OCC failure**: re-derive `ClientSeq` from fresh state (the aggregate moved, your seq assumption was wrong)
- **Timeout**: hold `ClientSeq` constant (the write may have already landed; changing the seq would bypass the dedup check)
- **Idempotency violation**: the seq landed, durably. With concurrent requests sharing one `ClientId`, it may have been a sibling's write, so verify before claiming success: point-read the contested seq (`ReadFilters` with `MinClientSeq`/`MaxClientSeq` plus `IncludeClientId`) and compare the `EventId`. Yours means the prior attempt landed. A sibling's means your event never landed; re-derive and retry. `Celeriant.Reference` implements the full loop.

### Dynamic consistency boundaries

This is the big one. In traditional event sourcing you pick a single aggregate as your consistency boundary. Business rules that span multiple aggregates? You're stuck with sagas, process managers, eventual consistency.

Celeriant lets you atomically write events across multiple aggregates in a single request, each with its own OCC guard. The server rejects the entire batch if any concurrency check fails. No partial writes, no distributed transactions.

This is what Sara Pellegrini and Milan Savic call [Dynamic Consistency Boundaries](https://sara.event-thinking.io/2023/04/kill-aggregate-chapter-1-I-am-here-to-kill-the-aggregate.html): your consistency boundary isn't baked into your aggregate design, it's defined at write time based on what the business rule actually needs.

```csharp
// Place an order: debit the account AND create the order atomically.
// If either aggregate has been touched since we last checked, the whole write fails.

var accountKey = new AggregateKey(orgId, accountTypeId, accountId);
var orderKey = new AggregateKey(orgId, orderTypeId, orderId);

// 1. Read current state
var accountState = await pool.ReadAsync(new ReadRequest
{
    AggregateKey = accountKey,
    Filters = ReadFilters.From(1),
});

var accountBatchIndex = accountState.EventBatches.LastOrDefault()?.AggregateVersion ?? 0;

// 2. Run your domain logic
var balance = RehydrateBalance(accountState);
if (balance < orderTotal)
    throw new InsufficientFundsException();

// 3. Write across both aggregates atomically, with OCC on each
try
{
    await pool.WriteAsync(new WriteRequest
    {
        ClientId = myClientId,
        Writes = new Dictionary<AggregateKey, SingleAggregateWrite>
        {
            [accountKey] = new()
            {
                Events = [AggregateEventExtensions.Create(1, new AccountDebited(orderTotal), serializer)],
                ExpectedVersion = accountBatchIndex,  // guard: account hasn't changed
            },
            [orderKey] = new()
            {
                AllowCreate = true,
                Events = [AggregateEventExtensions.Create(1, new OrderPlaced(orderId, orderTotal, customer), serializer)],
                ExpectedVersion = 0,  // guard: order must not already exist
            },
        }
    });
}
catch (WriteOccException ex)
{
    // Something changed between our read and write.
    // Nothing was written: no partial state. Re-read, re-validate, retry.
}
```

`ExpectedVersion = 0` means "this aggregate must not have any writes yet". It's how you guard creates. For existing aggregates, use the batch index you got from your last read. If anything has moved, the entire request is rejected atomically.

This eliminates a whole class of problems that normally require sagas or two-phase commit. Transfer between two accounts? Atomic. Reserve inventory while placing an order? Atomic. Any business rule that spans aggregates within the same shard: list them in the same `WriteRequest`.

The constraint: all aggregates in a single write must belong to the same shard. Shard assignment is deterministic (by aggregate ID, type, or org: configured server-side), so you know at design time which aggregates can participate in the same atomic write.

## Reading events

Read events from an aggregate starting at a batch index:

```csharp
var response = await client.ReadAsync(new ReadRequest
{
    AggregateKey = key,
    Filters = ReadFilters.From(1)
});
```

`ReadFilters` supports a range of filtering options: event type, client ID, user ID, timestamp ranges, event index ranges. You don't have to pull everything and filter client-side.

```csharp
var filters = new ReadFilters
{
    FromAggregateVersion = 50,
    ToAggregateVersion = 100,
    IncludeEventTypes = [1, 2, 3],
    MinEventTimestamp = DateTimeOffset.UtcNow.AddDays(-7),
};
```

### Streaming reads

For aggregates with a lot of history, `ReadAllAsync` handles pagination automatically and streams batches as they arrive:

```csharp
await foreach (var batch in pool.ReadAllAsync(key))
{
    foreach (var evt in batch.Events)
    {
        var order = evt.GetValue<OrderPlaced>(serializer);
    }
}
```

### Aggregate details

To check the state of an aggregate without pulling events:

```csharp
var details = await pool.AggregateDetailsAsync(new AggregateDetailsRequest
{
    AggregateKey = key,
});

// details.MinAggregateVersion, details.MaxAggregateVersion
// details.IsDeleted, details.LastServerTimestamp, etc.
```

## Schemas

Celeriant validates events against registered schemas at write time. This is server-side enforcement: malformed events are rejected before they hit the log.

A schema is scoped to an org, aggregate type, and event type version:

```csharp
await pool.RegisterSchemaAsync(new RegisterSchemaRequest
{
    ClientId = myClientId,
    SchemaKey = new SchemaKey(
        orgId: tenantId,
        aggregateTypeId: orderTypeId,
        eventTypeMajor: 1,
        eventTypeMinor: 0
    ),
    SchemaType = SchemaType.Json,
    Schema = jsonSchemaString,
});
```

Supported schema types are `Json`, `Avro`, and `Protobuf`.

Register your schemas as part of your deployment pipeline. When you introduce a breaking change, bump `EventTypeMajor` and register a new schema. Backwards-compatible changes bump `EventTypeMinor`. Old events remain valid: the schema only applies to new writes.

## Watching for changes

The watch API gives you a live stream of changes happening across the cluster. This is how you build reactive read models, trigger side effects, or feed downstream systems: without polling.

```csharp
await using var watch = await pool.WatchAsync(new WatchRequest
{
    Orgs = [tenantId],
    OperationTypes = [WatchOperationType.Write, WatchOperationType.Create],
});

while (!ct.IsCancellationRequested)
{
    var response = await watch.NextAsync(ct);

    foreach (var evt in response.Events)
    {
        // evt.OrgId, evt.AggregateTypeId, evt.AggregateId
        // evt.Operation: Write, Create, Delete, TrimStart, etc.
        // evt.FromAggregateVersion, evt.ToAggregateVersion: for writes
    }
}
```

Watch events tell you *what changed*, not *what the events contain*. You then read the aggregate to get the actual data. This keeps the watch stream lightweight and lets you decide what to fetch.

You can filter by org, aggregate type, specific aggregates, and operation types. Only subscribe to what you need.

The watch connection handles multi-shard routing internally. You don't need to think about shards: the pool figures it out.

## Trimming and deleting

### Trimming old events

Over time you might want to discard old events to free up disk space. `TrimStartAsync` removes all event batches before a given index:

```csharp
await pool.TrimStartAsync(new TrimStartRequest
{
    AggregateKey = key,
    KeepFromAggregateVersion = 100,  // batches 1–99 are gone
    ClientId = myClientId,
});
```

This is useful for aggregates with high event volume where you've already built snapshots or projections from the older events.

### Deleting aggregates

```csharp
await pool.DeleteAsync(new DeleteRequest
{
    ClientId = myClientId,
    Deletes = new Dictionary<AggregateKey, SingleAggregateDelete>
    {
        [key] = new()
        {
            AllowRecreate = true,
            AllowSequenceContinuation = false,
        }
    }
});
```

Two flags control what happens after deletion:

- `AllowRecreate`: can this aggregate be written to again? Set `false` for a permanent, irreversible delete.
- `AllowSequenceContinuation`: if recreated, do event indices continue from where they left off, or restart from 1?

You can also pass `ExpectedVersion` for optimistic concurrency on deletes.

## Listing and discovery

The pool provides listing operations to discover what's in the store:

```csharp
await foreach (var org in pool.ListOrgsAsync())
    Console.WriteLine(org.OrgId);

await foreach (var type in pool.ListAggregateTypesAsync(orgId: tenantId))
    Console.WriteLine(type.AggregateTypeId);

await foreach (var agg in pool.ListAggregatesAsync(orgId: tenantId, aggregateTypeId: orderTypeId))
{
    // agg.AggregateId, agg.EventBatchCount
    // agg.MinEventTimestamp, agg.MaxEventTimestamp
    // agg.CompressedSize, agg.UncompressedSize
    // agg.IsDeleted
}
```

`ListOptions` lets you include deleted aggregates:

```csharp
var options = new ListOptions { IncludeDeleted = true };
await foreach (var agg in pool.ListAggregatesAsync(options: options))
    // ...
```

## Compression

Compression is automatic, dictionary-based, and requires no configuration.

When the cluster uses dictionary compression, it ships a zstd dictionary to the client during the
Identify handshake. The pool caches that dictionary and shares it across connections (advertising
its sha on each new connection so the server can skip resending the bytes). The client then
compresses large variable-size requests: writes and schema registration whose payload is at least
1&#160;KB: with that dictionary, and transparently decompresses any dictionary-compressed responses.

There is nothing to configure and no per-request compression flag: a connection that has negotiated
a dictionary compresses eligible requests automatically, and connections that never identify (or
clusters not using dictionary compression) send everything uncompressed. The only wire compression
values are `None` and `ZstdDict`.
