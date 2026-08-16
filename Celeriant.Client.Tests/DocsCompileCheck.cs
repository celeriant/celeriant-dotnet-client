// Compile-only mirror of every C# snippet in README.md and docs/guide.md.
// Same pattern as celeriant-db's guide_compile_check.rs: nothing here runs,
// it just fails the build when a doc snippet drifts from the real API.
// Snippets are transcribed as close to verbatim as C# allows; free variables
// from the docs become method parameters or stub fields.
#pragma warning disable CS0168 // variable declared but never used
#pragma warning disable CS0219 // variable assigned but never used
#pragma warning disable CS8321 // local function never used
#pragma warning disable IDE0059

using System.Security.Cryptography;
using System.Text.Json;
using Celeriant.Client.Errors;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Serialization;
using Celeriant.Client.Streaming;
using Celeriant.Transport;
using Microsoft.Extensions.DependencyInjection;

namespace Celeriant.Client.Tests.Docs.ReadmeCheck
{
    // Stub for the README's domain type (two-argument form).
    public record OrderPlaced(Guid OrderId, decimal Total);

    internal static class ReadmeDocs
    {
        // README.md — Quick start §2 "Connect and write an event" + §3 "Read it back"
        private static async Task QuickStart(Guid myOrg, Guid myType, Guid orderId, Guid myClientId)
        {
            await using var client = await CeleriantClient.ConnectAsync("localhost:10000", ct: default);

            var serializer = JsonEventSerializer.Default;
            var key = new AggregateKey(orgId: myOrg, aggregateTypeId: myType, aggregateId: orderId);

            await client.WriteAsync(key, [
                AggregateEventExtensions.Create(eventTypeMajor: 1, new OrderPlaced(orderId, 99.95m), serializer)
            ], clientId: myClientId);

            var response = await client.ReadAsync(new ReadRequest
            {
                AggregateKey = key,
                Filters = ReadFilters.From(1)
            });

            var order = response.EventBatches[0].Events[0].GetValue<OrderPlaced>(serializer);
        }

        // README.md — "Connection pool"
        private static async Task ConnectionPool(
            AggregateKey key, AggregateEvent[] events, Guid myClientId, ReadRequest readRequest)
        {
            await using var pool = new CeleriantPool(new CeleriantPoolOptions
            {
                Address = "localhost:10000",
                MaxConnections = 20,
            });

            await pool.WriteAsync(key, events, myClientId);
            var read = await pool.ReadAsync(readRequest);
        }

        // README.md — "Connection pool", DI registration (builder.Services in the doc)
        private static void DiRegistration(IServiceCollection services)
        {
            services.AddCeleriantPool(options =>
            {
                options.Address = "localhost:10000";
                options.MaxConnections = 20;
            });
        }

        // README.md — "Connection pool", injecting ICeleriantPool.
        // The doc's "pool.ReadAsync(...)" placeholder is filled with a real request.
        public class MyService(ICeleriantPool pool)
        {
            public Task DoWork() => pool.ReadAsync(new ReadRequest
            {
                AggregateKey = default,
                Filters = ReadFilters.From(1),
            });
        }

        // README.md — "TLS / mTLS"
        private static async Task Tls()
        {
            var tls = ClientTlsConfig.WithClientCertificateFromPem("localhost", "client.crt", "client.key");
            await using var client = await CeleriantClient.ConnectTlsAsync("localhost:10010", tls);
        }
    }
}

namespace Celeriant.Client.Tests.Docs.GuideCheck
{
    // docs/guide.md — "Serialization": the guide declares these records verbatim.
    public record OrderPlaced(Guid OrderId, decimal Total, string Customer);
    public record OrderShipped(Guid OrderId, DateTimeOffset ShippedAt);

    // Stubs for the guide's "Dynamic consistency boundaries" domain types.
    public record AccountDebited(decimal Amount);
    public class InsufficientFundsException : Exception;

    internal static class GuideDocs
    {
        // docs/guide.md — "Aggregates and keys"
        private static void AggregatesAndKeys(Guid tenantId, Guid orderTypeId, Guid orderId)
        {
            var key = new AggregateKey(
                orgId: tenantId,
                aggregateTypeId: orderTypeId,
                aggregateId: orderId
            );
        }

        // docs/guide.md — "Connections are cheap"
        private static async Task Connect()
        {
            await using var client = await CeleriantClient.ConnectAsync("localhost:10000", ct: default);
        }

        // docs/guide.md — "The pool"
        private static async Task Pool()
        {
            await using var pool = new CeleriantPool(new CeleriantPoolOptions
            {
                Address = "localhost:10000",
                MaxConnections = 20,
                RouteReadsToFollowers = true,
            });
        }

        // docs/guide.md — "DI registration"
        private static void DiRegistration(IServiceCollection services)
        {
            services.AddCeleriantPool(options =>
            {
                options.Address = "localhost:10000";
                options.MaxConnections = 20;
            });
        }

        // docs/guide.md — "DI registration": injected service.
        // orgId/orderTypeId are free variables in the doc; stubbed as fields here.
        public class OrderService(ICeleriantPool pool)
        {
            private static readonly Guid orgId = Guid.NewGuid();
            private static readonly Guid orderTypeId = Guid.NewGuid();

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

        // docs/guide.md — "TLS and mTLS"
        private static async Task Tls()
        {
            var tls = ClientTlsConfig.WithClientCertificateFromPem("localhost", "client.crt", "client.key");
            await using var client = await CeleriantClient.ConnectTlsAsync("localhost:10010", tls);

            var serverOnly = ClientTlsConfig.Create("localhost");
        }

        // docs/guide.md — "Client identity": API key / client ID factories
        private static void IdentityFactories(Guid myServiceGuid)
        {
            var apiKeyIdentity = ClientIdentityConfig.FromApiKey("base64-encoded-32-byte-key");
            var clientIdIdentity = ClientIdentityConfig.FromClientId(myServiceGuid);
        }

        // docs/guide.md — "RSA key pair" + "Using identity"
        private static async Task RsaIdentity()
        {
            // Generate once, persist the keypair
            using var rsa = RSA.Create(2048);
            var publicKeyBase64 = Convert.ToBase64String(rsa.ExportSubjectPublicKeyInfo());
            var privateKeyBase64 = Convert.ToBase64String(rsa.ExportPkcs8PrivateKey());

            var identity = ClientIdentityConfig.FromRsaKeyPair(publicKeyBase64, privateKeyBase64);

            // Derive the client ID (Guid) from the public key if you need it
            var clientId = CeleriantCrypto.GenerateClientIdentity(publicKeyBase64);

            // Direct connection
            await using var client = await CeleriantClient.ConnectAsync("localhost:10000", ct: default);
            await client.IdentifyAsync(identity);

            // Or via pool (identifies automatically on each new connection)
            await using var pool = new CeleriantPool(new CeleriantPoolOptions
            {
                Address = "localhost:10000",
                IdentityConfig = identity,
            });
        }

        // docs/guide.md — "Serialization"
        private static void Serialization(Guid orderId)
        {
            var serializer = JsonEventSerializer.Default;

            var evt = AggregateEventExtensions.Create(
                eventTypeMajor: 1,
                new OrderPlaced(orderId, 99.95m, "Alice"),
                serializer);

            var order = evt.GetValue<OrderPlaced>(serializer);

            var custom = new JsonEventSerializer(new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            });
        }

        // docs/guide.md — "Raw byte[] payloads"
        private static AggregateEvent RawPayload(byte[] myBytes) =>
            new AggregateEvent
            {
                ClientSeq = 1,
                EventTimestamp = DateTimeOffset.UtcNow,
                EventTypeMajor = 1,
                EventValue = myBytes,
            };

        // docs/guide.md — "Client ID"
        private static readonly Guid MyClientId = Guid.Parse("...");

        // docs/guide.md — "Writing events"
        private static async Task WritingEvents(ICeleriantClient client, AggregateKey key, Guid orderId, Guid myClientId)
        {
            var serializer = JsonEventSerializer.Default;

            await client.WriteAsync(key, [
                AggregateEventExtensions.Create(eventTypeMajor: 1, new OrderPlaced(orderId, 99.95m, "Alice"), serializer)
            ], myClientId);
        }

        // docs/guide.md — "Optimistic concurrency control"
        private static async Task Occ(
            ICeleriantClient client, ICeleriantPool pool, AggregateKey key,
            AggregateEvent[] events, Guid myClientId, long currentBatchIndex, long staleIndex)
        {
            await client.WriteAsync(key, events, myClientId,
                expectedVersion: currentBatchIndex);

            try
            {
                await pool.WriteAsync(key, events, myClientId, expectedVersion: staleIndex);
            }
            catch (WriteOccException ex)
            {
                // Doc comments name these properties; verify they exist.
                long expected = ex.ExpectedVersion;
                long current = ex.CurrentAggregateVersion;
            }
        }

        // docs/guide.md — "Dynamic consistency boundaries"
        private static async Task DynamicConsistencyBoundaries(
            ICeleriantPool pool, IEventSerializer serializer, Guid orgId,
            Guid accountTypeId, Guid orderTypeId, Guid accountId, Guid orderId,
            Guid myClientId, decimal orderTotal, string customer)
        {
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
                            ExpectedVersion = accountBatchIndex,
                        },
                        [orderKey] = new()
                        {
                            AllowCreate = true,
                            Events = [AggregateEventExtensions.Create(1, new OrderPlaced(orderId, orderTotal, customer), serializer)],
                            ExpectedVersion = 0,
                        },
                    }
                });
            }
            catch (WriteOccException)
            {
                // Nothing was written: re-read, re-validate, retry.
            }
        }

        private static decimal RehydrateBalance(ReadResponse state) => 0m;

        // docs/guide.md — "Reading events"
        private static async Task ReadingEvents(ICeleriantClient client, AggregateKey key)
        {
            var response = await client.ReadAsync(new ReadRequest
            {
                AggregateKey = key,
                Filters = ReadFilters.From(1)
            });

            var filters = new ReadFilters
            {
                FromAggregateVersion = 50,
                ToAggregateVersion = 100,
                IncludeEventTypes = [1, 2, 3],
                MinEventTimestamp = DateTimeOffset.UtcNow.AddDays(-7),
            };
        }

        // docs/guide.md — "Streaming reads"
        private static async Task StreamingReads(ICeleriantPool pool, AggregateKey key, IEventSerializer serializer)
        {
            await foreach (var batch in pool.ReadAllAsync(key))
            {
                foreach (var evt in batch.Events)
                {
                    var order = evt.GetValue<OrderPlaced>(serializer);
                }
            }
        }

        // docs/guide.md — "Aggregate details"
        private static async Task AggregateDetails(ICeleriantPool pool, AggregateKey key)
        {
            var details = await pool.AggregateDetailsAsync(new AggregateDetailsRequest
            {
                AggregateKey = key,
            });

            // Doc comments name these properties; verify they exist.
            long min = details.MinAggregateVersion;
            long max = details.MaxAggregateVersion;
            bool deleted = details.IsDeleted;
            DateTimeOffset last = details.LastServerTimestamp;
        }

        // docs/guide.md — "Schemas"
        private static async Task Schemas(
            ICeleriantPool pool, Guid myClientId, Guid tenantId, Guid orderTypeId, string jsonSchemaString)
        {
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
        }

        // docs/guide.md — "Watching for changes"
        private static async Task Watching(ICeleriantPool pool, Guid tenantId, CancellationToken ct)
        {
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
                    // Doc comments name these properties; verify they exist.
                    Guid org = evt.OrgId;
                    Guid aggType = evt.AggregateTypeId;
                    Guid agg = evt.AggregateId;
                    WatchOperationType op = evt.Operation;
                    long? from = evt.FromAggregateVersion;
                    long? to = evt.ToAggregateVersion;
                }
            }
        }

        // docs/guide.md — "Trimming old events"
        private static async Task Trimming(ICeleriantPool pool, AggregateKey key, Guid myClientId)
        {
            await pool.TrimStartAsync(new TrimStartRequest
            {
                AggregateKey = key,
                KeepFromAggregateVersion = 100,  // batches 1–99 are gone
                ClientId = myClientId,
            });
        }

        // docs/guide.md — "Deleting aggregates"
        private static async Task Deleting(ICeleriantPool pool, AggregateKey key, Guid myClientId)
        {
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

            // Prose: "You can also pass ExpectedVersion for optimistic concurrency on deletes."
            var withOcc = new SingleAggregateDelete { ExpectedVersion = 1 };
        }

        // docs/guide.md — "Listing and discovery"
        private static async Task Listing(ICeleriantPool pool, Guid tenantId, Guid orderTypeId)
        {
            await foreach (var org in pool.ListOrgsAsync())
                Console.WriteLine(org.OrgId);

            await foreach (var type in pool.ListAggregateTypesAsync(orgId: tenantId))
                Console.WriteLine(type.AggregateTypeId);

            await foreach (var agg in pool.ListAggregatesAsync(orgId: tenantId, aggregateTypeId: orderTypeId))
            {
                // Doc comments name these properties; verify they exist.
                Guid id = agg.AggregateId;
                long batches = agg.EventBatchCount;
                DateTimeOffset? minTs = agg.MinEventTimestamp;
                DateTimeOffset? maxTs = agg.MaxEventTimestamp;
                long compressed = agg.CompressedSize;
                long uncompressed = agg.UncompressedSize;
                bool deleted = agg.IsDeleted;
            }

            var options = new ListOptions { IncludeDeleted = true };
            await foreach (var agg in pool.ListAggregatesAsync(options: options))
            {
                // ...
            }
        }
    }
}

namespace Celeriant.Client.Tests.Docs.GuideSerializerInterface
{
    // docs/guide.md — "Serialization": the guide shows the IEventSerializer shape verbatim.
    // Declared in its own namespace so it can't collide with the real interface;
    // the mirror below pins the real one to the same shape.
    public interface IEventSerializer
    {
        byte[] Serialize<T>(T value);
        T Deserialize<T>(ReadOnlySpan<byte> data);
    }

    // Fails to compile if the real interface stops matching the documented shape.
    internal sealed class ShapeCheck : Serialization.IEventSerializer, IEventSerializer
    {
        public byte[] Serialize<T>(T value) => [];
        public T Deserialize<T>(ReadOnlySpan<byte> data) => default!;
    }
}
