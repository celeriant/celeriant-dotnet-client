using System.Text.Json;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Serialization;

namespace Celeriant.Client.IntegrationTests;

/// <summary>
/// End-to-end tests for the <see cref="IEventSerializer"/> API surface,
/// including JSON schema registration and typed event round-trips.
/// </summary>
[Collection("Server")]
public sealed class EventSerializerIntegrationTests
{
    private readonly ServerFixture _fixture;

    public EventSerializerIntegrationTests(ServerFixture fixture)
    {
        _fixture = fixture;
    }

    private CeleriantClient Client
    {
        get
        {
            Skip.If(!_fixture.IsAvailable, "Server not running");
            return _fixture.Client!;
        }
    }

    private static AggregateKey NewKey() =>
        new(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());

    // -----------------------------------------------------------------------
    // Domain events used in tests
    // -----------------------------------------------------------------------

    private record OrderPlaced(int OrderId, decimal Total, string Customer);

    private record OrderShipped(int OrderId, string TrackingNumber);

    // -----------------------------------------------------------------------
    // JSON schema for OrderPlaced
    // -----------------------------------------------------------------------

    private const string OrderPlacedJsonSchema = """
        {
            "type": "object",
            "properties": {
                "OrderId":  { "type": "integer" },
                "Total":    { "type": "number" },
                "Customer": { "type": "string" }
            },
            "required": ["OrderId", "Total", "Customer"],
            "additionalProperties": false
        }
        """;

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    [SkippableFact]
    public async Task RegisterJsonSchema_Succeeds()
    {
        var key = NewKey();

        var result = await Client.RegisterSchemaAsync(new RegisterSchemaRequest
        {
            ClientId = Guid.NewGuid(),
            SchemaKey = new SchemaKey(key.OrgId, key.AggregateTypeId, eventTypeMajor: 1, eventTypeMinor: 0),
            SchemaType = SchemaType.Json,
            Schema = OrderPlacedJsonSchema,
        });

        Assert.NotNull(result);
    }

    [SkippableFact]
    public async Task WriteWithSerializer_ReadBack_PayloadPreserved()
    {
        var serializer = JsonEventSerializer.Default;
        var key = NewKey();
        var original = new OrderPlaced(42, 99.95m, "Alice");

        // Write using the new Create<T> helper
        var writeReq = new WriteRequest
        {
            ClientId = Guid.NewGuid(),
            Writes = new Dictionary<AggregateKey, SingleAggregateWrite>
            {
                [key] = new SingleAggregateWrite
                {
                    AllowCreate = true,
                    Events =
                    [
                        AggregateEventExtensions.Create(
                            eventTypeMajor: 1,
                            original,
                            serializer),
                    ],
                }
            }
        };

        await Client.WriteAsync(writeReq);

        // Read back and deserialize using GetValue<T>
        var readResp = await Client.ReadAsync(TestHelpers.ReadAllRequest(key));
        var events = readResp.EventBatches.SelectMany(b => b.Events).ToArray();

        Assert.Single(events);
        var result = events[0].GetValue<OrderPlaced>(serializer);
        Assert.Equal(original, result);
    }

    [SkippableFact]
    public async Task RegisterSchema_ThenWrite_ValidPayloadAccepted()
    {
        var serializer = JsonEventSerializer.Default;
        var key = NewKey();

        // Register JSON schema for event type (1, 0)
        await Client.RegisterSchemaAsync(new RegisterSchemaRequest
        {
            ClientId = Guid.NewGuid(),
            SchemaKey = new SchemaKey(key.OrgId, key.AggregateTypeId, eventTypeMajor: 1, eventTypeMinor: 0),
            SchemaType = SchemaType.Json,
            Schema = OrderPlacedJsonSchema,
        });

        // Write a valid event — should succeed
        var order = new OrderPlaced(1, 29.99m, "Bob");
        var writeReq = new WriteRequest
        {
            ClientId = Guid.NewGuid(),
            Writes = new Dictionary<AggregateKey, SingleAggregateWrite>
            {
                [key] = new SingleAggregateWrite
                {
                    AllowCreate = true,
                    Events =
                    [
                        AggregateEventExtensions.Create(eventTypeMajor: 1, order, serializer),
                    ],
                }
            }
        };

        await Client.WriteAsync(writeReq);

        // Verify round-trip
        var readResp = await Client.ReadAsync(TestHelpers.ReadAllRequest(key));
        var events = readResp.EventBatches.SelectMany(b => b.Events).ToArray();
        Assert.Single(events);
        Assert.Equal(order, events[0].GetValue<OrderPlaced>(serializer));
    }

    [SkippableFact]
    public async Task MultipleEventTypes_DifferentSerializers_RoundTrip()
    {
        var defaultJson = JsonEventSerializer.Default;
        var camelJson = new JsonEventSerializer(
            new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase });

        var key = NewKey();
        var order = new OrderPlaced(10, 150m, "Carol");
        var shipment = new OrderShipped(10, "TRACK-123");

        // Write two events with different serializers (simulating different wire formats)
        var writeReq = new WriteRequest
        {
            ClientId = Guid.NewGuid(),
            Writes = new Dictionary<AggregateKey, SingleAggregateWrite>
            {
                [key] = new SingleAggregateWrite
                {
                    AllowCreate = true,
                    Events =
                    [
                        AggregateEventExtensions.Create(eventTypeMajor: 1, order, defaultJson, clientSeq: 1),
                        AggregateEventExtensions.Create(eventTypeMajor: 2, shipment, camelJson, clientSeq: 2),
                    ],
                }
            }
        };

        await Client.WriteAsync(writeReq);

        // Read back and dispatch by event type
        var readResp = await Client.ReadAsync(TestHelpers.ReadAllRequest(key));
        var events = readResp.EventBatches.SelectMany(b => b.Events).ToArray();
        Assert.Equal(2, events.Length);

        foreach (var evt in events)
        {
            switch (evt.EventTypeMajor)
            {
                case 1:
                    Assert.Equal(order, evt.GetValue<OrderPlaced>(defaultJson));
                    break;
                case 2:
                    Assert.Equal(shipment, evt.GetValue<OrderShipped>(camelJson));
                    break;
                default:
                    Assert.Fail($"Unexpected event type {evt.EventTypeMajor}");
                    break;
            }
        }
    }
}
