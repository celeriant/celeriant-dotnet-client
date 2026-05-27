using System.Text;
using Celeriant.Client.Errors;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;

namespace Celeriant.Client.IntegrationTests;

/// <summary>
/// Tests for the typed convenience methods on <see cref="CeleriantClient"/>
/// (ReadAsync, WriteAsync, DeleteAsync, TrimStartAsync, AggregateDetailsAsync).
/// Existing tests use raw SendRequestAsync; these validate the typed wrappers.
/// </summary>
[Collection("Server")]
public sealed class TypedMethodTests
{
    private readonly ServerFixture _fixture;

    public TypedMethodTests(ServerFixture fixture) => _fixture = fixture;

    private CeleriantClient Client
    {
        get
        {
            Skip.If(!_fixture.IsAvailable, "Server not running");
            return _fixture.Client!;
        }
    }

    private string Address
    {
        get
        {
            Skip.If(!_fixture.IsAvailable, "Server not running");
            return _fixture.Address;
        }
    }

    // =========================================================================
    // WriteAsync convenience overload
    // =========================================================================

    [SkippableFact]
    public async Task WriteAsync_ConvenienceOverload_CreatesAggregate()
    {
        var key = TestHelpers.NewKey();
        var payload = "convenience-write"u8.ToArray();

        var result = await Client.WriteAsync(key, [MakeEvent(1, payload)]);

        Assert.NotNull(result);

        var read = await Client.ReadAsync(new ReadRequest
        {
            AggregateKey = key,
            Filters = ReadFilters.From(1),
        });
        var events = read.EventBatches.SelectMany(b => b.Events).ToArray();
        Assert.Single(events);
        Assert.Equal(payload, events[0].EventValue);
    }

    // =========================================================================
    // ReadAsync typed method
    // =========================================================================

    [SkippableFact]
    public async Task ReadAsync_ReturnsTypedResponse()
    {
        var key = TestHelpers.NewKey();
        await Client.WriteAsync(key, [MakeEvent(1, "typed-read")]);

        var response = await Client.ReadAsync(new ReadRequest
        {
            AggregateKey = key,
            Filters = ReadFilters.From(1),
        });

        Assert.NotEmpty(response.EventBatches);
        Assert.Equal("typed-read",
            Encoding.UTF8.GetString(response.EventBatches[0].Events[0].EventValue));
    }

    [SkippableFact]
    public async Task ReadAsync_NonexistentAggregate_ThrowsAggregateNotFoundException()
    {
        await Assert.ThrowsAsync<AggregateNotFoundException>(
            () => Client.ReadAsync(new ReadRequest
            {
                AggregateKey = TestHelpers.NewKey(),
                Filters = ReadFilters.From(1),
            }));
    }

    // =========================================================================
    // AggregateDetailsAsync typed method
    // =========================================================================

    [SkippableFact]
    public async Task AggregateDetailsAsync_ReturnsTypedResponse()
    {
        var key = TestHelpers.NewKey();
        await Client.WriteAsync(key, [MakeEvent(1, "typed-details")]);

        var details = await Client.AggregateDetailsAsync(
            new AggregateDetailsRequest { AggregateKey = key });

        Assert.Equal(1L, details.MaxAggregateVersion);
        Assert.False(details.IsDeleted);
    }

    [SkippableFact]
    public async Task AggregateDetailsAsync_NonexistentAggregate_ThrowsAggregateNotFoundException()
    {
        await Assert.ThrowsAsync<AggregateNotFoundException>(
            () => Client.AggregateDetailsAsync(
                new AggregateDetailsRequest { AggregateKey = TestHelpers.NewKey() }));
    }

    // =========================================================================
    // DeleteAsync typed method
    // =========================================================================

    [SkippableFact]
    public async Task DeleteAsync_ReturnsTypedResponse()
    {
        var key = TestHelpers.NewKey();
        await Client.WriteAsync(key, [MakeEvent(1, "typed-delete")]);

        var details = await Client.AggregateDetailsAsync(
            new AggregateDetailsRequest { AggregateKey = key });

        var result = await Client.DeleteAsync(new DeleteRequest
        {
            ClientId = Guid.NewGuid(),
            Deletes = new Dictionary<AggregateKey, SingleAggregateDelete>
            {
                [key] = new SingleAggregateDelete
                {
                    AllowRecreate = false,
                    ExpectedVersion = details.MaxAggregateVersion,
                }
            }
        });

        Assert.NotNull(result);
    }

    // =========================================================================
    // TrimStartAsync typed method
    // =========================================================================

    [SkippableFact]
    public async Task TrimStartAsync_ReturnsTypedResponse()
    {
        var key = TestHelpers.NewKey();
        await Client.WriteAsync(key, [MakeEvent(1, "trim-1")]);
        await Client.WriteAsync(key, [MakeEvent(2, "trim-2")], allowCreate: false);

        var details = await Client.AggregateDetailsAsync(
            new AggregateDetailsRequest { AggregateKey = key });

        var result = await Client.TrimStartAsync(new TrimStartRequest
        {
            AggregateKey = key,
            KeepFromAggregateVersion = details.MaxAggregateVersion,
            ClientId = Guid.NewGuid(),
        });

        Assert.NotNull(result);

        // Verify trim: only last batch remains (read from the kept batch index)
        var read = await Client.ReadAsync(new ReadRequest
        {
            AggregateKey = key,
            Filters = ReadFilters.From(details.MaxAggregateVersion),
        });
        var payloads = read.EventBatches
            .SelectMany(b => b.Events)
            .Select(e => Encoding.UTF8.GetString(e.EventValue))
            .ToList();
        Assert.Contains("trim-2", payloads);
        Assert.DoesNotContain("trim-1", payloads);
    }

    // =========================================================================
    // RegisterSchemaAsync typed method
    // =========================================================================

    [SkippableFact]
    public async Task RegisterSchemaAsync_ReturnsTypedResponse()
    {
        var key = TestHelpers.NewKey();
        await Client.WriteAsync(key, [MakeEvent(1, "schema-test")]);

        var result = await Client.RegisterSchemaAsync(new RegisterSchemaRequest
        {
            SchemaKey = new SchemaKey(key.OrgId, key.AggregateTypeId, 1, 0),
            Schema = """{"type": "object", "properties": {"msg": {"type": "string"}}}""",
        });

        Assert.NotNull(result);
    }

    // =========================================================================
    // Connection configuration
    // =========================================================================

    [SkippableFact]
    public async Task ConnectAsync_WithConnectionTimeout_Succeeds()
    {
        await using var client = await CeleriantClient.ConnectAsync(
            Address,
            connectionTimeout: TimeSpan.FromSeconds(5));

        var key = TestHelpers.NewKey();
        await client.WriteAsync(key, [MakeEvent(1, "timeout-test")]);

        var details = await client.AggregateDetailsAsync(
            new AggregateDetailsRequest { AggregateKey = key });
        Assert.Equal(1L, details.MaxAggregateVersion);
    }

    [SkippableFact]
    public async Task ConnectAsync_ToInvalidAddress_ThrowsConnectionFailed()
    {
        await Assert.ThrowsAsync<ConnectionFailedException>(
            () => CeleriantClient.ConnectAsync("localhost:1",
                connectionTimeout: TimeSpan.FromSeconds(2), tlsConfig: null));
    }

    [SkippableFact]
    public async Task ConnectAsync_WithTimeout_ToUnreachableHost_ThrowsTimeoutException()
    {
        // 192.0.2.1 is TEST-NET-1 (RFC 5737) — should be unreachable and timeout
        await Assert.ThrowsAsync<CeleriantTimeoutException>(
            () => CeleriantClient.ConnectAsync("192.0.2.1:10000",
                connectionTimeout: TimeSpan.FromMilliseconds(500), tlsConfig: null));
    }

    [SkippableFact]
    public async Task WithMaxRequestSize_LargePayload_Throws()
    {
        await using var client = await CeleriantClient.ConnectAsync(Address, ct: default);
        client.WithMaxRequestSize(100); // 100 bytes — tiny

        var key = TestHelpers.NewKey();
        var largePayload = new byte[200];

        await Assert.ThrowsAsync<ArgumentException>(
            () => client.WriteAsync(key, [MakeEvent(1, largePayload)]));
    }

    [SkippableFact]
    public async Task WithTimeout_NormalOperation_Succeeds()
    {
        await using var client = await CeleriantClient.ConnectAsync(Address, ct: default);
        client.WithTimeout(TimeSpan.FromSeconds(10));

        var key = TestHelpers.NewKey();
        await client.WriteAsync(key, [MakeEvent(1, "with-timeout")]);

        var read = await client.ReadAsync(new ReadRequest
        {
            AggregateKey = key,
            Filters = ReadFilters.From(1),
        });
        Assert.NotEmpty(read.EventBatches);
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private static AggregateEvent MakeEvent(long clientSeq, string payload) =>
        new()
        {
            ClientSeq = clientSeq,
            EventSeq = 0,
            EventTimestamp = DateTimeOffset.UtcNow,
            EventTypeMajor = 1,
            EventTypeMinor = 0,
            EventValue = Encoding.UTF8.GetBytes(payload),
        };

    private static AggregateEvent MakeEvent(long clientSeq, byte[] payload) =>
        new()
        {
            ClientSeq = clientSeq,
            EventSeq = 0,
            EventTimestamp = DateTimeOffset.UtcNow,
            EventTypeMajor = 1,
            EventTypeMinor = 0,
            EventValue = payload,
        };
}
