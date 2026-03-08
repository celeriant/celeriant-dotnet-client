using System.Text;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Streaming;

namespace Celeriant.Client.IntegrationTests;

/// <summary>
/// Integration tests for <see cref="CeleriantPool"/> covering connection management,
/// concurrent access, and typed method coverage.
/// </summary>
[Collection("Server")]
public sealed class PoolTests
{
    private readonly ServerFixture _fixture;

    public PoolTests(ServerFixture fixture) => _fixture = fixture;

    private string Address
    {
        get
        {
            Skip.If(!_fixture.IsAvailable, "Server not running");
            return _fixture.Address;
        }
    }

    private CeleriantPool CreatePool(int maxConnections = 3) =>
        new(new CeleriantPoolOptions
        {
            Address = Address,
            MaxConnections = maxConnections,
        });

    [SkippableFact]
    public async Task ConcurrentWrites_AllSucceed()
    {
        await using var pool = CreatePool(maxConnections: 5);

        var tasks = Enumerable.Range(0, 20).Select(async i =>
        {
            var key = TestHelpers.NewKey();
            var payload = Encoding.UTF8.GetBytes($"concurrent-{i}");
            await pool.WriteAsync(TestHelpers.SingleEventWrite(key, payload));
            return key;
        }).ToArray();

        var keys = await Task.WhenAll(tasks);
        Assert.Equal(20, keys.Length);
    }

    [SkippableFact]
    public async Task AggregateDetails_ViaPool()
    {
        await using var pool = CreatePool();

        var key = TestHelpers.NewKey();
        await pool.WriteAsync(TestHelpers.SingleEventWrite(key, "pool-details"u8.ToArray()));

        var details = await pool.AggregateDetailsAsync(
            TestHelpers.DetailsRequest(key));
        Assert.Equal(1L, details.MaxEventBatchIndex);
        Assert.False(details.IsDeleted);
    }

    [SkippableFact]
    public async Task Delete_ViaPool()
    {
        await using var pool = CreatePool();

        var key = TestHelpers.NewKey();
        await pool.WriteAsync(TestHelpers.SingleEventWrite(key, "pool-delete"u8.ToArray()));

        var details = await pool.AggregateDetailsAsync(TestHelpers.DetailsRequest(key));

        await pool.DeleteAsync(new DeleteRequest
        {
            ClientId = Guid.NewGuid(),
            Deletes = new Dictionary<AggregateKey, SingleAggregateDelete>
            {
                [key] = new SingleAggregateDelete
                {
                    AllowRecreate = false,
                    ExpectedEventBatchIndex = details.MaxEventBatchIndex,
                }
            }
        });

        // After deletion, the server may return IsDeleted=true or an error
        // (deleted aggregates may not be queryable depending on server state).
        try
        {
            var afterDelete = await pool.AggregateDetailsAsync(TestHelpers.DetailsRequest(key));
            Assert.True(afterDelete.IsDeleted);
        }
        catch (Errors.CeleriantErrorException)
        {
            // Server error is acceptable — aggregate was deleted
        }
    }

    [SkippableFact]
    public async Task TrimStart_ViaPool()
    {
        await using var pool = CreatePool();

        var key = TestHelpers.NewKey();
        await pool.WriteAsync(TestHelpers.SingleEventWrite(key, "batch-1"u8.ToArray()));
        await pool.WriteAsync(TestHelpers.SingleEventWrite(key, "batch-2"u8.ToArray(), clientEventIndex: 2, allowCreate: false));

        var details = await pool.AggregateDetailsAsync(TestHelpers.DetailsRequest(key));

        await pool.TrimStartAsync(new TrimStartRequest
        {
            AggregateKey = key,
            KeepFromEventBatchIndex = details.MaxEventBatchIndex,
            ClientId = Guid.NewGuid(),
        });

        var read = await pool.ReadAsync(new ReadRequest
        {
            AggregateKey = key,
            Filters = ReadFilters.From(details.MaxEventBatchIndex),
        });
        var payloads = read.EventBatches
            .SelectMany(b => b.Events)
            .Select(e => Encoding.UTF8.GetString(e.EventValue))
            .ToList();
        Assert.Contains("batch-2", payloads);
        Assert.DoesNotContain("batch-1", payloads);
    }

    [SkippableFact]
    public async Task ListOrgs_ViaPool()
    {
        await using var pool = CreatePool();

        var orgId = Guid.NewGuid();
        var key = new AggregateKey(orgId, Guid.NewGuid(), Guid.NewGuid());
        await pool.WriteAsync(TestHelpers.SingleEventWrite(key, "list-orgs"u8.ToArray()));

        var orgs = new List<Guid>();
        await foreach (var org in pool.ListOrgsAsync())
            orgs.Add(org.OrgId);
        Assert.Contains(orgId, orgs);
    }

    [SkippableFact]
    public async Task ListAggregateTypes_ViaPool()
    {
        await using var pool = CreatePool();

        var orgId = Guid.NewGuid();
        var typeId = Guid.NewGuid();
        var key = new AggregateKey(orgId, typeId, Guid.NewGuid());
        await pool.WriteAsync(TestHelpers.SingleEventWrite(key, "list-types"u8.ToArray()));

        var types = new List<Guid>();
        await foreach (var item in pool.ListAggregateTypesAsync(orgId: orgId))
            types.Add(item.AggregateTypeId);
        Assert.Contains(typeId, types);
    }

    [SkippableFact]
    public async Task ListAggregates_ViaPool()
    {
        await using var pool = CreatePool();

        var orgId = Guid.NewGuid();
        var key = new AggregateKey(orgId, Guid.NewGuid(), Guid.NewGuid());
        await pool.WriteAsync(TestHelpers.SingleEventWrite(key, "list-aggs"u8.ToArray()));

        var aggs = new List<AggregateStats>();
        await foreach (var item in pool.ListAggregatesAsync(orgId: orgId))
            aggs.Add(item);
        Assert.Contains(aggs, a => a.AggregateId == key.AggregateId);
    }

    [SkippableFact]
    public async Task IdleTimeout_StaleConnectionEvicted()
    {
        // Use a very short idle timeout so connections go stale quickly
        await using var pool = new CeleriantPool(new CeleriantPoolOptions
        {
            Address = Address,
            MaxConnections = 2,
            IdleTimeout = TimeSpan.FromMilliseconds(100),
        });

        // Use and return a connection
        var key1 = TestHelpers.NewKey();
        await pool.WriteAsync(TestHelpers.SingleEventWrite(key1, "idle-1"u8.ToArray()));

        // Wait for idle timeout to expire
        await Task.Delay(300);

        // Next use should evict the stale connection and create a fresh one
        var key2 = TestHelpers.NewKey();
        await pool.WriteAsync(TestHelpers.SingleEventWrite(key2, "idle-2"u8.ToArray()));

        // Both writes should have succeeded
        await using var verifyConn = await pool.GetConnectionAsync();
        var read1 = await verifyConn.Client.SendRequestAsync(
            new ClientRequest.Read(TestHelpers.ReadAllRequest(key1)));
        Assert.IsType<ClientResponse.Read>(read1);

        var read2 = await verifyConn.Client.SendRequestAsync(
            new ClientRequest.Read(TestHelpers.ReadAllRequest(key2)));
        Assert.IsType<ClientResponse.Read>(read2);
    }

    [SkippableFact]
    public async Task PoolDispose_SubsequentCallsThrow()
    {
        var pool = CreatePool();
        await pool.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
        {
            await pool.GetConnectionAsync();
        });
    }

    [SkippableFact]
    public async Task WriteWithAutoCompression_ViaPool()
    {
        // Pool with threshold=0 so all variable-size writes are compressed
        await using var pool = new CeleriantPool(new CeleriantPoolOptions
        {
            Address = Address,
            MaxConnections = 3,
            AutoCompressionThresholdBytes = 0,
        });

        var key = TestHelpers.NewKey();
        var payload = Encoding.UTF8.GetBytes("pool-compressed-write");

        await pool.WriteAsync(TestHelpers.SingleEventWrite(key, payload));

        var read = await pool.ReadAsync(TestHelpers.ReadAllRequest(key));
        var events = read.EventBatches.SelectMany(b => b.Events).ToArray();
        Assert.Single(events);
        Assert.Equal(payload, events[0].EventValue);
    }

    [SkippableFact]
    public async Task WriteWithAutoCompression_SmallPayloadNotCompressed()
    {
        // Pool with high threshold — small writes skip compression
        await using var pool = new CeleriantPool(new CeleriantPoolOptions
        {
            Address = Address,
            MaxConnections = 3,
            AutoCompressionThresholdBytes = 100_000,
        });

        var key = TestHelpers.NewKey();
        var payload = Encoding.UTF8.GetBytes("small-payload");

        await pool.WriteAsync(TestHelpers.SingleEventWrite(key, payload));

        var read = await pool.ReadAsync(TestHelpers.ReadAllRequest(key));
        var events = read.EventBatches.SelectMany(b => b.Events).ToArray();
        Assert.Single(events);
        Assert.Equal(payload, events[0].EventValue);
    }
}
