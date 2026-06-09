using System.Text;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Streaming;

namespace Celeriant.Client.IntegrationTests;

/// <summary>
/// Integration tests for <see cref="ReadExtensions.ReadAllAsync"/> and streaming pagination.
/// </summary>
[Collection("Server")]
public sealed class StreamingTests
{
    private readonly ServerFixture _fixture;

    public StreamingTests(ServerFixture fixture) => _fixture = fixture;

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
    // ReadAllAsync
    // =========================================================================

    [SkippableFact]
    public async Task ReadAllAsync_SingleBatch_YieldsAllEvents()
    {
        var key = TestHelpers.NewKey();
        var clientId = Guid.NewGuid();
        var payload = "streaming-single"u8.ToArray();
        await Client.WriteAsync(key, [MakeEvent(1, payload)], clientId);

        var batches = new List<AggregateEventBatch>();
        await foreach (var batch in Client.ReadAllAsync(key))
            batches.Add(batch);

        Assert.NotEmpty(batches);
        var events = batches.SelectMany(b => b.Events).ToArray();
        Assert.Single(events);
        Assert.Equal(payload, events[0].EventValue);
    }

    [SkippableFact]
    public async Task ReadAllAsync_MultipleBatches_CollectsAll()
    {
        var key = TestHelpers.NewKey();
        var clientId = Guid.NewGuid();

        // Write 5 separate batches
        for (int i = 1; i <= 5; i++)
        {
            var payload = Encoding.UTF8.GetBytes($"batch-{i}");
            await Client.WriteAsync(key, [MakeEvent(i, payload)], clientId, allowCreate: i == 1);
        }

        var allEvents = new List<AggregateEvent>();
        await foreach (var batch in Client.ReadAllAsync(key))
            allEvents.AddRange(batch.Events);

        Assert.Equal(5, allEvents.Count);

        for (int i = 1; i <= 5; i++)
        {
            var expected = Encoding.UTF8.GetBytes($"batch-{i}");
            Assert.Contains(allEvents, e => e.EventValue.SequenceEqual(expected));
        }
    }

    [SkippableFact]
    public async Task ReadAllAsync_WithFromFilter_SkipsEarlierBatches()
    {
        var key = TestHelpers.NewKey();
        var clientId = Guid.NewGuid();

        // Write 3 batches
        for (int i = 1; i <= 3; i++)
            await Client.WriteAsync(key, [MakeEvent(i, $"batch-{i}")], clientId, allowCreate: i == 1);

        // Read from batch index 2 onwards
        var events = new List<AggregateEvent>();
        await foreach (var batch in Client.ReadAllAsync(key, ReadFilters.From(2)))
            events.AddRange(batch.Events);

        var payloads = events.Select(e => Encoding.UTF8.GetString(e.EventValue)).ToList();
        Assert.DoesNotContain("batch-1", payloads);
        Assert.Contains("batch-2", payloads);
        Assert.Contains("batch-3", payloads);
    }

    [SkippableFact]
    public async Task ReadAllAsync_Cancellation_StopsEnumeration()
    {
        var key = TestHelpers.NewKey();
        var clientId = Guid.NewGuid();

        for (int i = 1; i <= 3; i++)
            await Client.WriteAsync(key, [MakeEvent(i, $"cancel-{i}")], clientId, allowCreate: i == 1);

        using var cts = new CancellationTokenSource();
        var count = 0;

        await foreach (var batch in Client.ReadAllAsync(key, ct: cts.Token))
        {
            count++;
            cts.Cancel();
            break;
        }

        Assert.Equal(1, count);
    }

    [SkippableFact]
    public async Task ReadAllAsync_NonexistentAggregate_ThrowsCeleriantError()
    {
        var key = TestHelpers.NewKey();

        await Assert.ThrowsAsync<Errors.AggregateNotFoundException>(async () =>
        {
            await foreach (var _ in Client.ReadAllAsync(key))
            {
            }
        });
    }

    // =========================================================================
    // Pool ReadAllAsync
    // =========================================================================

    [SkippableFact]
    public async Task Pool_ReadAllAsync_CollectsAll()
    {
        await using var pool = new CeleriantPool(new CeleriantPoolOptions
        {
            Address = Address,
            MaxConnections = 2,
        });

        var key = TestHelpers.NewKey();

        for (int i = 1; i <= 3; i++)
        {
            var payload = Encoding.UTF8.GetBytes($"pool-stream-{i}");
            await pool.WriteAsync(TestHelpers.SingleEventWrite(key, payload,
                clientSeq: i, allowCreate: i == 1));
        }

        var events = new List<AggregateEvent>();
        await foreach (var batch in pool.ReadAllAsync(key))
            events.AddRange(batch.Events);

        Assert.Equal(3, events.Count);
    }

    // =========================================================================
    // ListAggregates stats verification
    // =========================================================================

    [SkippableFact]
    public async Task ListAggregates_StatsPopulated()
    {
        var orgId = Guid.NewGuid();
        var typeId = Guid.NewGuid();
        var key = new AggregateKey(orgId, typeId, Guid.NewGuid());
        var clientId = Guid.NewGuid();

        // Write 2 batches
        await Client.WriteAsync(key, [MakeEvent(1, "stats-1")], clientId);
        await Client.WriteAsync(key, [MakeEvent(2, "stats-2")], clientId, allowCreate: false);

        var stats = new List<AggregateStats>();
        await foreach (var item in Client.ListAggregatesAsync(orgId: orgId, aggregateTypeId: typeId))
            stats.Add(item);

        var agg = stats.SingleOrDefault(s => s.AggregateId == key.AggregateId);
        Assert.NotNull(agg);
        Assert.Equal(orgId, agg.OrgId);
        Assert.Equal(typeId, agg.AggregateTypeId);
        Assert.False(agg.IsDeleted);
        Assert.True(agg.EventBatchCount >= 2);
        Assert.True(agg.MaxAggregateVersion >= 2);
        Assert.True(agg.CompressedSize > 0);
        Assert.True(agg.UncompressedSize > 0);
        Assert.NotNull(agg.MaxServerTimestamp);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

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
