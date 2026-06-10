using System.Text;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Watch;

namespace Celeriant.Client.IntegrationTests;

/// <summary>
/// Integration tests for <see cref="WatchConnection"/> against a real Celeriant server.
/// Watch is a push protocol: the client subscribes once and the server streams responses.
/// Events are forward-only (no backfill), and the server sends heartbeats (empty events)
/// every ~5 seconds when idle.
///
/// The server requires at least one aggregate ID in the watch request (shards by aggregate).
/// </summary>
[Collection("Server")]
public sealed class WatchTests
{
    private readonly ServerFixture _fixture;

    public WatchTests(ServerFixture fixture) => _fixture = fixture;

    private string Address
    {
        get
        {
            Skip.If(!_fixture.IsAvailable, "Server not running");
            return _fixture.Address;
        }
    }

    private CeleriantClient Client
    {
        get
        {
            Skip.If(!_fixture.IsAvailable, "Server not running");
            return _fixture.Client!;
        }
    }

    [SkippableFact]
    public async Task Watch_ReceivesWriteEvents()
    {
        var key = TestHelpers.NewKey();
        var clientId = Guid.NewGuid();

        // Create the aggregate first so the server knows the shard
        await Client.WriteAsync(key, [MakeEvent(1, "setup")], clientId);

        var watchRequest = new WatchRequest { Aggregates = new HashSet<Guid> { key.AggregateId } };
        await using var watch = await WatchConnection.ConnectAsync(
            Address, watchRequest, new WatchOptions());

        // Write another event
        await Client.WriteAsync(key, [MakeEvent(2, "watched-event")], clientId, allowCreate: false);

        var writeEvent = await WaitForWatchEvent(watch, TimeSpan.FromSeconds(10),
            e => e.Operation is WatchOperationType.Write or WatchOperationType.Create);

        Assert.NotNull(writeEvent);
        Assert.Equal(key.AggregateId, writeEvent.AggregateId);
    }

    [SkippableFact]
    public async Task Watch_FilterMismatchesRoutingRule_FallsBackToMultiShard()
    {
        var key = TestHelpers.NewKey();
        var clientId = Guid.NewGuid();
        await Client.WriteAsync(key, [MakeEvent(1, "fallback-setup")], clientId);

        // No Aggregates filter on an aggregate_id-routed server: the probe gets
        // 9002 (IncompatibleFilters) and the client must fan out per shard.
        var watchRequest = new WatchRequest
        {
            OperationTypes = new HashSet<WatchOperationType> { WatchOperationType.Write },
        };
        await using var watch = await WatchConnection.ConnectAsync(
            Address, watchRequest, new WatchOptions());

        await Client.WriteAsync(key, [MakeEvent(2, "fallback-event")], clientId, allowCreate: false);

        var writeEvent = await WaitForWatchEvent(watch, TimeSpan.FromSeconds(10),
            e => e.AggregateId == key.AggregateId && e.Operation == WatchOperationType.Write);

        Assert.NotNull(writeEvent);
    }

    [SkippableFact]
    public async Task Watch_NextAsyncWithTimeout_ReturnsNullOnExpiry()
    {
        var key = TestHelpers.NewKey();
        var clientId = Guid.NewGuid();
        await Client.WriteAsync(key, [MakeEvent(1, "timeout-setup")], clientId);

        var watchRequest = new WatchRequest { Aggregates = new HashSet<Guid> { key.AggregateId } };
        await using var watch = await WatchConnection.ConnectAsync(
            Address, watchRequest, new WatchOptions());

        // Drain the buffered first response
        await watch.NextAsync(TimeSpan.FromSeconds(6));

        // Very short timeout — shorter than the heartbeat interval (~5s)
        var response = await watch.NextAsync(TimeSpan.FromMilliseconds(100));
        Assert.Null(response);
    }

    [SkippableFact]
    public async Task Watch_Dispose_DoesNotThrow()
    {
        var key = TestHelpers.NewKey();
        var clientId = Guid.NewGuid();
        await Client.WriteAsync(key, [MakeEvent(1, "dispose-setup")], clientId);

        var watchRequest = new WatchRequest { Aggregates = new HashSet<Guid> { key.AggregateId } };
        var watch = await WatchConnection.ConnectAsync(
            Address, watchRequest, new WatchOptions());
        await watch.DisposeAsync();

        // Double dispose should be safe
        await watch.DisposeAsync();
    }

    [SkippableFact]
    public async Task Watch_AfterDispose_ThrowsObjectDisposedException()
    {
        var key = TestHelpers.NewKey();
        var clientId = Guid.NewGuid();
        await Client.WriteAsync(key, [MakeEvent(1, "disposed-setup")], clientId);

        var watchRequest = new WatchRequest { Aggregates = new HashSet<Guid> { key.AggregateId } };
        var watch = await WatchConnection.ConnectAsync(
            Address, watchRequest, new WatchOptions());
        await watch.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() => watch.NextAsync());
    }

    [SkippableFact]
    public async Task Watch_MultipleWriteEvents_AllReceived()
    {
        var key = TestHelpers.NewKey();
        var clientId = Guid.NewGuid();

        // Create the aggregate
        await Client.WriteAsync(key, [MakeEvent(1, "setup")], clientId);

        var watchRequest = new WatchRequest { Aggregates = new HashSet<Guid> { key.AggregateId } };
        await using var watch = await WatchConnection.ConnectAsync(
            Address, watchRequest, new WatchOptions());

        // Write 3 more batches
        for (int i = 2; i <= 4; i++)
            await Client.WriteAsync(key, [MakeEvent(i, $"event-{i}")], clientId, allowCreate: false);

        // Collect watch events
        var events = await CollectWatchEvents(watch, TimeSpan.FromSeconds(10), minCount: 3);

        var writeEvents = events
            .Where(e => e.Operation is WatchOperationType.Write or WatchOperationType.Create)
            .Where(e => e.AggregateId == key.AggregateId)
            .ToList();
        Assert.True(writeEvents.Count >= 3, $"Expected >= 3 write events, got {writeEvents.Count}");
    }

    [SkippableFact]
    public async Task Watch_ViaPool_ReceivesEvents()
    {
        var key = TestHelpers.NewKey();

        await using var pool = new CeleriantPool(new CeleriantPoolOptions
        {
            Address = Address,
            MaxConnections = 2,
        });

        // Create aggregate first
        await pool.WriteAsync(TestHelpers.SingleEventWrite(key, "setup"u8.ToArray()));

        var watchRequest = new WatchRequest { Aggregates = new HashSet<Guid> { key.AggregateId } };
        await using var watch = await pool.WatchAsync(watchRequest);

        // Write another event
        await pool.WriteAsync(TestHelpers.SingleEventWrite(key, "pool-watch"u8.ToArray(),
            clientSeq: 2, allowCreate: false));

        var writeEvent = await WaitForWatchEvent(watch, TimeSpan.FromSeconds(10),
            e => (e.Operation is WatchOperationType.Write or WatchOperationType.Create)
                 && e.AggregateId == key.AggregateId);

        Assert.NotNull(writeEvent);
    }

    [SkippableFact]
    public async Task Watch_DeleteEvent_Received()
    {
        var key = TestHelpers.NewKey();
        var clientId = Guid.NewGuid();

        // Create aggregate
        await Client.WriteAsync(key, [MakeEvent()], clientId);

        var watchRequest = new WatchRequest { Aggregates = new HashSet<Guid> { key.AggregateId } };
        await using var watch = await WatchConnection.ConnectAsync(
            Address, watchRequest, new WatchOptions());

        // Delete
        var details = await Client.AggregateDetailsAsync(
            new AggregateDetailsRequest { AggregateKey = key });
        await Client.DeleteAsync(new DeleteRequest
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

        var deleteEvent = await WaitForWatchEvent(watch, TimeSpan.FromSeconds(10),
            e => e.Operation == WatchOperationType.Delete && e.AggregateId == key.AggregateId);

        Assert.NotNull(deleteEvent);
    }

    [SkippableFact]
    public async Task Watch_Cancellation_StopsPolling()
    {
        var key = TestHelpers.NewKey();
        var clientId = Guid.NewGuid();
        await Client.WriteAsync(key, [MakeEvent(1, "cancel-setup")], clientId);

        var watchRequest = new WatchRequest { Aggregates = new HashSet<Guid> { key.AggregateId } };
        await using var watch = await WatchConnection.ConnectAsync(
            Address, watchRequest, new WatchOptions());

        // Drain buffered response
        await watch.NextAsync(TimeSpan.FromSeconds(6));

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

        await Assert.ThrowsAsync<OperationCanceledException>(
            () => watch.NextAsync(cts.Token));
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static AggregateEvent MakeEvent(long clientSeq = 1, string payload = "test") =>
        new()
        {
            ClientSeq = clientSeq,
            EventSeq = 0,
            EventTimestamp = DateTimeOffset.UtcNow,
            EventTypeMajor = 1,
            EventTypeMinor = 0,
            EventValue = Encoding.UTF8.GetBytes(payload),
        };

    /// <summary>
    /// Poll NextAsync until we find an event matching the predicate, or timeout.
    /// Skips heartbeats (empty responses) and non-matching events.
    /// </summary>
    private static async Task<WatchResponseEvent?> WaitForWatchEvent(
        WatchConnection watch,
        TimeSpan timeout,
        Func<WatchResponseEvent, bool> predicate)
    {
        using var cts = new CancellationTokenSource(timeout);

        try
        {
            while (!cts.IsCancellationRequested)
            {
                var response = await watch.NextAsync(cts.Token);
                foreach (var evt in response.Events)
                {
                    if (predicate(evt))
                        return evt;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Timeout — no matching event found.
        }

        return null;
    }

    /// <summary>
    /// Collect watch events until we have at least <paramref name="minCount"/> matching events
    /// or the timeout expires.
    /// </summary>
    private static async Task<List<WatchResponseEvent>> CollectWatchEvents(
        WatchConnection watch,
        TimeSpan timeout,
        int minCount)
    {
        var events = new List<WatchResponseEvent>();
        using var cts = new CancellationTokenSource(timeout);

        try
        {
            while (events.Count < minCount && !cts.IsCancellationRequested)
            {
                var response = await watch.NextAsync(cts.Token);
                events.AddRange(response.Events);
            }
        }
        catch (OperationCanceledException)
        {
            // Timeout — return what we have.
        }

        return events;
    }
}
