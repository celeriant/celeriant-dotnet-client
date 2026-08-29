using System.Collections.Concurrent;
using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Watch;

namespace Celeriant.Client.Tests;

/// <summary>
/// A watch subscription never reconnects and never catches a gap up, so the only thing standing
/// between a caller and silently missed notifications is that every way a subscription can stop
/// producing events is reported. These tests pin the three ways that guarantee could break, all
/// observable from the public surface:
///
/// <list type="bullet">
/// <item>a multi-shard watch whose shard readers have stopped must error on the next call, never
/// park the caller on a live-looking channel that will never produce anything;</item>
/// <item>the token passed to <see cref="WatchConnection.ConnectAsync"/> governs connecting only —
/// cancelling it afterwards must not silently retire a subscription the caller still holds;</item>
/// <item>one underlying failure produces one observable exception type, so
/// <c>catch (CeleriantClientException)</c> written against the single-shard path keeps working when
/// the same subscription happens to route across shards.</item>
/// </list>
///
/// Without these, the failure mode is invisible in every direction: no exception, no log, no
/// closed socket — just a caller that believes it is subscribed and a stream that has stopped.
/// </summary>
public class WatchFailClosedTests
{
    /// <summary>How long a call gets before the test calls it hung. Never used as a race timer.</summary>
    private static readonly TimeSpan HangBudget = TimeSpan.FromSeconds(5);

    /// <summary>The server's subscription ack: a watch frame carrying no events.</summary>
    private static readonly byte[] WatchAck =
        FakeServerSession.BuildFrame(MessageTypes.Responses.Watch, WireCodec.Serialize(new WatchResponse()));

    [Fact]
    public async Task MultiShard_ConnectTokenCancelledAfterConnect_SubscriptionKeepsDelivering()
    {
        var aggregateId = Guid.NewGuid();
        var deliver = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            await session.SendRawAsync(WatchAck);
            if (ShardOf(body) is 1)
                return;

            await deliver.Task;
            await session.SendRawAsync(EventFrame(aggregateId));
        });

        using var connectCts = new CancellationTokenSource();
        await using var watch = await ConnectAsync(server, maxShardHint: 2, connectCts.Token);

        // The caller's connect token is done its job the moment ConnectAsync returns. Cancelling it
        // is what a caller does with any connect-scoped token; the subscription is owned by the
        // WatchConnection and lives until it is disposed.
        await connectCts.CancelAsync();
        deliver.SetResult();

        var next = watch.NextAsync(CancellationToken.None);
        await AwaitCompletionAsync(
            next,
            "NextAsync on a multi-shard watch whose connect token was cancelled after connecting");

        var response = await next;
        Assert.Equal(
            aggregateId,
            Assert.Single(response.Events).AggregateId);
    }

    /// <summary>
    /// The fail-closed half of the same defect, written so it still holds once the connect token no
    /// longer reaches the shard readers. Cancelling the connect token is the only route the public
    /// surface offers into "every shard reader has stopped, nothing was delivered, nothing errored":
    /// disposal is the caller asking, and a socket failure already travels the error path. Whichever
    /// way that is resolved — readers that keep running, or readers that stop and say so — the one
    /// outcome the contract forbids is the caller waiting on a subscription nobody is reading.
    /// </summary>
    [Fact]
    public async Task MultiShard_ConnectTokenCancelledAfterConnect_NeverLeavesTheCallerWaitingInSilence()
    {
        var aggregateId = Guid.NewGuid();
        var deliver = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            await session.SendRawAsync(WatchAck);
            if (ShardOf(body) is 1)
                return;

            await deliver.Task;
            await session.SendRawAsync(EventFrame(aggregateId));
        });

        using var connectCts = new CancellationTokenSource();
        await using var watch = await ConnectAsync(server, maxShardHint: 2, connectCts.Token);

        await connectCts.CancelAsync();
        deliver.SetResult();

        WatchResponse? delivered = null;
        var failure = await Record.ExceptionAsync(async () => delivered = await watch.NextAsync(HangBudget));

        if (failure is not null)
        {
            Assert.True(
                failure is CeleriantClientException,
                "a watch that has stopped reading must fail closed through the client's own exception "
                + $"hierarchy so a typed catch sees it, but it surfaced {failure.GetType().Name}");
            return;
        }

        Assert.True(
            delivered is not null,
            "the shard readers stopped, said nothing, and left the channel open: the caller is "
            + "subscribed, receiving nothing, and will never be told — the exact silent blindness a "
            + "watch subscription exists to prevent");
    }

    [Fact]
    public async Task MultiShard_EveryShardConnectionDropped_ErrorsThroughTheClientHierarchy()
    {
        var failure = await CaptureFailureAfterConnectionDropAsync(maxShardHint: 2);

        Assert.True(
            failure is CeleriantClientException,
            "a caller writing catch (CeleriantClientException) around its watch loop must catch a "
            + $"dead multi-shard subscription; a leaked {failure.GetType().Name} escapes that catch "
            + "and kills the caller's process instead of its watch");
    }

    /// <summary>
    /// One dead shard means the watch is blind to that shard's aggregates while every other shard
    /// keeps looking healthy. A half-blind watch is worse than a dead one, so the first shard
    /// failure must kill the whole subscription and keep it dead.
    /// </summary>
    [Fact]
    public async Task MultiShard_OneShardConnectionDropped_PoisonsTheWholeWatchAndStaysDead()
    {
        var dropShardOne = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            await session.SendRawAsync(WatchAck);
            if (ShardOf(body) is not 1)
                return;

            await dropShardOne.Task;
            session.Close();
        });

        await using var watch = await ConnectAsync(server, maxShardHint: 2);

        var next = watch.NextAsync(CancellationToken.None);
        dropShardOne.SetResult();
        await AwaitCompletionAsync(
            next,
            "NextAsync on a multi-shard watch that lost one shard while the others stayed quiet");

        var firstFailure = await Record.ExceptionAsync(() => next);
        Assert.True(
            firstFailure is CeleriantClientException,
            "losing one shard means events from that shard are already being missed, so the whole "
            + $"watch must error through the client hierarchy; got {Describe(firstFailure)}");

        var again = watch.NextAsync(CancellationToken.None);
        await AwaitCompletionAsync(again, "the second NextAsync on an already-poisoned watch");

        var secondFailure = await Record.ExceptionAsync(() => again);
        Assert.True(
            secondFailure is CeleriantClientException,
            "a poisoned watch stays poisoned: a caller that retries must keep being told, not be "
            + $"handed a channel that silently never produces again; got {Describe(secondFailure)}");
    }

    /// <summary>
    /// Whether a subscription routed across shards is invisible to the caller: they wrote one
    /// try/catch and the server decides which path it takes.
    /// </summary>
    [Fact]
    public async Task DroppedConnection_SurfacesTheSameExceptionTypeOnBothPaths()
    {
        var single = await CaptureFailureAfterConnectionDropAsync(maxShardHint: null);
        var multi = await CaptureFailureAfterConnectionDropAsync(maxShardHint: 2);

        Assert.True(
            single is CeleriantClientException && multi is CeleriantClientException,
            $"a dropped watch connection must be a client error on both paths; single-shard gave "
            + $"{single.GetType().Name} and multi-shard gave {multi.GetType().Name}");

        Assert.True(
            single.GetType() == multi.GetType(),
            "the same failure — the server dropping the watch connection — must be the same "
            + $"exception to the caller, but single-shard gave {single.GetType().Name} and "
            + $"multi-shard gave {multi.GetType().Name}");
    }

    [Fact]
    public async Task ServerWatchError_SurfacesTheSameExceptionTypeOnBothPaths()
    {
        var single = await CaptureFailureAfterWatchErrorAsync(maxShardHint: null);
        var multi = await CaptureFailureAfterWatchErrorAsync(maxShardHint: 2);

        Assert.True(
            single is CeleriantClientException && multi is CeleriantClientException,
            $"a server watch error must be a client error on both paths; single-shard gave "
            + $"{single.GetType().Name} and multi-shard gave {multi.GetType().Name}");

        Assert.True(
            single.GetType() == multi.GetType(),
            "the server sent the identical error frame on both paths, so the caller must see the "
            + $"identical exception, but single-shard gave {single.GetType().Name} and multi-shard "
            + $"gave {multi.GetType().Name}");
    }

    /// <summary>
    /// The other entry point into a multi-shard subscription: no <c>MaxShardHint</c>, the server
    /// answers the unsharded probe with 9001 and the client fans out. Everything the multi-shard
    /// tests above pin is reachable this way too, by a caller who never mentioned shards.
    /// </summary>
    [Fact]
    public async Task ShardRoutingFallback_ProbeAnsweredWith9001_SubscribesEveryShardAndDelivers()
    {
        var aggregateId = Guid.NewGuid();
        var subscribedShards = new ConcurrentBag<long>();
        var everyShardSubscribed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            var request = WireCodec.Deserialize<WatchRequest>(body);
            if (request.ShardId is null)
            {
                await session.SendFrameAsync(
                    MessageTypes.Responses.GenericError,
                    FakeServerProtocol.ErrorFrame(
                        ErrorResponse.ShardRoutingMultipleShards,
                        "watch filter spans shards {\"num_shards\":2}",
                        request.CorrelationId));
                return;
            }

            await session.SendRawAsync(WatchAck);
            subscribedShards.Add(request.ShardId.Value);
            if (subscribedShards.Count >= 2)
                everyShardSubscribed.TrySetResult();

            if (request.ShardId is 1)
                await session.SendRawAsync(EventFrame(aggregateId));
        });

        await using var watch = await ConnectAsync(server, maxShardHint: null);

        await AwaitCompletionAsync(
            everyShardSubscribed.Task,
            "the 9001 fallback subscribing both shards");
        Assert.Equal([0L, 1L], subscribedShards.Order());

        var next = watch.NextAsync(CancellationToken.None);
        await AwaitCompletionAsync(next, "NextAsync on a watch that fell back to multi-shard");

        var response = await next;
        Assert.Equal(aggregateId, Assert.Single(response.Events).AggregateId);
    }

    /// <summary>
    /// A deadline that expires on a quiet socket is not a failure, so it must not cost the caller
    /// the subscription. A fix that makes silence terminal would break every polling caller.
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData(2L)]
    public async Task CleanTimeout_ReturnsNullAndLeavesTheSubscriptionUsable(long? maxShardHint)
    {
        var aggregateId = Guid.NewGuid();
        var deliver = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            await session.SendRawAsync(WatchAck);
            if (ShardOf(body) is 1)
                return;

            await deliver.Task;
            await session.SendRawAsync(EventFrame(aggregateId));
        });

        await using var watch = await ConnectAsync(server, maxShardHint);

        Assert.Null(await watch.NextAsync(TimeSpan.FromMilliseconds(200)));

        deliver.SetResult();
        var next = watch.NextAsync(CancellationToken.None);
        await AwaitCompletionAsync(next, "NextAsync after a clean timeout on the same subscription");

        var response = await next;
        Assert.Equal(aggregateId, Assert.Single(response.Events).AggregateId);
    }

    /// <summary>
    /// Disposal is how a caller ends a subscription, and it is the only thing that ends one. It must
    /// not need the socket to say anything first: a caller parked on a quiet watch must be able to
    /// shut it down. The exception the parked call ends with is deliberately not pinned here.
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData(2L)]
    public async Task DisposeWhileParkedInNextAsync_UnblocksBothTheDisposeAndTheCall(long? maxShardHint)
    {
        await using var server = FakeCeleriantServer.Start((session, _, _) => session.SendRawAsync(WatchAck));

        var watch = await ConnectAsync(server, maxShardHint);
        var next = watch.NextAsync(CancellationToken.None);

        var dispose = watch.DisposeAsync().AsTask();
        await AwaitCompletionAsync(dispose, "DisposeAsync while a caller was parked in NextAsync");
        await dispose;

        await AwaitCompletionAsync(next, "the NextAsync that was parked when the watch was disposed");
        await Record.ExceptionAsync(() => next);
    }

    /// <summary>
    /// A token the caller passes to <see cref="WatchConnection.NextAsync(CancellationToken)"/> is the
    /// caller ending one wait, not the stream failing. Reporting it as a protocol or connection error
    /// would make a caller with a per-iteration deadline think it had lost events.
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData(2L)]
    public async Task CallerTokenCancelled_SurfacesAsCancellation(long? maxShardHint)
    {
        await using var server = FakeCeleriantServer.Start((session, _, _) => session.SendRawAsync(WatchAck));

        await using var watch = await ConnectAsync(server, maxShardHint);

        using var callerCts = new CancellationTokenSource();
        var next = watch.NextAsync(callerCts.Token);
        await callerCts.CancelAsync();

        await AwaitCompletionAsync(next, "NextAsync after the caller cancelled its own token");

        var failure = await Record.ExceptionAsync(() => next);
        Assert.True(
            failure is OperationCanceledException,
            "the caller cancelled its own wait, so it must come back as cancellation; anything else "
            + $"reads as a lost subscription. Got {Describe(failure)}");
    }

    private static async Task<Exception> CaptureFailureAfterConnectionDropAsync(long? maxShardHint)
    {
        var drop = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var server = FakeCeleriantServer.Start(async (session, _, _) =>
        {
            await session.SendRawAsync(WatchAck);
            await drop.Task;
            session.Close();
        });

        await using var watch = await ConnectAsync(server, maxShardHint);

        var next = watch.NextAsync(CancellationToken.None);
        drop.SetResult();
        await AwaitCompletionAsync(
            next,
            $"NextAsync on a {Describe(maxShardHint)} watch whose connection the server dropped");

        var failure = await Record.ExceptionAsync(() => next);
        Assert.True(
            failure is not null,
            $"the {Describe(maxShardHint)} watch connection was dropped by the server, so the next "
            + "call must fail rather than return");
        return failure!;
    }

    private static async Task<Exception> CaptureFailureAfterWatchErrorAsync(long? maxShardHint)
    {
        var fail = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            var request = WireCodec.Deserialize<WatchRequest>(body);
            await session.SendRawAsync(WatchAck);
            await fail.Task;
            await session.SendFrameAsync(
                MessageTypes.Responses.GenericError,
                FakeServerProtocol.ErrorFrame(
                    ErrorResponse.WatchReadIo,
                    "watch read failed",
                    request.CorrelationId));
        });

        await using var watch = await ConnectAsync(server, maxShardHint);

        var next = watch.NextAsync(CancellationToken.None);
        fail.SetResult();
        await AwaitCompletionAsync(
            next,
            $"NextAsync on a {Describe(maxShardHint)} watch the server reported an error on");

        var failure = await Record.ExceptionAsync(() => next);
        Assert.True(
            failure is not null,
            $"the server reported a watch read failure on the {Describe(maxShardHint)} path, so the "
            + "next call must fail rather than return");
        return failure!;
    }

    private static Task<WatchConnection> ConnectAsync(
        FakeCeleriantServer server,
        long? maxShardHint,
        CancellationToken ct = default)
        => WatchConnection.ConnectAsync(
            server.Address,
            new WatchRequest(),
            new WatchOptions
            {
                ConnectionTimeout = TimeSpan.FromSeconds(5),
                MaxShardHint = maxShardHint,
            },
            ct);

    /// <summary>The shard a received watch request subscribes to, or null for the unsharded probe.</summary>
    private static long? ShardOf(byte[] requestBody)
        => WireCodec.Deserialize<WatchRequest>(requestBody).ShardId;

    private static byte[] EventFrame(Guid aggregateId)
        => FakeServerSession.BuildFrame(
            MessageTypes.Responses.Watch,
            WireCodec.Serialize(new WatchResponse
            {
                Events =
                [
                    new WatchResponseEvent
                    {
                        OrgId = Guid.NewGuid(),
                        AggregateTypeId = Guid.NewGuid(),
                        AggregateId = aggregateId,
                        Operation = WatchOperationType.Write,
                        FromAggregateVersion = 1,
                        ToAggregateVersion = 1,
                    },
                ],
            }));

    /// <summary>Fail loudly on a hang instead of deadlocking the run.</summary>
    private static async Task AwaitCompletionAsync(Task task, string whatWouldHang)
    {
        var finished = await Task.WhenAny(task, Task.Delay(HangBudget));
        Assert.True(
            ReferenceEquals(finished, task),
            $"{whatWouldHang} was still running after {HangBudget.TotalSeconds:0}s: it hangs where "
            + "the contract requires it to complete");
    }

    private static string Describe(long? maxShardHint)
        => maxShardHint is null ? "single-shard" : "multi-shard";

    private static string Describe(Exception? failure)
        => failure is null ? "no exception at all" : failure.GetType().Name;
}
