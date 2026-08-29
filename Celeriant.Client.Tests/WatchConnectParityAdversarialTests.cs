using System.Collections.Concurrent;
using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Watch;

namespace Celeriant.Client.Tests;

/// <summary>
/// Adversarial probes against the connect-time parity work. Each one hunts for a path where a
/// caller ends up holding a <see cref="WatchConnection"/> that is not watching what it believes it
/// is watching, or where connecting neither succeeds nor fails.
/// </summary>
public class WatchConnectParityAdversarialTests
{
    private static readonly TimeSpan HangBudget = TimeSpan.FromSeconds(5);

    private static readonly byte[] WatchAck =
        FakeServerSession.BuildFrame(MessageTypes.Responses.Watch, WireCodec.Serialize(new WatchResponse()));

    // ---------------------------------------------------------------------
    // A1: the single-shard twin of the multi-shard wrong-response-type guard
    // ---------------------------------------------------------------------

    /// <summary>
    /// The multi-shard path now rejects a shard that answers its watch request with a well-formed
    /// frame of the wrong kind. The single-shard probe takes the identical reply and treats it as a
    /// successful subscription: the caller is handed a live-looking watch over a connection that
    /// never acknowledged anything, and nothing will ever push to it.
    /// </summary>
    [Fact]
    public async Task SingleShard_AProbeAnsweredWithTheWrongResponseType_FailsTheConnect()
    {
        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            var request = WireCodec.Deserialize<WatchRequest>(body);

            // Well-formed, decodes fine, and acknowledges nothing. Byte-for-byte the reply the
            // multi-shard path is tested to reject.
            await session.SendFrameAsync(
                MessageTypes.Responses.AggregateDetails,
                WireCodec.Serialize(new AggregateDetailsResponse
                {
                    CorrelationId = request.CorrelationId,
                    MaxAggregateVersion = 1,
                }));
        });

        WatchConnection? connected = null;
        var failure = await Record.ExceptionAsync(async () => connected = await WatchConnection.ConnectAsync(
            server.Address,
            new WatchRequest(),
            new WatchOptions { ConnectionTimeout = TimeSpan.FromSeconds(5) }));

        WatchResponse? delivered = null;
        if (connected is not null)
        {
            delivered = await connected.NextAsync(TimeSpan.FromSeconds(1));
            await connected.DisposeAsync();
        }

        Assert.True(
            failure is CeleriantClientException,
            "the server never acknowledged the watch, so this connection is subscribed to nothing. "
            + $"ConnectAsync returned {(connected is null ? "no connection" : "a connection")} and the "
            + $"first NextAsync then produced {(delivered is null ? "nothing at all" : "an event")}: the "
            + "caller holds a watch it believes is live and is blind with nothing to tell it so. Got "
            + $"{failure?.GetType().Name ?? "no exception"}");
    }

    // ---------------------------------------------------------------------
    // A2: nothing bounds the subscription round trip
    // ---------------------------------------------------------------------

    /// <summary>
    /// Moving the subscription inside connect also moved an unbounded read there.
    /// <see cref="WatchOptions.ConnectionTimeout"/> bounds the dial and the TLS handshake only, so a
    /// node that accepts the socket and never answers parks <c>ConnectAsync</c> forever — and
    /// <c>CeleriantPool.WatchAsync</c> with it, which can only fail over on a connect that returns.
    /// </summary>
    [Fact]
    public async Task MultiShard_AShardThatAcceptsTheSocketAndNeverAcks_FailsTheConnectWithinTheTimeout()
    {
        var connectionTimeout = TimeSpan.FromMilliseconds(500);
        var everyRequestArrived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int arrived = 0;

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            long? shard = WireCodec.Deserialize<WatchRequest>(body).ShardId;
            if (Interlocked.Increment(ref arrived) == 2)
                everyRequestArrived.TrySetResult();

            // Shard 1 is a node that is up enough to accept a connection and read a request, and
            // dead enough never to answer it.
            if (shard is 1)
                return;

            await session.SendRawAsync(WatchAck);
        });

        var connect = Task.Run(() => WatchConnection.ConnectAsync(
            server.Address,
            new WatchRequest(),
            new WatchOptions { ConnectionTimeout = connectionTimeout, MaxShardHint = 2 }));

        await AwaitCompletionAsync(everyRequestArrived.Task, "both watch requests reaching the server");

        var settled = await Task.WhenAny(connect, Task.Delay(TimeSpan.FromSeconds(4)));

        // Let the hung connect fault instead of being collected unobserved.
        _ = connect.ContinueWith(
            t => { _ = t.Exception; if (t.Status == TaskStatus.RanToCompletion) _ = t.Result.DisposeAsync(); },
            TaskScheduler.Default);

        Assert.True(
            ReferenceEquals(settled, connect),
            $"ConnectionTimeout is {connectionTimeout.TotalMilliseconds:0}ms and ConnectAsync was still "
            + "running 4s after both watch requests reached the server. Nothing bounds the ack read, so "
            + "one half-dead node hangs the connect indefinitely and CeleriantPool.WatchAsync never "
            + "reaches the next candidate node");
    }

    // ---------------------------------------------------------------------
    // A3: the checked cast bounds nothing a real server can send
    // ---------------------------------------------------------------------

    /// <summary>
    /// A shard count past <see cref="int"/> range is rejected as unusable. A shard count inside
    /// <see cref="int"/> range but far past what can be dialled is not: the client tries to honour
    /// it and dies in a way no caller can catch through the client's own hierarchy.
    /// </summary>
    [Fact]
    public async Task ShardRoutingFallback_AnInRangeButUnopenableShardCount_IsRejectedAsAClientError()
    {
        const long unopenable = 2147483647L; // int.MaxValue: in range for the cast, not for sockets.

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            var request = WireCodec.Deserialize<WatchRequest>(body);
            if (request.ShardId is null)
            {
                await session.SendFrameAsync(
                    MessageTypes.Responses.GenericError,
                    FakeServerProtocol.ErrorFrame(
                        ErrorResponse.ShardRoutingMultipleShards,
                        $"watch filter spans shards {{\"num_shards\":{unopenable}}}",
                        request.CorrelationId));
                return;
            }

            await session.SendRawAsync(WatchAck);
        });

        // Budgeted, because the failure mode under test is a fan-out. If the 1024-shard cap ever
        // regresses, an unbudgeted await here becomes two billion sockets and the run stops
        // producing output at all — a hang reports nothing, a failed assertion reports the cap.
        var connect = Task.Run(() => WatchConnection.ConnectAsync(
            server.Address,
            new WatchRequest(),
            new WatchOptions { ConnectionTimeout = TimeSpan.FromSeconds(5) }));

        // Observe whatever the task carries even if the budget expires below.
        _ = connect.ContinueWith(
            t => { _ = t.Exception; if (t.Status == TaskStatus.RanToCompletion) _ = t.Result.DisposeAsync(); },
            TaskScheduler.Default);

        await AwaitCompletionAsync(
            connect,
            $"a connect against a server naming {unopenable} shards");

        var failure = await Record.ExceptionAsync(() => connect);

        Assert.True(
            failure is CeleriantClientException,
            $"the server named {unopenable} shards. That is exactly as unsubscribable as the value one "
            + "past the cast's range, which is rejected as a ProtocolException, but this one is taken at "
            + $"face value: got {failure?.GetType().Name ?? "no exception"}");
    }

    // ---------------------------------------------------------------------
    // A4: only one end of the range is validated
    // ---------------------------------------------------------------------

    /// <summary>
    /// The new range guard rejects <c>numShards &lt;= startShard</c>. It says nothing about a
    /// negative <c>StartShard</c>, which produces a subscription to a shard id no server has.
    /// </summary>
    [Fact]
    public async Task MultiShard_NegativeStartShard_IsRejectedRatherThanSubscribingANegativeShardId()
    {
        var subscribed = new ConcurrentQueue<long>();

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            subscribed.Enqueue(WireCodec.Deserialize<WatchRequest>(body).ShardId ?? long.MinValue);
            await session.SendRawAsync(WatchAck);
        });

        WatchConnection? connected = null;
        var failure = await Record.ExceptionAsync(async () => connected = await WatchConnection.ConnectAsync(
            server.Address,
            new WatchRequest(),
            new WatchOptions { ConnectionTimeout = TimeSpan.FromSeconds(5), StartShard = -2, MaxShardHint = 2 }));

        if (connected is not null)
            await connected.DisposeAsync();

        Assert.True(
            failure is not null,
            "StartShard -2 names shards no server has. The range guard only looks at the top end, so "
            + $"the client subscribed [{string.Join(", ", subscribed.Order())}] and handed back a watch "
            + "whose negative-shard connections can never carry an event");
    }

    // ---------------------------------------------------------------------
    // A5: the exception type the range guard raises
    // ---------------------------------------------------------------------

    /// <summary>
    /// Where the upper bound came from decides the exception, because the two cases are different
    /// kinds of failure. A <c>MaxShardHint</c> below <c>StartShard</c> is the caller's own constants
    /// contradicting each other, caught before any I/O and unfixable by retrying — an argument
    /// exception, deliberately outside the client's hierarchy so that a <c>catch</c> written to
    /// handle a node dying does not also swallow a bug in the caller's configuration. A bound the
    /// server reported is a runtime condition (the same StartShard is valid against a larger
    /// cluster) and belongs in the hierarchy, where a connect-time catch will see it.
    /// </summary>
    [Fact]
    public async Task RejectedRange_IsAnArgumentErrorFromTheCallerAndAClientErrorFromTheServer()
    {
        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            var request = WireCodec.Deserialize<WatchRequest>(body);
            if (request.ShardId is null)
            {
                await session.SendFrameAsync(
                    MessageTypes.Responses.GenericError,
                    FakeServerProtocol.ErrorFrame(
                        ErrorResponse.ShardRoutingMultipleShards,
                        "watch filter spans shards {\"num_shards\":3}",
                        request.CorrelationId));
                return;
            }

            await session.SendRawAsync(WatchAck);
        });

        var fromCaller = await Record.ExceptionAsync(() => WatchConnection.ConnectAsync(
            server.Address,
            new WatchRequest(),
            new WatchOptions { ConnectionTimeout = TimeSpan.FromSeconds(5), StartShard = 6, MaxShardHint = 3 }));

        Assert.True(
            fromCaller is ArgumentOutOfRangeException,
            "the caller's own MaxShardHint sits below its StartShard, which no retry or failover can "
            + $"fix; surfacing it as a client error invites a catch that hides it. Got {Describe(fromCaller)}");

        // Same empty range, but the 3 came from the server: valid options against a larger cluster.
        var fromServer = await Record.ExceptionAsync(() => WatchConnection.ConnectAsync(
            server.Address,
            new WatchRequest(),
            new WatchOptions { ConnectionTimeout = TimeSpan.FromSeconds(5), StartShard = 6 }));

        Assert.True(
            fromServer is CeleriantClientException,
            "the server reported fewer shards than StartShard names — a runtime condition a caller's "
            + $"connect-time catch must see, not an argument error. Got {Describe(fromServer)}");
    }

    private static string Describe(Exception? failure)
        => failure?.GetType().Name ?? "no exception";

    // ---------------------------------------------------------------------
    // A6: leak probes that should come back clean
    // ---------------------------------------------------------------------

    /// <summary>
    /// The connect token firing part-way through the fan-out must leave nothing dialled: the caller
    /// never receives a connection, so nothing else can ever close what connect opened.
    /// </summary>
    [Fact]
    public async Task MultiShard_ConnectCancelledMidFanOut_ClosesEverySocketItOpened()
    {
        const int shardCount = 4;
        using var cts = new CancellationTokenSource();
        var oneShardSubscribed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int closed = 0;

        await using var server = FakeCeleriantServer.Start(
            async (session, _, body) =>
            {
                long? shard = WireCodec.Deserialize<WatchRequest>(body).ShardId;
                if (shard is 0)
                {
                    await session.SendRawAsync(WatchAck);
                    oneShardSubscribed.TrySetResult();
                    return;
                }

                // Every other shard stays unacked, so the cancel lands with one shard subscribed
                // and the rest still waiting.
            },
            _ => Interlocked.Increment(ref closed));

        var connect = Task.Run(() => WatchConnection.ConnectAsync(
            server.Address,
            new WatchRequest(),
            new WatchOptions { ConnectionTimeout = TimeSpan.FromSeconds(5), MaxShardHint = shardCount },
            cts.Token));

        await AwaitCompletionAsync(oneShardSubscribed.Task, "the first shard's subscription being acked");
        await cts.CancelAsync();

        var failure = await Record.ExceptionAsync(() => connect);
        Assert.True(failure is not null, "a cancelled connect must not hand back a connection");

        // The server sees a session end once the client's socket is gone.
        await WaitForAsync(
            () => server.ConnectionsAccepted == shardCount && Volatile.Read(ref closed) >= shardCount,
            () => $"{Volatile.Read(ref closed)} of {server.ConnectionsAccepted} sockets opened by the "
                  + "cancelled connect were closed; the rest are still dialled into a subscription "
                  + "nobody holds and nobody can dispose");
    }

    /// <summary>
    /// Events riding on the subscription acks are queued before any reader runs. Within one shard
    /// that ordering is the whole contract: an ack event must reach the caller ahead of anything the
    /// same shard pushes afterwards.
    /// </summary>
    [Fact]
    public async Task MultiShard_AnAckEvent_IsDeliveredBeforeTheSameShardsFirstLiveEvent()
    {
        var onAck = Guid.NewGuid();
        var live = Guid.NewGuid();

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            long? shard = WireCodec.Deserialize<WatchRequest>(body).ShardId;
            if (shard is 0)
            {
                await session.SendRawAsync(EventFrame(onAck));
                await session.SendRawAsync(EventFrame(live));
                return;
            }

            await session.SendRawAsync(WatchAck);
        });

        await using var watch = await WatchConnection.ConnectAsync(
            server.Address,
            new WatchRequest(),
            new WatchOptions { ConnectionTimeout = TimeSpan.FromSeconds(5), MaxShardHint = 2 });

        var first = await watch.NextAsync(HangBudget);
        var second = await watch.NextAsync(HangBudget);

        Assert.Equal(onAck, first?.Events.Single().AggregateId);
        Assert.Equal(live, second?.Events.Single().AggregateId);
    }

    /// <summary>
    /// The connect-time subscription must not have weakened the live guarantee behind it: a shard
    /// whose socket dies after the watch is established has to surface as an error on the very next
    /// read, never as silence and never as a clean "nothing arrived yet".
    /// </summary>
    [Fact]
    public async Task MultiShard_AShardKilledAfterConnectReturned_ErrorsOnTheNextRead()
    {
        var killShardOne = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            long? shard = WireCodec.Deserialize<WatchRequest>(body).ShardId;
            await session.SendRawAsync(WatchAck);

            if (shard is 1)
            {
                await killShardOne.Task;
                session.Close();
            }
        });

        await using var watch = await WatchConnection.ConnectAsync(
            server.Address,
            new WatchRequest(),
            new WatchOptions { ConnectionTimeout = TimeSpan.FromSeconds(5), MaxShardHint = 2 });

        killShardOne.SetResult();

        var read = Task.Run(() => watch.NextAsync(TimeSpan.FromSeconds(2)));
        await AwaitCompletionAsync(read, "NextAsync after a shard's socket died");

        var failure = await Record.ExceptionAsync(() => read);
        Assert.True(
            failure is CeleriantClientException,
            "shard 1's connection is gone, so this watch can no longer see that shard. The read "
            + $"produced {(failure is null ? "no error at all" : failure.GetType().Name)} instead of "
            + "telling the caller the subscription is dead");
    }

    /// <summary>
    /// A range the client itself rejects must not be re-dialled against every candidate node: it is
    /// doomed at every one of them, and a failover loop that retries it turns one bad argument into
    /// a fan-out of pointless connects.
    /// </summary>
    [Fact]
    public async Task Pool_ARejectedShardRange_IsNotRetriedAgainstEveryNode()
    {
        await using var nodeA = FakeCeleriantServer.Start(async (session, _, _) =>
            await session.SendRawAsync(WatchAck));
        await using var nodeB = FakeCeleriantServer.Start(async (session, _, _) =>
            await session.SendRawAsync(WatchAck));

        await using var pool = new CeleriantPool(new CeleriantPoolOptions
        {
            Address = nodeA.Address,
            SeedAddresses = [nodeB.Address],
            ConnectionTimeout = TimeSpan.FromSeconds(5),
            RequestTimeout = TimeSpan.FromSeconds(10),
        });

        var failure = await Record.ExceptionAsync(() => pool.WatchAsync(
            new WatchRequest(),
            new WatchOptions { StartShard = 6, MaxShardHint = 3 }));

        // The type carries the weight. A connection count of zero is true whether the loop ran
        // once or once per node, because the range is rejected before any socket opens — so on its
        // own it cannot detect the retry this test is named for. ArgumentOutOfRangeException is not
        // in WatchAsync's failover catch set, which is what makes the loop unable to iterate.
        Assert.IsType<ArgumentOutOfRangeException>(failure);
        Assert.Equal(0, nodeA.ConnectionsAccepted + nodeB.ConnectionsAccepted);
    }

    // ---------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------

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

    private static async Task AwaitCompletionAsync(Task task, string whatWouldHang)
    {
        var finished = await Task.WhenAny(task, Task.Delay(HangBudget));
        Assert.True(
            ReferenceEquals(finished, task),
            $"{whatWouldHang} was still pending after {HangBudget.TotalSeconds:0}s");
    }

    private static async Task WaitForAsync(Func<bool> condition, Func<string> describeFailure)
    {
        var deadline = DateTime.UtcNow + HangBudget;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
                return;

            await Task.Delay(25);
        }

        Assert.Fail(describeFailure());
    }
}
