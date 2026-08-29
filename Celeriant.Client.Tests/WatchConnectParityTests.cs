using System.Collections.Concurrent;
using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Watch;

namespace Celeriant.Client.Tests;

/// <summary>
/// A <see cref="WatchConnection"/> handed back to a caller is a promise: the caller believes it is
/// watching the whole shard range it asked for. These tests pin the two ways that promise can be
/// broken at connect time, neither of which the caller can see afterwards:
///
/// <list type="bullet">
/// <item><b>Subscribed late.</b> If a shard's watch request is written after <c>ConnectAsync</c>
/// returns, the caller is already reading events from the shards that did subscribe while another
/// shard has not subscribed at all — and a shard that then refuses reports its failure long after
/// the caller committed. Every shard must be subscribed and acked before the connection exists, and
/// a shard that cannot subscribe must fail the connect and take every opened socket down with it.</item>
/// <item><b>Subscribed to the wrong range.</b> <c>StartShard</c>/<c>MaxShardHint</c> name a range;
/// the subscription must cover exactly it. An empty or inverted range that quietly collapses to a
/// single shard is the worst case of all — the caller is watching one shard, believes it is watching
/// a range, and nothing ever tells it otherwise.</item>
/// </list>
///
/// Without this file both defects are invisible: no exception, no closed socket, just events that
/// never arrive from shards nobody subscribed to.
/// </summary>
public class WatchConnectParityTests
{
    /// <summary>How long a call gets before the test calls it hung. Never used as a race timer.</summary>
    private static readonly TimeSpan HangBudget = TimeSpan.FromSeconds(5);

    /// <summary>
    /// The one unavoidable wall clock: proving a call has <em>not</em> returned needs a window. It is
    /// only ever used after a server-side causal signal has already fired, never as the sole timing.
    /// </summary>
    private static readonly TimeSpan SettleBudget = TimeSpan.FromMilliseconds(500);

    /// <summary>The server's subscription ack: a watch frame carrying no events.</summary>
    private static readonly byte[] WatchAck =
        FakeServerSession.BuildFrame(MessageTypes.Responses.Watch, WireCodec.Serialize(new WatchResponse()));

    /// <summary>
    /// The whole point of subscribing inside connect: while any shard is still unacked there is no
    /// connection, so no caller can be reading a range it is not fully subscribed to.
    /// </summary>
    [Fact]
    public async Task MultiShard_ConnectAsync_DoesNotReturnWhileAnyShardIsStillUnacked()
    {
        const int shardCount = 4;
        var ackTheLastShard = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var everyRequestArrived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int arrived = 0;

        // Withholding the ack for whichever request lands last, rather than for a fixed shard id,
        // keeps this independent of the order the client opens its shard connections in.
        await using var server = FakeCeleriantServer.Start(async (session, _, _) =>
        {
            if (Interlocked.Increment(ref arrived) == shardCount)
            {
                everyRequestArrived.SetResult();
                await ackTheLastShard.Task;
            }

            await session.SendRawAsync(WatchAck);
        });

        var connect = ConnectAsync(server, startShard: 0, maxShardHint: shardCount);

        await AwaitCompletionAsync(
            everyRequestArrived.Task,
            $"the client sending a watch request for all {shardCount} shards in the range");
        await Task.Delay(SettleBudget);

        Assert.False(
            connect.IsCompleted,
            $"all {shardCount} watch requests have been sent and one is deliberately unacked, yet "
            + "ConnectAsync already returned: the caller holds a connection whose last shard may "
            + "still refuse to subscribe, and will discover that only after acting on events from "
            + "the shards that did");

        ackTheLastShard.SetResult();
        await AwaitCompletionAsync(connect, "ConnectAsync once every shard had been acked");
        await using var watch = await connect;
    }

    [Theory]
    [InlineData(0L, 4L, new[] { 0L, 1L, 2L, 3L })]
    [InlineData(2L, 5L, new[] { 2L, 3L, 4L })]
    public async Task MultiShard_SubscribesExactlyTheRequestedShardRange(
        long startShard,
        long maxShardHint,
        long[] expectedShards)
    {
        var subscribed = new ConcurrentQueue<long>();
        var everyShardSubscribed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            subscribed.Enqueue(ShardOf(body) ?? -1);
            if (subscribed.Count >= expectedShards.Length)
                everyShardSubscribed.TrySetResult();

            await session.SendRawAsync(WatchAck);
        });

        await using var watch = await ConnectAsync(server, startShard, maxShardHint);

        await AwaitCompletionAsync(
            everyShardSubscribed.Task,
            $"the client subscribing every shard in [{startShard}, {maxShardHint})");

        Assert.Equal(expectedShards, subscribed.Order().ToArray());

        // One watch is terminal on its connection, so a shard subscribed twice — or one outside the
        // range — can only show up as an extra socket.
        Assert.Equal(expectedShards.Length, server.ConnectionsAccepted);
    }

    /// <summary>
    /// A shard that refuses the subscription means the caller can never see that shard's events. If
    /// connect hands back a connection anyway, the caller reads real events from the other shards
    /// first and is told about the hole later, by which time it has already acted on a partial view.
    /// </summary>
    [Fact]
    public async Task MultiShard_AShardThatCannotSubscribe_FailsTheConnect()
    {
        var aggregateId = Guid.NewGuid();

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            var request = WireCodec.Deserialize<WatchRequest>(body);
            if (request.ShardId is 2)
            {
                await session.SendFrameAsync(
                    MessageTypes.Responses.GenericError,
                    FakeServerProtocol.ErrorFrame(
                        ErrorResponse.WatchRequestInvalid,
                        "shard 2 refuses this watch",
                        request.CorrelationId));
                return;
            }

            await session.SendRawAsync(WatchAck);
            if (request.ShardId is 0)
                await session.SendRawAsync(EventFrame(aggregateId));
        });

        WatchConnection? connected = null;
        var connect = Task.Run(async () => connected = await ConnectAsync(server, startShard: 0, maxShardHint: 4));
        await AwaitCompletionAsync(connect, "ConnectAsync when one shard answers its watch request with an error");

        var failure = await Record.ExceptionAsync(() => connect);

        WatchResponse? leaked = null;
        if (connected is not null)
        {
            await Record.ExceptionAsync(async () => leaked = await connected.NextAsync(SettleBudget));
            await connected.DisposeAsync();
        }

        Assert.True(
            failure is not null,
            "shard 2 refused the subscription, so the watch can never see shard 2's events — yet "
            + "ConnectAsync returned a connection instead of failing"
            + (leaked is null
                ? string.Empty
                : ", and that connection then delivered an event from a shard that did subscribe, so "
                  + "the caller acted on a view it was told was complete"));

        Assert.True(
            failure is CeleriantClientException,
            "a shard refusing the subscription must fail the connect through the client's own "
            + $"exception hierarchy so a typed catch sees it; got {Describe(failure)}");
    }

    /// <summary>
    /// A failed connect owns everything it opened. Sockets left dialled into a subscription nobody
    /// holds keep server-side subscriber slots alive and are invisible to the caller that never got
    /// a connection to dispose.
    /// </summary>
    [Fact]
    public async Task MultiShard_WhenAShardFailsTheConnect_EveryOpenedConnectionIsClosed()
    {
        const int shardCount = 4;
        var everyConnectionClosed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int closed = 0;

        await using var server = FakeCeleriantServer.Start(
            async (session, _, body) =>
            {
                var request = WireCodec.Deserialize<WatchRequest>(body);
                if (request.ShardId is 2)
                {
                    await session.SendFrameAsync(
                        MessageTypes.Responses.GenericError,
                        FakeServerProtocol.ErrorFrame(
                            ErrorResponse.WatchRequestInvalid,
                            "shard 2 refuses this watch",
                            request.CorrelationId));
                    return;
                }

                await session.SendRawAsync(WatchAck);
            },
            _ =>
            {
                if (Interlocked.Increment(ref closed) >= shardCount)
                    everyConnectionClosed.TrySetResult();
            });

        WatchConnection? connected = null;
        var connect = Task.Run(async () =>
            connected = await ConnectAsync(server, startShard: 0, maxShardHint: shardCount));
        await AwaitCompletionAsync(connect, "ConnectAsync when one shard answers its watch request with an error");

        var failure = await Record.ExceptionAsync(() => connect);
        Assert.True(
            failure is not null,
            "the connect must fail before its cleanup can be judged: a returned connection means the "
            + "shard failure is being deferred to the caller instead of aborting the connect");

        await AwaitCompletionAsync(
            everyConnectionClosed.Task,
            $"the {shardCount} shard sockets opened by a ConnectAsync that then failed being closed");

        Assert.Equal(server.ConnectionsAccepted, Volatile.Read(ref closed));
    }

    /// <summary>
    /// The range the caller named cannot be honoured, so there is nothing to subscribe. Silently
    /// subscribing one shard is not a lenient reading of the request — it hands back a connection
    /// that looks like a range subscription and is blind to everything but one shard.
    /// </summary>
    [Theory]
    [InlineData(5L, 5L)]
    [InlineData(6L, 3L)]
    public async Task MultiShard_EmptyOrInvertedRange_IsRejectedRatherThanCollapsingToOneShard(
        long startShard,
        long maxShardHint)
    {
        var subscribed = new ConcurrentQueue<long>();

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            subscribed.Enqueue(ShardOf(body) ?? -1);
            await session.SendRawAsync(WatchAck);
        });

        WatchConnection? connected = null;
        var connect = Task.Run(async () => connected = await ConnectAsync(server, startShard, maxShardHint));
        await AwaitCompletionAsync(connect, $"ConnectAsync for the empty shard range [{startShard}, {maxShardHint})");

        var failure = await Record.ExceptionAsync(() => connect);
        if (connected is not null)
        {
            await Task.Delay(SettleBudget);
            await connected.DisposeAsync();
        }

        Assert.True(
            failure is not null,
            $"[{startShard}, {maxShardHint}) contains no shards, so no subscription can satisfy it. "
            + $"ConnectAsync returned a connection subscribed to shard(s) [{string.Join(", ", subscribed.Order())}] "
            + "instead of rejecting the range: the caller now believes it is watching a range while it "
            + "is watching one shard, and nothing will ever tell it about the shards it is blind to");
    }

    /// <summary>
    /// The same range contract reached by a caller who never mentioned shards: the server answers the
    /// unsharded probe with <c>num_shards</c> and the range becomes [StartShard, num_shards).
    /// </summary>
    [Theory]
    [InlineData(0L, 4, new[] { 0L, 1L, 2L, 3L })]
    [InlineData(2L, 4, new[] { 2L, 3L })]
    public async Task ShardRoutingFallback_SubscribesExactlyTheRangeUpToNumShards(
        long startShard,
        int numShards,
        long[] expectedShards)
    {
        var subscribed = new ConcurrentQueue<long>();
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
                        $"watch filter spans shards {{\"num_shards\":{numShards}}}",
                        request.CorrelationId));
                return;
            }

            subscribed.Enqueue(request.ShardId.Value);
            if (subscribed.Count >= expectedShards.Length)
                everyShardSubscribed.TrySetResult();

            await session.SendRawAsync(WatchAck);
        });

        await using var watch = await ConnectAsync(server, startShard, maxShardHint: null);

        await AwaitCompletionAsync(
            everyShardSubscribed.Task,
            $"the 9001 fallback subscribing every shard in [{startShard}, {numShards})");

        Assert.Equal(expectedShards, subscribed.Order().ToArray());

        // The probe is one connection; every shard after it is one more. Anything above that is a
        // shard subscribed twice or a shard outside the range.
        Assert.True(
            server.ConnectionsAccepted <= expectedShards.Length + 1,
            $"the fallback opened {server.ConnectionsAccepted} connections for the {expectedShards.Length} "
            + $"shards in [{startShard}, {numShards}) plus the probe: a duplicate subscription means the "
            + "range was fanned out more than once");
    }

    [Fact]
    public async Task ShardRoutingFallback_StartShardBeyondNumShards_IsRejectedRatherThanCollapsingToOneShard()
    {
        const int numShards = 4;
        var subscribed = new ConcurrentQueue<long>();

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            var request = WireCodec.Deserialize<WatchRequest>(body);
            if (request.ShardId is null)
            {
                await session.SendFrameAsync(
                    MessageTypes.Responses.GenericError,
                    FakeServerProtocol.ErrorFrame(
                        ErrorResponse.ShardRoutingMultipleShards,
                        $"watch filter spans shards {{\"num_shards\":{numShards}}}",
                        request.CorrelationId));
                return;
            }

            subscribed.Enqueue(request.ShardId.Value);
            await session.SendRawAsync(WatchAck);
        });

        WatchConnection? connected = null;
        var connect = Task.Run(async () =>
            connected = await ConnectAsync(server, startShard: numShards, maxShardHint: null));
        await AwaitCompletionAsync(connect, $"ConnectAsync when StartShard {numShards} is past the server's shard count");

        var failure = await Record.ExceptionAsync(() => connect);
        if (connected is not null)
        {
            await Task.Delay(SettleBudget);
            await connected.DisposeAsync();
        }

        Assert.True(
            failure is not null,
            $"the server reported {numShards} shards, so StartShard {numShards} names an empty range. "
            + $"ConnectAsync returned a connection subscribed to shard(s) [{string.Join(", ", subscribed.Order())}] "
            + "instead of rejecting it: the caller believes it is watching every shard from "
            + $"{numShards} upwards and is in fact watching a shard it never asked for");
    }

    private static Task<WatchConnection> ConnectAsync(
        FakeCeleriantServer server,
        long startShard,
        long? maxShardHint)
        => WatchConnection.ConnectAsync(
            server.Address,
            new WatchRequest(),
            new WatchOptions
            {
                ConnectionTimeout = TimeSpan.FromSeconds(5),
                StartShard = startShard,
                MaxShardHint = maxShardHint,
            },
            CancellationToken.None);

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

    private static string Describe(Exception? failure)
        => failure is null ? "no exception at all" : failure.GetType().Name;
}
