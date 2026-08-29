using System.Net;
using System.Net.Sockets;
using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Watch;

namespace Celeriant.Client.Tests;

/// <summary>
/// A watch subscription never reconnects and never catches a gap up, so when a caller loses one and
/// opens another it has restarted from some node's tip and is blind to everything in between. The
/// three things pinned here are what lets a caller reason about that instead of guessing:
///
/// <list type="bullet">
/// <item><b>Which node it is attached to.</b> A connection that cannot say which node it landed on
/// leaves a caller unable to tell a re-subscribe to the same node from one that silently moved —
/// the only signal that its position restarted somewhere else.</item>
/// <item><b>A failed watch dial must not leave the pool pinned to the node that failed.</b> The
/// watch failed over to a healthy node; if the pool keeps the dead one cached as leader, every
/// later operation pays that dial again and the caller sees latency it cannot explain.</item>
/// <item><b>A poisoned multi-shard watch releases its sibling sockets.</b> The subscription is
/// already terminally dead to the caller, so readers still parked on live sockets are holding
/// server-side subscriber slots for a watch nobody can ever read again.</item>
/// </list>
/// </summary>
public class WatchAddressParityTests
{
    /// <summary>How long a call gets before the test calls it hung. Never used as a race timer.</summary>
    private static readonly TimeSpan HangBudget = TimeSpan.FromSeconds(5);

    /// <summary>The server's subscription ack: a watch frame carrying no events.</summary>
    private static readonly byte[] WatchAck =
        FakeServerSession.BuildFrame(MessageTypes.Responses.Watch, WireCodec.Serialize(new WatchResponse()));

    // ---------------------------------------------------------------------
    // 1. WatchConnection.Address — the node the subscription is attached to.
    //
    // ---------------------------------------------------------------------

    /// <summary>
    /// The floor: a caller that named one node must be able to read back the node it got, in the
    /// form it passed, or it cannot compare this subscription with the next one.
    /// </summary>
    [Fact]
    public async Task SingleShard_Address_IsTheNodeTheSubscriptionIsAttachedTo()
    {
        await using var server = FakeCeleriantServer.Start((session, _, _) => session.SendRawAsync(WatchAck));

        await using var watch = await ConnectAsync(server.Address, maxShardHint: null);

        Assert.Equal(server.Address, watch.Address);
    }

    /// <summary>
    /// A multi-shard watch fans out several sockets to the same node, so there is still exactly one
    /// answer. Reporting nothing — or one shard's socket — would make the property useless on the
    /// path a caller cannot even tell it is on.
    /// </summary>
    [Fact]
    public async Task MultiShard_Address_IsTheOneNodeEveryShardIsAttachedTo()
    {
        await using var server = FakeCeleriantServer.Start((session, _, _) => session.SendRawAsync(WatchAck));

        await using var watch = await ConnectAsync(server.Address, maxShardHint: 3);

        Assert.Equal(server.Address, watch.Address);
        Assert.Equal(3, server.ConnectionsAccepted);
    }

    /// <summary>
    /// The property has to name the node actually connected to, not the string the caller handed in.
    /// <see cref="CeleriantPool.WatchAsync"/> picks among candidates and falls through on a connect
    /// failure, so the node that answered is routinely not the one the caller would have guessed —
    /// which is exactly the case the property exists for.
    /// </summary>
    [Fact]
    public async Task Pool_Address_NamesTheNodeThatAccepted_NotTheOneThatRefused()
    {
        var refusing = ReservedAddressThatRefusesConnections();
        await using var accepting = FakeCeleriantServer.Start((session, _, _) => session.SendRawAsync(WatchAck));

        await using var pool = new CeleriantPool(new CeleriantPoolOptions
        {
            Address = refusing,
            SeedAddresses = [accepting.Address],
            ConnectionTimeout = TimeSpan.FromMilliseconds(500),
            RequestTimeout = TimeSpan.FromSeconds(10),
        });

        await using var watch = await pool.WatchAsync(new WatchRequest());

        Assert.Equal(accepting.Address, watch.Address);
        Assert.True(
            watch.Address != refusing,
            $"the pool fell through to {accepting.Address} because {refusing} refused the dial, so a "
            + "connection reporting the refusing node is reporting a candidate it never reached: a "
            + "caller comparing addresses across re-subscribes would see no move where the whole "
            + "subscription moved node");
    }

    /// <summary>
    /// Address is read after the fact — after an error, when the caller is deciding whether to
    /// re-subscribe — so it has to survive everything a live subscription does. A value that only
    /// holds until the first event answers the question nobody asks.
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData(2L)]
    public async Task Address_IsStableForTheLifeOfTheConnection(long? maxShardHint)
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

        await using var watch = await ConnectAsync(server.Address, maxShardHint);
        var atConnect = watch.Address;

        Assert.Null(await watch.NextAsync(TimeSpan.FromMilliseconds(200)));
        Assert.Equal(atConnect, watch.Address);

        deliver.SetResult();
        var next = watch.NextAsync(CancellationToken.None);
        await AwaitCompletionAsync(next, "NextAsync delivering the event the address stability check reads around");
        Assert.Equal(aggregateId, Assert.Single((await next).Events).AggregateId);

        Assert.Equal(server.Address, watch.Address);
        Assert.Equal(atConnect, watch.Address);
    }

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

    // ---------------------------------------------------------------------
    // 2. A failed watch dial must not leave the pool pinned to the dead node.
    // ---------------------------------------------------------------------

    /// <summary>
    /// The watch dialled the cached leader, the leader was gone, and the watch failed over. The node
    /// is now known-bad to the pool. Leaving it cached as leader means every later operation opens
    /// with the same doomed dial, so the failover the pool just performed buys the caller one call
    /// and nothing after it.
    /// </summary>
    [Fact]
    public async Task Pool_AfterAWatchFailsOverFromTheCachedLeader_ThePoolIsNoLongerPinnedToIt()
    {
        var deadLeader = ReservedAddressThatRefusesConnections();
        await using var healthy = FakeCeleriantServer.Start((session, _, _) => session.SendRawAsync(WatchAck));

        await using var pool = new CeleriantPool(new CeleriantPoolOptions
        {
            Address = healthy.Address,
            SeedAddresses = [deadLeader],
            ConnectionTimeout = TimeSpan.FromMilliseconds(500),
            RequestTimeout = TimeSpan.FromSeconds(10),
        });

        // Mirrors the state write-path leader discovery leaves behind: the leader is a node other
        // than the configured primary, which is the only shape in which "still pinned" is visible.
        pool.SetLeaderForTesting(deadLeader);
        Assert.Equal(deadLeader, pool.GetWatchAddress());

        var connect = pool.WatchAsync(new WatchRequest());
        await AwaitCompletionAsync(connect, "pool.WatchAsync failing over from an unreachable cached leader");
        await using var watch = await connect;

        Assert.True(
            pool.GetWatchAddress() != deadLeader,
            $"the watch dial to the cached leader {deadLeader} failed and the pool fell through to "
            + $"{healthy.Address}, yet the pool still names the dead node as where the next operation "
            + "goes first: the failure taught it nothing and every later call re-pays the same "
            + "doomed dial");
    }

    /// <summary>
    /// The same contract without reaching for a routing accessor: a node the pool has already
    /// watched-and-failed must not be dialled first all over again. Its accept count is the
    /// instrument, so this holds whatever the pool calls its cached-leader field.
    /// </summary>
    [Fact]
    public async Task Pool_ASecondWatchAfterAFailover_DoesNotDialTheDeadLeaderAgain()
    {
        // Accept, then hang up before acking: a dial that reaches the node and still fails, which is
        // what makes it countable. A refused connect is indistinguishable from one never attempted.
        await using var deadLeader = FakeCeleriantServer.Start((session, _, _) =>
        {
            session.Close();
            return Task.CompletedTask;
        });
        await using var healthy = FakeCeleriantServer.Start((session, _, _) => session.SendRawAsync(WatchAck));

        await using var pool = new CeleriantPool(new CeleriantPoolOptions
        {
            Address = healthy.Address,
            SeedAddresses = [deadLeader.Address],
            ConnectionTimeout = TimeSpan.FromMilliseconds(500),
            RequestTimeout = TimeSpan.FromSeconds(10),
        });

        pool.SetLeaderForTesting(deadLeader.Address);

        var first = pool.WatchAsync(new WatchRequest());
        await AwaitCompletionAsync(first, "the first pool.WatchAsync, failing over from a dead cached leader");
        await using var firstWatch = await first;

        int dialsDuringFailover = deadLeader.ConnectionsAccepted;
        Assert.True(
            dialsDuringFailover >= 1,
            "the test cannot say anything about a leader that was never dialled: the pool did not "
            + $"try the cached leader {deadLeader.Address} at all");

        var second = pool.WatchAsync(new WatchRequest());
        await AwaitCompletionAsync(second, "the second pool.WatchAsync after the first had already failed over");
        await using var secondWatch = await second;

        Assert.True(
            deadLeader.ConnectionsAccepted == dialsDuringFailover,
            $"the pool dialled the dead leader {deadLeader.Address} again "
            + $"({deadLeader.ConnectionsAccepted} times in total, {dialsDuringFailover} of them before "
            + "the second call): a watch that fails over must clear the leader it just found dead, or "
            + "every subsequent operation opens with the same failed dial for as long as the node "
            + "stays down");
    }

    // ---------------------------------------------------------------------
    // 3. A poisoned multi-shard watch stops its sibling readers.
    // ---------------------------------------------------------------------

    /// <summary>
    /// Once one shard has failed the subscription is terminally dead, so the readers still parked on
    /// the surviving shards can never produce anything a caller will see. Disposing must not depend
    /// on those sockets saying something first, and it must end all of them: sockets outliving a
    /// subscription nobody can read hold server-side subscriber slots for nothing.
    /// </summary>
    [Fact]
    public async Task MultiShard_DisposingAPoisonedWatch_CompletesPromptlyAndEndsEveryShardSocket()
    {
        const int shardCount = 3;
        var killShardOne = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var everySocketEnded = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int ended = 0;

        await using var server = FakeCeleriantServer.Start(
            async (session, _, body) =>
            {
                await session.SendRawAsync(WatchAck);
                if (ShardOf(body) is not 1)
                    return;

                await killShardOne.Task;
                session.Close();
            },
            _ =>
            {
                if (Interlocked.Increment(ref ended) >= shardCount)
                    everySocketEnded.TrySetResult();
            });

        var watch = await ConnectAsync(server.Address, maxShardHint: shardCount);

        var next = watch.NextAsync(CancellationToken.None);
        killShardOne.SetResult();
        await AwaitCompletionAsync(next, "NextAsync on a multi-shard watch whose shard 1 socket died");

        var failure = await Record.ExceptionAsync(() => next);
        Assert.True(
            failure is CeleriantClientException,
            "the watch must be poisoned before its cleanup can be judged; the failing shard produced "
            + $"{Describe(failure)}");

        var dispose = watch.DisposeAsync().AsTask();
        await AwaitCompletionAsync(
            dispose,
            "DisposeAsync on a poisoned multi-shard watch whose two surviving shards are quiet");
        await dispose;

        await AwaitCompletionAsync(
            everySocketEnded.Task,
            $"all {shardCount} shard sockets of a disposed poisoned watch being released");
    }

    /// <summary>
    /// The sharper half, and the one Rust's <c>poison()</c> encodes: the sibling readers are stopped
    /// by the poisoning itself, not by a later disposal. A caller that stores the terminal error and
    /// moves on — the natural shape, since the subscription can never recover — otherwise leaves
    /// readers sitting on live sockets for as long as it holds the object.
    /// </summary>
    [Fact]
    public async Task MultiShard_OnceThePoisonHasSurfaced_TheSiblingShardSocketsAreReleasedWithoutADispose()
    {
        const int shardCount = 3;
        var killShardOne = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var everySocketEnded = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int ended = 0;

        await using var server = FakeCeleriantServer.Start(
            async (session, _, body) =>
            {
                await session.SendRawAsync(WatchAck);
                if (ShardOf(body) is not 1)
                    return;

                await killShardOne.Task;
                session.Close();
            },
            _ =>
            {
                if (Interlocked.Increment(ref ended) >= shardCount)
                    everySocketEnded.TrySetResult();
            });

        await using var watch = await ConnectAsync(server.Address, maxShardHint: shardCount);

        var next = watch.NextAsync(CancellationToken.None);
        killShardOne.SetResult();
        await AwaitCompletionAsync(next, "NextAsync on a multi-shard watch whose shard 1 socket died");

        var failure = await Record.ExceptionAsync(() => next);
        Assert.True(
            failure is CeleriantClientException,
            $"the watch must be poisoned before its readers can be judged; got {Describe(failure)}");

        var finished = await Task.WhenAny(everySocketEnded.Task, Task.Delay(HangBudget));
        Assert.True(
            ReferenceEquals(finished, everySocketEnded.Task),
            $"{shardCount - Volatile.Read(ref ended)} of {shardCount} shard sockets were still open "
            + $"{HangBudget.TotalSeconds:0}s after the watch was poisoned: the surviving shards' "
            + "readers are parked on subscriptions that can never deliver anything again, holding "
            + "server-side subscriber slots until the caller happens to dispose an object it has "
            + "already been told is dead");
    }

    // ---------------------------------------------------------------------
    // helpers
    // ---------------------------------------------------------------------

    private static Task<WatchConnection> ConnectAsync(string address, long? maxShardHint)
        => WatchConnection.ConnectAsync(
            address,
            new WatchRequest(),
            new WatchOptions
            {
                ConnectionTimeout = TimeSpan.FromSeconds(5),
                MaxShardHint = maxShardHint,
            },
            CancellationToken.None);

    /// <summary>
    /// A "host:port" nothing is listening on: bound to claim the port, then released, so the dial
    /// is refused immediately instead of waiting out a TCP timeout.
    /// </summary>
    private static string ReservedAddressThatRefusesConnections()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return $"127.0.0.1:{port}";
    }

    /// <summary>The shard a received watch request subscribes to, or null for the unsharded probe.</summary>
    private static long? ShardOf(byte[] requestBody)
        => WireCodec.Deserialize<WatchRequest>(requestBody).ShardId;

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
