using System.Net;
using System.Net.Sockets;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;

namespace Celeriant.Client.Tests;

/// <summary>
/// The leader-routing contract, tested through the public API only: <see cref="CeleriantPool"/>,
/// <see cref="CeleriantPoolOptions"/> and the exception types. Every node is a real socket, so
/// "asked once" means one request frame reached that process and "refused" means the kernel
/// refused the connect.
///
/// <para>
/// Local refusals are in PoolLocalConditionContractTests and post-send losses in
/// PostSendContractTests.
/// </para>
/// </summary>
public class LeaderRoutingContractTests
{
    // =====================================================================
    // The walk over pre-send failures and NotLeader hints
    // =====================================================================

    /// <summary>
    /// The primary refuses the connect and the only seed hints straight back at it. The walk has
    /// nowhere left to go, so it must fail with an error that names the address that failed, in
    /// its message or down the InnerException chain, so the caller can tell which node to look at.
    /// </summary>
    [Fact]
    public async Task ARefusedPrimaryHintedBackAt_FailsWithAnErrorNamingThatPrimary()
    {
        var (primary, _) = LeaderRoutingFakes.DeadAddress();
        int seedRequests = 0;

        await using var seed = FakeCeleriantServer.Start((session, _, _) =>
        {
            Interlocked.Increment(ref seedRequests);
            return LeaderRoutingFakes.NotLeaderAsync(session, primary);
        });

        await using var pool = new CeleriantPool(LeaderRoutingFakes.Options(primary, seed.Address));

        var failure = await LeaderRoutingFakes.FailureAsync(
            () => pool.WriteAsync(LeaderRoutingFakes.NewWrite()),
            "a write whose primary refuses and whose only seed hints back at it");

        Assert.True(
            seedRequests == 1,
            $"the seed answered {seedRequests} requests. It answered NotLeader once, naming a node "
            + "that is down: re-asking it cannot produce a different answer, and every extra ask "
            + "spends the attempt budget that an untried node needs");
        Assert.True(
            LeaderRoutingFakes.Names(failure, primary),
            $"the walk failed with \"{failure!.Message}\" (inner: {failure.InnerException?.GetType().Name ?? "none"}), "
            + $"which never mentions {primary}, the address that refused. The caller is left with a "
            + "count of nodes and no way to find the broken one");
    }

    /// <summary>
    /// Refusing the connect trips that node's breaker, a local, time-boxed condition rather than a
    /// verdict about the node. Once it lapses and something is listening on that address again,
    /// the next write must reach it with one request and no extra dials.
    /// </summary>
    [Fact]
    public async Task ARefusedPrimary_ServesAgainOnceItsBreakerLapsesAndItIsBackOnItsPort()
    {
        var (primary, port) = LeaderRoutingFakes.DeadAddress();
        await using var pool = new CeleriantPool(LeaderRoutingFakes.Options(primary, TimeSpan.FromSeconds(5)));

        await LeaderRoutingFakes.FailureAsync(
            () => pool.WriteAsync(LeaderRoutingFakes.NewWrite()),
            "a write to a primary that refuses connections");

        await Task.Delay(LeaderRoutingFakes.PastTheBreakerWindow);

        int requests = 0;
        await using var revived = FakeCeleriantServer.Start(
            (session, _, _) =>
            {
                Interlocked.Increment(ref requests);
                return LeaderRoutingFakes.WriteOkAsync(session);
            },
            port);

        var response = await pool.WriteAsync(LeaderRoutingFakes.NewWrite());

        Assert.Equal(1, response.MaxAggregateVersion);
        Assert.True(
            requests == 1,
            $"the revived node was sent {requests} requests for one write: a node coming back is "
            + "not a reason to send the write more than once");
    }

    /// <summary>
    /// A node that answers NotLeader naming itself is describing a state it cannot be in.
    /// Following that hint re-asks the node that just refused the work, and the only thing it can
    /// cost is the attempt budget, so the hint must be refused: exactly one request.
    /// </summary>
    [Fact]
    public async Task ANodeHintingAtItself_IsAskedExactlyOnce()
    {
        int requests = 0;
        FakeCeleriantServer? node = null;

        node = FakeCeleriantServer.Start((session, _, _) =>
        {
            Interlocked.Increment(ref requests);
            return LeaderRoutingFakes.NotLeaderAsync(session, node!.Address);
        });

        await using (node)
        {
            await using var pool = new CeleriantPool(LeaderRoutingFakes.Options(node.Address));

            await LeaderRoutingFakes.FailureAsync(
                () => pool.WriteAsync(LeaderRoutingFakes.NewWrite()),
                "a write to the only node, which hints at itself");

            Assert.True(
                requests == 1,
                $"the self-hinting node was asked {requests} times. Its first answer was \"the "
                + "leader is me, and it is not me\": asking again is the same question to the same "
                + "process");
        }
    }

    /// <summary>
    /// Two nodes each naming the other is the ping-pong case: following hints blindly is an
    /// infinite walk bounded only by an attempt budget. A node that has already answered NotLeader
    /// in this walk must not be asked again, so both are asked once and the write fails promptly
    /// rather than burning the budget on two addresses.
    /// </summary>
    [Fact]
    public async Task TwoNodesHintingAtEachOther_AreEachAskedOnceAndTheWalkEnds()
    {
        int aRequests = 0, bRequests = 0;
        FakeCeleriantServer? a = null, b = null;

        a = FakeCeleriantServer.Start((session, _, _) =>
        {
            Interlocked.Increment(ref aRequests);
            return LeaderRoutingFakes.NotLeaderAsync(session, b!.Address);
        });
        b = FakeCeleriantServer.Start((session, _, _) =>
        {
            Interlocked.Increment(ref bRequests);
            return LeaderRoutingFakes.NotLeaderAsync(session, a!.Address);
        });

        await using (a)
        await using (b)
        {
            await using var pool = new CeleriantPool(LeaderRoutingFakes.Options(a.Address, b.Address));

            await LeaderRoutingFakes.FailureAsync(
                () => pool.WriteAsync(LeaderRoutingFakes.NewWrite()),
                "a write between two nodes that each name the other as leader",
                TimeSpan.FromSeconds(5));

            Assert.True(
                aRequests == 1 && bRequests == 1,
                $"the two nodes were asked {aRequests} and {bRequests} times. Each answered "
                + "NotLeader once; every ask after that is the same round trip to a node whose "
                + "answer is already known");
        }
    }

    /// <summary>
    /// The stale-hint starvation case. Two seeds point at a primary that is down, and a
    /// third seed is the actual leader. Following the dead hint must not consume the attempts the
    /// untried seed needs: the write has to land.
    /// </summary>
    [Fact]
    public async Task AStaleHintAtADeadPrimary_DoesNotStarveTheUntriedSeedThatIsLeader()
    {
        var (primary, _) = LeaderRoutingFakes.DeadAddress();
        int leaderRequests = 0;

        await using var s1 = FakeCeleriantServer.Start((session, _, _) => LeaderRoutingFakes.NotLeaderAsync(session, primary));
        await using var s2 = FakeCeleriantServer.Start((session, _, _) => LeaderRoutingFakes.NotLeaderAsync(session, primary));
        await using var s3 = FakeCeleriantServer.Start((session, _, _) =>
        {
            Interlocked.Increment(ref leaderRequests);
            return LeaderRoutingFakes.WriteOkAsync(session);
        });

        await using var pool = new CeleriantPool(
            LeaderRoutingFakes.Options(primary, s1.Address, s2.Address, s3.Address));

        var response = await LeaderRoutingFakes.WithinBudgetAsync(
            pool.WriteAsync(LeaderRoutingFakes.NewWrite()),
            "a write with a dead primary, two seeds hinting at it, and one seed that is the leader");

        Assert.Equal(1, response.MaxAggregateVersion);
        Assert.True(
            leaderRequests == 1,
            $"the leader received {leaderRequests} copies of a write that was accepted once");
    }

    // =====================================================================
    // What an exhausted walk tells the caller
    // =====================================================================

    /// <summary>
    /// Every node refuses the connect, so every attempt produced a concrete error. The exception
    /// handed back has to carry at least one of them: an address in the message, or the last
    /// failure as InnerException. A bare count of nodes tried throws away everything the walk learned.
    /// </summary>
    [Fact]
    public async Task AnExhaustedWalk_NamesAnAddressItTriedOrCarriesTheLastError()
    {
        var (primary, _) = LeaderRoutingFakes.DeadAddress();
        var (seed, _) = LeaderRoutingFakes.DeadAddress();

        await using var pool = new CeleriantPool(LeaderRoutingFakes.Options(primary, seed));

        var failure = await LeaderRoutingFakes.FailureAsync(
            () => pool.WriteAsync(LeaderRoutingFakes.NewWrite()),
            "a write with every node refusing");

        bool namesANode = LeaderRoutingFakes.Names(failure, primary) || LeaderRoutingFakes.Names(failure, seed);
        Assert.True(
            namesANode || failure!.InnerException is not null,
            $"both nodes refused the connect, and the caller was handed \"{failure!.Message}\" with "
            + "no InnerException: neither the address that failed nor the error it failed with "
            + "survived the walk");
    }

    // =====================================================================
    // The cache that makes the walk worth doing
    // =====================================================================

    /// <summary>
    /// A redirect is only paid for once. After the first write follows a NotLeader hint, the leader
    /// is cached, so the second write goes straight there: across two writes the follower sees one
    /// request, not two.
    /// </summary>
    [Fact]
    public async Task AFollowedRedirect_IsCached_SoTheNextWriteSkipsTheFollower()
    {
        int followerRequests = 0, leaderRequests = 0;

        await using var leader = FakeCeleriantServer.Start((session, _, _) =>
        {
            Interlocked.Increment(ref leaderRequests);
            return LeaderRoutingFakes.WriteOkAsync(session);
        });
        await using var follower = FakeCeleriantServer.Start((session, _, _) =>
        {
            Interlocked.Increment(ref followerRequests);
            return LeaderRoutingFakes.NotLeaderAsync(session, leader.Address);
        });

        await using var pool = new CeleriantPool(
            LeaderRoutingFakes.Options(follower.Address, leader.Address));

        await LeaderRoutingFakes.WithinBudgetAsync(
            pool.WriteAsync(LeaderRoutingFakes.NewWrite()), "the redirected write");
        await LeaderRoutingFakes.WithinBudgetAsync(
            pool.WriteAsync(LeaderRoutingFakes.NewWrite()), "the write after the redirect");

        Assert.True(
            followerRequests == 1,
            $"the follower was asked {followerRequests} times across two writes: the leader the "
            + "first redirect discovered was not remembered, so every write pays a wasted round "
            + "trip to a node that cannot serve it");
        Assert.Equal(2, leaderRequests);
    }
}

/// <summary>
/// The scripted cluster the routing tests drive: one write, the two answers a node can give, and
/// an address that nothing is listening on.
/// </summary>
internal static class LeaderRoutingFakes
{
    /// <summary>Longer than the connection pool's breaker window, so a refused node is usable again.</summary>
    public static readonly TimeSpan PastTheBreakerWindow = TimeSpan.FromMilliseconds(2100);

    private static readonly TimeSpan HangBudget = TimeSpan.FromSeconds(8);

    /// <summary>
    /// Set by the test rather than filled in by the transport, so a fake can echo it without
    /// decoding a body that may have been dictionary-compressed.
    /// </summary>
    private static readonly Guid CorrelationId = Guid.Parse("00000000-0000-0000-0000-0000000000d4");

    private static readonly Guid ClientId = Guid.Parse("00000000-0000-0000-0000-0000000000c0");

    /// <summary>
    /// Short timeouts throughout: a routing bug must show up as a failed assertion inside the
    /// budget, never as a test run that stops producing output.
    /// </summary>
    public static CeleriantPoolOptions Options(string address, params string[] seeds)
        => Options(address, TimeSpan.FromSeconds(1), seeds);

    public static CeleriantPoolOptions Options(string address, TimeSpan connectionTimeout, params string[] seeds) => new()
    {
        Address = address,
        SeedAddresses = seeds.Length == 0 ? null : seeds,
        MaxConnections = 1,
        ConnectionTimeout = connectionTimeout,
        RequestTimeout = TimeSpan.FromSeconds(2),
    };

    public static WriteRequest NewWrite() => new()
    {
        CorrelationId = CorrelationId,
        ClientId = ClientId,
        Writes = new Dictionary<AggregateKey, SingleAggregateWrite>
        {
            [FakeServerProtocol.NewKey()] = new SingleAggregateWrite
            {
                Events = [new AggregateEvent
                {
                    EventTimestamp = DateTimeOffset.UtcNow,
                    EventTypeMajor = 1,
                    EventValue = "contract"u8.ToArray(),
                }],
                AllowCreate = true,
            },
        },
    };

    public static Task WriteOkAsync(FakeServerSession session)
        => session.SendFrameAsync(
            MessageTypes.Responses.Write,
            WireCodec.Serialize(new WriteResponse { CorrelationId = CorrelationId, MaxAggregateVersion = 1 }));

    public static Task NotLeaderAsync(FakeServerSession session, string leaderAddress)
        => session.SendFrameAsync(
            MessageTypes.Responses.GenericError,
            FakeServerProtocol.ErrorFrame(
                ErrorResponse.WriteNotLeader,
                $"{{\"leader_address\":\"{leaderAddress}\"}}",
                CorrelationId));

    /// <summary>
    /// A loopback address nothing is listening on: the port is bound to learn it is free, then
    /// released, so a connect to it is refused rather than timing out. The port is returned so a
    /// test can bring a node up at that same address later.
    /// </summary>
    public static (string Address, int Port) DeadAddress()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return ($"127.0.0.1:{port}", port);
    }

    /// <summary>True when <paramref name="address"/> appears anywhere in the exception chain.</summary>
    public static bool Names(Exception? failure, string address)
    {
        for (var e = failure; e is not null; e = e.InnerException)
        {
            if (e.Message.Contains(address, StringComparison.Ordinal))
                return true;
        }
        return false;
    }

    /// <summary>Run an operation that must fail, under a hang budget, and return what it threw.</summary>
    public static async Task<Exception?> FailureAsync(Func<Task> operation, string whatWouldHang, TimeSpan? budget = null)
    {
        var running = Task.Run(operation);
        await AwaitCompletionAsync(running, whatWouldHang, budget);

        var failure = await Record.ExceptionAsync(() => running);
        Assert.True(failure is not null, $"{whatWouldHang} was expected to fail and did not");
        return failure;
    }

    /// <summary>Await an operation that must succeed, under the hang budget.</summary>
    public static async Task<T> WithinBudgetAsync<T>(Task<T> operation, string whatWouldHang)
    {
        await AwaitCompletionAsync(operation, whatWouldHang, budget: null);
        return await operation;
    }

    private static async Task AwaitCompletionAsync(Task task, string whatWouldHang, TimeSpan? budget)
    {
        var limit = budget ?? HangBudget;
        var finished = await Task.WhenAny(task, Task.Delay(limit));
        Assert.True(
            ReferenceEquals(finished, task),
            $"{whatWouldHang} was still pending after {limit.TotalSeconds:0.#}s");
    }
}
