using System.Reflection;
using Celeriant.Client.Errors;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Moq;

namespace Celeriant.Client.Tests;

/// <summary>
/// Adversarial-review probes for the leader-default read routing change.
/// Temporary: passing tests are removed after review unless they pin a real gap.
/// </summary>
public class AdvRevRoutingTests
{
    private static readonly AggregateKey TestKey = new(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());

    private static CeleriantPoolOptions MakeOptions(
        string address,
        IReadOnlyList<string>? seeds = null,
        bool routeReadsToFollowers = false)
        => new()
        {
            Address = address,
            SeedAddresses = seeds,
            RouteReadsToFollowers = routeReadsToFollowers,
        };

    private static ReadRequest MakeReadRequest() => new()
    {
        AggregateKey = TestKey,
        Filters = ReadFilters.From(1),
    };

    private static ClientResponse.Read SuccessReadResponse()
        => new(new ReadResponse { EventBatches = [] });

    private static Mock<INodeConnectionPool> MockPool(string address)
    {
        var mock = new Mock<INodeConnectionPool>();
        mock.Setup(p => p.Address).Returns(address);
        mock.Setup(p => p.DisposeAsync()).Returns(ValueTask.CompletedTask);
        return mock;
    }

    private static Mock<INodeConnectionPool> MockPoolThatSucceeds(string address)
    {
        var mock = MockPool(address);
        mock.Setup(p => p.ExecuteRequestAsync(It.IsAny<ClientRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(SuccessReadResponse());
        return mock;
    }

    private static Mock<INodeConnectionPool> MockPoolThatThrows(string address, Exception exception)
    {
        var mock = MockPool(address);
        mock.Setup(p => p.ExecuteRequestAsync(It.IsAny<ClientRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(exception);
        return mock;
    }

    private static CeleriantPool CreatePool(
        CeleriantPoolOptions options,
        Dictionary<string, Mock<INodeConnectionPool>> mocks)
        => new(options, (addr, _, _) =>
        {
            if (mocks.TryGetValue(addr, out var mock))
                return mock.Object;
            var newMock = MockPool(addr);
            mocks[addr] = newMock;
            return newMock.Object;
        });

    // -----------------------------------------------------------------------
    // Angle 3: dial timeout in OPT-IN mode must also fail over (arm not gated
    // on default mode), and request timeout / busy in opt-in must skip.
    // -----------------------------------------------------------------------

    [Fact]
    public async Task AdvRev_OptIn_DialTimeout_FailsOverToOtherFollower()
    {
        var busted = MockPoolThatThrows("b:1", new ConnectionTimeoutException("dial timed out"));
        var healthy = MockPoolThatSucceeds("c:1");
        var mocks = new Dictionary<string, Mock<INodeConnectionPool>>
        {
            ["b:1"] = busted,
            ["c:1"] = healthy,
        };
        var options = MakeOptions("p:1", seeds: ["b:1", "c:1"], routeReadsToFollowers: true);

        await using var pool = CreatePool(options, mocks);
        // Rotation alternates which follower is first; every read must succeed either way.
        for (int i = 0; i < 4; i++)
            Assert.NotNull(await pool.ReadAsync(MakeReadRequest()));

        // The timing-out follower led the list at least once, so failover really ran.
        busted.Verify(p => p.ExecuteRequestAsync(It.IsAny<ClientRequest>(), It.IsAny<CancellationToken>()),
            Times.AtLeastOnce);
    }

    [Fact]
    public async Task AdvRev_OptIn_RequestTimeout_SkipsToNextFollower()
    {
        var timingOut = MockPoolThatThrows("b:1", new CeleriantTimeoutException("request timed out"));
        var healthy = MockPoolThatSucceeds("c:1");
        var mocks = new Dictionary<string, Mock<INodeConnectionPool>>
        {
            ["b:1"] = timingOut,
            ["c:1"] = healthy,
        };
        var options = MakeOptions("p:1", seeds: ["b:1", "c:1"], routeReadsToFollowers: true);

        await using var pool = CreatePool(options, mocks);
        for (int i = 0; i < 4; i++)
            Assert.NotNull(await pool.ReadAsync(MakeReadRequest()));
        timingOut.Verify(p => p.ExecuteRequestAsync(It.IsAny<ClientRequest>(), It.IsAny<CancellationToken>()),
            Times.AtLeastOnce);
    }

    [Fact]
    public async Task AdvRev_OptIn_ServerBusy_SkipsToNextFollower()
    {
        var busy = MockPoolThatThrows("b:1", new ServerBusyException(new ErrorResponse()));
        var healthy = MockPoolThatSucceeds("c:1");
        var mocks = new Dictionary<string, Mock<INodeConnectionPool>>
        {
            ["b:1"] = busy,
            ["c:1"] = healthy,
        };
        var options = MakeOptions("p:1", seeds: ["b:1", "c:1"], routeReadsToFollowers: true);

        await using var pool = CreatePool(options, mocks);
        for (int i = 0; i < 4; i++)
            Assert.NotNull(await pool.ReadAsync(MakeReadRequest()));
        busy.Verify(p => p.ExecuteRequestAsync(It.IsAny<ClientRequest>(), It.IsAny<CancellationToken>()),
            Times.AtLeastOnce);
    }

    // -----------------------------------------------------------------------
    // Angle 4: race: a read between "_leaderAddress = ex.LeaderAddress" and
    // "GetOrCreateNodePool(...)" in ExecuteLeaderOperationAsync sees a leader
    // with no registered pool. GetReadNodeAddresses now prepends that address
    // ([leader, ..all]) and ExecuteOnAnyNodeAsync indexes _nodePools[addr]
    // directly -> KeyNotFoundException. Deterministic repro: block the pool
    // factory for the discovered leader while a concurrent read runs.
    // -----------------------------------------------------------------------

    [Fact]
    public async Task AdvRev_ReadDuringLeaderDiscovery_MustNotThrowKeyNotFound()
    {
        var factoryEntered = new SemaphoreSlim(0);
        var factoryRelease = new SemaphoreSlim(0);

        var oldLeader = MockPoolThatThrows("a:1",
            new NotLeaderException(new ErrorResponse(), "new:1"));
        var seed = MockPoolThatSucceeds("b:1");
        var mocks = new Dictionary<string, Mock<INodeConnectionPool>>
        {
            ["a:1"] = oldLeader,
            ["b:1"] = seed,
        };

        var options = MakeOptions("a:1", seeds: ["b:1"]);
        await using var pool = new CeleriantPool(options, (addr, _, _) =>
        {
            if (addr == "new:1")
            {
                factoryEntered.Release();
                factoryRelease.Wait(TimeSpan.FromSeconds(10));
                var newLeader = MockPool("new:1");
                newLeader.Setup(p => p.ExecuteRequestAsync(It.IsAny<ClientRequest>(), It.IsAny<CancellationToken>()))
                    .ReturnsAsync(new ClientResponse.Write(new WriteResponse()));
                return newLeader.Object;
            }
            return mocks[addr].Object;
        });

        // Write hits NotLeader -> sets _leaderAddress = "new:1", then blocks in the factory.
        var writeTask = Task.Run(() => pool.WriteAsync(new WriteRequest
        {
            ClientId = Guid.NewGuid(),
            Writes = new Dictionary<AggregateKey, SingleAggregateWrite>
            {
                [TestKey] = new() { AllowCreate = true, Events = [new AggregateEvent
                {
                    EventTypeMajor = 1, EventTypeMinor = 0, EventValue = [1], ClientSeq = 1,
                }] }
            }
        }));

        Assert.True(await factoryEntered.WaitAsync(TimeSpan.FromSeconds(10)));

        // Concurrent read in the discovery window: must not surface KeyNotFoundException.
        var ex = await Record.ExceptionAsync(() => pool.ReadAsync(MakeReadRequest()));
        factoryRelease.Release();
        await writeTask;

        Assert.IsNotType<KeyNotFoundException>(ex);
    }

    // -----------------------------------------------------------------------
    // Angle 4: rotation validity at counter wraparound and no dup/miss per call.
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // Amendment 2: the execution loop (not just the address list) must reach
    // the tail leader, and exhaustion must not mask the leader's real error.
    // Pins the divergence from Rust read_route!, which turns a busy leader at
    // list exhaustion into ConnectionFailed("all nodes unreachable").
    // -----------------------------------------------------------------------

    [Fact]
    public async Task AdvRev_OptIn_AllFollowersDown_LeaderServesRead()
    {
        var deadB = MockPoolThatThrows("b:1", new ConnectionFailedException("refused"));
        var deadC = MockPoolThatThrows("c:1", new ConnectionFailedException("refused"));
        var leader = MockPoolThatSucceeds("p:1");
        var mocks = new Dictionary<string, Mock<INodeConnectionPool>>
        {
            ["b:1"] = deadB,
            ["c:1"] = deadC,
            ["p:1"] = leader,
        };
        var options = MakeOptions("p:1", seeds: ["b:1", "c:1"], routeReadsToFollowers: true);

        await using var pool = CreatePool(options, mocks);
        Assert.NotNull(await pool.ReadAsync(MakeReadRequest()));
        leader.Verify(p => p.ExecuteRequestAsync(It.IsAny<ClientRequest>(), It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task AdvRev_OptIn_AllCandidatesBusy_PropagatesServerBusyNotUnreachable()
    {
        var busyB = MockPoolThatThrows("b:1", new ServerBusyException(new ErrorResponse()));
        var busyC = MockPoolThatThrows("c:1", new ServerBusyException(new ErrorResponse()));
        var busyLeader = MockPoolThatThrows("p:1", new ServerBusyException(new ErrorResponse()));
        var mocks = new Dictionary<string, Mock<INodeConnectionPool>>
        {
            ["b:1"] = busyB,
            ["c:1"] = busyC,
            ["p:1"] = busyLeader,
        };
        var options = MakeOptions("p:1", seeds: ["b:1", "c:1"], routeReadsToFollowers: true);

        await using var pool = CreatePool(options, mocks);
        // The whole list is busy, not unreachable: the caller must see ServerBusy.
        await Assert.ThrowsAsync<ServerBusyException>(() => pool.ReadAsync(MakeReadRequest()));
    }

    [Fact]
    public async Task AdvRev_OptIn_Rotation_ValidAtIntMaxWraparound()
    {
        var options = MakeOptions("p:1", seeds: ["b:1", "c:1", "d:1"], routeReadsToFollowers: true);
        await using var pool = new CeleriantPool(options, (addr, _, _) => MockPool(addr).Object);

        var field = typeof(CeleriantPool).GetField("_roundRobinIndex",
            BindingFlags.NonPublic | BindingFlags.Instance)!;
        field.SetValue(pool, int.MaxValue);

        // Amendment 2 (leader-last): list is followers then the leader at the tail.
        var followers = new[] { "b:1", "c:1", "d:1" };
        for (int i = 0; i < 6; i++)
        {
            var addrs = pool.GetReadNodeAddresses();
            Assert.Equal(4, addrs.Length);
            Assert.Equal(4, addrs.Distinct().Count());
            Assert.Equal("p:1", addrs[^1]);
            foreach (var a in addrs[..^1])
                Assert.Contains(a, followers);
        }
    }
}
