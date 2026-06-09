using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Moq;

namespace Celeriant.Client.Tests;

/// <summary>
/// Unit tests for CeleriantPool routing, failover, and leader discovery logic.
/// Uses mock INodeConnectionPool instances — no real TCP connections.
/// </summary>
public class CeleriantPoolTests
{
    private static readonly AggregateKey TestKey = new(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());
    private static readonly Guid TestClientId = Guid.NewGuid();

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static CeleriantPoolOptions MakeOptions(
        string address = "leader:10000",
        IReadOnlyList<string>? seeds = null,
        bool routeReadsToFollowers = false)
        => new()
        {
            Address = address,
            SeedAddresses = seeds,
            RouteReadsToFollowers = routeReadsToFollowers,
        };

    private static WriteRequest MakeWriteRequest() => new()
    {
        ClientId = Guid.NewGuid(),
        Writes = new Dictionary<AggregateKey, SingleAggregateWrite>
        {
            [TestKey] = new() { AllowCreate = true, Events = [MakeEvent()] }
        }
    };

    private static ReadRequest MakeReadRequest() => new()
    {
        AggregateKey = TestKey,
        Filters = ReadFilters.From(1),
    };

    private static AggregateEvent MakeEvent() => new()
    {
        EventTypeMajor = 1,
        EventTypeMinor = 0,
        EventValue = [1, 2, 3],
        ClientSeq = 1,
    };

    private static ClientResponse.Write SuccessWriteResponse()
        => new(new WriteResponse());

    private static ClientResponse.Read SuccessReadResponse()
        => new(new ReadResponse { EventBatches = [] });

    private static Mock<INodeConnectionPool> MockPool(string address)
    {
        var mock = new Mock<INodeConnectionPool>();
        mock.Setup(p => p.Address).Returns(address);
        mock.Setup(p => p.DisposeAsync()).Returns(ValueTask.CompletedTask);
        return mock;
    }

    private static Mock<INodeConnectionPool> MockPoolThatSucceeds(string address, ClientResponse response)
    {
        var mock = MockPool(address);
        mock.Setup(p => p.ExecuteRequestAsync(
                It.IsAny<ClientRequest>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(response);
        return mock;
    }

    private static Mock<INodeConnectionPool> MockPoolThatThrows(string address, Exception exception)
    {
        var mock = MockPool(address);
        mock.Setup(p => p.ExecuteRequestAsync(
                It.IsAny<ClientRequest>(),
                It.IsAny<CancellationToken>()))
            .ThrowsAsync(exception);
        return mock;
    }

    private static Mock<INodeConnectionPool> MockPoolConnectionFailed(string address)
        => MockPoolThatThrows(address, new ConnectionFailedException("unreachable"));

    private static CeleriantPool CreatePool(
        CeleriantPoolOptions options,
        Dictionary<string, Mock<INodeConnectionPool>> mocks)
    {
        return new CeleriantPool(options, (addr, _, _) =>
        {
            if (mocks.TryGetValue(addr, out var mock))
                return mock.Object;
            // Auto-create a mock for dynamically discovered nodes
            var newMock = MockPool(addr);
            mocks[addr] = newMock;
            return newMock.Object;
        });
    }

    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    [Fact]
    public void Constructor_NullOptions_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new CeleriantPool(null!));
    }

    [Fact]
    public void Constructor_CreatesPoolForPrimaryAddress()
    {
        var options = MakeOptions("leader:10000");
        var mocks = new Dictionary<string, Mock<INodeConnectionPool>>();
        var pool = CreatePool(options, mocks);

        Assert.Contains("leader:10000", mocks.Keys);
    }

    [Fact]
    public void Constructor_CreatesPoolsForSeedAddresses()
    {
        var options = MakeOptions("leader:10000", seeds: ["follower1:10000", "follower2:10000"]);
        var mocks = new Dictionary<string, Mock<INodeConnectionPool>>();
        var pool = CreatePool(options, mocks);

        Assert.Contains("leader:10000", mocks.Keys);
        Assert.Contains("follower1:10000", mocks.Keys);
        Assert.Contains("follower2:10000", mocks.Keys);
    }

    // -----------------------------------------------------------------------
    // Write — leader routing
    // -----------------------------------------------------------------------

    [Fact]
    public async Task WriteAsync_RoutesToLeader_ReturnsSuccess()
    {
        var leaderMock = MockPoolThatSucceeds("leader:10000", SuccessWriteResponse());
        var mocks = new Dictionary<string, Mock<INodeConnectionPool>>
        {
            ["leader:10000"] = leaderMock,
        };

        await using var pool = CreatePool(MakeOptions(), mocks);
        var result = await pool.WriteAsync(MakeWriteRequest());

        Assert.NotNull(result);
        leaderMock.Verify(p => p.ExecuteRequestAsync(
            It.IsAny<ClientRequest>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    // -----------------------------------------------------------------------
    // Write — NotLeaderException with leader address triggers failover
    // -----------------------------------------------------------------------

    [Fact]
    public async Task WriteAsync_NotLeaderWithAddress_FailsOverToNewLeader()
    {
        var error = new ErrorResponse { ErrorCode = ErrorResponse.WriteNotLeader, ErrorMessage = "{}" };
        var notLeaderEx = new NotLeaderException(error, "new-leader:10000");

        var oldLeaderMock = MockPool("leader:10000");
        oldLeaderMock.Setup(p => p.ExecuteRequestAsync(
                It.IsAny<ClientRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(notLeaderEx);

        var newLeaderMock = MockPoolThatSucceeds("new-leader:10000", SuccessWriteResponse());

        var mocks = new Dictionary<string, Mock<INodeConnectionPool>>
        {
            ["leader:10000"] = oldLeaderMock,
            ["new-leader:10000"] = newLeaderMock,
        };

        await using var pool = CreatePool(MakeOptions(), mocks);
        var result = await pool.WriteAsync(MakeWriteRequest());

        Assert.NotNull(result);
        newLeaderMock.Verify(p => p.ExecuteRequestAsync(
            It.IsAny<ClientRequest>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    // -----------------------------------------------------------------------
    // Write — NotLeaderException without address tries next node
    // -----------------------------------------------------------------------

    [Fact]
    public async Task WriteAsync_NotLeaderNoAddress_TriesNextKnownNode()
    {
        var error = new ErrorResponse { ErrorCode = ErrorResponse.WriteNotLeader };
        var notLeaderEx = new NotLeaderException(error, leaderAddress: null);

        var leaderMock = MockPool("leader:10000");
        leaderMock.Setup(p => p.ExecuteRequestAsync(
                It.IsAny<ClientRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(notLeaderEx);

        var followerMock = MockPoolThatSucceeds("follower:10000", SuccessWriteResponse());

        var mocks = new Dictionary<string, Mock<INodeConnectionPool>>
        {
            ["leader:10000"] = leaderMock,
            ["follower:10000"] = followerMock,
        };
        var options = MakeOptions("leader:10000", seeds: ["follower:10000"]);

        await using var pool = CreatePool(options, mocks);
        var result = await pool.WriteAsync(MakeWriteRequest());

        Assert.NotNull(result);
    }

    // -----------------------------------------------------------------------
    // Write — ConnectionFailedException tries next node
    // -----------------------------------------------------------------------

    [Fact]
    public async Task WriteAsync_ConnectionFailed_TriesNextNode()
    {
        var leaderMock = MockPoolConnectionFailed("leader:10000");
        var followerMock = MockPoolThatSucceeds("follower:10000", SuccessWriteResponse());

        var mocks = new Dictionary<string, Mock<INodeConnectionPool>>
        {
            ["leader:10000"] = leaderMock,
            ["follower:10000"] = followerMock,
        };
        var options = MakeOptions("leader:10000", seeds: ["follower:10000"]);

        await using var pool = CreatePool(options, mocks);
        var result = await pool.WriteAsync(MakeWriteRequest());

        Assert.NotNull(result);
    }

    // -----------------------------------------------------------------------
    // Write — all nodes unreachable
    // -----------------------------------------------------------------------

    [Fact]
    public async Task WriteAsync_AllNodesUnreachable_ThrowsConnectionFailed()
    {
        var leaderMock = MockPoolConnectionFailed("leader:10000");
        var followerMock = MockPoolConnectionFailed("follower:10000");

        var mocks = new Dictionary<string, Mock<INodeConnectionPool>>
        {
            ["leader:10000"] = leaderMock,
            ["follower:10000"] = followerMock,
        };
        var options = MakeOptions("leader:10000", seeds: ["follower:10000"]);

        await using var pool = CreatePool(options, mocks);
        await Assert.ThrowsAsync<ConnectionFailedException>(
            () => pool.WriteAsync(MakeWriteRequest()));
    }

    // -----------------------------------------------------------------------
    // Write — NotLeaderException with no address and no untried nodes
    // -----------------------------------------------------------------------

    [Fact]
    public async Task WriteAsync_NotLeaderNoAddressNoUntried_ThrowsNotLeader()
    {
        var error = new ErrorResponse { ErrorCode = ErrorResponse.WriteNotLeader };
        var notLeaderEx = new NotLeaderException(error, leaderAddress: null);

        var leaderMock = MockPool("leader:10000");
        leaderMock.Setup(p => p.ExecuteRequestAsync(
                It.IsAny<ClientRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(notLeaderEx);

        var mocks = new Dictionary<string, Mock<INodeConnectionPool>>
        {
            ["leader:10000"] = leaderMock,
        };

        await using var pool = CreatePool(MakeOptions(), mocks);
        await Assert.ThrowsAsync<NotLeaderException>(
            () => pool.WriteAsync(MakeWriteRequest()));
    }

    // -----------------------------------------------------------------------
    // Write — leader discovery creates new node pool
    // -----------------------------------------------------------------------

    [Fact]
    public async Task WriteAsync_LeaderDiscovery_CreatesNewNodePool()
    {
        var error = new ErrorResponse { ErrorCode = ErrorResponse.WriteNotLeader, ErrorMessage = "{}" };
        var notLeaderEx = new NotLeaderException(error, "discovered-leader:10000");

        var oldLeaderMock = MockPool("leader:10000");
        oldLeaderMock.Setup(p => p.ExecuteRequestAsync(
                It.IsAny<ClientRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(notLeaderEx);

        var mocks = new Dictionary<string, Mock<INodeConnectionPool>>
        {
            ["leader:10000"] = oldLeaderMock,
        };

        // Pre-register the discovered leader mock so the factory returns it
        var discoveredMock = MockPoolThatSucceeds("discovered-leader:10000", SuccessWriteResponse());
        mocks["discovered-leader:10000"] = discoveredMock;

        await using var pool = CreatePool(MakeOptions(), mocks);
        await pool.WriteAsync(MakeWriteRequest());

        // Verify the discovered leader was used
        discoveredMock.Verify(p => p.ExecuteRequestAsync(
            It.IsAny<ClientRequest>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    // -----------------------------------------------------------------------
    // Write — generic error propagated
    // -----------------------------------------------------------------------

    [Fact]
    public async Task WriteAsync_GenericError_ThrowsCeleriantErrorException()
    {
        var errorResponse = new ErrorResponse { ErrorCode = 7001, ErrorMessage = "aggregate not found" };
        var leaderMock = MockPoolThatThrows("leader:10000", new CeleriantErrorException(errorResponse));

        var mocks = new Dictionary<string, Mock<INodeConnectionPool>>
        {
            ["leader:10000"] = leaderMock,
        };

        await using var pool = CreatePool(MakeOptions(), mocks);
        var ex = await Assert.ThrowsAsync<CeleriantErrorException>(
            () => pool.WriteAsync(MakeWriteRequest()));
        Assert.Equal(7001u, ex.Error.ErrorCode);
    }

    // -----------------------------------------------------------------------
    // Read — round-robin distribution
    // -----------------------------------------------------------------------

    [Fact]
    public async Task ReadAsync_SingleNode_RoutesToThatNode()
    {
        var leaderMock = MockPoolThatSucceeds("leader:10000", SuccessReadResponse());

        var mocks = new Dictionary<string, Mock<INodeConnectionPool>>
        {
            ["leader:10000"] = leaderMock,
        };

        await using var pool = CreatePool(MakeOptions(), mocks);
        var result = await pool.ReadAsync(MakeReadRequest());

        Assert.NotNull(result);
    }

    [Fact]
    public async Task ReadAsync_ConnectionFailed_TriesNextNode()
    {
        var failingMock = MockPoolConnectionFailed("node1:10000");
        var successMock = MockPoolThatSucceeds("node2:10000", SuccessReadResponse());

        var mocks = new Dictionary<string, Mock<INodeConnectionPool>>
        {
            ["node1:10000"] = failingMock,
            ["node2:10000"] = successMock,
        };
        var options = MakeOptions("node1:10000", seeds: ["node2:10000"]);

        await using var pool = CreatePool(options, mocks);
        var result = await pool.ReadAsync(MakeReadRequest());

        Assert.NotNull(result);
    }

    [Fact]
    public async Task ReadAsync_AllNodesUnreachable_ThrowsConnectionFailed()
    {
        var mock1 = MockPoolConnectionFailed("node1:10000");
        var mock2 = MockPoolConnectionFailed("node2:10000");

        var mocks = new Dictionary<string, Mock<INodeConnectionPool>>
        {
            ["node1:10000"] = mock1,
            ["node2:10000"] = mock2,
        };
        var options = MakeOptions("node1:10000", seeds: ["node2:10000"]);

        await using var pool = CreatePool(options, mocks);
        await Assert.ThrowsAsync<ConnectionFailedException>(
            () => pool.ReadAsync(MakeReadRequest()));
    }

    // -----------------------------------------------------------------------
    // RouteReadsToFollowers
    // -----------------------------------------------------------------------

    [Fact]
    public async Task ReadAsync_RouteReadsToFollowers_SkipsLeader()
    {
        var leaderMock = MockPoolThatSucceeds("leader:10000", SuccessReadResponse());
        var followerMock = MockPoolThatSucceeds("follower:10000", SuccessReadResponse());

        var mocks = new Dictionary<string, Mock<INodeConnectionPool>>
        {
            ["leader:10000"] = leaderMock,
            ["follower:10000"] = followerMock,
        };
        var options = MakeOptions("leader:10000", seeds: ["follower:10000"], routeReadsToFollowers: true);

        await using var pool = CreatePool(options, mocks);
        await pool.ReadAsync(MakeReadRequest());

        // Follower should be used, not leader
        followerMock.Verify(p => p.ExecuteRequestAsync(
            It.IsAny<ClientRequest>(), It.IsAny<CancellationToken>()), Times.Once);
        leaderMock.Verify(p => p.ExecuteRequestAsync(
            It.IsAny<ClientRequest>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task ReadAsync_RouteReadsToFollowers_SingleNode_FallsBackToLeader()
    {
        var leaderMock = MockPoolThatSucceeds("leader:10000", SuccessReadResponse());

        var mocks = new Dictionary<string, Mock<INodeConnectionPool>>
        {
            ["leader:10000"] = leaderMock,
        };
        var options = MakeOptions("leader:10000", routeReadsToFollowers: true);

        await using var pool = CreatePool(options, mocks);
        var result = await pool.ReadAsync(MakeReadRequest());

        Assert.NotNull(result);
        leaderMock.Verify(p => p.ExecuteRequestAsync(
            It.IsAny<ClientRequest>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    // -----------------------------------------------------------------------
    // Delete/TrimStart also route to leader
    // -----------------------------------------------------------------------

    [Fact]
    public async Task DeleteAsync_RoutesToLeader()
    {
        var response = new ClientResponse.Delete(new SuccessResponse());
        var leaderMock = MockPoolThatSucceeds("leader:10000", response);

        var mocks = new Dictionary<string, Mock<INodeConnectionPool>>
        {
            ["leader:10000"] = leaderMock,
        };

        await using var pool = CreatePool(MakeOptions(), mocks);
        var result = await pool.DeleteAsync(new DeleteRequest
        {
            ClientId = TestClientId,
            Deletes = new Dictionary<AggregateKey, SingleAggregateDelete>
            {
                [TestKey] = new()
            }
        });

        Assert.NotNull(result);
    }

    [Fact]
    public async Task TrimStartAsync_RoutesToLeader()
    {
        var response = new ClientResponse.TrimStart(new SuccessResponse());
        var leaderMock = MockPoolThatSucceeds("leader:10000", response);

        var mocks = new Dictionary<string, Mock<INodeConnectionPool>>
        {
            ["leader:10000"] = leaderMock,
        };

        await using var pool = CreatePool(MakeOptions(), mocks);
        var result = await pool.TrimStartAsync(new TrimStartRequest
        {
            ClientId = TestClientId,
            AggregateKey = TestKey,
            KeepFromAggregateVersion = 5,
        });

        Assert.NotNull(result);
    }

    // -----------------------------------------------------------------------
    // RegisterSchemaAsync routes to any node
    // -----------------------------------------------------------------------

    [Fact]
    public async Task RegisterSchemaAsync_RoutesToAnyNode()
    {
        var response = new ClientResponse.RegisterSchema(new SuccessResponse());
        var nodeMock = MockPoolThatSucceeds("leader:10000", response);

        var mocks = new Dictionary<string, Mock<INodeConnectionPool>>
        {
            ["leader:10000"] = nodeMock,
        };

        await using var pool = CreatePool(MakeOptions(), mocks);
        var result = await pool.RegisterSchemaAsync(new RegisterSchemaRequest
        {
            ClientId = TestClientId,
            SchemaKey = new SchemaKey(TestKey.OrgId, TestKey.AggregateTypeId, 1, 0),
            Schema = "{}",
        });

        Assert.NotNull(result);
    }

    // -----------------------------------------------------------------------
    // Disposal
    // -----------------------------------------------------------------------

    [Fact]
    public async Task DisposeAsync_DisposesAllNodePools()
    {
        var mock1 = MockPool("leader:10000");
        var mock2 = MockPool("follower:10000");

        var mocks = new Dictionary<string, Mock<INodeConnectionPool>>
        {
            ["leader:10000"] = mock1,
            ["follower:10000"] = mock2,
        };
        var options = MakeOptions("leader:10000", seeds: ["follower:10000"]);

        var pool = CreatePool(options, mocks);
        await pool.DisposeAsync();

        mock1.Verify(p => p.DisposeAsync(), Times.Once);
        mock2.Verify(p => p.DisposeAsync(), Times.Once);
    }

    [Fact]
    public async Task GetConnectionAsync_AfterDispose_ThrowsObjectDisposed()
    {
        var leaderMock = MockPoolThatSucceeds("leader:10000", SuccessWriteResponse());
        var mocks = new Dictionary<string, Mock<INodeConnectionPool>>
        {
            ["leader:10000"] = leaderMock,
        };

        var pool = CreatePool(MakeOptions(), mocks);
        await pool.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => pool.GetConnectionAsync());
    }

    // -----------------------------------------------------------------------
    // WriteAsync convenience overload
    // -----------------------------------------------------------------------

    [Fact]
    public async Task WriteAsync_ConvenienceOverload_CreatesCorrectRequest()
    {
        ClientRequest? captured = null;
        var leaderMock = MockPool("leader:10000");
        leaderMock.Setup(p => p.ExecuteRequestAsync(
                It.IsAny<ClientRequest>(), It.IsAny<CancellationToken>()))
            .Callback<ClientRequest, CancellationToken>(
                (req, _) => captured = req)
            .ReturnsAsync(SuccessWriteResponse());

        var mocks = new Dictionary<string, Mock<INodeConnectionPool>>
        {
            ["leader:10000"] = leaderMock,
        };

        await using var pool = CreatePool(MakeOptions(), mocks);
        var events = new[] { MakeEvent() };
        await pool.WriteAsync(TestKey, events, TestClientId);

        Assert.NotNull(captured);
        var writeReq = Assert.IsType<ClientRequest.Write>(captured);
        Assert.Contains(TestKey, writeReq.Value.Writes.Keys);
        Assert.True(writeReq.Value.Writes[TestKey].AllowCreate);
    }

    // -----------------------------------------------------------------------
    // Multi-hop failover: A → NotLeader(B) → NotLeader(C) → Success
    // -----------------------------------------------------------------------

    [Fact]
    public async Task WriteAsync_MultiHopFailover_FollowsLeaderChain()
    {
        var error = new ErrorResponse { ErrorCode = ErrorResponse.WriteNotLeader, ErrorMessage = "{}" };

        var mockA = MockPool("node-a:10000");
        mockA.Setup(p => p.ExecuteRequestAsync(
                It.IsAny<ClientRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NotLeaderException(error, "node-b:10000"));

        var mockB = MockPool("node-b:10000");
        mockB.Setup(p => p.ExecuteRequestAsync(
                It.IsAny<ClientRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new NotLeaderException(error, "node-c:10000"));

        var mockC = MockPoolThatSucceeds("node-c:10000", SuccessWriteResponse());

        var mocks = new Dictionary<string, Mock<INodeConnectionPool>>
        {
            ["node-a:10000"] = mockA,
            ["node-b:10000"] = mockB,
            ["node-c:10000"] = mockC,
        };
        var options = MakeOptions("node-a:10000");

        await using var pool = CreatePool(options, mocks);
        var result = await pool.WriteAsync(MakeWriteRequest());

        Assert.NotNull(result);
        mockC.Verify(p => p.ExecuteRequestAsync(
            It.IsAny<ClientRequest>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    // -----------------------------------------------------------------------
    // Timeout propagated
    // -----------------------------------------------------------------------

    [Fact]
    public async Task WriteAsync_TimeoutException_Propagated()
    {
        var leaderMock = MockPool("leader:10000");
        leaderMock.Setup(p => p.ExecuteRequestAsync(
                It.IsAny<ClientRequest>(), It.IsAny<CancellationToken>()))
            .ThrowsAsync(new CeleriantTimeoutException("timed out"));

        var mocks = new Dictionary<string, Mock<INodeConnectionPool>>
        {
            ["leader:10000"] = leaderMock,
        };

        await using var pool = CreatePool(MakeOptions(), mocks);
        await Assert.ThrowsAsync<CeleriantTimeoutException>(
            () => pool.WriteAsync(MakeWriteRequest()));
    }
}
