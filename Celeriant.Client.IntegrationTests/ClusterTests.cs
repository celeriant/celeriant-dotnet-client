using System.Text;
using Celeriant.Client;
using Celeriant.Client.Errors;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Streaming;

namespace Celeriant.Client.IntegrationTests;

/// <summary>
/// Integration tests for cluster behaviour: pool failover, NotLeaderException handling,
/// read routing, and replication verification.
///
/// <para>
/// Requires a two-node cluster. Set <c>CELERIANT_NODE1_ADDRESS</c> and
/// <c>CELERIANT_NODE2_ADDRESS</c> (defaults: <c>localhost:10000</c> and <c>localhost:10002</c>)
/// and run <c>docker compose up -d</c> with the cluster compose file before running these tests.
/// </para>
///
/// <para>
/// Note: the server's advertised-client-address uses Docker container names
/// (e.g. <c>celeriant-node-1:10000</c>), which are not reachable from the host.
/// Tests that exercise <see cref="NotLeaderException.LeaderAddress"/> should not attempt
/// to connect to that address directly.
/// </para>
/// </summary>
[Collection("Cluster")]
public sealed class ClusterTests
{
    private readonly ClusterFixture _fixture;

    public ClusterTests(ClusterFixture fixture) => _fixture = fixture;

    private void SkipIfUnavailable() =>
        Skip.If(!_fixture.IsAvailable, "Cluster not running");

    private string LeaderAddress
    {
        get
        {
            SkipIfUnavailable();
            return _fixture.LeaderAddress!;
        }
    }

    private string FollowerAddress
    {
        get
        {
            SkipIfUnavailable();
            return _fixture.FollowerAddress!;
        }
    }

    private CeleriantPool CreatePool(
        string address,
        IReadOnlyList<string>? seeds = null,
        bool routeReadsToFollowers = false) =>
        new(new CeleriantPoolOptions
        {
            Address = address,
            SeedAddresses = seeds,
            MaxConnections = 3,
            RouteReadsToFollowers = routeReadsToFollowers,
        });

    // -------------------------------------------------------------------------
    // Write routing
    // -------------------------------------------------------------------------

    [SkippableFact]
    public async Task WriteToLeader_Succeeds()
    {
        await using var pool = CreatePool(LeaderAddress);

        var key = TestHelpers.NewKey();
        await pool.WriteAsync(TestHelpers.SingleEventWrite(key, "write-to-leader"u8.ToArray()));

        var read = await pool.ReadAsync(TestHelpers.ReadAllRequest(key));
        var events = read.EventBatches.SelectMany(b => b.Events).ToArray();
        Assert.Single(events);
    }

    [SkippableFact]
    public async Task WriteToFollower_PoolFailsOverToLeader()
    {
        // Pool points at follower initially; seed contains the leader so failover can succeed.
        await using var pool = CreatePool(
            address: FollowerAddress,
            seeds: [LeaderAddress]);

        var key = TestHelpers.NewKey();
        // NotLeaderException should be caught internally; the pool retries on the leader.
        await pool.WriteAsync(TestHelpers.SingleEventWrite(key, "failover-write"u8.ToArray()));

        var read = await pool.ReadAsync(TestHelpers.ReadAllRequest(key));
        var events = read.EventBatches.SelectMany(b => b.Events).ToArray();
        Assert.Single(events);
    }

    [SkippableFact]
    public async Task WriteToFollower_WithoutSeedAddresses_Throws()
    {
        // Pool only knows about the follower — nowhere to fail over to after NotLeaderException.
        // The LeaderAddress from the exception is a Docker container name (unreachable from host),
        // and there are no additional seed addresses, so the pool must give up.
        await using var pool = CreatePool(address: FollowerAddress);

        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await pool.WriteAsync(
                TestHelpers.SingleEventWrite(TestHelpers.NewKey(), "no-seed-write"u8.ToArray()));
        });
    }

    // -------------------------------------------------------------------------
    // Read routing
    // -------------------------------------------------------------------------

    [SkippableFact]
    public async Task ReadFromFollower_Succeeds()
    {
        // Write via the leader, then read directly from the follower to confirm replication.
        await using var leaderClient = await CeleriantClient.ConnectAsync(LeaderAddress, ct: default);
        var key = TestHelpers.NewKey();
        await leaderClient.SendRequestAsync(
            new ClientRequest.Write(TestHelpers.SingleEventWrite(key, "replication-check"u8.ToArray())));

        // Allow time for the write to replicate.
        await Task.Delay(2000);

        await using var followerClient = await CeleriantClient.ConnectAsync(FollowerAddress, ct: default);
        var response = await followerClient.SendRequestAsync(
            new ClientRequest.Read(TestHelpers.ReadAllRequest(key)));

        var read = Assert.IsType<ClientResponse.Read>(response);
        var events = read.Value.EventBatches.SelectMany(b => b.Events).ToArray();
        Assert.Single(events);
        Assert.Equal("replication-check"u8.ToArray(), events[0].EventValue);
    }

    [SkippableFact]
    public async Task Pool_RouteReadsToFollowers_ReadsSucceed()
    {
        await using var pool = CreatePool(
            address: LeaderAddress,
            seeds: [FollowerAddress],
            routeReadsToFollowers: true);

        var key = TestHelpers.NewKey();
        await pool.WriteAsync(TestHelpers.SingleEventWrite(key, "follower-read"u8.ToArray()));

        // Allow replication before reading from follower.
        await Task.Delay(2000);

        var read = await pool.ReadAsync(TestHelpers.ReadAllRequest(key));
        var events = read.EventBatches.SelectMany(b => b.Events).ToArray();
        Assert.Single(events);
    }

    // -------------------------------------------------------------------------
    // Multi-node pool
    // -------------------------------------------------------------------------

    [SkippableFact]
    public async Task Pool_WithBothNodes_WriteAndReadSucceed()
    {
        // Regardless of which node is leader, the pool should route writes to the leader.
        await using var pool = CreatePool(
            address: _fixture.Node1Address,
            seeds: [_fixture.Node2Address]);

        var key = TestHelpers.NewKey();
        await pool.WriteAsync(TestHelpers.SingleEventWrite(key, "both-nodes"u8.ToArray()));

        var read = await pool.ReadAsync(TestHelpers.ReadAllRequest(key));
        var events = read.EventBatches.SelectMany(b => b.Events).ToArray();
        Assert.Single(events);
    }

    [SkippableFact]
    public async Task ConcurrentWrites_BothNodesInPool_AllSucceed()
    {
        await using var pool = CreatePool(
            address: _fixture.Node1Address,
            seeds: [_fixture.Node2Address]);

        var tasks = Enumerable.Range(0, 20).Select(async i =>
        {
            var key = TestHelpers.NewKey();
            var payload = Encoding.UTF8.GetBytes($"concurrent-cluster-{i}");
            await pool.WriteAsync(TestHelpers.SingleEventWrite(key, payload));
            return key;
        }).ToArray();

        var keys = await Task.WhenAll(tasks);
        Assert.Equal(20, keys.Length);
    }

    // -------------------------------------------------------------------------
    // Replication verification
    // -------------------------------------------------------------------------

    [SkippableFact]
    public async Task WriteReplicatesToFollower()
    {
        await using var leaderClient = await CeleriantClient.ConnectAsync(LeaderAddress, ct: default);
        var key = TestHelpers.NewKey();
        var payload = "replicated-event"u8.ToArray();
        await leaderClient.SendRequestAsync(
            new ClientRequest.Write(TestHelpers.SingleEventWrite(key, payload)));

        // Wait for replication.
        await Task.Delay(2000);

        await using var followerClient = await CeleriantClient.ConnectAsync(FollowerAddress, ct: default);
        var response = await followerClient.SendRequestAsync(
            new ClientRequest.Read(TestHelpers.ReadAllRequest(key)));

        var read = Assert.IsType<ClientResponse.Read>(response);
        Assert.NotEmpty(read.Value.EventBatches);
    }

    // -------------------------------------------------------------------------
    // Direct follower writes — NotLeaderException
    // -------------------------------------------------------------------------

    [SkippableFact]
    public async Task DirectWriteToFollower_ThrowsNotLeaderException()
    {
        await using var client = await CeleriantClient.ConnectAsync(FollowerAddress, ct: default);

        await Assert.ThrowsAsync<NotLeaderException>(async () =>
        {
            await client.SendRequestAsync(
                new ClientRequest.Write(
                    TestHelpers.SingleEventWrite(TestHelpers.NewKey(), "direct-follower-write"u8.ToArray())));
        });
    }

    [SkippableFact]
    public async Task NotLeaderException_ContainsLeaderAddress()
    {
        await using var client = await CeleriantClient.ConnectAsync(FollowerAddress, ct: default);

        var ex = await Assert.ThrowsAsync<NotLeaderException>(async () =>
        {
            await client.SendRequestAsync(
                new ClientRequest.Write(
                    TestHelpers.SingleEventWrite(TestHelpers.NewKey(), "leader-addr-check"u8.ToArray())));
        });

        Assert.NotNull(ex.LeaderAddress);
        Assert.NotEmpty(ex.LeaderAddress);
    }
}
