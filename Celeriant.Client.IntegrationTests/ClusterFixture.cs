using System.Net.Sockets;
using Celeriant.Client;
using Celeriant.Client.Errors;
using Celeriant.Client.Requests;

namespace Celeriant.Client.IntegrationTests;

/// <summary>
/// Shared test fixture for two-node cluster integration tests.
///
/// <para>
/// Reads node addresses from <c>CELERIANT_NODE1_ADDRESS</c> and <c>CELERIANT_NODE2_ADDRESS</c>
/// environment variables (defaults: <c>localhost:10000</c> and <c>localhost:10002</c>).
/// Polls both nodes for TCP readiness, then probes for the leader by attempting a write.
/// </para>
///
/// <para>
/// If either node is unreachable, <see cref="IsAvailable"/> is set to false and tests should
/// skip rather than fail using <c>Skip.If(!fixture.IsAvailable, "Cluster not running")</c>.
/// </para>
///
/// <para>Thread safety: fixture is initialised once and then read-only during tests.</para>
/// </summary>
public sealed class ClusterFixture : IAsyncLifetime
{
    private const int PollIntervalMs = 100;
    private const int NodePollTimeoutMs = 30_000;
    private const int LeaderElectionDelayMs = 5_000;

    /// <summary>
    /// Address of node 1, read from <c>CELERIANT_NODE1_ADDRESS</c> (default <c>localhost:10000</c>).
    /// </summary>
    public string Node1Address { get; } =
        Environment.GetEnvironmentVariable("CELERIANT_NODE1_ADDRESS") ?? "localhost:10000";

    /// <summary>
    /// Address of node 2, read from <c>CELERIANT_NODE2_ADDRESS</c> (default <c>localhost:10002</c>).
    /// </summary>
    public string Node2Address { get; } =
        Environment.GetEnvironmentVariable("CELERIANT_NODE2_ADDRESS") ?? "localhost:10002";

    /// <summary>
    /// The host-accessible address of the current leader node. Only valid when <see cref="IsAvailable"/> is true.
    /// </summary>
    public string? LeaderAddress { get; private set; }

    /// <summary>
    /// The host-accessible address of the current follower node. Only valid when <see cref="IsAvailable"/> is true.
    /// </summary>
    public string? FollowerAddress { get; private set; }

    /// <summary>
    /// True if both nodes were reachable and a leader was identified during initialisation.
    /// Tests that require the cluster should call <c>Skip.If(!fixture.IsAvailable, "Cluster not running")</c>.
    /// </summary>
    public bool IsAvailable { get; private set; }

    public async Task InitializeAsync()
    {
        // Both nodes must be up before we probe for the leader.
        bool node1Up = await PollForServerAsync(Node1Address, NodePollTimeoutMs).ConfigureAwait(false);
        bool node2Up = await PollForServerAsync(Node2Address, NodePollTimeoutMs).ConfigureAwait(false);

        if (!node1Up || !node2Up)
        {
            IsAvailable = false;
            return;
        }

        // Give the cluster time to complete leader election after both nodes are accepting TCP.
        await Task.Delay(LeaderElectionDelayMs).ConfigureAwait(false);

        await ProbeLeaderAsync().ConfigureAwait(false);
    }

    public Task DisposeAsync() => Task.CompletedTask;

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private async Task ProbeLeaderAsync()
    {
        // Attempt a write on node 1. If it succeeds, node 1 is the leader.
        // If it throws NotLeaderException, node 2 is the leader.
        // Any other exception means the cluster is not ready.
        try
        {
            await using var client = await CeleriantClient.ConnectAsync(Node1Address, ct: default).ConfigureAwait(false);
            var key = TestHelpers.NewKey();
            await client.SendRequestAsync(
                new ClientRequest.Write(
                    TestHelpers.SingleEventWrite(key, "leader-probe"u8.ToArray())))
                .ConfigureAwait(false);

            // Write succeeded — node 1 is the leader.
            LeaderAddress = Node1Address;
            FollowerAddress = Node2Address;
            IsAvailable = true;
        }
        catch (NotLeaderException)
        {
            // Node 1 redirected us — node 2 is the leader.
            LeaderAddress = Node2Address;
            FollowerAddress = Node1Address;
            IsAvailable = true;
        }
        catch
        {
            // Unexpected failure; treat cluster as unavailable so tests skip.
            IsAvailable = false;
        }
    }

    private static async Task<bool> PollForServerAsync(string address, int timeoutMs)
    {
        (string host, int port) = ParseAddress(address);
        using var cts = new CancellationTokenSource(timeoutMs);

        while (!cts.IsCancellationRequested)
        {
            using var probe = new TcpClient();
            try
            {
                await probe.ConnectAsync(host, port, cts.Token).ConfigureAwait(false);
                return true;
            }
            catch (OperationCanceledException)
            {
                return false;
            }
            catch (SocketException)
            {
                try
                {
                    await Task.Delay(PollIntervalMs, cts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return false;
                }
            }
        }

        return false;
    }

    private static (string host, int port) ParseAddress(string address)
    {
        int lastColon = address.LastIndexOf(':');
        if (lastColon < 0 || lastColon == address.Length - 1)
            throw new ArgumentException($"Invalid address '{address}'. Expected 'host:port'.");

        string host = address[..lastColon];
        if (!int.TryParse(address[(lastColon + 1)..], out int port))
            throw new ArgumentException($"Invalid port in address '{address}'.");

        return (host, port);
    }
}

/// <summary>
/// xUnit collection that shares a single <see cref="ClusterFixture"/> instance across all
/// cluster integration tests.
/// </summary>
[CollectionDefinition("Cluster")]
public sealed class ClusterCollection : ICollectionFixture<ClusterFixture> { }
