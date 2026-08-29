using Celeriant.Client.Errors;
using Celeriant.Client.Requests;
using Celeriant.Client.Watch;

namespace Celeriant.Client.IntegrationTests;

/// <summary>
/// The one scenario the unit tests cannot reach: a real node dying under a live watch.
///
/// <para>
/// A watch never reconnects and never catches a gap up, so the whole design rests on the client
/// being honest about stopping. Everything else is verified against a scripted fake over loopback,
/// where the socket always closes cleanly and the timing is the test's to choose. Here the node is
/// killed by the container runtime and the client finds out however TCP tells it.
/// </para>
///
/// <para>
/// Requires the two-node cluster. Set <c>CELERIANT_NODE1_ADDRESS</c> and
/// <c>CELERIANT_NODE2_ADDRESS</c>, and note that these tests STOP AND RESTART a container:
/// they are destructive to cluster state and restore it on the way out.
/// </para>
/// </summary>
[Collection("Cluster")]
public sealed class WatchLeadershipFailoverTests
{
    private readonly ClusterFixture _fixture;

    public WatchLeadershipFailoverTests(ClusterFixture fixture) => _fixture = fixture;

    /// <summary>How long a dead watch gets to report itself before we call it silent.</summary>
    private static readonly TimeSpan TellBudget = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Kill the node a watch is subscribed to and the next read must fail. Returning null — which is
    /// what <c>NextAsync(TimeSpan)</c> says for "nothing arrived yet" — would tell a caller its
    /// subscription was merely quiet while it was in fact attached to a node that no longer exists.
    /// </summary>
    [SkippableFact]
    public async Task WatchOnAKilledNode_TellsTheCallerRatherThanGoingQuiet()
    {
        Skip.If(!_fixture.IsAvailable, "Cluster not running");
        Skip.If(!DockerAvailable.Value, "docker CLI not usable; these tests kill a container");
        string leader = _fixture.LeaderAddress!;
        string container = ContainerFor(leader);
        Skip.If(container is "", $"No container mapping for {leader}");
        Skip.If(!await WaitForPortAsync(leader), $"{leader} did not come back up");

        await using var watch = await WatchConnection.ConnectAsync(
            leader, new WatchRequest(), new WatchOptions { ConnectionTimeout = TimeSpan.FromSeconds(5) });

        Assert.Equal(leader, watch.Address);

        // Nothing is writing, so this is the ordinary quiet case: null, and the watch stays usable.
        Assert.Null(await watch.NextAsync(TimeSpan.FromMilliseconds(500)));

        string? restartFailure;
        try
        {
            AssertDocker($"stop -t 0 {container}");

            // The node is gone. Every read from here must fail, and must keep failing.
            var failure = await Record.ExceptionAsync(async () =>
            {
                var deadline = DateTime.UtcNow + TellBudget;
                while (DateTime.UtcNow < deadline)
                {
                    var response = await watch.NextAsync(TimeSpan.FromSeconds(2));
                    if (response is not null)
                        Assert.Fail("events arrived from a node that was killed");
                }
            });

            Assert.True(
                failure is CeleriantClientException,
                $"the node this watch was subscribed to was killed, so the subscription is dead and the "
                + $"caller must be told through the client's own exception hierarchy within "
                + $"{TellBudget.TotalSeconds:0}s. Got {Describe(failure)} — a null return here is the "
                + "silent blindness the whole fail-closed design exists to prevent");

            var again = await Record.ExceptionAsync(() => watch.NextAsync(CancellationToken.None));
            Assert.True(
                again is CeleriantClientException,
                $"a dead watch stays dead: a caller that retries must keep being told. Got {Describe(again)}");
        }
        finally
        {
            restartFailure = RunDocker($"start {container}");
            await WaitForPortAsync(leader);
        }

        // Only reached when the body succeeded, so this reports a broken restore without
        // masking the failure that would otherwise be the interesting one.
        Assert.True(restartFailure is null, restartFailure);
    }

    /// <summary>
    /// After the node it was on dies, a caller reconnects through the pool and lands somewhere. The
    /// address is how it learns whether that somewhere is a different node — which means the new
    /// subscription started from that node's tip and the gap between the two is unrecoverable.
    /// </summary>
    [SkippableFact]
    public async Task ReconnectingAfterTheNodeDied_ReportsWhichNodeTheNewSubscriptionIsOn()
    {
        Skip.If(!_fixture.IsAvailable, "Cluster not running");
        Skip.If(!DockerAvailable.Value, "docker CLI not usable; these tests kill a container");
        string dead = _fixture.LeaderAddress!;
        string survivor = _fixture.FollowerAddress!;
        string container = ContainerFor(dead);
        Skip.If(container is "", $"No container mapping for {dead}");
        Skip.If(!await WaitForPortAsync(dead), $"{dead} did not come back up");
        Skip.If(!await WaitForPortAsync(survivor), $"{survivor} is not up");

        await using var pool = new CeleriantPool(new CeleriantPoolOptions
        {
            Address = dead,
            SeedAddresses = [survivor],
            ConnectionTimeout = TimeSpan.FromSeconds(3),
            RequestTimeout = TimeSpan.FromSeconds(10),
        });

        string? restartFailure;
        try
        {
            AssertDocker($"stop -t 0 {container}");

            await using var reconnected = await pool.WatchAsync(new WatchRequest());

            Assert.Equal(survivor, reconnected.Address);
            Assert.NotEqual(dead, reconnected.Address);
        }
        finally
        {
            restartFailure = RunDocker($"start {container}");
            await WaitForPortAsync(dead);
        }

        Assert.True(restartFailure is null, restartFailure);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /// <summary>
    /// Map a host address back to the container serving it. Read from
    /// <c>CELERIANT_NODE1_CONTAINER</c> / <c>CELERIANT_NODE2_CONTAINER</c> so the test is not tied
    /// to one compose project name.
    /// </summary>
    private string ContainerFor(string address)
    {
        if (address == _fixture.Node1Address)
            return Environment.GetEnvironmentVariable("CELERIANT_NODE1_CONTAINER") ?? "";
        if (address == _fixture.Node2Address)
            return Environment.GetEnvironmentVariable("CELERIANT_NODE2_CONTAINER") ?? "";
        return "";
    }

    /// <summary>Poll for TCP readiness, so a restarted node is up before anything depends on it.</summary>
    private static async Task<bool> WaitForPortAsync(string address, int timeoutMs = 60_000)
    {
        int lastColon = address.LastIndexOf(':');
        string host = address[..lastColon];
        int port = int.Parse(address[(lastColon + 1)..]);

        using var cts = new CancellationTokenSource(timeoutMs);
        while (!cts.IsCancellationRequested)
        {
            try
            {
                using var probe = new System.Net.Sockets.TcpClient();
                await probe.ConnectAsync(host, port, cts.Token);
                // Accepting TCP is not the same as ready to serve; give the shard a moment to settle.
                await Task.Delay(2_000, cts.Token);
                return true;
            }
            catch (OperationCanceledException) { return false; }
            catch
            {
                try { await Task.Delay(250, cts.Token); } catch (OperationCanceledException) { return false; }
            }
        }
        return false;
    }

    /// <summary>
    /// A cluster can be reachable on a host that has no usable docker CLI. Probed once, because
    /// nothing about the client is under test when the kill cannot even be delivered.
    /// </summary>
    private static readonly Lazy<bool> DockerAvailable = new(() => RunDocker("version") is null);

    /// <summary>Run a docker command and fail the test with the real cause if it did not succeed.</summary>
    private static void AssertDocker(string arguments)
    {
        string? failure = RunDocker(arguments);
        if (failure is not null)
            Assert.Fail(failure);
    }

    /// <summary>
    /// Run a docker command to completion. Null on success; otherwise what actually went wrong.
    ///
    /// <para>
    /// The exit code must travel: dropped, a `docker stop` that never stopped anything surfaces
    /// as a 30-second assertion failure blaming the client for going quiet on a node that was
    /// still running the whole time.
    /// </para>
    /// </summary>
    private static string? RunDocker(string arguments)
    {
        System.Diagnostics.Process? process;
        try
        {
            process = System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo
            {
                FileName = "docker",
                Arguments = arguments,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            });
        }
        catch (Exception ex)
        {
            return $"`docker {arguments}` could not be launched: {ex.GetType().Name}: {ex.Message}";
        }

        if (process is null)
            return $"`docker {arguments}` could not be launched: no process was started.";

        using (process)
        {
            // Both pipes must be drained or a chatty command deadlocks on a full buffer.
            var stdout = process.StandardOutput.ReadToEndAsync();
            var stderr = process.StandardError.ReadToEndAsync();

            if (!process.WaitForExit(60_000))
            {
                try { process.Kill(entireProcessTree: true); } catch { /* already gone */ }
                return $"`docker {arguments}` was still running after 60s.";
            }

            if (process.ExitCode == 0)
                return null;

            string detail = string.Concat(Safe(stderr), Safe(stdout)).Trim();
            return $"`docker {arguments}` exited {process.ExitCode}"
                + (detail.Length == 0 ? "." : $": {detail}");
        }

        static string Safe(Task<string> read)
        {
            try { return read.Wait(TimeSpan.FromSeconds(5)) ? read.Result : ""; }
            catch { return ""; }
        }
    }

    private static string Describe(Exception? failure)
        => failure?.GetType().Name ?? "no exception at all (the read returned normally)";
}
