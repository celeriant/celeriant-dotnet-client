using System.Net.Sockets;
using Celeriant.Client;

namespace Celeriant.Client.IntegrationTests;

/// <summary>
/// Shared test fixture that establishes a single <see cref="CeleriantClient"/> connection
/// to the live Celeriant server for the duration of the test collection.
///
/// If the server is not reachable within the poll timeout, <see cref="IsAvailable"/> is
/// set to false and tests should skip rather than fail.
/// </summary>
public sealed class ServerFixture : IAsyncLifetime
{
    private const int PollIntervalMs = 100;
    private const int PollTimeoutMs = 10_000;

    /// <summary>
    /// Server address read from the <c>CELERIANT_SERVER_ADDRESS</c> environment variable,
    /// defaulting to <c>localhost:10000</c>.
    /// </summary>
    public string Address { get; } =
        Environment.GetEnvironmentVariable("CELERIANT_SERVER_ADDRESS") ?? "localhost:10000";

    /// <summary>
    /// The connected client. Only valid when <see cref="IsAvailable"/> is true.
    /// </summary>
    public CeleriantClient? Client { get; private set; }

    /// <summary>
    /// True if the server was reachable when the fixture initialised.
    /// Tests that require the server should call <c>Skip.If(!fixture.IsAvailable, "Server not running")</c>.
    /// </summary>
    public bool IsAvailable { get; private set; }

    public async Task InitializeAsync()
    {
        (string host, int port) = ParseAddress(Address);

        // Poll for server readiness by attempting a raw TCP connect.
        bool reachable = await PollForServerAsync(host, port).ConfigureAwait(false);
        if (!reachable)
        {
            IsAvailable = false;
            return;
        }

        try
        {
            Client = await CeleriantClient.ConnectAsync(Address, connectionTimeout: null).ConfigureAwait(false);
            IsAvailable = true;
        }
        catch
        {
            IsAvailable = false;
        }
    }

    public async Task DisposeAsync()
    {
        if (Client is not null)
            await Client.DisposeAsync().ConfigureAwait(false);
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static async Task<bool> PollForServerAsync(string host, int port)
    {
        using var cts = new CancellationTokenSource(PollTimeoutMs);

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
                // Server not yet accepting connections; wait and retry.
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
/// xUnit collection that shares a single <see cref="ServerFixture"/> instance across all
/// integration tests. This prevents repeated TCP connects and server state contamination
/// from fixture-level teardown between tests.
/// </summary>
[CollectionDefinition("Server")]
public sealed class ServerCollection : ICollectionFixture<ServerFixture> { }
