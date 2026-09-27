using System.Diagnostics;
using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Responses;
using Celeriant.Client.Watch;
using static Celeriant.Client.Tests.IdentifyFirstContractTests;

namespace Celeriant.Client.Tests;

/// <summary>
/// Identity rejections on a fake. The server rejects an Identify with a GenericError
/// and closes; each identity code reaches the caller as its own typed exception, thrown from the
/// first request on a pool and from connect on a watch, never wrapped in a probe or connection
/// error. A ProtocolError reply to Identify is a <see cref="ProtocolException"/>, the connection is
/// never reused, and the pool does not keep redialling.
///
/// Run: dotnet test Celeriant.Client.Tests --filter FullyQualifiedName~IdentityErrorContractTests
/// </summary>
public class IdentityErrorContractTests
{
    /// <summary>What a rejected handshake may take, well under every timeout the entries are given.</summary>
    private static readonly TimeSpan Prompt = TimeSpan.FromSeconds(3);

    private static readonly TimeSpan Generous = TimeSpan.FromSeconds(20);

    private static CeleriantPool SlowPool(string[] addresses, ClientIdentityConfig? identity) => new(new CeleriantPoolOptions
    {
        Address = addresses[0],
        SeedAddresses = addresses.Skip(1).ToArray(),
        IdentityConfig = identity,
        MaxConnections = 4,
        ConnectionTimeout = Generous,
        RequestTimeout = Generous,
    });

    /// <summary>
    /// The operation that meets the handshake first, per entry point: the first request on a pool or
    /// a standalone client, connect on a watch. A standalone client with an identity identifies explicitly.
    /// </summary>
    private static readonly Dictionary<string, Func<string[], ClientIdentityConfig?, Task>> Entries = new()
    {
        ["pool write"] = async (addresses, identity) =>
        {
            await using var pool = SlowPool(addresses, identity);
            await pool.WriteAsync(OccTestData.Write());
        },
        ["pool.WatchAsync"] = async (addresses, identity) =>
        {
            await using var pool = SlowPool(addresses, identity);
            await using var watch = await pool.WatchAsync(AnyWatch());
        },
        ["WatchConnection.ConnectAsync"] = async (addresses, identity) =>
        {
            await using var watch = await WatchConnection.ConnectAsync(addresses[0], AnyWatch(),
                new WatchOptions { IdentityConfig = identity, ConnectionTimeout = Generous });
        },
        ["CeleriantClient"] = async (addresses, identity) =>
        {
            await using var client = await CeleriantClient.ConnectAsync(addresses[0], connectionTimeout: Generous);
            if (identity is null)
                await client.WriteAsync(OccTestData.Write());
            else
                await client.IdentifyAsync(identity);
        },
    };

    /// <summary>
    /// Server code, the identity the client holds (as in the scenario that produces the code), and
    /// the exact exception type the caller must see.
    /// </summary>
    private static readonly Dictionary<string, (uint Code, ClientIdentityConfig? Identity, Type Expected)> Codes = new()
    {
        ["10004 IDENTIFY_REQUIRED"] = (ErrorResponse.IdentifyRequired, null, typeof(IdentityRequiredException)),
        ["10005 AUTH_REQUIRED"] = (ErrorResponse.AuthRequired, null, typeof(AuthRequiredException)),
        ["10006 AUTH_INVALID_KEY"] = (ErrorResponse.AuthInvalidKey, ApiKey, typeof(AuthInvalidKeyException)),
    };

    public static TheoryData<string, string> CodeByEntry()
    {
        var rows = new TheoryData<string, string>();
        foreach (string code in Codes.Keys)
            foreach (string entry in Entries.Keys)
                rows.Add(code, entry);
        return rows;
    }

    [Theory]
    [MemberData(nameof(CodeByEntry))]
    public async Task ARejectedIdentifySurfacesItsOwnTypedErrorUnwrapped(string code, string entry)
    {
        var (errorCode, identity, expected) = Codes[code];
        await using var server = new HandshakeScriptServer(frame => frame.Type == MessageTypes.Requests.Identify
            ? HandshakeReplies.Rejected(frame, errorCode)
            : HandshakeReplies.Served(frame));

        var clock = Stopwatch.StartNew();
        Exception? failure = await Record.ExceptionAsync(() => Entries[entry]([server.Address], identity).WaitAsync(Generous * 2));
        clock.Stop();

        string row = $"row '{code}' x '{entry}'";
        Assert.True(failure is not null, $"{row}: the server rejected the Identify, yet the operation succeeded");
        Assert.True(failure.GetType() == expected,
            $"{row}: expected {expected.Name} thrown directly, got {failure.GetType().Name}: {failure.Message} (inner: {failure.InnerException?.GetType().Name})");
        if (failure is CeleriantErrorException error)
            Assert.Equal(errorCode, error.Error.ErrorCode);
        if (failure is IdentityRequiredException required)
            Assert.Equal(errorCode, required.Error.ErrorCode);
        Assert.True(clock.Elapsed < Prompt, $"{row}: took {clock.Elapsed.TotalSeconds:F1}s to surface");
        Assert.True(server.Accepted <= 1, $"{row}: dialled {server.Accepted} times for one rejected handshake");
        Assert.DoesNotContain(server.Frames, f => f.Type != MessageTypes.Requests.Identify);
        Assert.Empty(server.Faults);
    }

    /// <summary>Both new types are auth errors, so callers catching <see cref="AuthErrorException"/> keep working.</summary>
    [Fact]
    public void TheNewAuthErrorsAreAuthErrors()
    {
        Assert.True(typeof(AuthErrorException).IsAssignableFrom(typeof(AuthRequiredException)));
        Assert.True(typeof(AuthErrorException).IsAssignableFrom(typeof(AuthInvalidKeyException)));
    }

    // ---------------------------------------------------------------------------------------------
    // A ProtocolError reply to Identify
    // ---------------------------------------------------------------------------------------------

    public static TheoryData<string, int, bool> ProtocolErrorRows()
    {
        var rows = new TheoryData<string, int, bool>();
        foreach (string entry in Entries.Keys)
            foreach (bool echo in new[] { true, false })
                rows.Add(entry, 1, echo);
        rows.Add("pool write", 2, true);
        rows.Add("pool.WatchAsync", 2, true);
        return rows;
    }

    /// <summary>
    /// Every node answers Identify with a ProtocolError and closes, the way the server refuses a
    /// handshake it cannot accept. One operation surfaces a <see cref="ProtocolException"/> well
    /// inside the timeouts, and dials each node at most once rather than storming it.
    /// </summary>
    [Theory]
    [MemberData(nameof(ProtocolErrorRows))]
    public async Task AProtocolErrorReplyToIdentifyIsAProtocolExceptionWithoutARetryStorm(string entry, int nodes, bool echo)
    {
        var servers = Enumerable.Range(0, nodes)
            .Select(_ => new HandshakeScriptServer(frame => frame.Type == MessageTypes.Requests.Identify
                ? HandshakeReplies.ProtocolError(frame, echo, close: true)
                : HandshakeReplies.Served(frame)))
            .ToArray();
        try
        {
            var clock = Stopwatch.StartNew();
            Exception? failure = await Record.ExceptionAsync(() =>
                Entries[entry](servers.Select(s => s.Address).ToArray(), null).WaitAsync(Generous * 2));
            clock.Stop();

            string row = $"row '{entry}', {nodes} node(s), correlation {(echo ? "echoed" : "null")}";
            Assert.True(failure is ProtocolException,
                $"{row}: expected ProtocolException thrown directly, got {failure?.GetType().Name ?? "success"}: {failure?.Message}");
            Assert.True(clock.Elapsed < Prompt, $"{row}: took {clock.Elapsed.TotalSeconds:F1}s to surface");
            int[] dials = servers.Select(s => s.Accepted).ToArray();
            Assert.True(dials.All(d => d <= 1), $"{row}: dials per node [{string.Join(", ", dials)}], expected at most 1 each");
            Assert.DoesNotContain(servers.SelectMany(s => s.Frames), f => f.Type != MessageTypes.Requests.Identify);
        }
        finally
        {
            foreach (var server in servers)
                await server.DisposeAsync();
        }
    }

    /// <summary>
    /// The first connection answers Identify with a ProtocolError and, adversarially, stays open. The
    /// pool must retire it: the next request goes out on a fresh connection that identifies again, and
    /// nothing but that one Identify is ever written to the refused connection.
    /// </summary>
    [Fact]
    public async Task APoolRetiresAConnectionWhoseIdentifyGotAProtocolError()
    {
        await using var server = new HandshakeScriptServer(frame =>
            frame.Type != MessageTypes.Requests.Identify ? HandshakeReplies.Served(frame)
            : frame.Connection == 1 ? HandshakeReplies.ProtocolError(frame, echo: true, close: false)
            : HandshakeReplies.Identified(frame));
        await using var pool = new CeleriantPool(new CeleriantPoolOptions
        {
            Address = server.Address,
            MaxConnections = 1,
            ConnectionTimeout = Generous,
            RequestTimeout = Generous,
        });

        Exception? first = await Record.ExceptionAsync(() => pool.WriteAsync(OccTestData.Write()).WaitAsync(Generous));
        Exception? second = await Record.ExceptionAsync(() => pool.WriteAsync(OccTestData.Write()).WaitAsync(Generous));

        var connections = server.ByConnection();
        string wire = string.Join("; ", connections.Select(c => $"conn {c.Key}: [{string.Join(",", c.Value.Select(f => f.Type))}]"));
        Assert.True(first is ProtocolException, $"first write: expected ProtocolException, got {first?.GetType().Name ?? "success"}. Wire: {wire}");
        Assert.True(second is null, $"second write, on a fresh connection to a now-healthy server, failed: {second}. Wire: {wire}");
        Assert.True(connections.TryGetValue(1, out var refused) && refused.Length == 1 && refused[0].Type == MessageTypes.Requests.Identify,
            $"the refused connection must carry only its Identify. Wire: {wire}");
        Assert.True(server.Accepted == 2, $"expected exactly one redial for the second write, saw {server.Accepted} connections. Wire: {wire}");
        Assert.Empty(server.Faults);
    }

    /// <summary>A standalone client whose Identify got a ProtocolError is poisoned: it cannot be reused.</summary>
    [Fact]
    public async Task AStandaloneClientWhoseIdentifyGotAProtocolErrorIsPoisoned()
    {
        await using var server = new HandshakeScriptServer(frame => frame.Type == MessageTypes.Requests.Identify
            ? HandshakeReplies.ProtocolError(frame, echo: true, close: false)
            : HandshakeReplies.Served(frame));
        await using var client = await CeleriantClient.ConnectAsync(server.Address, connectionTimeout: Generous);

        Exception? failure = await Record.ExceptionAsync(() => client.WriteAsync(OccTestData.Write()).WaitAsync(Generous));

        Assert.IsType<ProtocolException>(failure);
        Assert.True(client.IsPoisoned, "a client whose handshake was refused must not be reused");
        Assert.DoesNotContain(server.Frames, f => f.Type != MessageTypes.Requests.Identify);
    }
}
