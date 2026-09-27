using System.Diagnostics;
using System.Security.Cryptography;
using Celeriant.Client.Errors;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Watch;
using Celeriant.Transport;

namespace Celeriant.Client.IntegrationTests;

/// <summary>
/// The Identify handshake against the real Rust server: every connection kind works without an
/// identity, a configured identity still works, and each identity rejection reaches the caller as its
/// own typed exception. Each test spawns its own <see cref="RustServerProcess"/> with the flag and
/// keys file the case needs; the server's rules are in its <c>docs/client-protocol.md</c> and
/// <c>celeriant_integration_tests/src/identity_api_key_matrix.rs</c>.
///
/// Run: dotnet test Celeriant.Client.IntegrationTests --filter FullyQualifiedName~LiveIdentifyHandshakeTests
/// (set CELERIANT_SERVER_BIN to point at a server binary other than the sibling checkout's).
/// </summary>
public sealed class LiveIdentifyHandshakeTests
{
    private static readonly TimeSpan Deadline = TimeSpan.FromSeconds(10);

    private enum ServerKind
    {
        /// <summary>No flag, no keys file.</summary>
        Plain,
        /// <summary><c>--require-client-identity</c>.</summary>
        Flag,
        /// <summary>A keys file issuing <see cref="IssuedKey"/>.</summary>
        Keys,
        /// <summary>Both the flag and the keys file.</summary>
        FlagAndKeys,
    }

    private static readonly byte[] IssuedKey = RandomNumberGenerator.GetBytes(32);

    private static Task<RustServerProcess> Start(ServerKind kind) => RustServerProcess.StartAsync(new RustServerOptions
    {
        RequireClientIdentity = kind is ServerKind.Flag or ServerKind.FlagAndKeys,
        ReadWriteApiKey = kind is ServerKind.Keys or ServerKind.FlagAndKeys ? IssuedKey : null,
    });

    private static CeleriantPool Pool(string address, ClientIdentityConfig? identity) => new(new CeleriantPoolOptions
    {
        Address = address,
        IdentityConfig = identity,
        ConnectionTimeout = Deadline,
        RequestTimeout = Deadline,
    });

    private static AggregateEvent[] OneEvent() =>
    [
        new() { ClientSeq = 1, EventTimestamp = DateTimeOffset.UtcNow, EventTypeMajor = 1, EventValue = "{\"m\":1}"u8.ToArray() },
    ];

    private static WatchRequest WatchOf(AggregateKey key) => new()
    {
        Aggregates = [key.AggregateId],
        RequestedLatency = TimeSpan.FromMilliseconds(100),
    };

    /// <summary>Read the watch until it reports <paramref name="key"/>, or fail with <paramref name="row"/>.</summary>
    private static async Task AssertSees(WatchConnection watch, AggregateKey key, string row)
    {
        var clock = Stopwatch.StartNew();
        while (clock.Elapsed < Deadline)
        {
            WatchResponse? batch = await watch.NextAsync(TimeSpan.FromSeconds(1));
            if (batch?.Events.Any(e => e.AggregateId == key.AggregateId) == true)
                return;
        }
        Assert.Fail($"{row}: the watch opened but never reported the write to {key.AggregateId}");
    }

    // ---------------------------------------------------------------------------------------------
    // No identity against a plain server
    // ---------------------------------------------------------------------------------------------

    /// <summary>Each connection kind, with no identity configured, writing and watching against <c>address</c>.</summary>
    private static readonly Dictionary<string, Func<string, AggregateKey, Task>> NoIdentityKinds = new()
    {
        ["pool write"] = async (address, key) =>
        {
            await using var pool = Pool(address, null);
            var written = await pool.WriteAsync(key, OneEvent(), Guid.NewGuid());
            Assert.Equal(1, written.MaxAggregateVersion);
        },
        ["pool.WatchAsync"] = async (address, key) =>
        {
            await using var pool = Pool(address, null);
            await using var watch = await pool.WatchAsync(WatchOf(key));
            await pool.WriteAsync(key, OneEvent(), Guid.NewGuid());
            await AssertSees(watch, key, "pool.WatchAsync");
        },
        ["WatchConnection"] = async (address, key) =>
        {
            await using var watch = await WatchConnection.ConnectAsync(address, WatchOf(key),
                new WatchOptions { ConnectionTimeout = Deadline });
            await using var writer = await CeleriantClient.ConnectAsync(address, connectionTimeout: Deadline);
            await writer.WriteAsync(key, OneEvent(), Guid.NewGuid());
            await AssertSees(watch, key, "WatchConnection");
        },
        ["CeleriantClient write"] = async (address, key) =>
        {
            await using var client = await CeleriantClient.ConnectAsync(address, connectionTimeout: Deadline);
            var written = await client.WriteAsync(key, OneEvent(), Guid.NewGuid());
            Assert.Equal(1, written.MaxAggregateVersion);
        },
    };

    public static TheoryData<string> NoIdentityKindNames() => new(NoIdentityKinds.Keys);

    [SkippableTheory]
    [MemberData(nameof(NoIdentityKindNames))]
    public async Task EveryConnectionKindWorksWithoutIdentityAgainstAPlainServer(string kind)
    {
        Skip.If(RustServerProcess.SkipReason is not null, RustServerProcess.SkipReason);
        await using var server = await Start(ServerKind.Plain);

        Exception? failure = await Record.ExceptionAsync(() =>
            NoIdentityKinds[kind](server.Address, new AggregateKey(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid())).WaitAsync(Deadline * 3));

        Assert.True(failure is null, $"row '{kind}': {failure}{Environment.NewLine}server log:{Environment.NewLine}{server.LogTail()}");
    }

    // ---------------------------------------------------------------------------------------------
    // A configured identity still works
    // ---------------------------------------------------------------------------------------------

    public static TheoryData<string> IdentityRows() => new() { "key pair, --require-client-identity", "API key, keys file" };

    [SkippableTheory]
    [MemberData(nameof(IdentityRows))]
    public async Task APoolWithAnIdentityWritesAndWatches(string row)
    {
        Skip.If(RustServerProcess.SkipReason is not null, RustServerProcess.SkipReason);
        bool keyPair = row.StartsWith("key pair");
        await using var server = await Start(keyPair ? ServerKind.Flag : ServerKind.Keys);

        using var rsa = RSA.Create(2048);
        string pub = Convert.ToBase64String(rsa.ExportSubjectPublicKeyInfo());
        string priv = Convert.ToBase64String(rsa.ExportPkcs8PrivateKey());
        ClientIdentityConfig identity = keyPair
            ? ClientIdentityConfig.FromRsaKeyPair(pub, priv)
            : ClientIdentityConfig.FromApiKey(Convert.ToBase64String(IssuedKey));
        // Under the flag a write must carry the verified id; under a keys file any id writes.
        Guid clientId = keyPair ? CeleriantCrypto.GenerateClientIdentity(pub) : Guid.NewGuid();
        var key = new AggregateKey(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());

        Exception? failure = await Record.ExceptionAsync(async () =>
        {
            await using var pool = Pool(server.Address, identity);
            await using var watch = await pool.WatchAsync(WatchOf(key));
            var written = await pool.WriteAsync(key, OneEvent(), clientId);
            Assert.Equal(1, written.MaxAggregateVersion);
            await AssertSees(watch, key, row);
        });

        Assert.True(failure is null, $"row '{row}': {failure}{Environment.NewLine}server log:{Environment.NewLine}{server.LogTail()}");
    }

    // ---------------------------------------------------------------------------------------------
    // Typed identity errors
    // ---------------------------------------------------------------------------------------------

    /// <summary>
    /// The operation that meets the handshake first, per entry point: the first request on a pool or
    /// a standalone client, connect on a watch. A standalone client with an identity identifies explicitly.
    /// </summary>
    private static readonly Dictionary<string, Func<string, ClientIdentityConfig?, Task>> Entries = new()
    {
        ["pool write"] = async (address, identity) =>
        {
            await using var pool = Pool(address, identity);
            await pool.WriteAsync(new AggregateKey(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid()), OneEvent(), Guid.NewGuid());
        },
        ["pool.WatchAsync"] = async (address, identity) =>
        {
            await using var pool = Pool(address, identity);
            await using var watch = await pool.WatchAsync(WatchOf(new AggregateKey(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid())));
        },
        ["WatchConnection.ConnectAsync"] = async (address, identity) =>
        {
            await using var watch = await WatchConnection.ConnectAsync(address,
                WatchOf(new AggregateKey(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid())),
                new WatchOptions { IdentityConfig = identity, ConnectionTimeout = Deadline });
        },
        ["CeleriantClient"] = async (address, identity) =>
        {
            await using var client = await CeleriantClient.ConnectAsync(address, connectionTimeout: Deadline);
            if (identity is null)
                await client.WriteAsync(new AggregateKey(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid()), OneEvent(), Guid.NewGuid());
            else
                await client.IdentifyAsync(identity);
        },
    };

    private static readonly Dictionary<string, (ServerKind Server, bool UnissuedApiKey, Type Expected, uint Code)> Rejections = new()
    {
        ["flag, no identity"] = (ServerKind.Flag, false, typeof(IdentityRequiredException), ErrorResponse.IdentifyRequired),
        ["flag and keys file, no identity"] = (ServerKind.FlagAndKeys, false, typeof(IdentityRequiredException), ErrorResponse.IdentifyRequired),
        ["keys file, no API key"] = (ServerKind.Keys, false, typeof(AuthRequiredException), ErrorResponse.AuthRequired),
        ["no keys file, API key"] = (ServerKind.Plain, true, typeof(AuthInvalidKeyException), ErrorResponse.AuthInvalidKey),
    };

    public static TheoryData<string, string> RejectionByEntry()
    {
        var rows = new TheoryData<string, string>();
        foreach (string rejection in Rejections.Keys)
            foreach (string entry in Entries.Keys)
                rows.Add(rejection, entry);
        return rows;
    }

    [SkippableTheory]
    [MemberData(nameof(RejectionByEntry))]
    public async Task ARejectedIdentifyReachesTheCallerAsItsOwnTypedError(string rejection, string entry)
    {
        Skip.If(RustServerProcess.SkipReason is not null, RustServerProcess.SkipReason);
        var (kind, unissuedApiKey, expected, code) = Rejections[rejection];
        await using var server = await Start(kind);
        ClientIdentityConfig? identity = unissuedApiKey
            ? ClientIdentityConfig.FromApiKey(Convert.ToBase64String(RandomNumberGenerator.GetBytes(32)))
            : null;

        Exception? failure = await Record.ExceptionAsync(() => Entries[entry](server.Address, identity).WaitAsync(Deadline * 2));

        string row = $"row '{rejection}' x '{entry}'";
        Assert.True(failure is not null, $"{row}: the operation succeeded against a server that must reject it");
        Assert.True(failure.GetType() == expected,
            $"{row}: expected {expected.Name} thrown directly, got {failure.GetType().Name}: {failure.Message} (inner: {failure.InnerException?.GetType().Name}: {failure.InnerException?.Message})");
        uint? got = failure switch
        {
            CeleriantErrorException e => e.Error.ErrorCode,
            IdentityRequiredException e => e.Error.ErrorCode,
            _ => null,
        };
        Assert.True(got == code, $"{row}: carried error code {got}, expected {code}");
    }
}
