using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Watch;
using Celeriant.Transport;
using Xunit.Abstractions;

namespace Celeriant.Client.IntegrationTests;

/// <summary>
/// Dictionary shipping against the real Rust server: a built-in server never ships its dictionary,
/// a pool downloads a custom dictionary once across many connections and across its watch, and the
/// embedded built-in matches what a live server ships. Every scenario reads
/// <c>celeriant_identify_dictionary_bytes_shipped_total</c> before and after and asserts the delta as
/// a whole number of ships. Ports the intent of the server's
/// <c>celeriant_integration_tests/src/dictionary_handshake.rs</c>.
///
/// Run: dotnet test Celeriant.Client.IntegrationTests --filter FullyQualifiedName~LiveDictionaryShippingTests
/// (set CELERIANT_SERVER_BIN to point at a server binary other than the sibling checkout's).
/// </summary>
public sealed class LiveDictionaryShippingTests(ITestOutputHelper output)
{
    private static readonly TimeSpan Deadline = TimeSpan.FromSeconds(10);

    /// <summary>A custom dictionary in the 16 to 64 KiB band, distinct from the built-in's 14,027 bytes.</summary>
    private const int CustomLength = 20_000;

    /// <summary>How many pool connections the download-once test holds open at once, so each is its own dial.</summary>
    private const int PoolConnections = 8;

    private static readonly byte[] IssuedKey = RandomNumberGenerator.GetBytes(32);

    private static AggregateKey NewKey() => new(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());

    /// <summary>A JSON event of a few KiB with the field names the built-in dictionary trained on, so the client compresses it.</summary>
    private static byte[] LargeJson(int seq)
    {
        var sb = new StringBuilder("{\"events\":[");
        for (int i = 0; i < 80; i++)
            sb.Append($"{{\"event_type\":\"page_view\",\"user_id\":\"u{seq}-{i}\",\"timestamp\":\"2026-09-27T00:00:{i % 60:00}Z\",\"url\":\"https://example.com/p/{i}\",\"session_id\":\"s{i}\"}},");
        sb.Length--;
        sb.Append("]}");
        return Encoding.UTF8.GetBytes(sb.ToString());
    }

    private static AggregateEvent[] Events(int seq, byte[] value) =>
        [new() { ClientSeq = seq, EventTimestamp = DateTimeOffset.UtcNow, EventTypeMajor = 1, EventValue = value }];

    private static ReadRequest ReadOf(AggregateKey key) => new() { AggregateKey = key, Filters = ReadFilters.From(1) };

    private static WatchRequest WatchOf(AggregateKey key) => new()
    {
        Aggregates = [key.AggregateId],
        RequestedLatency = TimeSpan.FromMilliseconds(100),
    };

    private static CeleriantPool Pool(string address, ClientIdentityConfig? identity = null, int maxConnections = 10) => new(new CeleriantPoolOptions
    {
        Address = address,
        IdentityConfig = identity,
        MaxConnections = maxConnections,
        ConnectionTimeout = Deadline,
        RequestTimeout = Deadline,
    });

    /// <summary>Every event read back for <paramref name="key"/> carries exactly <paramref name="value"/>: the dictionary in use decoded it.</summary>
    private static void AssertRoundTrip(ReadResponse read, byte[] value, string step)
    {
        var events = read.EventBatches.SelectMany(b => b.Events).ToArray();
        Assert.True(events.Length == 1, $"{step}: read back {events.Length} events, expected 1");
        Assert.True(events[0].EventValue.AsSpan().SequenceEqual(value), $"{step}: the event read back differs from the event written");
    }

    private static async Task AssertSees(WatchConnection watch, AggregateKey key, string step)
    {
        var clock = Stopwatch.StartNew();
        while (clock.Elapsed < Deadline)
        {
            WatchResponse? batch = await watch.NextAsync(TimeSpan.FromSeconds(1));
            if (batch?.Events.Any(e => e.AggregateId == key.AggregateId) == true)
                return;
        }
        Assert.Fail($"{step}: the watch never reported the write to {key.AggregateId}");
    }

    /// <summary>A delta of exactly <paramref name="ships"/> × <paramref name="dictLength"/> bytes, or a message naming the step and the ships counted.</summary>
    private static void AssertShips(string step, long delta, int dictLength, int ships, RustServerProcess server)
    {
        long expected = (long)ships * dictLength;
        Assert.True(delta == expected,
            $"{step}: {LiveDictionaryProbe.ShippedCounter} grew by {delta} bytes ({(double)delta / dictLength:F2} ships of {dictLength}), expected exactly {ships} ship(s) = {expected}"
            + $"{Environment.NewLine}server log:{Environment.NewLine}{server.LogTail()}");
    }

    // ---------------------------------------------------------------------------------------------
    // Decision: the embedded bytes are the bytes a live built-in server holds
    // ---------------------------------------------------------------------------------------------

    /// <summary>
    /// A raw Identify with no known sha makes a built-in server ship its dictionary: its sha must be
    /// <see cref="BuiltinDictionary.Dict"/>'s sha and its bytes the embedded bytes. The ship also proves
    /// the counter moves by exactly the dictionary length, so the zero deltas from a built-in server mean something.
    /// </summary>
    [SkippableFact]
    public async Task TheEmbeddedDictionaryIsTheOneALiveBuiltinServerShips()
    {
        Skip.If(RustServerProcess.SkipReason is not null, RustServerProcess.SkipReason);
        await using var server = await RustServerProcess.StartAsync(new RustServerOptions());

        long before = await LiveDictionaryProbe.Shipped(server);
        RawIdentifyReply reply = await LiveDictionaryProbe.RawIdentify(server.Address, knownSha: null);
        long delta = await LiveDictionaryProbe.ShippedSince(server, before);

        Assert.Equal(BuiltinDictionary.Dict.Sha, reply.Sha);
        Assert.NotNull(reply.Bytes);
        Assert.Equal(14_027, BuiltinDictionary.Dict.Bytes.Length);
        Assert.True(reply.Bytes.AsSpan().SequenceEqual(BuiltinDictionary.Dict.Bytes),
            $"live built-in server shipped {reply.Bytes.Length} bytes that differ from the embedded {BuiltinDictionary.Dict.Bytes.Length}");
        Assert.Equal(BuiltinDictionary.Dict.Sha, LiveDictionaryProbe.Sha(reply.Bytes));
        AssertShips("raw Identify, no known sha, built-in server (harness control)", delta, BuiltinDictionary.Dict.Bytes.Length, 1, server);

        before = await LiveDictionaryProbe.Shipped(server);
        RawIdentifyReply known = await LiveDictionaryProbe.RawIdentify(server.Address, BuiltinDictionary.Dict.Sha);
        Assert.Equal(BuiltinDictionary.Dict.Sha, known.Sha);
        Assert.Null(known.Bytes);
        AssertShips("raw Identify advertising the embedded sha", await LiveDictionaryProbe.ShippedSince(server, before), BuiltinDictionary.Dict.Bytes.Length, 0, server);
    }

    // ---------------------------------------------------------------------------------------------
    // A built-in server never ships, on any connection kind
    // ---------------------------------------------------------------------------------------------

    private enum BuiltinServer { Plain, ApiKeys, KeyPairFlag }

    /// <summary>Each fresh client kind doing real work, large dictionary-compressed payloads included.</summary>
    private static readonly Dictionary<string, (BuiltinServer Server, Func<string, Task> Run)> BuiltinSteps = new()
    {
        ["fresh pool, no identity: write, read"] = (BuiltinServer.Plain, async address =>
        {
            await using var pool = Pool(address);
            var key = NewKey();
            byte[] value = LargeJson(1);
            await pool.WriteAsync(key, Events(1, value), Guid.NewGuid());
            AssertRoundTrip(await pool.ReadAsync(ReadOf(key)), value, "pool");
        }),
        ["fresh standalone CeleriantClient: write, read"] = (BuiltinServer.Plain, async address =>
        {
            await using var client = await CeleriantClient.ConnectAsync(address, connectionTimeout: Deadline);
            var key = NewKey();
            byte[] value = LargeJson(2);
            await client.WriteAsync(key, Events(1, value), Guid.NewGuid());
            AssertRoundTrip(await client.ReadAsync(ReadOf(key)), value, "CeleriantClient");
        }),
        ["fresh standalone WatchConnection"] = (BuiltinServer.Plain, async address =>
        {
            var key = NewKey();
            await using var watch = await WatchConnection.ConnectAsync(address, WatchOf(key), new WatchOptions { ConnectionTimeout = Deadline });
        }),
        ["fresh pool.WatchAsync"] = (BuiltinServer.Plain, async address =>
        {
            await using var pool = Pool(address);
            await using var watch = await pool.WatchAsync(WatchOf(NewKey()));
        }),
        ["fresh pool, API key identity: write, read"] = (BuiltinServer.ApiKeys, async address =>
        {
            await using var pool = Pool(address, ClientIdentityConfig.FromApiKey(Convert.ToBase64String(IssuedKey)));
            var key = NewKey();
            byte[] value = LargeJson(3);
            await pool.WriteAsync(key, Events(1, value), Guid.NewGuid());
            AssertRoundTrip(await pool.ReadAsync(ReadOf(key)), value, "pool with API key");
        }),
        ["fresh pool, key pair identity under --require-client-identity: write, read"] = (BuiltinServer.KeyPairFlag, async address =>
        {
            using var rsa = RSA.Create(2048);
            string pub = Convert.ToBase64String(rsa.ExportSubjectPublicKeyInfo());
            var identity = ClientIdentityConfig.FromRsaKeyPair(pub, Convert.ToBase64String(rsa.ExportPkcs8PrivateKey()));
            await using var pool = Pool(address, identity);
            var key = NewKey();
            byte[] value = LargeJson(4);
            await pool.WriteAsync(key, Events(1, value), CeleriantCrypto.GenerateClientIdentity(pub));
            AssertRoundTrip(await pool.ReadAsync(ReadOf(key)), value, "pool with key pair");
        }),
    };

    public static TheoryData<string> BuiltinStepNames() => new(BuiltinSteps.Keys);

    [SkippableTheory]
    [MemberData(nameof(BuiltinStepNames))]
    public async Task ABuiltinServerNeverShipsItsDictionary(string step)
    {
        Skip.If(RustServerProcess.SkipReason is not null, RustServerProcess.SkipReason);
        var (kind, run) = BuiltinSteps[step];
        await using var server = await RustServerProcess.StartAsync(new RustServerOptions
        {
            ReadWriteApiKey = kind == BuiltinServer.ApiKeys ? IssuedKey : null,
            RequireClientIdentity = kind == BuiltinServer.KeyPairFlag,
        });

        long before = await LiveDictionaryProbe.Shipped(server);
        Exception? failure = await Record.ExceptionAsync(() => run(server.Address).WaitAsync(Deadline * 3));
        Assert.True(failure is null, $"built-in server '{step}': {failure}{Environment.NewLine}server log:{Environment.NewLine}{server.LogTail()}");

        AssertShips($"built-in server '{step}'", await LiveDictionaryProbe.ShippedSince(server, before), BuiltinDictionary.Dict.Bytes.Length, 0, server);
    }

    // ---------------------------------------------------------------------------------------------
    // A pool without identity downloads a custom dictionary once across many connections
    // ---------------------------------------------------------------------------------------------

    /// <summary>
    /// A write on a fresh pool learns the dictionary; then <see cref="PoolConnections"/> leases are
    /// taken one after another and held, so each past the first is a new dial, and a read goes through
    /// each. The server's open-connection gauge proves how many connections were really open.
    /// </summary>
    [SkippableFact]
    public async Task APoolWithoutIdentityDownloadsACustomDictionaryOnceAcrossManyConnections()
    {
        Skip.If(RustServerProcess.SkipReason is not null, RustServerProcess.SkipReason);
        byte[] dict = LiveDictionaryProbe.CustomDictionary(CustomLength);
        await using var server = await RustServerProcess.StartAsync(new RustServerOptions { CustomDictionary = dict });

        // Harness control: the server holds the staged dictionary and counts a ship at its length.
        long before = await LiveDictionaryProbe.Shipped(server);
        RawIdentifyReply raw = await LiveDictionaryProbe.RawIdentify(server.Address, BuiltinDictionary.Dict.Sha);
        Assert.Equal(LiveDictionaryProbe.Sha(dict), raw.Sha);
        Assert.True(raw.Bytes?.AsSpan().SequenceEqual(dict) == true, "the custom server did not ship the staged dictionary to a built-in advertiser");
        AssertShips("raw Identify advertising the built-in sha (harness control)", await LiveDictionaryProbe.ShippedSince(server, before), CustomLength, 1, server);
        Assert.Equal(0, await LiveDictionaryProbe.AwaitOpenClientConnections(server, 0, Deadline));

        before = await LiveDictionaryProbe.Shipped(server);
        var key = NewKey();
        byte[] value = LargeJson(1);
        long openWhileHeld;
        await using (var pool = Pool(server.Address, maxConnections: PoolConnections))
        {
            await pool.WriteAsync(key, Events(1, value), Guid.NewGuid());
            var held = new List<PooledConnection>();
            try
            {
                for (int i = 0; i < PoolConnections; i++)
                {
                    PooledConnection lease = await pool.GetConnectionAsync().WaitAsync(Deadline);
                    held.Add(lease);
                    AssertRoundTrip(await lease.Client.ReadAsync(ReadOf(key)), value, $"lease {i}");
                }
                openWhileHeld = await LiveDictionaryProbe.AwaitOpenClientConnections(server, PoolConnections, TimeSpan.FromSeconds(2));
            }
            finally
            {
                foreach (var lease in held)
                    await lease.DisposeAsync();
            }
        }

        long delta = await LiveDictionaryProbe.ShippedSince(server, before);
        output.WriteLine($"pool without identity: {openWhileHeld} client connections open while held; {delta} bytes shipped ({(double)delta / CustomLength:F2} ships)");
        Assert.True(openWhileHeld == PoolConnections,
            $"harness: the server saw {openWhileHeld} open client connections while {PoolConnections} leases were held");
        AssertShips($"pool without identity, {PoolConnections} connections", delta, CustomLength, 1, server);
    }

    // ---------------------------------------------------------------------------------------------
    // A pool's watch and its data connections share one download
    // ---------------------------------------------------------------------------------------------

    public static TheoryData<string> WatchWriteOrders() => new() { "watch then write", "write then watch" };

    /// <summary>
    /// On a fresh pool, <see cref="CeleriantPool.WatchAsync"/> and a write (plus a read) in either
    /// order. The watch is its own connection, so without a shared cache each would ship.
    /// </summary>
    [SkippableTheory]
    [MemberData(nameof(WatchWriteOrders))]
    public async Task APoolWatchAndAPoolWriteShareOneDownload(string order)
    {
        Skip.If(RustServerProcess.SkipReason is not null, RustServerProcess.SkipReason);
        byte[] dict = LiveDictionaryProbe.CustomDictionary(CustomLength);
        await using var server = await RustServerProcess.StartAsync(new RustServerOptions { CustomDictionary = dict });
        var key = NewKey();
        byte[] value = LargeJson(1);

        long before = await LiveDictionaryProbe.Shipped(server);
        long afterFirst = 0, open = 0;
        Exception? failure = await Record.ExceptionAsync(async () =>
        {
            await using var pool = Pool(server.Address);
            if (order == "watch then write")
            {
                await using var watch = await pool.WatchAsync(WatchOf(key));
                afterFirst = await LiveDictionaryProbe.ShippedSince(server, before);
                await pool.WriteAsync(key, Events(1, value), Guid.NewGuid());
                AssertRoundTrip(await pool.ReadAsync(ReadOf(key)), value, order);
                await AssertSees(watch, key, order);
                open = await LiveDictionaryProbe.OpenClientConnections(server);
            }
            else
            {
                await pool.WriteAsync(key, Events(1, value), Guid.NewGuid());
                AssertRoundTrip(await pool.ReadAsync(ReadOf(key)), value, order);
                afterFirst = await LiveDictionaryProbe.ShippedSince(server, before);
                await using var watch = await pool.WatchAsync(WatchOf(key));
                await pool.WriteAsync(key, Events(2, LargeJson(2)), Guid.NewGuid());
                await AssertSees(watch, key, order);
                open = await LiveDictionaryProbe.OpenClientConnections(server);
            }
        }).WaitAsync(Deadline * 3);
        Assert.True(failure is null, $"watch and write '{order}': {failure}{Environment.NewLine}server log:{Environment.NewLine}{server.LogTail()}");

        long total = await LiveDictionaryProbe.ShippedSince(server, before);
        output.WriteLine($"watch and write '{order}': after the first step {afterFirst} bytes; total {total}; {open} client connections open at the end");
        Assert.True(open >= 2, $"harness: expected the watch and the data connection to be separate server connections, saw {open}");
        AssertShips($"watch and write '{order}'", total, CustomLength, 1, server);
    }

    // ---------------------------------------------------------------------------------------------
    // Documentation: a standalone WatchConnection owns its own cache
    // ---------------------------------------------------------------------------------------------

    /// <summary>
    /// Two standalone <see cref="WatchConnection"/>s on one custom server. Neither has a pool cache,
    /// so the expected (Rust D11) result is a ship each. Asserted loosely: each connect ships at most
    /// once, and the pair ships at least once. The exact count is written to the test output.
    /// </summary>
    [SkippableFact]
    public async Task StandaloneWatchConnectionsEachOwnTheirCache()
    {
        Skip.If(RustServerProcess.SkipReason is not null, RustServerProcess.SkipReason);
        byte[] dict = LiveDictionaryProbe.CustomDictionary(CustomLength);
        await using var server = await RustServerProcess.StartAsync(new RustServerOptions { CustomDictionary = dict });

        var perConnect = new List<long>();
        for (int i = 0; i < 2; i++)
        {
            long before = await LiveDictionaryProbe.Shipped(server);
            await using (await WatchConnection.ConnectAsync(server.Address, WatchOf(NewKey()), new WatchOptions { ConnectionTimeout = Deadline }))
            {
            }
            perConnect.Add(await LiveDictionaryProbe.ShippedSince(server, before));
        }

        output.WriteLine($"standalone WatchConnection ships per connect: {string.Join(", ", perConnect.Select(d => $"{d} bytes"))}");
        Assert.All(perConnect, d => Assert.True(d == 0 || d == CustomLength, $"a standalone connect shipped {d} bytes, not 0 or one dictionary"));
        Assert.True(perConnect.Sum() >= CustomLength, "no standalone connect downloaded the custom dictionary, yet neither had it");
    }
}
