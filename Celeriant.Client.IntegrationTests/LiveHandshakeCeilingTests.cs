using System.Diagnostics;
using System.Security.Cryptography;
using Celeriant.Client.Errors;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Watch;
using Celeriant.Transport;

namespace Celeriant.Client.IntegrationTests;

/// <summary>
/// The handshake size limit against the real Rust server. The server holds a pre-staged custom
/// dictionary near or at the 1 MiB ceiling, so its V5 IdentifyResponse is 1 to 2 MiB. Every
/// connection kind, capped at 4096 response bytes, still completes the handshake and serves small
/// data, and a data or watch frame past 4096 bytes is still a <see cref="ProtocolException"/>.
/// Ports the server's <c>celeriant_integration_tests/src/dictionary_ceiling.rs</c>.
///
/// Run: dotnet test Celeriant.Client.IntegrationTests --filter FullyQualifiedName~LiveHandshakeCeilingTests
/// (set CELERIANT_SERVER_BIN to point at a server binary other than the sibling checkout's).
/// </summary>
public sealed class LiveHandshakeCeilingTests
{
    private static readonly TimeSpan Deadline = TimeSpan.FromSeconds(15);
    private const long TinyCap = 4096;

    /// <summary>
    /// Deterministic noise, as the Rust test generates it. With <paramref name="allHigh"/>, every byte
    /// but the first has its top bit set, so each costs two bytes in the V5 integer array. The first
    /// byte is 'D', so the file never opens with the zstd dictionary magic and the server adopts it as
    /// a content-only dictionary.
    /// </summary>
    private static byte[] DictionaryBytes(int length, bool allHigh)
    {
        var dict = new byte[length];
        ulong state = 0x9E37_79B9_7F4A_7C15UL;
        for (int i = 0; i < length; i++)
        {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            dict[i] = allHigh ? (byte)(0x80 | (byte)state) : (byte)state;
        }
        dict[0] = (byte)'D';
        return dict;
    }

    /// <summary>(dictionary length, all bytes high): about 1,000,000 noise bytes, and exactly the ceiling with every byte high.</summary>
    private static readonly Dictionary<string, (int Length, bool AllHigh)> Dictionaries = new()
    {
        ["1,000,000 noise bytes"] = (1_000_000, false),
        ["exactly 1 MiB, all bytes >= 0x80"] = (HandshakeLimits.DictionaryCeilingBytes, true),
    };

    private static AggregateKey NewKey() => new(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());

    private static AggregateEvent Event(int seq, string json) =>
        new() { ClientSeq = seq, EventTimestamp = DateTimeOffset.UtcNow, EventTypeMajor = 1, EventValue = System.Text.Encoding.UTF8.GetBytes(json) };

    private static AggregateEvent[] Small() => [Event(1, "{\"dict\":1}")];

    /// <summary>An event whose value is random hex: past 4096 bytes on the wire even after dictionary compression.</summary>
    private static AggregateEvent[] Large() => [Event(1, $"{{\"blob\":\"{Convert.ToHexString(RandomNumberGenerator.GetBytes(16 * 1024))}\"}}")];

    private static CeleriantPool Pool(string address, long? cap) => new(new CeleriantPoolOptions
    {
        Address = address,
        ConnectionTimeout = Deadline,
        RequestTimeout = Deadline,
        MaxResponseSize = cap ?? 64 * 1024 * 1024,
    });

    private static WatchRequest WatchOrg(AggregateKey key) => new()
    {
        Orgs = [key.OrgId],
        RequestedLatency = TimeSpan.FromMilliseconds(100),
    };

    private static ReadRequest ReadOf(AggregateKey key) => new() { AggregateKey = key, Filters = ReadFilters.From(1) };

    /// <summary>
    /// Each connection kind with response cap <c>cap</c> (null: the default): handshake, write a small
    /// event, read it back, and open a watch, the way the Rust <c>pool_round_trip</c> does. Standalone
    /// watches need a writer, which is a default-cap client so only the watch is under test.
    /// </summary>
    private static readonly Dictionary<string, Func<string, long?, Task>> RoundTrips = new()
    {
        ["pool write, read, watch"] = async (address, cap) =>
        {
            await using var pool = Pool(address, cap);
            var key = NewKey();
            await pool.WriteAsync(key, Small(), Guid.NewGuid());
            var read = await pool.ReadAsync(ReadOf(key));
            Assert.Equal(1, read.EventBatches.Sum(b => b.Events.Length));
            await using var watch = await pool.WatchAsync(WatchOrg(key));
        },
        ["WatchConnection"] = async (address, cap) =>
        {
            await using var watch = await WatchConnection.ConnectAsync(address, WatchOrg(NewKey()),
                new WatchOptions { ConnectionTimeout = Deadline, MaxResponseSize = cap });
        },
        ["CeleriantClient write, read"] = async (address, cap) =>
        {
            await using var client = await CeleriantClient.ConnectAsync(address, connectionTimeout: Deadline);
            client.WithTimeout(Deadline);
            if (cap is not null)
                client.WithMaxResponseSize(cap.Value);
            var key = NewKey();
            await client.WriteAsync(key, Small(), Guid.NewGuid());
            var read = await client.ReadAsync(ReadOf(key));
            Assert.Equal(1, read.EventBatches.Sum(b => b.Events.Length));
        },
    };

    public static TheoryData<string, string, string> DictionaryByKindByCap()
    {
        var rows = new TheoryData<string, string, string>();
        foreach (string dict in Dictionaries.Keys)
            foreach (string kind in RoundTrips.Keys)
                foreach (string cap in new[] { "4096", "default" })
                    rows.Add(dict, kind, cap);
        return rows;
    }

    /// <summary>
    /// The handshake is not data and does not meet the caller cap: a 4096-byte cap round-trips against
    /// a server holding a near-ceiling or exact-ceiling dictionary. The default-cap rows are the control
    /// proving the staged dictionary is usable, so the tiny-cap rows fail on the cap and nothing else.
    /// </summary>
    [SkippableTheory]
    [MemberData(nameof(DictionaryByKindByCap))]
    public async Task EveryConnectionKindHandshakesAgainstALargeDictionaryWhateverTheCap(string dict, string kind, string cap)
    {
        Skip.If(RustServerProcess.SkipReason is not null, RustServerProcess.SkipReason);
        var (length, allHigh) = Dictionaries[dict];
        await using var server = await RustServerProcess.StartAsync(new RustServerOptions { CustomDictionary = DictionaryBytes(length, allHigh) });

        Exception? failure = await Record.ExceptionAsync(() =>
            RoundTrips[kind](server.Address, cap == "default" ? null : long.Parse(cap)).WaitAsync(Deadline * 3));

        Assert.True(failure is null,
            $"row '{dict}' x '{kind}' x cap {cap}: {failure}{Environment.NewLine}server log:{Environment.NewLine}{server.LogTail()}");
    }

    /// <summary>
    /// After a 1,000,000-byte-dictionary handshake through a 4096-byte-cap pool, a read whose
    /// reply is past 4096 bytes is a <see cref="ProtocolException"/>. The small read first proves the
    /// handshake succeeded, so the exception comes from the data frame.
    /// </summary>
    [SkippableFact]
    public async Task AReadPastTheCapStillErrorsAfterALargeHandshake()
    {
        Skip.If(RustServerProcess.SkipReason is not null, RustServerProcess.SkipReason);
        await using var server = await RustServerProcess.StartAsync(new RustServerOptions { CustomDictionary = DictionaryBytes(1_000_000, allHigh: false) });
        await using var pool = Pool(server.Address, TinyCap);

        var small = NewKey();
        var large = NewKey();
        Exception? setup = await Record.ExceptionAsync(async () =>
        {
            await pool.WriteAsync(small, Small(), Guid.NewGuid());
            await pool.ReadAsync(ReadOf(small));
            await pool.WriteAsync(large, Large(), Guid.NewGuid());
        });
        Assert.True(setup is null, $"the handshake or the small operations failed under a {TinyCap}-byte cap: {setup}{Environment.NewLine}{server.LogTail()}");

        Exception? failure = await Record.ExceptionAsync(() => pool.ReadAsync(ReadOf(large)).WaitAsync(Deadline));
        Assert.True(failure is ProtocolException, $"a read reply past {TinyCap} bytes must be a ProtocolException, got {failure?.GetType().Name ?? "success"}: {failure?.Message}");
    }

    public static TheoryData<string> WatchKinds() => new() { "pool.WatchAsync", "WatchConnection" };

    /// <summary>
    /// A watch capped at 4096 bytes opens against a 1,000,000-byte-dictionary server, then one
    /// write of 300 aggregates in its org produces a watch frame well past 4096 bytes, which must be a
    /// <see cref="ProtocolException"/>. The writer is a separate default-cap pool.
    /// </summary>
    [SkippableTheory]
    [MemberData(nameof(WatchKinds))]
    public async Task AWatchFramePastTheCapStillErrorsAfterALargeHandshake(string kind)
    {
        Skip.If(RustServerProcess.SkipReason is not null, RustServerProcess.SkipReason);
        await using var server = await RustServerProcess.StartAsync(new RustServerOptions { CustomDictionary = DictionaryBytes(1_000_000, allHigh: false) });
        await using var watchPool = Pool(server.Address, TinyCap);
        await using var writer = Pool(server.Address, null);
        var org = Guid.NewGuid();

        WatchConnection? watch = null;
        Exception? open = await Record.ExceptionAsync(async () => watch = kind == "pool.WatchAsync"
            ? await watchPool.WatchAsync(WatchOrg(new AggregateKey(org, Guid.NewGuid(), Guid.NewGuid())))
            : await WatchConnection.ConnectAsync(server.Address, WatchOrg(new AggregateKey(org, Guid.NewGuid(), Guid.NewGuid())),
                new WatchOptions { ConnectionTimeout = Deadline, MaxResponseSize = TinyCap }));
        Assert.True(open is null, $"'{kind}': the watch did not open under a {TinyCap}-byte cap: {open}{Environment.NewLine}{server.LogTail()}");

        await using (watch!)
        {
            var type = Guid.NewGuid();
            await writer.WriteAsync(new WriteRequest
            {
                ClientId = Guid.NewGuid(),
                Writes = Enumerable.Range(0, 300).ToDictionary(
                    _ => new AggregateKey(org, type, Guid.NewGuid()),
                    _ => new SingleAggregateWrite { Events = Small(), AllowCreate = true }),
            });

            Exception? failure = null;
            var clock = Stopwatch.StartNew();
            while (failure is null && clock.Elapsed < Deadline)
                failure = await Record.ExceptionAsync(() => watch!.NextAsync(TimeSpan.FromSeconds(1)));

            Assert.True(failure is ProtocolException,
                $"'{kind}': a watch frame of 300 events past {TinyCap} bytes must be a ProtocolException, got {failure?.GetType().Name ?? "nothing within the deadline"}: {failure?.Message}");
        }
    }

    /// <summary>
    /// Server side of the ceiling, documented here: a dictionary one byte past 1 MiB refuses to start,
    /// naming the ceiling. Green whatever the client does.
    /// </summary>
    [SkippableFact]
    public async Task AServerRefusesToStartWithADictionaryPastTheCeiling()
    {
        Skip.If(RustServerProcess.SkipReason is not null, RustServerProcess.SkipReason);

        Exception? failure = await Record.ExceptionAsync(async () =>
        {
            await using var server = await RustServerProcess.StartAsync(new RustServerOptions
            {
                CustomDictionary = DictionaryBytes(HandshakeLimits.DictionaryCeilingBytes + 1, allHigh: false),
            });
        });

        Assert.True(failure is InvalidOperationException, $"a {HandshakeLimits.DictionaryCeilingBytes + 1}-byte dictionary must stop the server at startup, got {failure?.GetType().Name ?? "a running server"}: {failure?.Message}");
        string text = failure!.Message.ToLowerInvariant();
        Assert.True(text.Contains("1048576") || text.Contains("1 mib") || text.Contains("1mib"),
            $"the startup failure must name the {HandshakeLimits.DictionaryCeilingBytes}-byte ceiling: {failure.Message}");
    }
}
