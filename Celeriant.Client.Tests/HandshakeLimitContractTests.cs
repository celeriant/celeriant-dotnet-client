using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Watch;
using Celeriant.Transport;
using MessagePack;

namespace Celeriant.Client.Tests;

/// <summary>
/// The handshake size limit on a fake. The reply to
/// Identify is read under <see cref="HandshakeLimits.IdentifyResponseMaxBytes"/>, not the caller's
/// response cap: an IdentifyResponse up to that limit is accepted under any cap, one advertised past
/// it is refused from the header alone, a non-Identify reply to Identify is bounded by
/// min(caller cap, limit) from the header, and data frames stay under the caller's cap.
///
/// Ports the intent of the Rust client's <c>identify_ceiling_contract.rs</c> to the V5 MessagePack wire, where the dictionary is an
/// integer array and a byte at or above 0x80 costs two bytes.
///
/// Run: dotnet test Celeriant.Client.Tests --filter FullyQualifiedName~HandshakeLimitContractTests
/// </summary>
public class HandshakeLimitContractTests
{
    /// <summary>A refusal from the header has no reason to take longer than this.</summary>
    private static readonly TimeSpan Prompt = TimeSpan.FromSeconds(2);

    /// <summary>Every connect and request timeout, far past <see cref="Prompt"/>, so a hang is not rescued into a timeout error.</summary>
    private static readonly TimeSpan Generous = TimeSpan.FromSeconds(30);

    private const long TinyCap = 4096;
    private const long GiB = 1L << 30;
    private const uint MiB = 1024 * 1024;
    private static readonly uint Limit = (uint)HandshakeLimits.IdentifyResponseMaxBytes;

    // ---------------------------------------------------------------------------------------------
    // The contract constants
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public void TheLimitIsTwiceTheDictionaryCeilingPlusTheEnvelopeAllowance()
    {
        Assert.Equal(1_048_576, HandshakeLimits.DictionaryCeilingBytes);
        Assert.Equal(2_097_280, HandshakeLimits.IdentifyResponseMaxBytes);
    }

    // ---------------------------------------------------------------------------------------------
    // Connection kinds
    // ---------------------------------------------------------------------------------------------

    /// <summary>
    /// Each connection kind, opened with response cap <c>cap</c> (null keeps the kind's default) and
    /// driven through the operation that meets the handshake first: the first write on a pool or a
    /// standalone client, connect on a watch. Returns once that operation succeeded.
    /// </summary>
    private static readonly Dictionary<string, Func<string, long?, Task>> Kinds = new()
    {
        ["pool write"] = async (address, cap) =>
        {
            await using var pool = Pool(address, cap);
            await pool.WriteAsync(OccTestData.Write());
        },
        ["pool.WatchAsync"] = async (address, cap) =>
        {
            await using var pool = Pool(address, cap);
            await using var watch = await pool.WatchAsync(AnyWatch());
        },
        ["WatchConnection.ConnectAsync"] = async (address, cap) =>
        {
            await using var watch = await WatchConnection.ConnectAsync(address, AnyWatch(), WatchOptionsFor(cap));
        },
        ["CeleriantClient write"] = async (address, cap) =>
        {
            await using var client = await Client(address, cap);
            await client.WriteAsync(OccTestData.Write());
        },
    };

    private static readonly long DefaultPoolCap = new CeleriantPoolOptions { Address = "127.0.0.1:1" }.MaxResponseSize;

    private static CeleriantPool Pool(string address, long? cap) => new(new CeleriantPoolOptions
    {
        Address = address,
        MaxConnections = 1,
        ConnectionTimeout = Generous,
        RequestTimeout = Generous,
        MaxResponseSize = cap ?? DefaultPoolCap,
    });

    private static WatchOptions WatchOptionsFor(long? cap) => new() { ConnectionTimeout = Generous, MaxResponseSize = cap };

    private static async Task<CeleriantClient> Client(string address, long? cap)
    {
        var client = await CeleriantClient.ConnectAsync(address, connectionTimeout: Generous);
        client.WithTimeout(Generous);
        if (cap is not null)
            client.WithMaxResponseSize(cap.Value);
        return client;
    }

    private static WatchRequest AnyWatch() => new() { Aggregates = [Guid.NewGuid()] };

    private static string CapName(long? cap) => cap is null ? "default cap" : $"cap {cap}";

    /// <summary>
    /// Run <paramref name="operation"/> with a hard bound. A hang comes back as a <see cref="TimeoutException"/>
    /// so the assertion can name it; the abandoned operation is left to the server's disposal.
    /// </summary>
    private static async Task<(Exception? Failure, TimeSpan Elapsed)> Bounded(Func<Task> operation, TimeSpan bound)
    {
        var clock = Stopwatch.StartNew();
        Exception? failure = await Record.ExceptionAsync(() => Task.Run(operation).WaitAsync(bound));
        return (failure, clock.Elapsed);
    }

    private static string Describe(Exception? failure) => failure switch
    {
        null => "success",
        TimeoutException => "no answer: the client is waiting for the advertised body",
        _ => $"{failure.GetType().Name}: {failure.Message} (inner: {failure.InnerException?.GetType().Name})",
    };

    // ---------------------------------------------------------------------------------------------
    // Wire builders
    // ---------------------------------------------------------------------------------------------

    /// <summary>A bare 17-byte V5 header advertising the given lengths; no body follows.</summary>
    private static byte[] HeaderOnly(uint type, uint compressed, uint uncompressed, CompressionType compression = CompressionType.None)
    {
        var raw = new byte[WireHeader.Size];
        new WireHeader(WireHeader.ProtocolVersionV5, type, compressed, uncompressed, (byte)compression).WriteTo(raw);
        return raw;
    }

    /// <summary>A 1 MiB dictionary whose every byte but the first is at or above 0x80, so each costs two msgpack bytes. Does not start with the zstd dictionary magic.</summary>
    private static readonly byte[] MiBHighDict = HighByteDict(HandshakeLimits.DictionaryCeilingBytes);

    private static byte[] HighByteDict(int length)
    {
        var dict = new byte[length];
        ulong state = 0x9E37_79B9_7F4A_7C15UL;
        for (int i = 0; i < length; i++)
        {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            dict[i] = (byte)(0x80 | (byte)state);
        }
        dict[0] = (byte)'D';
        return dict;
    }

    /// <summary>
    /// An IdentifyResponse body as the Rust server writes it on V5: a positional array whose dictionary
    /// is an integer array. With <paramref name="padTo"/>, a trailing bin element (an unknown field the
    /// client must skip) pads the body to exactly that many bytes.
    /// </summary>
    private static byte[] RustIdentifyResponse(Guid? correlation, byte[] dict, bool shipBytes, int? padTo = null)
    {
        byte[] Encode(int? padLength)
        {
            var buffer = new ArrayBufferWriter<byte>();
            var writer = new MessagePackWriter(buffer);
            writer.WriteArrayHeader(padLength is null ? 5 : 6);
            CeleriantNullableGuidFormatter.Instance.Serialize(ref writer, correlation, WireCodec.Options);
            writer.WriteNil(); // client id
            writer.WriteNil(); // access level
            writer.Write(HandshakeReplies.Sha(dict));
            if (shipBytes)
            {
                writer.WriteArrayHeader(dict.Length);
                foreach (byte b in dict)
                    writer.Write(b);
            }
            else
            {
                writer.WriteNil();
            }
            if (padLength is not null)
                writer.Write(new byte[padLength.Value]);
            writer.Flush();
            return buffer.WrittenSpan.ToArray();
        }

        if (padTo is null)
            return Encode(null);
        // A bin8 header is 2 bytes; the padding is sized so the whole body lands exactly on padTo.
        int unpadded = Encode(0).Length;
        int padLength = padTo.Value - unpadded;
        if (padLength < 0 || padLength > 255)
            throw new InvalidOperationException($"padding {padLength} out of bin8 range; the fixture needs rework");
        byte[] body = Encode(padLength);
        Assert.Equal(padTo.Value, body.Length);
        return body;
    }

    /// <summary>Accept the Identify with <paramref name="dict"/> encoded the Rust way, shipping it on a sha miss.</summary>
    private static byte[] RustIdentified(HandshakeFrame frame, byte[] dict, int? padTo = null)
    {
        var identify = HandshakeReplies.Identify(frame);
        bool ship = identify.KnownDictSha256 != HandshakeReplies.Sha(dict);
        return FakeServerSession.BuildFrame(MessageTypes.Responses.Identify,
            RustIdentifyResponse(identify.CorrelationId, dict, ship, padTo));
    }

    /// <summary>Answer a Write, a Read or a Watch; <paramref name="oversize"/> makes Read and Watch replies well past 4096 bytes.</summary>
    private static RawReply ServeData(HandshakeFrame frame, bool oversize) => frame.Type switch
    {
        MessageTypes.Requests.Write => RawReply.Of(FakeServerSession.BuildFrame(MessageTypes.Responses.Write, WireCodec.Serialize(new WriteResponse
        {
            CorrelationId = WireCodec.Deserialize<WriteRequest>(frame.Body).CorrelationId,
            MaxAggregateVersion = 1,
        }))),
        MessageTypes.Requests.Read => RawReply.Of(FakeServerSession.BuildFrame(MessageTypes.Responses.Read,
            ReadReply(WireCodec.Deserialize<ReadRequest>(frame.Body).CorrelationId, oversize ? OversizeCount : 1))),
        MessageTypes.Requests.Watch => RawReply.Of(
            FakeServerSession.BuildFrame(MessageTypes.Responses.Watch, WireCodec.Serialize(new WatchResponse())),
            FakeServerSession.BuildFrame(MessageTypes.Responses.Watch, WatchEvents(oversize ? OversizeCount : 1))),
        _ => throw new InvalidOperationException($"connection {frame.Connection}: unexpected request type {frame.Type}"),
    };

    private const int OversizeCount = 600;

    private static byte[] ReadReply(Guid? correlation, int batches) => WireCodec.Serialize(new ReadResponse
    {
        CorrelationId = correlation,
        EventBatches = Enumerable.Range(1, batches)
            .Select(i => new AggregateEventBatch { AggregateVersion = i, ClientId = Guid.NewGuid(), ServerTimestamp = DateTimeOffset.UnixEpoch })
            .ToArray(),
    });

    private static byte[] WatchEvents(int count) => WireCodec.Serialize(new WatchResponse
    {
        Events = Enumerable.Range(0, count)
            .Select(_ => new WatchResponseEvent
            {
                OrgId = Guid.NewGuid(),
                AggregateTypeId = Guid.NewGuid(),
                AggregateId = Guid.NewGuid(),
                Operation = WatchOperationType.Write,
                FromAggregateVersion = 1,
                ToAggregateVersion = 1,
            })
            .ToArray(),
    });

    // ---------------------------------------------------------------------------------------------
    // An IdentifyResponse advertised past the limit is refused from the header
    // ---------------------------------------------------------------------------------------------

    /// <summary>(row, compressed length, uncompressed length, compression, caller cap). Every row advertises past the limit and sends no body.</summary>
    private static readonly Dictionary<string, (uint Compressed, uint Uncompressed, CompressionType Compression, long? Cap)> OversizeIdentify = new()
    {
        ["limit+1, cap 4096 (control: the caller cap alone refuses it)"] = (Limit + 1, Limit + 1, CompressionType.None, TinyCap),
        ["limit+1, default cap"] = (Limit + 1, Limit + 1, CompressionType.None, null),
        ["64 MiB, cap 1 GiB"] = (64 * MiB, 64 * MiB, CompressionType.None, GiB),
        ["uint.MaxValue, cap 1 GiB"] = (uint.MaxValue, uint.MaxValue, CompressionType.None, GiB),
        ["compressed 64, uncompressed limit+1, default cap"] = (64, Limit + 1, CompressionType.ZstdDict, null),
        ["compressed 64, uncompressed uint.MaxValue, cap 1 GiB"] = (64, uint.MaxValue, CompressionType.ZstdDict, GiB),
    };

    public static TheoryData<string, string> OversizeIdentifyByKind()
    {
        var rows = new TheoryData<string, string>();
        foreach (string row in OversizeIdentify.Keys)
            foreach (string kind in Kinds.Keys)
                rows.Add(row, kind);
        return rows;
    }

    /// <summary>
    /// The fake answers Identify with a header alone and then holds the socket open. The client must
    /// refuse it from the header with a <see cref="ProtocolException"/>: waiting for the body hangs past
    /// <see cref="Prompt"/>, and allocating the advertised size fails some other way (or not at all).
    /// </summary>
    [Theory]
    [MemberData(nameof(OversizeIdentifyByKind))]
    public async Task AnIdentifyResponseAdvertisedPastTheLimitIsRefusedFromTheHeader(string row, string kind)
    {
        var (compressed, uncompressed, compression, cap) = OversizeIdentify[row];
        await using var server = new RawScriptServer(frame => frame.Type == MessageTypes.Requests.Identify
            ? RawReply.Hold(HeaderOnly(MessageTypes.Responses.Identify, compressed, uncompressed, compression))
            : ServeData(frame, oversize: false));

        var (failure, elapsed) = await Bounded(() => Kinds[kind](server.Address, cap), Prompt);

        string name = $"row '{row}' x '{kind}'";
        Assert.True(failure is ProtocolException, $"{name}: expected ProtocolException from the header, got {Describe(failure)} after {elapsed.TotalSeconds:F1}s");
        Assert.True(elapsed < Prompt, $"{name}: took {elapsed.TotalSeconds:F1}s");
        Assert.DoesNotContain(server.Frames, f => f.Type != MessageTypes.Requests.Identify);
        Assert.Empty(server.Faults);
    }

    /// <summary>
    /// A pool retires the connection whose Identify was refused on size: the first connection sees
    /// nothing after its Identify, and a second request dials a fresh connection that is served.
    /// </summary>
    [Fact]
    public async Task APoolRetiresAConnectionWhoseIdentifyResponseWasOversize()
    {
        await using var server = new RawScriptServer(frame =>
            frame.Type != MessageTypes.Requests.Identify ? ServeData(frame, oversize: false)
            : frame.Connection == 1 ? RawReply.Hold(HeaderOnly(MessageTypes.Responses.Identify, Limit + 1, Limit + 1))
            : RawReply.Of(RustIdentified(frame, MiBHighDict)));
        await using var pool = Pool(server.Address, null);

        var (first, _) = await Bounded(() => pool.WriteAsync(OccTestData.Write()), Prompt);
        var (second, _) = await Bounded(() => pool.WriteAsync(OccTestData.Write()), Generous);

        var connections = server.ByConnection();
        string wire = string.Join("; ", connections.Select(c => $"conn {c.Key}: [{string.Join(",", c.Value.Select(f => f.Type))}]"));
        Assert.True(first is ProtocolException, $"first write: expected ProtocolException, got {Describe(first)}. Wire: {wire}");
        Assert.True(second is null, $"second write, on a fresh connection, failed: {Describe(second)}. Wire: {wire}");
        Assert.True(connections.TryGetValue(1, out var refused) && refused.Length == 1,
            $"the refused connection must carry only its Identify. Wire: {wire}");
        Assert.Empty(server.Faults);
    }

    // ---------------------------------------------------------------------------------------------
    // An IdentifyResponse within the limit is accepted whatever the caller cap
    // ---------------------------------------------------------------------------------------------

    /// <summary>The V5 body carrying a 1 MiB dictionary of high bytes: past 2 MiB, inside the limit.</summary>
    [Fact]
    public void AMiBHighByteDictionaryEncodesPastTwoMiBButInsideTheLimit()
    {
        int length = RustIdentifyResponse(Guid.NewGuid(), MiBHighDict, shipBytes: true).Length;
        Assert.InRange(length, 2 * HandshakeLimits.DictionaryCeilingBytes, HandshakeLimits.IdentifyResponseMaxBytes);
    }

    public static TheoryData<string, string> KindByCap()
    {
        var rows = new TheoryData<string, string>();
        foreach (string kind in Kinds.Keys)
            foreach (string cap in new[] { "4096", "default" })
                rows.Add(kind, cap);
        return rows;
    }

    private static long? ParseCap(string cap) => cap == "default" ? null : long.Parse(cap);

    /// <summary>
    /// The fake ships a 1 MiB dictionary of bytes at or above 0x80, about 2 MiB on the V5 wire. The
    /// handshake succeeds under a 4096-byte cap as under the default. The default-cap rows are the
    /// fixture control: they prove the hand-encoded reply decodes, so the tiny-cap rows fail on the cap.
    /// </summary>
    [Theory]
    [MemberData(nameof(KindByCap))]
    public async Task AnIdentifyResponseCarryingAMiBDictionaryIsAcceptedWhateverTheCap(string kind, string cap)
    {
        await using var server = new RawScriptServer(
            frame => frame.Type == MessageTypes.Requests.Identify ? RawReply.Of(RustIdentified(frame, MiBHighDict)) : ServeData(frame, oversize: false),
            MiBHighDict);

        var (failure, _) = await Bounded(() => Kinds[kind](server.Address, ParseCap(cap)), Generous);

        Assert.True(failure is null, $"'{kind}', cap {cap}: a {HandshakeLimits.DictionaryCeilingBytes}-byte dictionary handshake failed: {Describe(failure)}");
        Assert.Empty(server.Faults);
    }

    /// <summary>
    /// The bound is exact: an IdentifyResponse of exactly <see cref="HandshakeLimits.IdentifyResponseMaxBytes"/>
    /// (a 1 MiB high-byte dictionary plus a trailing unknown field) is accepted under a 4096-byte cap,
    /// and one byte more is refused (the limit+1 rows of the oversize test). The default-cap row is the fixture control.
    /// </summary>
    [Theory]
    [MemberData(nameof(KindByCap))]
    public async Task AnIdentifyResponseOfExactlyTheLimitIsAccepted(string kind, string cap)
    {
        await using var server = new RawScriptServer(
            frame => frame.Type == MessageTypes.Requests.Identify
                ? RawReply.Of(RustIdentified(frame, MiBHighDict, padTo: HandshakeLimits.IdentifyResponseMaxBytes))
                : ServeData(frame, oversize: false),
            MiBHighDict);

        var (failure, _) = await Bounded(() => Kinds[kind](server.Address, ParseCap(cap)), Generous);

        Assert.True(failure is null, $"'{kind}', cap {cap}: an IdentifyResponse of exactly {HandshakeLimits.IdentifyResponseMaxBytes} bytes failed: {Describe(failure)}");
        Assert.Empty(server.Faults);
    }

    // ---------------------------------------------------------------------------------------------
    // A non-Identify reply to Identify is bounded by min(caller cap, limit) from the header
    // ---------------------------------------------------------------------------------------------

    /// <summary>(row, reply type, advertised length, caller cap). Each is past min(cap, limit) and sends no body.</summary>
    private static readonly Dictionary<string, (uint Type, uint Advertised, long? Cap)> OversizeNonIdentify = new()
    {
        ["GenericError 8 KiB, cap 4096"] = (MessageTypes.Responses.GenericError, 8 * 1024, TinyCap),
        ["ProtocolError 8 KiB, cap 4096"] = (MessageTypes.Responses.ProtocolError, 8 * 1024, TinyCap),
        ["Read 8 KiB, cap 4096"] = (MessageTypes.Responses.Read, 8 * 1024, TinyCap),
        ["GenericError limit+1, default cap"] = (MessageTypes.Responses.GenericError, Limit + 1, null),
        ["ProtocolError limit+1, default cap"] = (MessageTypes.Responses.ProtocolError, Limit + 1, null),
        ["GenericError 64 MiB, cap 1 GiB"] = (MessageTypes.Responses.GenericError, 64 * MiB, GiB),
        ["Read uint.MaxValue, cap 1 GiB"] = (MessageTypes.Responses.Read, uint.MaxValue, GiB),
    };

    public static TheoryData<string, string> OversizeNonIdentifyByKind()
    {
        var rows = new TheoryData<string, string>();
        foreach (string row in OversizeNonIdentify.Keys)
            foreach (string kind in Kinds.Keys)
                rows.Add(row, kind);
        return rows;
    }

    [Theory]
    [MemberData(nameof(OversizeNonIdentifyByKind))]
    public async Task ANonIdentifyReplyToIdentifyPastMinOfCapAndLimitIsRefusedFromTheHeader(string row, string kind)
    {
        var (type, advertised, cap) = OversizeNonIdentify[row];
        await using var server = new RawScriptServer(frame => frame.Type == MessageTypes.Requests.Identify
            ? RawReply.Hold(HeaderOnly(type, advertised, advertised))
            : ServeData(frame, oversize: false));

        var (failure, elapsed) = await Bounded(() => Kinds[kind](server.Address, cap), Prompt);

        string name = $"row '{row}' x '{kind}'";
        Assert.True(failure is ProtocolException, $"{name}: expected ProtocolException from the header, got {Describe(failure)} after {elapsed.TotalSeconds:F1}s");
        Assert.True(elapsed < Prompt, $"{name}: took {elapsed.TotalSeconds:F1}s");
        Assert.DoesNotContain(server.Frames, f => f.Type != MessageTypes.Requests.Identify);
        Assert.Empty(server.Faults);
    }

    /// <summary>(row, error message length, caller cap). Each whole GenericError body is within min(cap, limit).</summary>
    private static readonly Dictionary<string, (int MessageLength, long? Cap)> DecodableErrors = new()
    {
        ["small, cap 4096"] = (64, TinyCap),
        ["100 KiB, default cap"] = (100 * 1024, null),
        ["1.5 MiB, cap 1 GiB"] = (3 * 512 * 1024, GiB),
    };

    public static TheoryData<string, string> DecodableErrorByKind()
    {
        var rows = new TheoryData<string, string>();
        foreach (string row in DecodableErrors.Keys)
            foreach (string kind in Kinds.Keys)
                rows.Add(row, kind);
        return rows;
    }

    /// <summary>A GenericError reply to Identify inside the bound is decoded and surfaces as its typed error (10005 here).</summary>
    [Theory]
    [MemberData(nameof(DecodableErrorByKind))]
    public async Task ANonIdentifyReplyWithinTheBoundIsDecodedAsItsTypedError(string row, string kind)
    {
        var (messageLength, cap) = DecodableErrors[row];
        string message = new('x', messageLength);
        await using var server = new RawScriptServer(frame => frame.Type == MessageTypes.Requests.Identify
            ? RawReply.Closing(FakeServerSession.BuildFrame(MessageTypes.Responses.GenericError,
                FakeServerProtocol.ErrorFrame(ErrorResponse.AuthRequired, message, HandshakeReplies.Identify(frame).CorrelationId)))
            : ServeData(frame, oversize: false));

        var (failure, _) = await Bounded(() => Kinds[kind](server.Address, cap), Generous);

        string name = $"row '{row}' x '{kind}'";
        Assert.True(failure is AuthRequiredException, $"{name}: expected AuthRequiredException, got {Describe(failure)}");
        Assert.Equal(ErrorResponse.AuthRequired, ((AuthRequiredException)failure!).Error.ErrorCode);
        Assert.Equal(messageLength, ((AuthRequiredException)failure!).Error.ErrorMessage?.Length);
    }

    // ---------------------------------------------------------------------------------------------
    // Data frames stay under the caller cap after a large handshake
    // ---------------------------------------------------------------------------------------------

    /// <summary>
    /// Each kind does a 1 MiB-dictionary handshake and one small data operation (so the handshake is
    /// known to have succeeded), then meets a data frame of <see cref="OversizeCount"/> entries, past
    /// 4096 bytes. Returns the failure of that last step only.
    /// </summary>
    private static readonly Dictionary<string, Func<string, long?, Task<Exception?>>> DataKinds = new()
    {
        ["pool read"] = async (address, cap) =>
        {
            await using var pool = Pool(address, cap);
            await pool.WriteAsync(OccTestData.Write());
            return await Record.ExceptionAsync(async () => Assert.Equal(OversizeCount,
                (await pool.ReadAsync(new ReadRequest { AggregateKey = OccTestData.Keys[0], Filters = ReadFilters.From(1) })).EventBatches.Length));
        },
        ["pool.WatchAsync event"] = async (address, cap) =>
        {
            await using var pool = Pool(address, cap);
            await using var watch = await pool.WatchAsync(AnyWatch());
            return await Record.ExceptionAsync(async () => Assert.Equal(OversizeCount, (await watch.NextAsync()).Events.Length));
        },
        ["WatchConnection event"] = async (address, cap) =>
        {
            await using var watch = await WatchConnection.ConnectAsync(address, AnyWatch(), WatchOptionsFor(cap));
            return await Record.ExceptionAsync(async () => Assert.Equal(OversizeCount, (await watch.NextAsync()).Events.Length));
        },
        ["CeleriantClient read"] = async (address, cap) =>
        {
            await using var client = await Client(address, cap);
            await client.WriteAsync(OccTestData.Write());
            return await Record.ExceptionAsync(async () => Assert.Equal(OversizeCount,
                (await client.ReadAsync(new ReadRequest { AggregateKey = OccTestData.Keys[0], Filters = ReadFilters.From(1) })).EventBatches.Length));
        },
    };

    public static TheoryData<string, string> DataKindByCap()
    {
        var rows = new TheoryData<string, string>();
        foreach (string kind in DataKinds.Keys)
            foreach (string cap in new[] { "4096", "default" })
                rows.Add(kind, cap);
        return rows;
    }

    [Fact]
    public void TheOversizeDataFramesArePastTheTinyCap()
    {
        int read = ReadReply(Guid.NewGuid(), OversizeCount).Length, watch = WatchEvents(OversizeCount).Length;
        Assert.True(read > TinyCap * 2 && watch > TinyCap * 2, $"read reply {read} bytes, watch frame {watch} bytes; both must be well past {TinyCap}");
    }

    /// <summary>
    /// Under a 4096-byte cap the handshake succeeds and the oversize data frame is a
    /// <see cref="ProtocolException"/>. Under the default cap (the control) the same frame is delivered whole.
    /// </summary>
    [Theory]
    [MemberData(nameof(DataKindByCap))]
    public async Task DataFramesStayUnderTheCallerCapAfterALargeHandshake(string kind, string cap)
    {
        await using var server = new RawScriptServer(
            frame => frame.Type == MessageTypes.Requests.Identify ? RawReply.Of(RustIdentified(frame, MiBHighDict)) : ServeData(frame, oversize: true),
            MiBHighDict);

        Exception? dataFailure = null;
        var (setupFailure, _) = await Bounded(async () => dataFailure = await DataKinds[kind](server.Address, ParseCap(cap)), Generous);

        string name = $"'{kind}', cap {cap}";
        Assert.True(setupFailure is null, $"{name}: the handshake or the small first operation failed: {Describe(setupFailure)}");
        if (cap == "default")
            Assert.True(dataFailure is null, $"{name}: control, the oversize frame must be delivered under the default cap: {Describe(dataFailure)}");
        else
            Assert.True(dataFailure is ProtocolException, $"{name}: a data frame past the cap must be a ProtocolException, got {Describe(dataFailure)}");
        Assert.Empty(server.Faults);
    }
}

/// <summary>What a <see cref="RawScriptServer"/> writes for one request frame: raw bytes, then hold the socket open, hang up, or carry on.</summary>
internal sealed record RawReply(byte[][] Writes, bool Close = false, bool HoldOpen = false)
{
    public static RawReply Of(params byte[][] writes) => new(writes);
    public static RawReply Closing(params byte[][] writes) => new(writes, Close: true);

    /// <summary>Write the bytes and then say nothing more, keeping the socket open, the way a peer that never sends the advertised body does.</summary>
    public static RawReply Hold(params byte[][] writes) => new(writes, HoldOpen: true);
}

/// <summary>
/// A <see cref="HandshakeScriptServer"/> whose replies are raw bytes, so a script can send a header
/// with no body, a hand-encoded IdentifyResponse, or anything else the typed reply builders cannot.
/// Frames the client compresses with a dictionary are decompressed with <c>dict</c>.
/// </summary>
internal sealed class RawScriptServer : IAsyncDisposable
{
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _stop = new(TimeSpan.FromSeconds(120));
    private readonly Func<HandshakeFrame, RawReply> _script;
    private readonly byte[]? _dict;
    private readonly Task _accepting;
    private readonly ConcurrentBag<Task> _sessions = [];
    private int _accepted;

    public readonly ConcurrentQueue<HandshakeFrame> Frames = [];
    public readonly ConcurrentQueue<Exception> Faults = [];

    public RawScriptServer(Func<HandshakeFrame, RawReply> script, byte[]? dict = null)
    {
        _script = script;
        _dict = dict;
        _listener.Start();
        _accepting = AcceptAsync();
    }

    public string Address => $"127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}";

    public int Accepted => Volatile.Read(ref _accepted);

    public IReadOnlyDictionary<int, HandshakeFrame[]> ByConnection()
        => Frames.GroupBy(f => f.Connection).ToDictionary(g => g.Key, g => g.OrderBy(f => f.Index).ToArray());

    private async Task AcceptAsync()
    {
        try
        {
            while (!_stop.IsCancellationRequested)
            {
                var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                int id = Interlocked.Increment(ref _accepted);
                _sessions.Add(Task.Run(() => ServeAsync(client, id)));
            }
        }
        catch (OperationCanceledException) { }
        catch (ObjectDisposedException) { }
        catch (SocketException) { }
    }

    private async Task ServeAsync(TcpClient client, int id)
    {
        using (client)
        {
            try
            {
                client.NoDelay = true;
                var stream = client.GetStream();
                for (int index = 0; !_stop.IsCancellationRequested; index++)
                {
                    var headerBytes = new byte[WireHeader.Size];
                    await stream.ReadExactlyAsync(headerBytes, _stop.Token);
                    var header = WireHeader.ParseFrom(headerBytes);
                    var body = new byte[header.CompressedLength];
                    await stream.ReadExactlyAsync(body, _stop.Token);
                    if (header.CompressionType == (byte)CompressionType.ZstdDict)
                    {
                        if (_dict is null)
                            throw new InvalidOperationException($"connection {id}: a dict-compressed frame, but this server ships no dictionary");
                        body = DictCompression.DecompressWithDict(body, header.UncompressedLength, _dict);
                    }

                    var frame = new HandshakeFrame(id, index, header.MessageType, body);
                    Frames.Enqueue(frame);
                    RawReply reply = _script(frame);
                    foreach (byte[] bytes in reply.Writes)
                        await stream.WriteAsync(bytes, _stop.Token);
                    await stream.FlushAsync(_stop.Token);
                    if (reply.Close)
                        return;
                    if (reply.HoldOpen)
                    {
                        // Read (and drop) until the client hangs up, so a client close is seen promptly.
                        var sink = new byte[4096];
                        while (await stream.ReadAsync(sink, _stop.Token) > 0) { }
                        return;
                    }
                }
            }
            catch (EndOfStreamException) { }
            catch (IOException) { }
            catch (OperationCanceledException) { }
            catch (ObjectDisposedException) { }
            catch (Exception error) { Faults.Enqueue(error); }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync();
        _listener.Stop();
        await _accepting;
        await Task.WhenAll(_sessions);
        _stop.Dispose();
    }
}
