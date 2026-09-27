using System.Diagnostics;
using System.Net;
using System.Net.Sockets;

namespace Celeriant.Transport.Tests;

/// <summary>
/// Hostile headers on the reply to Identify, caller caps at the integer
/// edges, the sync and watch read paths after a large handshake, and a slow handshake body.
/// </summary>
public class IdentifyReplyEdgeCaseTests
{
    private const uint IdentifyRequest = 14;
    private const uint IdentifyResponse = 16;
    private const uint ErrorResponse = 7;
    private const uint DataRequest = 3;
    private const uint DataResponse = 4;
    private const byte None = 0;
    private const byte ZstdDict = 1;
    private const long Limit = HandshakeLimits.IdentifyResponseMaxBytes;

    // A compressed IdentifyResponse can never decode: no dictionary exists before the handshake.
    // With a body it must fail as a protocol error and poison, not hang or succeed.
    [Fact]
    public async Task ACompressedIdentifyResponseWithABodyIsAProtocolError()
    {
        await RunAsync(async stream =>
        {
            await ConsumeFrameAsync(stream);
            await WriteHeaderAsync(stream, WireHeader.ProtocolVersionV3, IdentifyResponse, 64, 1000, ZstdDict);
            await WriteAsync(stream, new byte[64]);
            await Task.Delay(TimeSpan.FromSeconds(10));
        }, async conn =>
        {
            conn.WithMaxResponseSize(4096).WithTimeout(TimeSpan.FromSeconds(5));
            var sw = Stopwatch.StartNew();
            Exception? ex = await Record.ExceptionAsync(() => Identify(conn));
            Assert.True(ex is InvalidDataException, $"expected a protocol error, got {ex}");
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds(2), $"took {sw.Elapsed}");
            Assert.True(conn.IsPoisoned);
        });
    }

    // Header-only compressed IdentifyResponse within the limit: the client could refuse it from
    // the header (no dictionary can exist yet) but reads the body first. This row documents that
    // behaviour; it holds the socket and expects the refusal within 2 s.
    [Fact]
    public async Task ACompressedIdentifyResponseHeaderIsRefusedBeforeItsBody()
    {
        await RunAsync(async stream =>
        {
            await ConsumeFrameAsync(stream);
            await WriteHeaderAsync(stream, WireHeader.ProtocolVersionV3, IdentifyResponse, (uint)Limit, (uint)Limit, ZstdDict);
            await Task.Delay(TimeSpan.FromSeconds(10));
        }, async conn =>
        {
            conn.WithMaxResponseSize(4096).WithTimeout(TimeSpan.FromSeconds(5));
            var sw = Stopwatch.StartNew();
            Exception? ex = await Record.ExceptionAsync(() => Identify(conn));
            Assert.True(ex is InvalidDataException && sw.Elapsed < TimeSpan.FromSeconds(2),
                $"expected a prompt protocol error from the header, got {ex?.GetType().Name} after {sw.Elapsed}: {ex?.Message}");
        });
    }

    public static TheoryData<string, uint, uint, uint, byte> HostileHeaders => new()
    {
        { "version mismatch, uint.MaxValue lengths", WireHeader.ProtocolVersionV3 + 1, uint.MaxValue, uint.MaxValue, None },
        { "IdentifyResponse, uint.MaxValue",          WireHeader.ProtocolVersionV3,     uint.MaxValue, uint.MaxValue, None },
        { "IdentifyResponse, uncompressed only huge", WireHeader.ProtocolVersionV3,     16,            uint.MaxValue, ZstdDict },
        { "IdentifyResponse, None, unequal lengths",  WireHeader.ProtocolVersionV3,     16,            (uint)Limit + 1, None },
        { "IdentifyResponse, unknown compression",    WireHeader.ProtocolVersionV3,     (uint)Limit + 1, 16,          9 },
    };

    [Theory]
    [MemberData(nameof(HostileHeaders))]
    public async Task AHostileIdentifyHeaderIsRefusedPromptlyUnderAHugeCap(
        string row, uint version, uint compressed, uint uncompressed, byte compression)
    {
        await RunAsync(async stream =>
        {
            await ConsumeFrameAsync(stream);
            await WriteHeaderAsync(stream, version, IdentifyResponse, compressed, uncompressed, compression);
            await Task.Delay(TimeSpan.FromSeconds(10));
        }, async conn =>
        {
            conn.WithMaxResponseSize(long.MaxValue).WithTimeout(TimeSpan.FromSeconds(5));
            var sw = Stopwatch.StartNew();
            Exception? ex = await Record.ExceptionAsync(() => Identify(conn));
            Assert.True(ex is InvalidDataException, $"{row}: expected a protocol error, got {ex}");
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds(2), $"{row}: took {sw.Elapsed}");
            Assert.True(conn.IsPoisoned, $"{row}: not poisoned");
        });
    }

    public static TheoryData<long> EdgeCaps => new() { 0, -1, long.MinValue, long.MaxValue };

    // A cap at or below zero, or at long.MaxValue, must not overflow min(cap, limit): the
    // IdentifyResponse still gets the full limit.
    [Theory]
    [MemberData(nameof(EdgeCaps))]
    public async Task AnIdentifyResponseAtTheLimitIsAcceptedWhateverTheCapEdge(long cap)
    {
        await RunAsync(async stream =>
        {
            await ConsumeFrameAsync(stream);
            await WriteAsync(stream, Frame(IdentifyResponse, new byte[Limit]));
            await Task.Delay(TimeSpan.FromSeconds(10));
        }, async conn =>
        {
            conn.WithMaxResponseSize(cap).WithTimeout(TimeSpan.FromSeconds(5));
            Exception? ex = await Record.ExceptionAsync(() => Identify(conn));
            Assert.True(ex is null, $"cap {cap}: expected acceptance, got {ex}");
        });
    }

    // An error reply to Identify is bounded by min(cap, limit): nothing under a cap <= 0, the limit
    // under long.MaxValue.
    [Theory]
    [InlineData(0L, 1u, true)]
    [InlineData(-1L, 1u, true)]
    [InlineData(long.MinValue, 1u, true)]
    [InlineData(long.MaxValue, 1u, false)]
    [InlineData(long.MaxValue, (uint)Limit + 1, true)]
    public async Task AnErrorReplyToIdentifyIsBoundedAtTheCapEdges(long cap, uint length, bool refused)
    {
        await RunAsync(async stream =>
        {
            await ConsumeFrameAsync(stream);
            await WriteHeaderAsync(stream, WireHeader.ProtocolVersionV3, ErrorResponse, length, length, None);
            if (!refused)
                await WriteAsync(stream, new byte[length]);
            await Task.Delay(TimeSpan.FromSeconds(10));
        }, async conn =>
        {
            conn.WithMaxResponseSize(cap).WithTimeout(TimeSpan.FromSeconds(5));
            var sw = Stopwatch.StartNew();
            Exception? ex = await Record.ExceptionAsync(() => Identify(conn));
            if (refused)
                Assert.True(ex is InvalidDataException && sw.Elapsed < TimeSpan.FromSeconds(2),
                    $"cap {cap}, len {length}: expected a prompt protocol error, got {ex} after {sw.Elapsed}");
            else
                Assert.True(ex is MappedError, $"cap {cap}, len {length}: expected the mapped error, got {ex}");
        });
    }

    // The sync SendRequest path, after a large handshake, stays under the caller cap.
    [Theory]
    [InlineData(4096u, false)]
    [InlineData(4097u, true)]
    public async Task TheSyncPathStaysUnderTheCallerCapAfterALargeHandshake(uint dataLen, bool refused)
    {
        await RunAsync(async stream =>
        {
            await ConsumeFrameAsync(stream);
            await WriteAsync(stream, Frame(IdentifyResponse, new byte[Limit]));
            await ConsumeFrameAsync(stream);
            await WriteAsync(stream, Frame(DataResponse, new byte[dataLen]));
            await Task.Delay(TimeSpan.FromSeconds(10));
        }, async conn =>
        {
            conn.WithMaxResponseSize(4096).WithTimeout(TimeSpan.FromSeconds(5));
            await Identify(conn);
            Exception? ex = Record.Exception(() => conn.SendRequest(DataRequest, [], compressible: false, logicalPayloadBytes: 0));
            if (refused)
                Assert.True(ex is InvalidDataException && ex.Message.Contains("maximum allowed size 4096."), $"got {ex}");
            else
                Assert.True(ex is null, $"got {ex}");
        });
    }

    // The watch read path (ReadFrameAsync), after a large handshake, stays under the caller cap.
    [Theory]
    [InlineData(4096u, false)]
    [InlineData(4097u, true)]
    public async Task TheWatchReadStaysUnderTheCallerCapAfterALargeHandshake(uint dataLen, bool refused)
    {
        await RunAsync(async stream =>
        {
            await ConsumeFrameAsync(stream);
            await WriteAsync(stream, Frame(IdentifyResponse, new byte[Limit]));
            await WriteAsync(stream, Frame(DataResponse, new byte[dataLen]));
            await Task.Delay(TimeSpan.FromSeconds(10));
        }, async conn =>
        {
            conn.WithMaxResponseSize(4096).WithTimeout(TimeSpan.FromSeconds(5));
            await Identify(conn);
            using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            Exception? ex = await Record.ExceptionAsync(() => conn.ReadFrameAsync(cts.Token));
            if (refused)
                Assert.True(ex is InvalidDataException && ex.Message.Contains("maximum allowed size 4096."), $"got {ex}");
            else
                Assert.True(ex is null, $"got {ex}");
        });
    }

    // A handshake body trickled slower than the timeout ends in the connect-class timeout and
    // poisons, rather than parking the caller.
    [Fact]
    public async Task ATrickledHandshakeBodyTimesOut()
    {
        await RunAsync(async stream =>
        {
            await ConsumeFrameAsync(stream);
            await WriteHeaderAsync(stream, WireHeader.ProtocolVersionV3, IdentifyResponse, (uint)Limit, (uint)Limit, None);
            for (int i = 0; i < 100; i++)
            {
                await WriteAsync(stream, new byte[1024]);
                await Task.Delay(100);
            }
        }, async conn =>
        {
            conn.WithMaxResponseSize(4096).WithTimeout(TimeSpan.FromSeconds(1));
            var sw = Stopwatch.StartNew();
            Exception? ex = await Record.ExceptionAsync(() => Identify(conn));
            Assert.True(ex is TimeoutException, $"expected the handshake timeout, got {ex}");
            Assert.True(sw.Elapsed < TimeSpan.FromSeconds(3), $"took {sw.Elapsed}");
            Assert.True(conn.IsPoisoned);
        });
    }

    // -------------------------------------------------------------------------

    private static Task Identify(CeleriantConnection conn)
        => conn.IdentifyAsync(new IdentifyParams(null, null, null, null, null), BuiltinDictionary.Dict);

    private sealed class MappedError() : Exception("mapped Identify error frame");

    private sealed class Ex : ITransportExceptionFactory
    {
        public Exception Timeout(string m) => new TimeoutException(m);
        public Exception ConnectTimeout(string m) => new TimeoutException(m);
        public Exception ConnectionFailed(string m, Exception? i = null) => new IOException(m, i);
        public Exception Protocol(string m, Exception? i = null) => new InvalidDataException(m, i);
    }

    private sealed class Codec : IConnectionCodec
    {
        public uint ProtocolVersion => WireHeader.ProtocolVersionV3;
        public uint IdentifyRequestType => IdentifyRequest;
        public uint IdentifyResponseType => IdentifyResponse;
        public byte[] EncodeIdentify(in IdentifyParams identity) => [];
        public IdentifyResult DecodeIdentify(ReadOnlySpan<byte> body) => new(Guid.Empty, null, null, null);
        public Exception? TryMapErrorFrame(uint t, ReadOnlySpan<byte> b) => t == ErrorResponse ? new MappedError() : null;
    }

    private static async Task RunAsync(Func<NetworkStream, Task> server, Func<CeleriantConnection, Task> client)
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        var session = Task.Run(async () =>
        {
            using var accepted = await listener.AcceptTcpClientAsync();
            accepted.NoDelay = true;
            try { await server(accepted.GetStream()); } catch { }
        });
        try
        {
            await using var conn = await CeleriantConnection.ConnectAsync(
                $"127.0.0.1:{port}", TimeSpan.FromSeconds(5), null, new Codec(), new Ex());
            await client(conn);
        }
        finally
        {
            listener.Stop();
            await Task.WhenAny(session, Task.Delay(1000));
        }
    }

    private static async Task WriteHeaderAsync(Stream s, uint version, uint type, uint compressed, uint uncompressed, byte compression)
    {
        var header = new byte[WireHeader.Size];
        new WireHeader(version, type, compressed, uncompressed, compression).WriteTo(header);
        await WriteAsync(s, header);
    }

    private static byte[] Frame(uint type, byte[] body)
    {
        var frame = new byte[WireHeader.Size + body.Length];
        WireHeader.ForRequest(WireHeader.ProtocolVersionV3, type, (uint)body.Length).WriteTo(frame);
        body.CopyTo(frame, WireHeader.Size);
        return frame;
    }

    private static async Task WriteAsync(Stream s, byte[] bytes)
    {
        await s.WriteAsync(bytes);
        await s.FlushAsync();
    }

    private static async Task ConsumeFrameAsync(Stream stream)
    {
        var header = new byte[WireHeader.Size];
        await ReadExactAsync(stream, header, header.Length);
        var parsed = WireHeader.ParseFrom(header);
        var body = new byte[parsed.CompressedLength];
        await ReadExactAsync(stream, body, body.Length);
    }

    private static async Task ReadExactAsync(Stream stream, byte[] buffer, int count)
    {
        int read = 0;
        while (read < count)
        {
            int n = await stream.ReadAsync(buffer.AsMemory(read, count - read));
            if (n == 0) throw new EndOfStreamException();
            read += n;
        }
    }
}
