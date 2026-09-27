using System.Net;
using System.Net.Sockets;

namespace Celeriant.Transport.Tests;

/// <summary>
/// The reply to Identify is bounded from its header, before any body read: an IdentifyResponse by
/// <see cref="HandshakeLimits.IdentifyResponseMaxBytes"/> whatever the caller's cap, anything else
/// by the smaller of the cap and that limit. Frames after the handshake stay under the caller's cap.
/// </summary>
public class IdentifyReplyBoundTests
{
    private const uint IdentifyRequest = 14;
    private const uint IdentifyResponse = 16;
    private const uint ErrorResponse = 7;
    private const uint DataRequest = 3;
    private const uint DataResponse = 4;

    private const long Limit = HandshakeLimits.IdentifyResponseMaxBytes;
    private const long OneGiB = 1L << 30;

    public static TheoryData<string, bool, long, long> BoundRows => new()
    {
        { "IdentifyResponse, tiny cap",      true,  4096,   Limit },
        { "IdentifyResponse, default cap",   true,  64L << 20, Limit },
        { "IdentifyResponse, 1 GiB cap",     true,  OneGiB, Limit },
        { "error frame, tiny cap",           false, 4096,   4096 },
        { "error frame, cap at the limit",   false, Limit,  Limit },
        { "error frame, default cap",        false, 64L << 20, Limit },
        { "error frame, 1 GiB cap",          false, OneGiB, Limit },
    };

    [Theory]
    [MemberData(nameof(BoundRows))]
    public void AnIdentifyReplyIsBoundedByItsTypeAndTheCallerCap(
        string row, bool isIdentifyResponse, long callerCap, long expected)
    {
        long actual = HandshakeLimits.IdentifyReplyMaxBytes(isIdentifyResponse, callerCap);
        Assert.True(expected == actual, $"{row}: expected {expected}, got {actual}");
    }

    public static TheoryData<string, uint, long, byte, uint, uint, long?> WireRows => new()
    {
        // row, reply type, caller cap, compression, compressed len, uncompressed len, refused bound (null = accepted)
        { "IdentifyResponse at the limit, tiny cap",        IdentifyResponse, 4096,   0, (uint)Limit,       (uint)Limit,       null },
        { "IdentifyResponse one past the limit, tiny cap",  IdentifyResponse, 4096,   0, (uint)Limit + 1,   (uint)Limit + 1,   Limit },
        { "IdentifyResponse one past the limit, 1 GiB cap", IdentifyResponse, OneGiB, 0, (uint)Limit + 1,   (uint)Limit + 1,   Limit },
        { "IdentifyResponse, only uncompressed past",       IdentifyResponse, OneGiB, 1, 64,                (uint)Limit + 1,   Limit },
        { "IdentifyResponse, only compressed past",         IdentifyResponse, OneGiB, 1, (uint)Limit + 1,   64,                Limit },
        { "error frame at the cap",                         ErrorResponse,    4096,   0, 4096,              4096,              null },
        { "error frame one past the cap",                   ErrorResponse,    4096,   0, 4097,              4097,              4096 },
        { "error frame at the limit, 1 GiB cap",            ErrorResponse,    OneGiB, 0, (uint)Limit,       (uint)Limit,       null },
        { "error frame one past the limit, 1 GiB cap",      ErrorResponse,    OneGiB, 0, (uint)Limit + 1,   (uint)Limit + 1,   Limit },
    };

    [Theory]
    [MemberData(nameof(WireRows))]
    public async Task AnIdentifyReplyPastItsBoundIsRefusedFromTheHeader(
        string row, uint replyType, long callerCap, byte compression, uint compressedLen, uint uncompressedLen, long? refusedBound)
    {
        bool accepted = refusedBound is null;
        await RunAsync(async stream =>
        {
            await ConsumeFrameAsync(stream);
            var header = new byte[WireHeader.Size];
            new WireHeader(WireHeader.ProtocolVersionV3, replyType, compressedLen, uncompressedLen, compression).WriteTo(header);
            await WriteAsync(stream, header);
            // A refused reply sends no body and holds the socket: a client that waits for the
            // advertised body hits the 5 s deadline instead of the protocol error.
            if (accepted)
                await WriteAsync(stream, new byte[compressedLen]);
            await Task.Delay(TimeSpan.FromSeconds(10));
        }, async conn =>
        {
            conn.WithMaxResponseSize(callerCap).WithTimeout(TimeSpan.FromSeconds(5));
            Exception ex = await Record.ExceptionAsync(
                () => conn.IdentifyAsync(new IdentifyParams(null, null, null, null, null), BuiltinDictionary.Dict));

            if (accepted)
            {
                // An accepted body reaches the codec: success decodes, an error frame maps.
                if (replyType == IdentifyResponse)
                    Assert.True(ex is null, $"{row}: expected acceptance, got {ex}");
                else
                    Assert.True(ex is MappedError, $"{row}: expected the mapped error, got {ex}");
                return;
            }

            Assert.True(ex is InvalidDataException, $"{row}: expected a protocol error, got {ex}");
            Assert.True(ex.Message.Contains($"maximum allowed size {refusedBound}."),
                $"{row}: expected the bound {refusedBound} in '{ex.Message}'");
            Assert.True(conn.IsPoisoned, $"{row}: a refused handshake must poison the connection");
        });
    }

    [Theory]
    [InlineData(4096u, false)]
    [InlineData(4097u, true)]
    public async Task ADataFrameAfterALargeHandshakeStaysUnderTheCallerCap(uint dataReplyLen, bool refused)
    {
        await RunAsync(async stream =>
        {
            await ConsumeFrameAsync(stream);
            await WriteAsync(stream, Frame(IdentifyResponse, new byte[HandshakeLimits.DictionaryCeilingBytes]));
            await ConsumeFrameAsync(stream);
            await WriteAsync(stream, Frame(DataResponse, new byte[dataReplyLen]));
            await Task.Delay(TimeSpan.FromSeconds(10));
        }, async conn =>
        {
            conn.WithMaxResponseSize(4096).WithTimeout(TimeSpan.FromSeconds(5));
            await conn.IdentifyAsync(new IdentifyParams(null, null, null, null, null), BuiltinDictionary.Dict);

            Exception? ex = await Record.ExceptionAsync(
                () => conn.SendAsync(DataRequest, [], compressible: false, logicalPayloadBytes: 0));

            if (refused)
                Assert.True(ex is InvalidDataException && ex.Message.Contains("maximum allowed size 4096."),
                    $"expected the caller cap to refuse {dataReplyLen} bytes, got {ex}");
            else
                Assert.True(ex is null, $"expected {dataReplyLen} bytes to pass the 4096 cap, got {ex}");
        });
    }

    // -------------------------------------------------------------------------

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
