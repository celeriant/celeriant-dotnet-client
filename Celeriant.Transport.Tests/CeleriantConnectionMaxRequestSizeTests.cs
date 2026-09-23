using System.Net;
using System.Net.Sockets;

namespace Celeriant.Transport.Tests;

/// <summary>
/// `BuildRequestHeader` must reject a request whose uncompressed body exceeds MaxRequestSize even
/// when dictionary compression shrinks the wire body under the cap. Without that guard a request
/// is emitted and then dropped by the server, surfacing as a connection failure instead of a clean
/// client-side ArgumentException.
/// </summary>
public class CeleriantConnectionMaxRequestSizeTests
{
    private const uint IdentifyRequest = 14;
    private const uint IdentifyResponse = 16;
    private const uint DataRequest = 3;
    private const uint DataResponse = 4;

    private sealed class Ex : ITransportExceptionFactory
    {
        public Exception Timeout(string m) => new TimeoutException(m);
        public Exception ConnectTimeout(string m) => new TimeoutException(m);
        public Exception ConnectionFailed(string m, Exception? i = null) => new IOException(m, i);
        public Exception Protocol(string m, Exception? i = null) => new InvalidDataException(m, i);
    }

    private sealed class Codec(byte[]? dict = null) : IConnectionCodec
    {
        public uint ProtocolVersion => WireHeader.ProtocolVersionV3;
        public uint IdentifyRequestType => IdentifyRequest;
        public uint IdentifyResponseType => IdentifyResponse;
        public byte[] EncodeIdentify(in IdentifyParams identity) => [];
        public IdentifyResult DecodeIdentify(ReadOnlySpan<byte> body)
            => dict is null ? new(Guid.Empty, null, null, null) : new(Guid.Empty, null, "sha", dict);
        public Exception? TryMapErrorFrame(uint t, ReadOnlySpan<byte> b) => null;
    }

    [Fact]
    public async Task CompressedRequest_UncompressedBodyOverMaxRequestSize_ThrowsArgumentException()
    {
        byte[] dict = new byte[4096];
        new Random(7).NextBytes(dict);

        // 1 MiB of zeros: compresses to a few bytes, far under a 4096-byte cap.
        byte[] payload = new byte[1_048_576];

        await RunAsync(new Codec(dict), async (stream, _) =>
        {
            await ConsumeFrameAsync(stream);                       // Identify
            await WriteAsync(stream, Frame(IdentifyResponse, []));
            await ConsumeFrameAsync(stream);                       // never reached: the guard throws first
            await WriteAsync(stream, Frame(DataResponse, []));
        }, async conn =>
        {
            await conn.IdentifyAsync(new IdentifyParams(null, null, null, null, null));
            conn.WithMaxRequestSize(4096).WithCompressionThreshold(0);

            await Assert.ThrowsAsync<ArgumentException>(
                () => conn.SendAsync(DataRequest, payload, compressible: true, logicalPayloadBytes: payload.Length));
        });
    }

    // -------------------------------------------------------------------------

    private static async Task RunAsync(
        IConnectionCodec codec,
        Func<NetworkStream, CancellationToken, Task> server,
        Func<CeleriantConnection, Task> client)
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        var session = Task.Run(async () =>
        {
            using var accepted = await listener.AcceptTcpClientAsync();
            accepted.NoDelay = true;
            try { await server(accepted.GetStream(), default); } catch { }
        });
        try
        {
            await using var conn = await CeleriantConnection.ConnectAsync(
                $"127.0.0.1:{port}", TimeSpan.FromSeconds(5), null, codec, new Ex());
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