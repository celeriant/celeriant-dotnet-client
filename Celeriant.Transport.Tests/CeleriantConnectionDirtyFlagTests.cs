using System.Net;
using System.Net.Sockets;

namespace Celeriant.Transport.Tests;

/// <summary>
/// The dirty flag means "the socket may be desynced", so it must be cleared the instant the frame
/// is off the socket, ahead of decode. Armed around work that is not socket work, a purely local
/// failure on a fully drained stream permanently retires a healthy connection.
/// </summary>
public class CeleriantConnectionDirtyFlagTests
{
    private const uint IdentifyRequest = 14;
    private const uint IdentifyResponse = 16;
    private const uint DataRequest = 3;
    private const uint DataResponse = 4;

    private sealed class ThrowingExceptionFactory : ITransportExceptionFactory
    {
        public Exception Timeout(string message) => new TimeoutException(message);
        public Exception ConnectTimeout(string message) => new TimeoutException(message);
        public Exception ConnectionFailed(string message, Exception? inner = null) => new IOException(message, inner);
        public Exception Protocol(string message, Exception? inner = null) => new InvalidDataException(message, inner);
    }

    /// <summary>Hands the connection a real zstd dictionary through the normal Identify path.</summary>
    private sealed class DictCodec(byte[] dict) : IConnectionCodec
    {
        public uint ProtocolVersion => WireHeader.ProtocolVersionV3;
        public uint IdentifyRequestType => IdentifyRequest;
        public uint IdentifyResponseType => IdentifyResponse;
        public byte[] EncodeIdentify(in IdentifyParams identity) => [];
        public IdentifyResult DecodeIdentify(ReadOnlySpan<byte> body)
            => new(Guid.Empty, null, "sha-under-test", dict);
        public Exception? TryMapErrorFrame(uint messageType, ReadOnlySpan<byte> body) => null;
    }

    /// <summary>
    /// A response frame read off the socket in full leaves the stream on a clean message boundary,
    /// so a failure in the local decompress that follows must not retire the connection.
    /// </summary>
    [Fact]
    public async Task DecodeFailureOnAFullyDrainedFrame_DoesNotRetireTheConnection()
    {
        byte[] dict = new byte[4096];
        new Random(7).NextBytes(dict);
        byte[] goodBody = "the-second-answer"u8.ToArray();

        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;

        var session = Task.Run(async () =>
        {
            using var accepted = await listener.AcceptTcpClientAsync();
            accepted.NoDelay = true;
            var stream = accepted.GetStream();

            await ConsumeFrameAsync(stream);                      // Identify
            await WriteFrameAsync(stream, Frame(IdentifyResponse, []));

            await ConsumeFrameAsync(stream);                      // request 1
            // A well-formed 17-byte header claiming ZstdDict over a body that is not a zstd
            // frame: every byte is consumed, and only the unwrap fails.
            byte[] garbage = [0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04];
            await WriteFrameAsync(stream, CompressedFrame(DataResponse, garbage, uncompressedLength: 32));

            await ConsumeFrameAsync(stream);                      // request 2
            await WriteFrameAsync(stream, Frame(DataResponse, goodBody));

            await Task.Delay(2000);
        });

        try
        {
            await using var conn = await CeleriantConnection.ConnectAsync(
                $"127.0.0.1:{port}", TimeSpan.FromSeconds(5), null, new DictCodec(dict), new ThrowingExceptionFactory());

            await conn.IdentifyAsync(new IdentifyParams(null, null, null, null, null));
            Assert.NotNull(conn.CurrentDict);

            var decodeFailure = await Record.ExceptionAsync(() => conn.SendAsync(DataRequest, [1, 2, 3], false, 0));
            Assert.NotNull(decodeFailure);
            Assert.False(
                decodeFailure is IOException,
                $"precondition: the failure must be the local zstd unwrap, got {decodeFailure.GetType().Name}");

            Assert.False(
                conn.IsPoisoned,
                "the response frame was read off the socket in full and only the local decode "
                + "failed, so the stream is on a clean message boundary and the connection must "
                + "stay usable");

            var second = await conn.SendAsync(DataRequest, [4, 5, 6], false, 0);
            Assert.Equal(goodBody, second.Body);
        }
        finally
        {
            listener.Stop();
            await Task.WhenAny(session, Task.Delay(1000));
        }
    }

    // -------------------------------------------------------------------------

    private static byte[] Frame(uint messageType, byte[] body)
    {
        var frame = new byte[WireHeader.Size + body.Length];
        WireHeader.ForRequest(WireHeader.ProtocolVersionV3, messageType, (uint)body.Length).WriteTo(frame);
        body.CopyTo(frame, WireHeader.Size);
        return frame;
    }

    private static byte[] CompressedFrame(uint messageType, byte[] body, uint uncompressedLength)
    {
        var frame = new byte[WireHeader.Size + body.Length];
        new WireHeader(
            WireHeader.ProtocolVersionV3, messageType, (uint)body.Length, uncompressedLength,
            (byte)CompressionType.ZstdDict).WriteTo(frame);
        body.CopyTo(frame, WireHeader.Size);
        return frame;
    }

    private static async Task WriteFrameAsync(Stream stream, byte[] frame)
    {
        await stream.WriteAsync(frame);
        await stream.FlushAsync();
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
            if (n == 0)
                throw new EndOfStreamException();
            read += n;
        }
    }
}
