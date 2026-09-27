using System.Net;
using System.Net.Sockets;

namespace Celeriant.Transport.Tests;

/// <summary>
/// Lifecycle-safety: disposing a <see cref="CeleriantConnection"/> while a request is in flight
/// (or while another task is queued on its send lock) must surface a clean transport failure to
/// the in-flight/queued caller, never a raw <see cref="ObjectDisposedException"/> or other
/// unexpected exception. A connection being disposed must not corrupt or crash concurrent users.
/// </summary>
public class CeleriantConnectionDisposeDuringInflightTests
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

    private sealed class Codec : IConnectionCodec
    {
        public uint ProtocolVersion => WireHeader.ProtocolVersionV3;
        public uint IdentifyRequestType => IdentifyRequest;
        public uint IdentifyResponseType => IdentifyResponse;
        public byte[] EncodeIdentify(in IdentifyParams identity) => [];
        public IdentifyResult DecodeIdentify(ReadOnlySpan<byte> body) => new(Guid.Empty, null, null, null);
        public Exception? TryMapErrorFrame(uint messageType, ReadOnlySpan<byte> body) => null;
    }

    /// <summary>
    /// Dispose while a request is in flight (the server has consumed the request frame but withholds
    /// the response, so the caller is blocked in the read). The in-flight caller must not observe an
    /// <see cref="ObjectDisposedException"/>.
    /// </summary>
    [Fact]
    public async Task DisposeWhileRequestInFlight_DoesNotThrowObjectDisposedToInflightCaller()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;

        var server = Task.Run(async () =>
        {
            using var accepted = await listener.AcceptTcpClientAsync();
            accepted.NoDelay = true;
            var stream = accepted.GetStream();

            await ConsumeFrameAsync(stream);                 // Identify
            await WriteFrameAsync(stream, Frame(IdentifyResponse, []));

            await ConsumeFrameAsync(stream);                 // in-flight request
            await Task.Delay(5000);                          // withhold the response
        });

        try
        {
            await using var conn = await CeleriantConnection.ConnectAsync(
                $"127.0.0.1:{port}", TimeSpan.FromSeconds(5), null, new Codec(), new ThrowingExceptionFactory());

            await conn.IdentifyAsync(new IdentifyParams(null, null, null, null, null), BuiltinDictionary.Dict);

            var inflight = conn.SendAsync(DataRequest, [1, 2, 3], false, 0);

            await Task.Delay(200);                            // let the request reach the read
            await conn.DisposeAsync();

            var ex = await Record.ExceptionAsync(() => inflight);

            Assert.NotNull(ex);
            Assert.False(
                ex is ObjectDisposedException,
                $"disposing the connection must not surface ObjectDisposedException to the "
                + $"in-flight caller; got {ex.GetType().Name}: {ex.Message}");
        }
        finally
        {
            listener.Stop();
            await Task.WhenAny(server, Task.Delay(1000));
        }
    }

    /// <summary>
    /// Dispose while a second caller is queued on the send lock behind an in-flight request. The
    /// queued caller must not observe an <see cref="ObjectDisposedException"/> from the disposed
    /// semaphore.
    /// </summary>
    [Fact]
    public async Task DisposeWhileCallerQueuedOnSendLock_DoesNotThrowObjectDisposedToQueuedCaller()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;

        var server = Task.Run(async () =>
        {
            using var accepted = await listener.AcceptTcpClientAsync();
            accepted.NoDelay = true;
            var stream = accepted.GetStream();

            await ConsumeFrameAsync(stream);                 // Identify
            await WriteFrameAsync(stream, Frame(IdentifyResponse, []));

            await ConsumeFrameAsync(stream);                 // first request (holds the lock)
            await Task.Delay(5000);                          // withhold, keeping the lock held
        });

        try
        {
            await using var conn = await CeleriantConnection.ConnectAsync(
                $"127.0.0.1:{port}", TimeSpan.FromSeconds(5), null, new Codec(), new ThrowingExceptionFactory());

            await conn.IdentifyAsync(new IdentifyParams(null, null, null, null, null), BuiltinDictionary.Dict);

            var inflight = conn.SendAsync(DataRequest, [1, 2, 3], false, 0);   // acquires the lock
            await Task.Delay(200);

            var queued = conn.SendAsync(DataRequest, [4, 5, 6], false, 0);     // queued on the lock
            await Task.Delay(200);

            await conn.DisposeAsync();

            var ex = await Record.ExceptionAsync(() => queued);

            Assert.NotNull(ex);
            Assert.False(
                ex is ObjectDisposedException,
                $"disposing the connection must not surface ObjectDisposedException to a caller "
                + $"queued on the send lock; got {ex.GetType().Name}: {ex.Message}");

            await Record.ExceptionAsync(() => inflight);
        }
        finally
        {
            listener.Stop();
            await Task.WhenAny(server, Task.Delay(1000));
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