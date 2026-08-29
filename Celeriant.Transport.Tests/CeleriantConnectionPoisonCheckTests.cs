using System.Net;
using System.Net.Sockets;

namespace Celeriant.Transport.Tests;

/// <summary>
/// A request in flight is not a poisoned connection.
///
/// <para>
/// <see cref="CeleriantConnection.IsPoisoned"/> includes <c>_streamDirty</c>, which is exactly what
/// an exchange that has started and not yet finished looks like. Checked on the way in — before the
/// send lock — a second caller sharing one connection would see a perfectly healthy connection
/// mid-request reported as poisoned, and the pool would believe it and retire the connection.
/// Checked under the lock, dirty can only mean the previous holder left without clearing it, which
/// is the thing the guard is actually for.
/// </para>
/// </summary>
public class CeleriantConnectionPoisonCheckTests
{
    private const uint DataRequest = 3;
    private const uint DataResponse = 4;

    /// <summary>Fail loudly on a hang instead of deadlocking the run.</summary>
    private static readonly TimeSpan HangBudget = TimeSpan.FromSeconds(10);

    private sealed class ThrowingExceptionFactory : ITransportExceptionFactory
    {
        public Exception Timeout(string message) => new TimeoutException(message);
        public Exception ConnectTimeout(string message) => new TimeoutException(message);
        public Exception ConnectionFailed(string message, Exception? inner = null) => new IOException(message, inner);
        public Exception Protocol(string message, Exception? inner = null) => new InvalidDataException(message, inner);
    }

    private sealed class PlainCodec : IConnectionCodec
    {
        public uint ProtocolVersion => WireHeader.ProtocolVersionV3;
        public uint IdentifyRequestType => 14;
        public uint IdentifyResponseType => 16;
        public byte[] EncodeIdentify(in IdentifyParams identity) => [];
        public IdentifyResult DecodeIdentify(ReadOnlySpan<byte> body) => new(Guid.Empty, null, null, null);
        public Exception? TryMapErrorFrame(uint messageType, ReadOnlySpan<byte> body) => null;
    }

    [Fact]
    public async Task ASecondRequestWhileTheFirstIsInFlight_QueuesInsteadOfReportingPoisoned()
    {
        // The server holds the first reply until the test says the second caller has arrived, so
        // the overlap is caused rather than raced for: the second request is provably issued while
        // the first still owns the stream and `_streamDirty` is armed.
        var secondCallerArrived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        byte[] firstBody = "first"u8.ToArray();
        byte[] secondBody = "second"u8.ToArray();

        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;

        var session = Task.Run(async () =>
        {
            using var accepted = await listener.AcceptTcpClientAsync();
            accepted.NoDelay = true;
            var stream = accepted.GetStream();

            await ConsumeFrameAsync(stream);                       // request 1
            await secondCallerArrived.Task;                        // keep request 1 in flight
            await WriteFrameAsync(stream, Frame(DataResponse, firstBody));

            await ConsumeFrameAsync(stream);                       // request 2
            await WriteFrameAsync(stream, Frame(DataResponse, secondBody));
        });

        try
        {
            await using var conn = await CeleriantConnection.ConnectAsync(
                $"127.0.0.1:{port}", TimeSpan.FromSeconds(5), null, new PlainCodec(), new ThrowingExceptionFactory());

            var first = conn.SendAsync(DataRequest, [1, 2, 3], false, 0);

            // Wait until the connection actually reports dirty, so this is the real overlap and not
            // a second request that simply arrived after the first had finished.
            await SpinUntilAsync(() => conn.IsPoisoned, "the first request arming the dirty flag");

            var second = Task.Run(() => conn.SendAsync(DataRequest, [4, 5, 6], false, 0));
            secondCallerArrived.SetResult();

            await AwaitCompletionAsync(Task.WhenAll(first, second), "two concurrent requests on one connection");

            Assert.Equal(firstBody, (await first).Body);

            // The whole point: the second caller gets its answer, not "Connection is poisoned".
            Assert.Equal(secondBody, (await second).Body);
            Assert.False(conn.IsPoisoned, "both exchanges completed, so nothing is dirty");
        }
        finally
        {
            secondCallerArrived.TrySetResult();
            listener.Stop();
            await Task.WhenAny(session, Task.Delay(1000));
        }
    }

    // -------------------------------------------------------------------------

    private static async Task SpinUntilAsync(Func<bool> condition, string whatWouldNeverHappen)
    {
        var deadline = DateTime.UtcNow + HangBudget;
        while (DateTime.UtcNow < deadline)
        {
            if (condition())
                return;
            await Task.Delay(5);
        }

        Assert.Fail($"{whatWouldNeverHappen} never happened within {HangBudget.TotalSeconds:0}s");
    }

    private static async Task AwaitCompletionAsync(Task task, string whatWouldHang)
    {
        var finished = await Task.WhenAny(task, Task.Delay(HangBudget));
        Assert.True(
            ReferenceEquals(finished, task),
            $"{whatWouldHang} was still running after {HangBudget.TotalSeconds:0}s");
    }

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
