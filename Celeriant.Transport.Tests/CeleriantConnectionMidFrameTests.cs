using System.Net;
using System.Net.Sockets;

namespace Celeriant.Transport.Tests;

/// <summary>
/// `_framePartialBytes` (surfaced as <c>IsMidFrame</c>) must be non-zero exactly when a read was
/// abandoned part-way through a frame. These pin the three halves of that contract: every complete
/// exchange clears it, a read that starts over an abandoned frame is refused rather than
/// resynchronised by luck, and the pool discards a connection that is still mid-frame.
/// </summary>
public class CeleriantConnectionMidFrameTests
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

    // --- every complete exchange leaves the counter at zero -------------------

    /// <summary>A complete async request/response leaves the counter at 0.</summary>
    [Fact]
    public async Task HealthyAsyncRequestResponse_IsNotMidFrame()
    {
        await RunAsync(new Codec(), async (stream, _) =>
        {
            await ConsumeFrameAsync(stream);
            await WriteAsync(stream, Frame(DataResponse, "ok"u8.ToArray()));
        }, async conn =>
        {
            var f = await conn.SendAsync(DataRequest, [1, 2, 3], false, 0);
            Assert.Equal("ok"u8.ToArray(), f.Body);
            Assert.False(conn.IsMidFrame);
        });
    }

    /// <summary>A complete Identify handshake leaves the counter at 0.</summary>
    [Fact]
    public async Task HealthyIdentifyHandshake_IsNotMidFrame()
    {
        await RunAsync(new Codec(), async (stream, _) =>
        {
            await ConsumeFrameAsync(stream);
            await WriteAsync(stream, Frame(IdentifyResponse, []));
        }, async conn =>
        {
            await conn.IdentifyAsync(new IdentifyParams(null, null, null, null, null), BuiltinDictionary.Dict);
            Assert.False(conn.IsMidFrame);
        });
    }

    /// <summary>A response with an empty body still completes a frame, so the counter is 0.</summary>
    [Fact]
    public async Task ZeroLengthResponseBody_IsNotMidFrame()
    {
        await RunAsync(new Codec(), async (stream, _) =>
        {
            await ConsumeFrameAsync(stream);
            await WriteAsync(stream, Frame(DataResponse, []));
        }, async conn =>
        {
            await conn.SendAsync(DataRequest, [1], false, 0);
            Assert.False(conn.IsMidFrame);
        });
    }

    /// <summary>The reset happens once the frame is off the socket, ahead of decompression.</summary>
    [Fact]
    public async Task CompressedResponse_ResetHappensBeforeDecompress_IsNotMidFrame()
    {
        byte[] dict = new byte[4096];
        new Random(7).NextBytes(dict);
        byte[] plain = new byte[512];
        new Random(9).NextBytes(plain);

        await RunAsync(new Codec(dict), async (stream, _) =>
        {
            await ConsumeFrameAsync(stream);                                    // Identify
            await WriteAsync(stream, Frame(IdentifyResponse, []));
            await ConsumeFrameAsync(stream);                                    // request
            byte[] comp = DictCompression.CompressWithDict(plain, dict);
            await WriteAsync(stream, CompressedFrame(DataResponse, comp, (uint)plain.Length));
        }, async conn =>
        {
            await conn.IdentifyAsync(new IdentifyParams(null, null, null, null, null), BuiltinDictionary.Dict);
            var f = await conn.SendAsync(DataRequest, [1], false, 0);
            Assert.Equal(plain, f.Body);
            Assert.False(conn.IsMidFrame);
        });
    }

    /// <summary>A local unwrap failure on a fully drained frame is not mid-frame.</summary>
    [Fact]
    public async Task DecodeFailureOnAFullyDrainedFrame_IsNotMidFrame()
    {
        byte[] dict = new byte[4096];
        new Random(7).NextBytes(dict);

        await RunAsync(new Codec(dict), async (stream, _) =>
        {
            await ConsumeFrameAsync(stream);
            await WriteAsync(stream, Frame(IdentifyResponse, []));
            await ConsumeFrameAsync(stream);
            await WriteAsync(stream, CompressedFrame(DataResponse, [0xDE, 0xAD, 0xBE, 0xEF], 32));
        }, async conn =>
        {
            await conn.IdentifyAsync(new IdentifyParams(null, null, null, null, null), BuiltinDictionary.Dict);
            await Record.ExceptionAsync(() => conn.SendAsync(DataRequest, [1], false, 0));
            Assert.False(conn.IsMidFrame);
        });
    }

    /// <summary>The synchronous read path counts and resets too, so a clean exchange stays reusable.</summary>
    [Fact]
    public async Task CompleteSynchronousExchange_LeavesTheConnectionCleanAndReusable()
    {
        await RunAsync(new Codec(), async (stream, _) =>
        {
            await ConsumeFrameAsync(stream);
            await WriteAsync(stream, Frame(DataResponse, "first"u8.ToArray()));
            await ConsumeFrameAsync(stream);
            await WriteAsync(stream, Frame(DataResponse, "second"u8.ToArray()));
            await Task.Delay(3000);
        }, conn =>
        {
            RawFrame first = conn.SendRequest(DataRequest, [1, 2, 3], false, 0);
            Assert.Equal("first"u8.ToArray(), first.Body);
            Assert.False(conn.IsMidFrame, "the sync body read must reset the counter, as the async path does");
            Assert.False(conn.IsPoisoned);

            // The counter feeds IsPoisoned, which SendRequest refuses on: a stuck counter
            // would make the very next synchronous request throw.
            RawFrame second = conn.SendRequest(DataRequest, [4, 5, 6], false, 0);
            Assert.Equal("second"u8.ToArray(), second.Body);
            Assert.False(conn.IsMidFrame);
            return Task.CompletedTask;
        });
    }

    // --- a read over an abandoned frame's tail is refused ---------------------

    /// <summary>A read that would start over an abandoned frame's tail is refused, naming the byte count.</summary>
    [Fact]
    public async Task ReadStartingOverAnAbandonedFrame_IsRefusedAndNamesTheByteCount()
    {
        byte[] innerFrame = Frame(DataResponse, "inner-payload"u8.ToArray());
        byte[] filler = new byte[5];
        // Frame A claims a payload that is really `innerFrame` + 5 more bytes, so a fresh header
        // read over A's tail would parse and succeed - the failure mode being refused here.
        byte[] headerOfA = new byte[WireHeader.Size];
        WireHeader.ForRequest(WireHeader.ProtocolVersionV3, DataResponse,
            (uint)(innerFrame.Length + filler.Length)).WriteTo(headerOfA);

        var headerSent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var abandoned = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await RunAsync(new Codec(), async (stream, _) =>
        {
            await WriteAsync(stream, headerOfA);          // header only: the body never follows
            headerSent.SetResult();
            await abandoned.Task;
            await WriteAsync(stream, innerFrame);         // A's declared payload, byte for byte
            await WriteAsync(stream, filler);
            await Task.Delay(3000);
        }, async conn =>
        {
            await headerSent.Task;
            using (var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(400)))
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => conn.ReadFrameAsync(cts.Token));

            Assert.True(conn.IsMidFrame, "A's header was consumed, its body was not");
            abandoned.SetResult();

            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(3));
            var refused = await Assert.ThrowsAsync<InvalidDataException>(
                () => conn.ReadFrameAsync(deadline.Token));
            Assert.Contains($"{WireHeader.Size} bytes into an abandoned frame", refused.Message);
            Assert.True(conn.IsPoisoned, "the refusal poisons: the offset is permanent, not transient");
        });
    }

    // --- the counter's relationship to the cancellation point -----------------

    /// <summary>A deadline that expires on a quiet socket consumes nothing and is not mid-frame.</summary>
    [Fact]
    public async Task QuietDeadline_ConsumesNothing_IsNotMidFrame()
    {
        await RunAsync(new Codec(), async (_, _) => await Task.Delay(3000), async conn =>
        {
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(250));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => conn.ReadFrameAsync(cts.Token));
            Assert.False(conn.IsMidFrame);
        });
    }

    /// <summary>Bytes taken before a deadline fires are counted, so a truncated read is mid-frame.</summary>
    [Fact]
    public async Task DeadlineAfterPartialBytes_CountsThemBeforeCancelling()
    {
        var sent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await RunAsync(new Codec(), async (stream, _) =>
        {
            await WriteAsync(stream, Frame(DataResponse, "x"u8.ToArray())[..8]);
            sent.SetResult();
            await Task.Delay(3000);
        }, async conn =>
        {
            await sent.Task;
            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => conn.ReadFrameAsync(cts.Token));
            Assert.True(conn.IsMidFrame);
        });
    }

    // --- the pool discards a mid-frame connection -----------------------------

    /// <summary>A mid-frame connection is discarded on return, never handed to the next borrower.</summary>
    [Fact]
    public async Task MidFrameConnection_IsDiscardedByThePool_NotHandedToTheNextBorrower()
    {
        var sent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;

        var stop = new CancellationTokenSource();
        var accepted = new List<TcpClient>();
        var session = Task.Run(async () =>
        {
            try
            {
                bool first = true;
                while (!stop.IsCancellationRequested)
                {
                    var client = await listener.AcceptTcpClientAsync(stop.Token);
                    client.NoDelay = true;
                    lock (accepted) accepted.Add(client);
                    if (!first)
                        continue;
                    first = false;
                    // Eight bytes of a frame and nothing that completes it.
                    await WriteAsync(client.GetStream(), Frame(DataResponse, "x"u8.ToArray())[..8]);
                    sent.SetResult();
                }
            }
            catch { }
        });

        try
        {
            await using var pool = new ConnectionPool<CeleriantConnection>(
                $"127.0.0.1:{port}", 1, TimeSpan.FromMinutes(5),
                ct => CeleriantConnection.ConnectAsync(
                    $"127.0.0.1:{port}", TimeSpan.FromSeconds(5), null, new Codec(), new Ex(), ct),
                static c => c.IsPoisoned,           // exactly what NodeConnectionPool passes
                new Ex());

            var lease = await pool.GetConnectionAsync(default);
            CeleriantConnection borrowed = lease.Connection;
            await sent.Task;
            using (var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(400)))
                await Assert.ThrowsAnyAsync<OperationCanceledException>(
                    () => borrowed.ReadFrameAsync(cts.Token));

            Assert.True(borrowed.IsMidFrame);
            Assert.True(borrowed.IsPoisoned, "mid-frame is what the pool's isBroken predicate consults");

            await lease.DisposeAsync();             // an ordinary return, not MarkBroken()

            var next = await pool.GetConnectionAsync(default);
            Assert.NotSame(borrowed, next.Connection);
            Assert.False(next.Connection.IsMidFrame);
            await next.DisposeAsync();
        }
        finally
        {
            stop.Cancel();
            listener.Stop();
            await Task.WhenAny(session, Task.Delay(1000));
            lock (accepted)
                foreach (var c in accepted)
                    c.Dispose();
            stop.Dispose();
        }
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

    private static byte[] CompressedFrame(uint type, byte[] body, uint uncompressed)
    {
        var frame = new byte[WireHeader.Size + body.Length];
        new WireHeader(WireHeader.ProtocolVersionV3, type, (uint)body.Length, uncompressed,
            (byte)CompressionType.ZstdDict).WriteTo(frame);
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
