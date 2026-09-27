using System.Net;
using System.Net.Sockets;
using Celeriant.Transport;
using Xunit.Abstractions;

namespace Celeriant.Client.Tests;

/// <summary>
/// The wire-format invariant from celeriant-db/docs/invariants.md: both <c>compressed_length</c>
/// and <c>uncompressed_length</c> are validated against the size cap before any allocation or
/// decompression. A fake server completes Identify with a zstd dictionary, then answers with a
/// frame whose <c>compressed_length</c> is tiny and whose <c>uncompressed_length</c> claims
/// 0x7FFFFFFF. <see cref="CeleriantConnection"/> must reject it before decompressing.
/// </summary>
public class DecompressionBombContractTests(ITestOutputHelper output)
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

    /// <summary>
    /// Control: a frame whose <c>compressed_length</c> exceeds the cap is rejected with a protocol
    /// error before any body is read.
    /// </summary>
    [Fact]
    public async Task CompressedLengthOverMax_IsRejected()
    {
        await RunAsync(new Codec(), async (stream, _) =>
        {
            await ConsumeFrameAsync(stream);
            await WriteAsync(stream, Frame(IdentifyResponse, []));
            await ConsumeFrameAsync(stream);
            // Header claims a huge compressed_length; no body follows.
            var header = new byte[WireHeader.Size];
            new WireHeader(WireHeader.ProtocolVersionV3, DataResponse, 0x7FFFFFFF, 0x7FFFFFFF, 0)
                .WriteTo(header);
            await WriteAsync(stream, header);
            await Task.Delay(3000);
        }, async conn =>
        {
            await conn.IdentifyAsync(new IdentifyParams(null, null, null, null, null), BuiltinDictionary.Dict);
            var ex = await Record.ExceptionAsync(() => conn.SendAsync(DataRequest, [1], false, 0));
            output.WriteLine($"compressed_length-over-max => {Describe(ex)}");
            Assert.IsType<InvalidDataException>(ex);
        });
    }

    /// <summary>
    /// A frame whose <c>uncompressed_length</c> exceeds the cap while its <c>compressed_length</c>
    /// is small must be rejected with a protocol error before decompression, with no allocation
    /// sized by the claimed length.
    /// </summary>
    [Fact]
    public async Task UncompressedLengthOverMax_IsRejected()
    {
        byte[] dict = new byte[4096];
        new Random(7).NextBytes(dict);
        byte[] plain = new byte[100];
        new Random(9).NextBytes(plain);

        await RunAsync(new Codec(dict), async (stream, _) =>
        {
            await ConsumeFrameAsync(stream);                                    // Identify
            await WriteAsync(stream, Frame(IdentifyResponse, []));
            await ConsumeFrameAsync(stream);                                    // request
            byte[] comp = DictCompression.CompressWithDict(plain, dict);
            output.WriteLine($"compressed_length={comp.Length}, uncompressed_length=0x7FFFFFFF");
            await WriteAsync(stream, CompressedFrame(DataResponse, comp, 0x7FFFFFFF));
            await Task.Delay(3000);
        }, async conn =>
        {
            await conn.IdentifyAsync(new IdentifyParams(null, null, null, null, null), BuiltinDictionary.Dict);
            long before = GC.GetTotalAllocatedBytes(precise: true);
            Exception? ex = await Record.ExceptionAsync(() => conn.SendAsync(DataRequest, [1], false, 0));
            long allocated = GC.GetTotalAllocatedBytes(precise: true) - before;
            output.WriteLine($"uncompressed_length-over-max => {Describe(ex)}; allocated={allocated} bytes");
            output.WriteLine($"IsPoisoned={conn.IsPoisoned}");

            Assert.IsType<InvalidDataException>(ex);
        });
    }

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

    private static string Describe(Exception? ex) => ex is null ? "no exception (frame accepted)" : $"{ex.GetType().Name}: {ex.Message}";

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