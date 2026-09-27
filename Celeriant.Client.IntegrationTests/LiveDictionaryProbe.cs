using System.Buffers;
using System.Globalization;
using System.Net.Sockets;
using Celeriant.Transport;
using MessagePack;

namespace Celeriant.Client.IntegrationTests;

/// <summary>What a live server answered to one raw Identify.</summary>
internal sealed record RawIdentifyReply(string? Sha, byte[]? Bytes);

/// <summary>
/// Black-box probes for the dictionary handshake: a strict scrape of the server's metrics endpoint,
/// and a hand-encoded V5 Identify on a bare socket that uses no client code, so the server's sha and
/// bytes are read straight off the wire.
/// </summary>
internal static class LiveDictionaryProbe
{
    public const string ShippedCounter = "celeriant_identify_dictionary_bytes_shipped_total";
    public const string ConnectionGauge = "celeriant_client_connections_active";

    private const uint IdentifyRequestType = 14;
    private const uint IdentifyResponseType = 16;
    private static readonly HttpClient Http = new() { Timeout = TimeSpan.FromSeconds(5) };

    /// <summary>
    /// Sum of <paramref name="metric"/> over the series whose labels contain <paramref name="labelFilter"/>.
    /// A metric the server does not expose is a failure, never a silent zero.
    /// </summary>
    public static async Task<long> Scrape(RustServerProcess server, string metric, string? labelFilter = null)
    {
        string url = $"http://127.0.0.1:{server.MetricsPort}/metrics";
        string body = await Http.GetStringAsync(url);
        long? total = null;
        foreach (string line in body.Split('\n'))
        {
            if (line.StartsWith('#') || string.IsNullOrWhiteSpace(line))
                continue;
            string[] parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length < 2)
                continue;
            string bare = parts[0].Split('{')[0];
            if (bare != metric || (labelFilter is not null && !parts[0].Contains(labelFilter)))
                continue;
            total = (total ?? 0) + (long)double.Parse(parts[1], CultureInfo.InvariantCulture);
        }
        return total ?? throw new InvalidOperationException($"{metric} ({labelFilter}) is not exposed on {url}");
    }

    public static Task<long> Shipped(RustServerProcess server) => Scrape(server, ShippedCounter);

    /// <summary>Counter delta since <paramref name="before"/>, after letting any in-flight increment land.</summary>
    public static async Task<long> ShippedSince(RustServerProcess server, long before)
    {
        await Task.Delay(200);
        return await Shipped(server) - before;
    }

    /// <summary>Open client-port connections on the server.</summary>
    public static Task<long> OpenClientConnections(RustServerProcess server) =>
        Scrape(server, ConnectionGauge, "port_type=\"client\"");

    /// <summary>Poll the connection gauge until it reads <paramref name="expected"/>, returning the last value seen.</summary>
    public static async Task<long> AwaitOpenClientConnections(RustServerProcess server, long expected, TimeSpan budget)
    {
        var deadline = DateTime.UtcNow + budget;
        long seen;
        do
        {
            seen = await OpenClientConnections(server);
            if (seen == expected)
                return seen;
            await Task.Delay(50);
        } while (DateTime.UtcNow < deadline);
        return seen;
    }

    /// <summary>
    /// One Identify on a fresh socket: V5 positional array (correlation_id, public_key, nonce,
    /// signature, api_key, known_dict_sha256), all nil but the sha. Returns the server's
    /// compression_dict_sha256 and compression_dict_bytes.
    /// </summary>
    public static async Task<RawIdentifyReply> RawIdentify(string address, string? knownSha)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new MessagePackWriter(buffer);
        writer.WriteArrayHeader(6);
        for (int i = 0; i < 5; i++)
            writer.WriteNil();
        if (knownSha is null)
            writer.WriteNil();
        else
            writer.Write(knownSha);
        writer.Flush();
        byte[] body = buffer.WrittenSpan.ToArray();

        var frame = new byte[WireHeader.Size + body.Length];
        WireHeader.ForRequest(WireHeader.ProtocolVersionV5, IdentifyRequestType, (uint)body.Length).WriteTo(frame);
        body.CopyTo(frame, WireHeader.Size);

        string[] hostPort = address.Split(':');
        using var tcp = new TcpClient { NoDelay = true };
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await tcp.ConnectAsync(hostPort[0], int.Parse(hostPort[1]), timeout.Token);
        NetworkStream stream = tcp.GetStream();
        await stream.WriteAsync(frame, timeout.Token);

        var headerBytes = new byte[WireHeader.Size];
        await stream.ReadExactlyAsync(headerBytes, timeout.Token);
        WireHeader header = WireHeader.ParseFrom(headerBytes);
        if (header.MessageType != IdentifyResponseType)
            throw new InvalidOperationException($"raw Identify: reply type {header.MessageType}, expected {IdentifyResponseType}");
        if (header.CompressionType != 0 || header.CompressedLength != header.UncompressedLength)
            throw new InvalidOperationException($"raw Identify: reply compressed with type {header.CompressionType}; the probe decodes plain bodies only");
        if (header.CompressedLength > HandshakeLimits.IdentifyResponseMaxBytes)
            throw new InvalidOperationException($"raw Identify: reply of {header.CompressedLength} bytes is past the handshake limit");
        var replyBody = new byte[header.CompressedLength];
        await stream.ReadExactlyAsync(replyBody, timeout.Token);
        return DecodeIdentifyResponse(replyBody);
    }

    /// <summary>Positional: correlation_id, client_id, access_level, compression_dict_sha256, compression_dict_bytes, then anything newer.</summary>
    private static RawIdentifyReply DecodeIdentifyResponse(byte[] body)
    {
        var reader = new MessagePackReader(body);
        int count = reader.ReadArrayHeader();
        if (count < 5)
            throw new InvalidOperationException($"raw Identify: response array has {count} elements, expected at least 5");
        reader.Skip(); // correlation_id
        reader.Skip(); // client_id
        reader.Skip(); // access_level
        string? sha = reader.TryReadNil() ? null : reader.ReadString();
        byte[]? bytes;
        if (reader.TryReadNil())
        {
            bytes = null;
        }
        else if (reader.NextMessagePackType == MessagePackType.Binary)
        {
            bytes = reader.ReadBytes()!.Value.ToArray();
        }
        else
        {
            int length = reader.ReadArrayHeader();
            bytes = new byte[length];
            for (int i = 0; i < length; i++)
                bytes[i] = reader.ReadByte();
        }
        return new RawIdentifyReply(sha, bytes);
    }

    /// <summary>Deterministic xorshift noise; the first byte is 'H' so it never opens with the zstd dictionary magic.</summary>
    public static byte[] CustomDictionary(int length)
    {
        var dict = new byte[length];
        ulong state = 0x2545_F491_4F6C_DD1DUL;
        for (int i = 0; i < length; i++)
        {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            dict[i] = (byte)state;
        }
        dict[0] = (byte)'H';
        return dict;
    }

    public static string Sha(byte[] bytes) =>
        Convert.ToHexString(System.Security.Cryptography.SHA256.HashData(bytes)).ToLowerInvariant();
}
