using System.IO.Compression;
using MessagePack;
using Snappier;
using ZstdSharp;

namespace Celeriant.Client.Protocol;

/// <summary>
/// Handles MessagePack serialization/deserialization and wire-level compression
/// for the Celeriant V3 protocol.
/// </summary>
internal static class WireCodec
{
    /// <summary>
    /// MessagePack serializer options using the Celeriant resolver.
    /// </summary>
    public static MessagePackSerializerOptions Options => CeleriantResolver.Options;

    /// <summary>
    /// Serialize a value to MessagePack bytes.
    /// </summary>
    public static byte[] Serialize<T>(T value)
    {
        return MessagePackSerializer.Serialize(value, Options);
    }

    /// <summary>
    /// Deserialize MessagePack bytes to a value.
    /// </summary>
    public static T Deserialize<T>(ReadOnlyMemory<byte> data)
    {
        return MessagePackSerializer.Deserialize<T>(data, Options);
    }

    /// <summary>
    /// Compress bytes using the specified algorithm.
    /// Returns the original array unchanged when <paramref name="compression"/> is None.
    /// </summary>
    public static byte[] Compress(byte[] data, CompressionType compression)
    {
        return compression switch
        {
            CompressionType.None   => data,
            CompressionType.Zstd   => CompressZstd(data),
            CompressionType.Snappy => CompressSnappy(data),
            CompressionType.Brotli => CompressBrotli(data),
            CompressionType.Gzip   => CompressGzip(data),
            _ => throw new ArgumentOutOfRangeException(nameof(compression), compression, "Unknown compression type.")
        };
    }

    /// <summary>
    /// Decompress bytes using the specified algorithm.
    /// Returns the original array unchanged when <paramref name="compression"/> is None.
    /// </summary>
    public static byte[] Decompress(byte[] data, CompressionType compression, uint uncompressedLength)
    {
        return compression switch
        {
            CompressionType.None   => data,
            CompressionType.Zstd   => DecompressZstd(data, uncompressedLength),
            CompressionType.Snappy => DecompressSnappy(data),
            CompressionType.Brotli => DecompressBrotli(data, uncompressedLength),
            CompressionType.Gzip   => DecompressGzip(data, uncompressedLength),
            _ => throw new ArgumentOutOfRangeException(nameof(compression), compression, "Unknown compression type.")
        };
    }

    // --- Zstd ---

    private static byte[] CompressZstd(byte[] data)
    {
        using var compressor = new Compressor();
        return compressor.Wrap(data).ToArray();
    }

    private static byte[] DecompressZstd(byte[] data, uint uncompressedLength)
    {
        using var decompressor = new Decompressor();
        return decompressor.Unwrap(data, (int)uncompressedLength).ToArray();
    }

    // --- Snappy ---

    private static byte[] CompressSnappy(byte[] data)
    {
        return Snappy.CompressToArray(data);
    }

    private static byte[] DecompressSnappy(byte[] data)
    {
        return Snappy.DecompressToArray(data);
    }

    // --- Brotli ---

    private static byte[] CompressBrotli(byte[] data)
    {
        // Quality 4 balances speed and ratio; window 22 is the default maximum.
        using var output = new MemoryStream();
        using (var brotliStream = new BrotliStream(output, CompressionLevel.Fastest, leaveOpen: true))
        {
            brotliStream.Write(data, 0, data.Length);
        }
        return output.ToArray();
    }

    private static byte[] DecompressBrotli(byte[] data, uint uncompressedLength)
    {
        using var input = new MemoryStream(data);
        using var brotliStream = new BrotliStream(input, CompressionMode.Decompress);
        var output = new byte[uncompressedLength];
        int totalRead = 0;
        while (totalRead < (int)uncompressedLength)
        {
            int read = brotliStream.Read(output, totalRead, output.Length - totalRead);
            if (read == 0)
                throw new InvalidDataException("Brotli decompression ended prematurely.");
            totalRead += read;
        }
        return output;
    }

    // --- Gzip ---

    private static byte[] CompressGzip(byte[] data)
    {
        using var output = new MemoryStream();
        using (var gzipStream = new GZipStream(output, CompressionLevel.Fastest, leaveOpen: true))
        {
            gzipStream.Write(data, 0, data.Length);
        }
        return output.ToArray();
    }

    private static byte[] DecompressGzip(byte[] data, uint uncompressedLength)
    {
        using var input = new MemoryStream(data);
        using var gzipStream = new GZipStream(input, CompressionMode.Decompress);
        var output = new byte[uncompressedLength];
        int totalRead = 0;
        while (totalRead < (int)uncompressedLength)
        {
            int read = gzipStream.Read(output, totalRead, output.Length - totalRead);
            if (read == 0)
                throw new InvalidDataException("Gzip decompression ended prematurely.");
            totalRead += read;
        }
        return output;
    }
}
