using MessagePack;
using ZstdSharp;

namespace Celeriant.Client.Protocol;

/// <summary>
/// Handles MessagePack serialization/deserialization and dictionary-based zstd
/// compression for the Celeriant wire protocol.
/// </summary>
internal static class WireCodec
{
    /// <summary>
    /// Compression level for outbound dictionary compression. Mirrors the server default;
    /// the level affects compressed size only, not decompressibility.
    /// </summary>
    private const int CompressionLevel = 3;

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
    /// Compress <paramref name="data"/> with the cluster's zstd dictionary.
    /// Produces a standard zstd frame compatible with the Rust <c>zstd::bulk</c> dictionary path.
    /// </summary>
    public static byte[] CompressWithDict(byte[] data, byte[] dict)
    {
        using var compressor = new Compressor(CompressionLevel);
        compressor.LoadDictionary(dict);
        return compressor.Wrap(data).ToArray();
    }

    /// <summary>
    /// Decompress a dictionary-compressed zstd frame.
    /// </summary>
    /// <param name="data">The compressed frame.</param>
    /// <param name="uncompressedLength">Exact decompressed size from the wire header.</param>
    /// <param name="dict">The cluster dictionary the frame was compressed with.</param>
    public static byte[] DecompressWithDict(byte[] data, uint uncompressedLength, byte[] dict)
    {
        using var decompressor = new Decompressor();
        decompressor.LoadDictionary(dict);
        return decompressor.Unwrap(data, (int)uncompressedLength).ToArray();
    }
}
