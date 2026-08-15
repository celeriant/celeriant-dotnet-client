using ZstdSharp;

namespace Celeriant.Transport;

/// <summary>
/// Zstd-dictionary compression for data-bearing frames. The body itself is serialized by the
/// product's codec (MessagePack or bincode); this only wraps/unwraps it with the dictionary the
/// cluster shipped during Identify. Produces a standard zstd frame compatible with the Rust
/// <c>zstd::bulk</c> dictionary path.
/// </summary>
public static class DictCompression
{
    /// <summary>
    /// Compression level for outbound dictionary compression. Mirrors the server default;
    /// the level affects compressed size only, not decompressibility.
    /// </summary>
    private const int CompressionLevel = 3;

    public static byte[] CompressWithDict(byte[] data, byte[] dict)
    {
        using var compressor = new Compressor(CompressionLevel);
        compressor.LoadDictionary(dict);
        return compressor.Wrap(data).ToArray();
    }

    public static byte[] DecompressWithDict(byte[] data, uint uncompressedLength, byte[] dict)
    {
        using var decompressor = new Decompressor();
        decompressor.LoadDictionary(dict);
        return decompressor.Unwrap(data, (int)uncompressedLength).ToArray();
    }
}
