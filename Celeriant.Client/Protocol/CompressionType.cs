namespace Celeriant.Client.Protocol;

/// <summary>
/// Compression algorithm used for wire-level message encoding.
/// Matches the Rust CompressionType enum values.
/// </summary>
public enum CompressionType : byte
{
    None = 0,
    Zstd = 1,
    Snappy = 2,
    Brotli = 3,
    Gzip = 4,
}
