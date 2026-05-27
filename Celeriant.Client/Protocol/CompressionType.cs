namespace Celeriant.Client.Protocol;

/// <summary>
/// Compression algorithm used for wire-level message encoding.
/// Matches the Rust <c>CompressionType</c> enum discriminants.
///
/// <para>
/// The cluster ships a zstd dictionary to the client during the Identify handshake.
/// Variable-size requests (writes, schema registration) above a size threshold are
/// compressed with that dictionary; responses may arrive dictionary-compressed too.
/// A client that has not received a dictionary always uses <see cref="None"/>.
/// </para>
/// </summary>
public enum CompressionType : byte
{
    None = 0,
    ZstdDict = 1,
}
