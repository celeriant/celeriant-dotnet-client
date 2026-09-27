namespace Celeriant.Transport;

/// <summary>
/// A zstd compression dictionary received from the cluster during the Identify handshake.
/// Shared (by content sha) across connections via a pool's dictionary cache. <see cref="Bytes"/> is
/// the one copy every connection compresses and decompresses with, so it must never be written.
/// </summary>
/// <param name="Sha">SHA-256 hex of <paramref name="Bytes"/>, as reported by the server.</param>
/// <param name="Bytes">Raw dictionary bytes used for zstd compression and decompression.</param>
public sealed record CachedDict(string Sha, byte[] Bytes);
