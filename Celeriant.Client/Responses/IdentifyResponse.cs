using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Responses;

[MessagePackObject]
public sealed class IdentifyResponse
{
    [Key(0)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? CorrelationId { get; init; }

    [Key(1)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? ClientId { get; init; }

    [Key(2)]
    public AccessLevel? AccessLevel { get; init; }

    /// <summary>
    /// SHA-256 hex of the cluster's current compression dictionary.
    /// Null when the cluster's compression algorithm is not <see cref="Protocol.CompressionType.ZstdDict"/>.
    /// </summary>
    [Key(3)]
    public string? CompressionDictSha256 { get; init; }

    /// <summary>
    /// Raw dictionary bytes (~14&#160;KiB). Present only when the client did not already
    /// advertise a matching <c>KnownDictSha256</c>; otherwise null and the bytes are
    /// resolved from the pool's dictionary cache by <see cref="CompressionDictSha256"/>.
    /// </summary>
    [Key(4)]
    public byte[]? CompressionDictBytes { get; init; }
}
