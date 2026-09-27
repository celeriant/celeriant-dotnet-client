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
    /// Null when the cluster's compression algorithm is not zstd-dictionary based.
    /// </summary>
    [Key(3)]
    public string? CompressionDictSha256 { get; init; }

    /// <summary>
    /// Raw dictionary bytes. Present only when the client did not already advertise a matching
    /// <c>KnownDictSha256</c>; otherwise null and the bytes are resolved by
    /// <see cref="CompressionDictSha256"/> against the dictionary the connection advertised.
    /// </summary>
    [Key(4)]
    [MessagePackFormatter(typeof(SeqOrBinBytesFormatter))]
    public byte[]? CompressionDictBytes { get; init; }
}
