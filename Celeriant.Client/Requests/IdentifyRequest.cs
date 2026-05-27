using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Requests;

[MessagePackObject]
public sealed class IdentifyRequest
{
    [Key(0)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? CorrelationId { get; init; }

    /// <summary>DER public key, base64-encoded.</summary>
    [Key(1)]
    public string? PublicKey { get; init; }

    /// <summary>Nonce (UTC epoch milliseconds as decimal string).</summary>
    [Key(2)]
    public string? Nonce { get; init; }

    /// <summary>RSA-PKCS1v15-SHA256 signature of the nonce, base64-encoded.</summary>
    [Key(3)]
    public string? Signature { get; init; }

    /// <summary>32-byte API key, base64-encoded. Alternative to RSA key pair.</summary>
    [Key(4)]
    public string? ApiKey { get; init; }

    /// <summary>
    /// SHA-256 hex of the compression dictionary the client already has cached, if any.
    /// When it matches the cluster's current dictionary, the server returns the sha only
    /// (no bytes), avoiding a redundant ~14&#160;KiB transfer. Null on the first connection.
    /// </summary>
    [Key(5)]
    public string? KnownDictSha256 { get; init; }
}
