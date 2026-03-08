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
}
