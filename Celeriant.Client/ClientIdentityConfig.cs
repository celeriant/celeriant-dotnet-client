namespace Celeriant.Client;

/// <summary>
/// Authentication configuration for Celeriant client identity verification.
///
/// <para>
/// Use one of the static factory methods to create an instance:
/// <list type="bullet">
///   <item><see cref="FromApiKey"/> — authenticate with a base64-encoded API key.</item>
///   <item><see cref="FromClientId"/> — authenticate with a <see cref="Guid"/> client ID (u128).
///   Easy to store alongside offsets in PostgreSQL as a UUID column.</item>
///   <item><see cref="FromRsaKeyPair"/> — authenticate with an RSA key pair for nonce signing.</item>
/// </list>
/// </para>
/// </summary>
public sealed class ClientIdentityConfig
{
    /// <summary>
    /// DER-encoded RSA public key, base64-encoded.
    /// Used together with <see cref="PrivateKeyBase64"/> for RSA signing authentication.
    /// </summary>
    public string? PublicKeyBase64 { get; init; }

    /// <summary>
    /// DER-encoded PKCS#8 RSA private key, base64-encoded.
    /// Used to sign the nonce during the Identify handshake.
    /// </summary>
    public string? PrivateKeyBase64 { get; init; }

    /// <summary>
    /// 32-byte API key, base64-encoded.
    /// Used as an alternative to RSA key-pair authentication.
    /// </summary>
    public string? ApiKeyBase64 { get; init; }

    /// <summary>
    /// 128-bit client identifier. Converted to a base64-encoded API key for the wire protocol.
    /// Maps to the server's u128 client ID and can be stored as a UUID in PostgreSQL.
    /// </summary>
    public Guid? ClientId { get; init; }

    /// <summary>
    /// Create an identity configuration from a base64-encoded API key.
    /// </summary>
    /// <param name="base64Key">The 32-byte API key, base64-encoded.</param>
    public static ClientIdentityConfig FromApiKey(string base64Key)
        => new() { ApiKeyBase64 = base64Key ?? throw new ArgumentNullException(nameof(base64Key)) };

    /// <summary>
    /// Create an identity configuration from a <see cref="Guid"/> client ID.
    /// The Guid (u128) is converted to a base64-encoded key for the wire protocol.
    /// This is convenient when storing client IDs as UUID columns in PostgreSQL
    /// alongside event offsets.
    /// </summary>
    /// <param name="clientId">The 128-bit client identifier.</param>
    public static ClientIdentityConfig FromClientId(Guid clientId)
        => new() { ClientId = clientId };

    /// <summary>
    /// Create an identity configuration from a DER-encoded RSA key pair.
    /// The private key is used to sign a server-provided nonce during the Identify handshake.
    /// </summary>
    /// <param name="publicKeyBase64">DER-encoded RSA public key, base64-encoded.</param>
    /// <param name="privateKeyBase64">DER-encoded PKCS#8 RSA private key, base64-encoded.</param>
    public static ClientIdentityConfig FromRsaKeyPair(string publicKeyBase64, string privateKeyBase64)
        => new()
        {
            PublicKeyBase64 = publicKeyBase64 ?? throw new ArgumentNullException(nameof(publicKeyBase64)),
            PrivateKeyBase64 = privateKeyBase64 ?? throw new ArgumentNullException(nameof(privateKeyBase64)),
        };

    /// <summary>
    /// Resolve the effective API key for the wire protocol.
    /// Returns <see cref="ApiKeyBase64"/> if set, otherwise converts <see cref="ClientId"/> to base64.
    /// Returns null if neither is set.
    /// </summary>
    internal string? ResolveApiKeyBase64()
    {
        if (ApiKeyBase64 is not null)
            return ApiKeyBase64;

        if (ClientId.HasValue)
            return Convert.ToBase64String(ClientId.Value.ToByteArray());

        return null;
    }
}
