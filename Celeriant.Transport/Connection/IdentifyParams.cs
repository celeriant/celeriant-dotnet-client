namespace Celeriant.Transport;

/// <summary>
/// Product-neutral inputs for the Identify handshake. The connection serializes these via the
/// injected <see cref="IConnectionCodec"/> (MessagePack or bincode) into the celeriant_msg
/// IdentifyRequest the server expects.
/// </summary>
public readonly record struct IdentifyParams(
    string? PublicKey,
    string? Nonce,
    string? Signature,
    string? ApiKey,
    string? KnownDictSha256)
{
    /// <summary>
    /// Build credentials from a resolved config: API key wins; otherwise an RSA key pair signs a
    /// fresh nonce; otherwise an anonymous Identify (valid only on auth-disabled loopback nodes
    /// when <paramref name="allowAnonymous"/> is true).
    /// </summary>
    public static IdentifyParams ForCredentials(
        string? apiKeyBase64,
        string? publicKeyBase64,
        string? privateKeyBase64,
        string? knownDictSha,
        bool allowAnonymous)
    {
        if (!string.IsNullOrEmpty(apiKeyBase64))
            return new IdentifyParams(null, null, null, apiKeyBase64, knownDictSha);

        if (!string.IsNullOrEmpty(publicKeyBase64) && !string.IsNullOrEmpty(privateKeyBase64))
        {
            string nonce = CeleriantCrypto.GenerateNonce();
            string signature = CeleriantCrypto.SignNonce(privateKeyBase64!, nonce);
            return new IdentifyParams(publicKeyBase64, nonce, signature, null, knownDictSha);
        }

        if (allowAnonymous)
            return new IdentifyParams(null, null, null, null, knownDictSha);

        throw new ArgumentException(
            "Identity config must have either an API key, a client id, or an RSA key pair.");
    }
}
