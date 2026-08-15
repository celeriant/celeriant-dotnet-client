using System.Security.Cryptography;
using System.Text;

namespace Celeriant.Transport;

/// <summary>
/// Cryptographic utilities for the Celeriant Identify handshake. Identical across products:
/// RSASSA-PKCS1-v1_5 with SHA-256 over a millisecond-epoch nonce.
/// </summary>
public static class CeleriantCrypto
{
    /// <summary>
    /// Sign a nonce with an RSA private key (PKCS#8 DER, base64-encoded).
    /// Returns the base64-encoded signature.
    /// </summary>
    public static string SignNonce(string privateKeyBase64, string nonce)
    {
        byte[] privateKeyDer = Convert.FromBase64String(privateKeyBase64);
        using var rsa = RSA.Create();
        rsa.ImportPkcs8PrivateKey(privateKeyDer, out _);

        byte[] nonceBytes = Encoding.UTF8.GetBytes(nonce);
        byte[] signature = rsa.SignData(nonceBytes, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        return Convert.ToBase64String(signature);
    }

    /// <summary>Generate a nonce: current UTC epoch milliseconds as a decimal string.</summary>
    public static string GenerateNonce()
        => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString();

    /// <summary>
    /// Derive client identity (u128 / Guid) from a DER-encoded public key: SHA-256 of the DER
    /// bytes, first 16 bytes as a little-endian u128 (matching the Rust representation).
    /// </summary>
    public static Guid GenerateClientIdentity(string publicKeyBase64)
    {
        byte[] publicKeyDer = Convert.FromBase64String(publicKeyBase64);
        byte[] hash = SHA256.HashData(publicKeyDer);
        return new Guid(hash.AsSpan(0, 16));
    }
}
