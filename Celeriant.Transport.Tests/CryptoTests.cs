using System.Security.Cryptography;
using System.Text;
using Celeriant.Transport;

namespace Celeriant.Transport.Tests;

/// <summary>
/// Unit tests for CeleriantCrypto: nonce generation, RSA signing, and client identity derivation.
/// </summary>
public class CryptoTests
{
    // -----------------------------------------------------------------------
    // GenerateNonce
    // -----------------------------------------------------------------------

    [Fact]
    public void GenerateNonce_ReturnsNumericString()
    {
        var nonce = CeleriantCrypto.GenerateNonce();

        Assert.False(string.IsNullOrEmpty(nonce));
        Assert.True(long.TryParse(nonce, out _), $"Nonce '{nonce}' is not a valid long integer.");
    }

    [Fact]
    public void GenerateNonce_ValueIsReasonableEpochMillis()
    {
        var before = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var nonce  = CeleriantCrypto.GenerateNonce();
        var after  = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        long value = long.Parse(nonce);
        Assert.True(value >= before, $"Nonce {value} is before the call time {before}.");
        Assert.True(value <= after,  $"Nonce {value} is after the call returned {after}.");
    }

    [Fact]
    public void GenerateNonce_TwoConsecutiveCalls_AreMonotonicallyNonDecreasing()
    {
        var first  = long.Parse(CeleriantCrypto.GenerateNonce());
        var second = long.Parse(CeleriantCrypto.GenerateNonce());

        Assert.True(second >= first, $"Second nonce {second} is less than first nonce {first}.");
    }

    // -----------------------------------------------------------------------
    // SignNonce + verify
    // -----------------------------------------------------------------------

    [Fact]
    public void SignNonce_WithGeneratedKeyPair_SignatureVerifies()
    {
        var (privateKeyBase64, publicKeyBase64) = GenerateKeyPair();
        var nonce     = CeleriantCrypto.GenerateNonce();
        var signature = CeleriantCrypto.SignNonce(privateKeyBase64, nonce);

        // Verify using the public key directly
        byte[] publicKeyDer  = Convert.FromBase64String(publicKeyBase64);
        byte[] signatureBytes = Convert.FromBase64String(signature);
        byte[] nonceBytes     = Encoding.UTF8.GetBytes(nonce);

        using var rsa = RSA.Create();
        rsa.ImportSubjectPublicKeyInfo(publicKeyDer, out _);

        bool valid = rsa.VerifyData(nonceBytes, signatureBytes, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        Assert.True(valid, "Signature verification failed.");
    }

    [Fact]
    public void SignNonce_ProducesValidBase64Output()
    {
        var (privateKeyBase64, _) = GenerateKeyPair();
        var nonce     = "1700000000000";
        var signature = CeleriantCrypto.SignNonce(privateKeyBase64, nonce);

        // Should decode without exception
        var bytes = Convert.FromBase64String(signature);
        Assert.NotEmpty(bytes);
    }

    [Fact]
    public void SignNonce_DifferentNonces_ProduceDifferentSignatures()
    {
        var (privateKeyBase64, _) = GenerateKeyPair();

        var sig1 = CeleriantCrypto.SignNonce(privateKeyBase64, "1000000000000");
        var sig2 = CeleriantCrypto.SignNonce(privateKeyBase64, "2000000000000");

        Assert.NotEqual(sig1, sig2);
    }

    [Fact]
    public void SignNonce_WrongKey_SignatureDoesNotVerify()
    {
        var (privateKey1, _)      = GenerateKeyPair();
        var (_, publicKey2)       = GenerateKeyPair();

        var nonce     = "1700000000000";
        var signature = CeleriantCrypto.SignNonce(privateKey1, nonce);

        byte[] publicKeyDer   = Convert.FromBase64String(publicKey2);
        byte[] signatureBytes = Convert.FromBase64String(signature);
        byte[] nonceBytes     = Encoding.UTF8.GetBytes(nonce);

        using var rsa = RSA.Create();
        rsa.ImportSubjectPublicKeyInfo(publicKeyDer, out _);

        bool valid = rsa.VerifyData(nonceBytes, signatureBytes, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        Assert.False(valid, "Signature from wrong key should not verify.");
    }

    // -----------------------------------------------------------------------
    // GenerateClientIdentity
    // -----------------------------------------------------------------------

    [Fact]
    public void GenerateClientIdentity_IsDeteministic_SameKeyProducesSameGuid()
    {
        var (_, publicKeyBase64) = GenerateKeyPair();

        var id1 = CeleriantCrypto.GenerateClientIdentity(publicKeyBase64);
        var id2 = CeleriantCrypto.GenerateClientIdentity(publicKeyBase64);

        Assert.Equal(id1, id2);
    }

    [Fact]
    public void GenerateClientIdentity_DifferentKeys_ProduceDifferentGuids()
    {
        var (_, publicKey1) = GenerateKeyPair();
        var (_, publicKey2) = GenerateKeyPair();

        var id1 = CeleriantCrypto.GenerateClientIdentity(publicKey1);
        var id2 = CeleriantCrypto.GenerateClientIdentity(publicKey2);

        Assert.NotEqual(id1, id2);
    }

    [Fact]
    public void GenerateClientIdentity_ReturnsNonEmptyGuid()
    {
        var (_, publicKeyBase64) = GenerateKeyPair();
        var id = CeleriantCrypto.GenerateClientIdentity(publicKeyBase64);
        Assert.NotEqual(Guid.Empty, id);
    }

    [Fact]
    public void GenerateClientIdentity_ProducesFirst16BytesOfSha256()
    {
        var (_, publicKeyBase64) = GenerateKeyPair();
        byte[] publicKeyDer = Convert.FromBase64String(publicKeyBase64);
        byte[] hash         = SHA256.HashData(publicKeyDer);

        // The Guid should be constructed from the first 16 bytes of the SHA-256 hash.
        // We reconstruct the expected Guid using the same approach as the implementation.
        var expectedGuid = new Guid(hash.AsSpan(0, 16));
        var actualGuid   = CeleriantCrypto.GenerateClientIdentity(publicKeyBase64);

        Assert.Equal(expectedGuid, actualGuid);
    }

    // -----------------------------------------------------------------------
    // Helper: generate an RSA 2048 key pair, return (privateKeyPkcs8DerBase64, subjectPublicKeyInfoDerBase64)
    // -----------------------------------------------------------------------

    private static (string PrivateKeyBase64, string PublicKeyBase64) GenerateKeyPair()
    {
        using var rsa = RSA.Create(2048);

        byte[] privateKeyDer = rsa.ExportPkcs8PrivateKey();
        byte[] publicKeyDer  = rsa.ExportSubjectPublicKeyInfo();

        return (Convert.ToBase64String(privateKeyDer), Convert.ToBase64String(publicKeyDer));
    }
}
