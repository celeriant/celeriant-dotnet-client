using System.Net.Security;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Celeriant.Client;

/// <summary>
/// TLS configuration for an encrypted Celeriant connection.
///
/// <para>
/// Use one of the static factory methods for common scenarios:
/// <list type="bullet">
///   <item><see cref="Create"/> — server-only TLS with standard certificate validation.</item>
///   <item><see cref="WithClientCertificate(string, X509Certificate2)"/> — mTLS using a client certificate with an embedded private key.</item>
///   <item><see cref="WithClientCertificate(string, X509Certificate2, AsymmetricAlgorithm)"/> — mTLS with a
///   separate signing key (e.g. backed by AWS KMS, Azure Key Vault, or an HSM).</item>
///   <item><see cref="WithClientCertificateFromPem"/> — mTLS loading certificate and key from PEM files.</item>
///   <item><see cref="FromSslOptions"/> — full control via <see cref="SslClientAuthenticationOptions"/>.</item>
/// </list>
/// </para>
/// </summary>
public sealed class ClientTlsConfig
{
    /// <summary>
    /// The SSL/TLS options to use when authenticating the client to the server.
    /// </summary>
    public SslClientAuthenticationOptions SslOptions { get; }

    private ClientTlsConfig(SslClientAuthenticationOptions sslOptions)
    {
        SslOptions = sslOptions ?? throw new ArgumentNullException(nameof(sslOptions));
    }

    /// <summary>
    /// Create a TLS configuration for server-only authentication (no client certificate).
    /// Uses standard system certificate validation.
    /// </summary>
    /// <param name="targetHost">
    /// The server hostname expected in the certificate. Typically the DNS name or IP of the server.
    /// </param>
    public static ClientTlsConfig Create(string targetHost)
        => new(new SslClientAuthenticationOptions
        {
            TargetHost = targetHost ?? throw new ArgumentNullException(nameof(targetHost)),
        });

    /// <summary>
    /// Create a mutual TLS (mTLS) configuration using a pre-loaded client certificate.
    /// </summary>
    /// <param name="targetHost">
    /// The server hostname expected in the certificate.
    /// </param>
    /// <param name="clientCertificate">
    /// The client certificate (with private key) to present to the server.
    /// </param>
    public static ClientTlsConfig WithClientCertificate(
        string targetHost,
        X509Certificate2 clientCertificate)
    {
        ArgumentNullException.ThrowIfNull(targetHost);
        ArgumentNullException.ThrowIfNull(clientCertificate);

        return new(new SslClientAuthenticationOptions
        {
            TargetHost = targetHost,
            ClientCertificates = new X509Certificate2Collection { clientCertificate },
        });
    }

    /// <summary>
    /// Create a mutual TLS (mTLS) configuration with a separate signing key.
    /// Use this when the private key is not embedded in the certificate — for example,
    /// when signing is performed remotely by AWS KMS, Azure Key Vault, Google Cloud KMS,
    /// or a hardware security module (HSM).
    ///
    /// <para>
    /// Pass the public certificate (without a private key) and an <see cref="AsymmetricAlgorithm"/>
    /// implementation that delegates signing to the remote key store. The .NET runtime calls
    /// <see cref="RSA.SignHash"/> or <see cref="ECDsa.SignHash"/> during the TLS handshake —
    /// your implementation performs that operation via the KMS/HSM API.
    /// </para>
    ///
    /// <para>Example with a custom KMS-backed RSA key:</para>
    /// <code>
    /// var publicCert = new X509Certificate2("client.crt");
    /// var kmsKey = new AwsKmsRsa(keyId); // your RSA subclass that calls KMS
    /// var tls = ClientTlsConfig.WithClientCertificate("db.example.com", publicCert, kmsKey);
    /// </code>
    /// </summary>
    /// <param name="targetHost">
    /// The server hostname expected in the certificate.
    /// </param>
    /// <param name="publicCertificate">
    /// The client certificate (public portion only, no private key required).
    /// </param>
    /// <param name="privateKey">
    /// An <see cref="RSA"/> or <see cref="ECDsa"/> implementation that performs signing.
    /// This can be backed by a remote key store (KMS/HSM) — the private key material
    /// never needs to be present locally.
    /// </param>
    public static ClientTlsConfig WithClientCertificate(
        string targetHost,
        X509Certificate2 publicCertificate,
        AsymmetricAlgorithm privateKey)
    {
        ArgumentNullException.ThrowIfNull(targetHost);
        ArgumentNullException.ThrowIfNull(publicCertificate);
        ArgumentNullException.ThrowIfNull(privateKey);

        var certWithKey = privateKey switch
        {
            RSA rsa => publicCertificate.CopyWithPrivateKey(rsa),
            ECDsa ecdsa => publicCertificate.CopyWithPrivateKey(ecdsa),
            _ => throw new ArgumentException(
                $"Unsupported key algorithm '{privateKey.GetType().Name}'. " +
                "Expected an RSA or ECDsa implementation.",
                nameof(privateKey)),
        };

        return new(new SslClientAuthenticationOptions
        {
            TargetHost = targetHost,
            ClientCertificates = new X509Certificate2Collection { certWithKey },
        });
    }

    /// <summary>
    /// Create a mutual TLS (mTLS) configuration by loading a certificate and private key
    /// from PEM-encoded files on disk.
    /// </summary>
    /// <param name="targetHost">
    /// The server hostname expected in the certificate.
    /// </param>
    /// <param name="certPemFilePath">Path to the PEM-encoded client certificate file.</param>
    /// <param name="keyPemFilePath">Path to the PEM-encoded private key file.</param>
    public static ClientTlsConfig WithClientCertificateFromPem(
        string targetHost,
        string certPemFilePath,
        string keyPemFilePath)
    {
        ArgumentNullException.ThrowIfNull(targetHost);
        ArgumentNullException.ThrowIfNull(certPemFilePath);
        ArgumentNullException.ThrowIfNull(keyPemFilePath);

        var certificate = X509Certificate2.CreateFromPemFile(certPemFilePath, keyPemFilePath);
        return new(new SslClientAuthenticationOptions
        {
            TargetHost = targetHost,
            ClientCertificates = new X509Certificate2Collection { certificate },
        });
    }

    /// <summary>
    /// Create a TLS configuration from raw <see cref="SslClientAuthenticationOptions"/>
    /// for full control over TLS behavior (custom validation callbacks, cipher suites, etc.).
    /// </summary>
    /// <param name="sslOptions">The SSL/TLS options to use.</param>
    public static ClientTlsConfig FromSslOptions(SslClientAuthenticationOptions sslOptions)
        => new(sslOptions);
}
