using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using Celeriant.Client;

namespace Celeriant.Client.IntegrationTests;

/// <summary>
/// Shared test fixture for TLS and mTLS integration tests.
///
/// <para>
/// Reads the server address from <c>CELERIANT_TLS_ADDRESS</c> (default <c>localhost:10010</c>).
/// Locates test certificates by walking up the directory tree from the test assembly, looking
/// for a <c>test-certs/</c> directory containing <c>ca.crt</c>.
/// </para>
///
/// <para>
/// If the TLS server is unreachable or certificates are not found, <see cref="IsAvailable"/> is
/// set to false and tests should skip using <c>Skip.If(!fixture.IsAvailable, "TLS server not running")</c>.
/// </para>
///
/// <para>Thread safety: fixture is initialised once and then read-only during tests.</para>
/// </summary>
public sealed class TlsServerFixture : IAsyncLifetime
{
    private const int PollIntervalMs = 100;
    private const int PollTimeoutMs = 15_000;

    private X509Certificate2? _caCert;

    /// <summary>
    /// Server address read from <c>CELERIANT_TLS_ADDRESS</c> (default <c>localhost:10010</c>).
    /// </summary>
    public string Address { get; } =
        Environment.GetEnvironmentVariable("CELERIANT_TLS_ADDRESS") ?? "localhost:10010";

    /// <summary>
    /// True if the TLS server was reachable and certificates were found during initialisation.
    /// Tests should call <c>Skip.If(!fixture.IsAvailable, "TLS server not running")</c>.
    /// </summary>
    public bool IsAvailable { get; private set; }

    /// <summary>
    /// Absolute path to the test certificate directory. Only valid when <see cref="IsAvailable"/> is true.
    /// </summary>
    public string CertDir { get; private set; } = "";

    /// <summary>Path to the test CA certificate (PEM).</summary>
    public string CaCertPath => Path.Combine(CertDir, "ca.crt");

    /// <summary>Path to the trusted client certificate (PEM).</summary>
    public string ClientCertPath => Path.Combine(CertDir, "client.crt");

    /// <summary>Path to the trusted client private key (PEM).</summary>
    public string ClientKeyPath => Path.Combine(CertDir, "client.key");

    /// <summary>Path to the untrusted client certificate (PEM).</summary>
    public string UntrustedClientCertPath => Path.Combine(CertDir, "untrusted-client.crt");

    /// <summary>Path to the untrusted client private key (PEM).</summary>
    public string UntrustedClientKeyPath => Path.Combine(CertDir, "untrusted-client.key");

    public async Task InitializeAsync()
    {
        // Locate test-certs/ directory.
        string certDir = FindCertDir();
        if (certDir == "" || !File.Exists(Path.Combine(certDir, "ca.crt")))
        {
            IsAvailable = false;
            return;
        }

        CertDir = certDir;
        _caCert = X509CertificateLoader.LoadCertificateFromFile(CaCertPath);

        // Poll for TCP readiness before attempting a TLS handshake.
        bool reachable = await PollForServerAsync(Address, PollTimeoutMs).ConfigureAwait(false);
        if (!reachable)
        {
            IsAvailable = false;
            return;
        }

        // Verify a full TLS handshake succeeds with the trusted client certificate.
        try
        {
            var tlsConfig = BuildClientTlsConfig();
            await using var client = await CeleriantClient.ConnectAsync(
                Address,
                connectionTimeout: TimeSpan.FromSeconds(10),
                tlsConfig: tlsConfig)
                .ConfigureAwait(false);

            IsAvailable = true;
        }
        catch
        {
            IsAvailable = false;
        }
    }

    public Task DisposeAsync()
    {
        _caCert?.Dispose();
        return Task.CompletedTask;
    }

    // -------------------------------------------------------------------------
    // TLS config factories
    // -------------------------------------------------------------------------

    /// <summary>
    /// Build a <see cref="ClientTlsConfig"/> that presents the trusted client certificate
    /// and validates the server certificate against the test CA.
    /// </summary>
    public ClientTlsConfig BuildClientTlsConfig()
    {
        var caCert = X509CertificateLoader.LoadCertificateFromFile(CaCertPath);
        var clientCert = X509Certificate2.CreateFromPemFile(ClientCertPath, ClientKeyPath);

        return ClientTlsConfig.FromSslOptions(new SslClientAuthenticationOptions
        {
            TargetHost = "localhost",
            ClientCertificates = new X509Certificate2Collection { clientCert },
            RemoteCertificateValidationCallback = BuildCaValidator(caCert),
        });
    }

    /// <summary>
    /// Build a <see cref="ClientTlsConfig"/> that presents the untrusted client certificate.
    /// The server should reject this during the mTLS handshake.
    /// </summary>
    public ClientTlsConfig BuildUntrustedClientTlsConfig()
    {
        var caCert = X509CertificateLoader.LoadCertificateFromFile(CaCertPath);
        var untrustedCert = X509Certificate2.CreateFromPemFile(UntrustedClientCertPath, UntrustedClientKeyPath);

        return ClientTlsConfig.FromSslOptions(new SslClientAuthenticationOptions
        {
            TargetHost = "localhost",
            ClientCertificates = new X509Certificate2Collection { untrustedCert },
            RemoteCertificateValidationCallback = BuildCaValidator(caCert),
        });
    }

    /// <summary>
    /// Build a <see cref="ClientTlsConfig"/> for server-only TLS (no client certificate).
    /// Validates the server certificate against the test CA.
    /// </summary>
    public ClientTlsConfig BuildServerOnlyTlsConfig()
    {
        var caCert = X509CertificateLoader.LoadCertificateFromFile(CaCertPath);

        return ClientTlsConfig.FromSslOptions(new SslClientAuthenticationOptions
        {
            TargetHost = "localhost",
            RemoteCertificateValidationCallback = BuildCaValidator(caCert),
        });
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /// <summary>
    /// Build a <see cref="RemoteCertificateValidationCallback"/> that trusts the given CA
    /// and no others. Revocation checking is disabled for test certificates.
    /// </summary>
    private static RemoteCertificateValidationCallback BuildCaValidator(X509Certificate2 caCert)
        => (_, cert, _, _) =>
        {
            if (cert is null) return false;

            using var chain = new X509Chain();
            chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
            chain.ChainPolicy.CustomTrustStore.Add(caCert);
            chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
            return chain.Build(new X509Certificate2(cert));
        };

    /// <summary>
    /// Walk up the directory tree from the test assembly location to find a
    /// <c>test-certs/</c> directory containing <c>ca.crt</c>.
    /// </summary>
    private static string FindCertDir()
    {
        var dir = Path.GetDirectoryName(typeof(TlsServerFixture).Assembly.Location);
        while (dir is not null)
        {
            var candidate = Path.Combine(dir, "test-certs");
            if (Directory.Exists(candidate) && File.Exists(Path.Combine(candidate, "ca.crt")))
                return candidate;
            dir = Path.GetDirectoryName(dir);
        }
        return "";
    }

    private static async Task<bool> PollForServerAsync(string address, int timeoutMs)
    {
        (string host, int port) = ParseAddress(address);
        using var cts = new CancellationTokenSource(timeoutMs);

        while (!cts.IsCancellationRequested)
        {
            using var probe = new TcpClient();
            try
            {
                await probe.ConnectAsync(host, port, cts.Token).ConfigureAwait(false);
                return true;
            }
            catch (OperationCanceledException)
            {
                return false;
            }
            catch (SocketException)
            {
                try
                {
                    await Task.Delay(PollIntervalMs, cts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return false;
                }
            }
        }

        return false;
    }

    private static (string host, int port) ParseAddress(string address)
    {
        int lastColon = address.LastIndexOf(':');
        if (lastColon < 0 || lastColon == address.Length - 1)
            throw new ArgumentException($"Invalid address '{address}'. Expected 'host:port'.");

        string host = address[..lastColon];
        if (!int.TryParse(address[(lastColon + 1)..], out int port))
            throw new ArgumentException($"Invalid port in address '{address}'.");

        return (host, port);
    }
}

/// <summary>
/// xUnit collection that shares a single <see cref="TlsServerFixture"/> instance across all
/// TLS integration tests.
/// </summary>
[CollectionDefinition("TlsServer")]
public sealed class TlsServerCollection : ICollectionFixture<TlsServerFixture> { }
