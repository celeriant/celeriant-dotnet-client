using System.IO;
using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Celeriant.Client;
using Celeriant.Client.Errors;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;

namespace Celeriant.Client.IntegrationTests;

/// <summary>
/// Integration tests for TLS and mutual TLS (mTLS) connections.
///
/// <para>
/// Requires a Celeriant server running with TLS and client auth enabled.
/// Tests are skipped automatically when the TLS server is unavailable.
/// </para>
///
/// <para>
/// The Celeriant server uses kTLS which has a ~30% failure rate on first reads.
/// All tests that perform a read retry on IOException via <see cref="WithKtlsRetryAsync"/>.
/// </para>
/// </summary>
[Collection("TlsServer")]
public sealed class MtlsTests
{
    private readonly TlsServerFixture _fixture;

    public MtlsTests(TlsServerFixture fixture) => _fixture = fixture;

    private void SkipIfUnavailable() => Skip.If(!_fixture.IsAvailable, "TLS server not running");

    /// <summary>
    /// Retry wrapper for kTLS flakiness. The Celeriant server uses kTLS which can fail
    /// on the first read attempt ~30% of the time. Retries with a fresh connection.
    /// </summary>
    private static async Task WithKtlsRetryAsync(Func<Task> action, int maxRetries = 5)
    {
        for (int attempt = 0; attempt < maxRetries; attempt++)
        {
            try
            {
                await action();
                return;
            }
            catch (ConnectionFailedException) when (attempt < maxRetries - 1)
            {
                await Task.Delay(200);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Test 1: trusted client cert: full round-trip
    // -------------------------------------------------------------------------

    [SkippableFact]
    public async Task MtlsConnect_TrustedClientCert_Succeeds()
    {
        SkipIfUnavailable();

        var key = TestHelpers.NewKey();
        var payload = "mtls-trusted"u8.ToArray();

        await WithKtlsRetryAsync(async () =>
        {
            await using var client = await CeleriantClient.ConnectAsync(
                _fixture.Address,
                tlsConfig: _fixture.BuildClientTlsConfig());

            await client.WriteAsync(TestHelpers.SingleEventWrite(key, payload));

            var read = await client.ReadAsync(TestHelpers.ReadAllRequest(key));
            var events = read.EventBatches.SelectMany(b => b.Events).ToArray();
            Assert.Single(events);
            Assert.Equal(payload, events[0].EventValue);
        });
    }

    // -------------------------------------------------------------------------
    // Test 2: untrusted client cert: server rejects during handshake
    // -------------------------------------------------------------------------

    [SkippableFact]
    public async Task MtlsConnect_UntrustedClientCert_Rejected()
    {
        SkipIfUnavailable();

        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var client = await CeleriantClient.ConnectAsync(
                _fixture.Address,
                tlsConfig: _fixture.BuildUntrustedClientTlsConfig());

            // If the handshake somehow succeeded, attempt a write to trigger a read which
            // would expose the rejection.
            await client.WriteAsync(TestHelpers.SingleEventWrite(TestHelpers.NewKey(), "rejected"u8.ToArray()));
        });
    }

    // -------------------------------------------------------------------------
    // Test 3: no client cert: server requires client auth, should reject
    // -------------------------------------------------------------------------

    [SkippableFact]
    public async Task Connect_WithoutClientCert_Rejected()
    {
        SkipIfUnavailable();

        // The server uses --tls-client-auth require, so a connection with no client
        // certificate must be rejected during the TLS handshake.
        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var client = await CeleriantClient.ConnectAsync(
                _fixture.Address,
                tlsConfig: _fixture.BuildServerOnlyTlsConfig());

            await client.WriteAsync(TestHelpers.SingleEventWrite(TestHelpers.NewKey(), "no-cert"u8.ToArray()));
        });
    }

    // -------------------------------------------------------------------------
    // Test 4: plaintext to TLS server: must fail
    // -------------------------------------------------------------------------

    [SkippableFact]
    public async Task PlaintextConnect_ToTlsServer_Fails()
    {
        SkipIfUnavailable();

        // A plain TCP connection (no TLS config) to a TLS-only server should fail.
        // The server will speak TLS; our client will treat the TLS greeting as a
        // MessagePack response, which will be unparseable.
        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await using var client = await CeleriantClient.ConnectAsync(
                _fixture.Address,
                connectionTimeout: null,
                tlsConfig: null);
            await client.WriteAsync(TestHelpers.SingleEventWrite(TestHelpers.NewKey(), "plaintext"u8.ToArray()));
        });
    }

    // -------------------------------------------------------------------------
    // Test 5: full round-trip data integrity
    // -------------------------------------------------------------------------

    [SkippableFact]
    public async Task MtlsConnect_WriteAndRead_DataPreserved()
    {
        SkipIfUnavailable();

        var key = TestHelpers.NewKey();
        var payload = Encoding.UTF8.GetBytes("data-preserved-over-mtls-" + Guid.NewGuid());

        await WithKtlsRetryAsync(async () =>
        {
            await using var client = await CeleriantClient.ConnectAsync(
                _fixture.Address,
                tlsConfig: _fixture.BuildClientTlsConfig());

            await client.WriteAsync(TestHelpers.SingleEventWrite(key, payload));

            var read = await client.ReadAsync(TestHelpers.ReadAllRequest(key));
            var events = read.EventBatches.SelectMany(b => b.Events).ToArray();
            Assert.Single(events);
            Assert.Equal(payload, events[0].EventValue);
        });
    }

    // -------------------------------------------------------------------------
    // Test 6: pool over mTLS
    // -------------------------------------------------------------------------

    [SkippableFact]
    public async Task MtlsConnect_Pool_WriteAndReadSucceed()
    {
        SkipIfUnavailable();

        await using var pool = new CeleriantPool(new CeleriantPoolOptions
        {
            Address = _fixture.Address,
            MaxConnections = 2,
            TlsConfig = _fixture.BuildClientTlsConfig(),
        });

        var key = TestHelpers.NewKey();
        var payload = "pool-mtls"u8.ToArray();

        await WithKtlsRetryAsync(async () =>
        {
            await pool.WriteAsync(TestHelpers.SingleEventWrite(key, payload));

            var read = await pool.ReadAsync(TestHelpers.ReadAllRequest(key));
            var events = read.EventBatches.SelectMany(b => b.Events).ToArray();
            Assert.Single(events);
            Assert.Equal(payload, events[0].EventValue);
        });
    }

    // -------------------------------------------------------------------------
    // Test 7: multiple sequential operations: TLS session stays stable
    // -------------------------------------------------------------------------

    [SkippableFact]
    public async Task MtlsConnect_MultipleOperations_Stable()
    {
        SkipIfUnavailable();

        await WithKtlsRetryAsync(async () =>
        {
            await using var client = await CeleriantClient.ConnectAsync(
                _fixture.Address,
                tlsConfig: _fixture.BuildClientTlsConfig());

            for (int i = 0; i < 5; i++)
            {
                var key = TestHelpers.NewKey();
                var payload = Encoding.UTF8.GetBytes($"stable-op-{i}");

                await client.WriteAsync(TestHelpers.SingleEventWrite(key, payload));

                var read = await client.ReadAsync(TestHelpers.ReadAllRequest(key));
                var events = read.EventBatches.SelectMany(b => b.Events).ToArray();
                Assert.Single(events);
                Assert.Equal(payload, events[0].EventValue);
            }
        });
    }

    // -------------------------------------------------------------------------
    // Test 8: PEM file loading via FromSslOptions
    // -------------------------------------------------------------------------

    [SkippableFact]
    public async Task MtlsConnect_WithCertificateLoadedFromPemFiles_Succeeds()
    {
        SkipIfUnavailable();

        // Load the cert and key from PEM files directly, then wire up custom CA validation
        // via FromSslOptions (WithClientCertificateFromPem uses system trust store which
        // won't trust our test CA).
        var caCert = X509CertificateLoader.LoadCertificateFromFile(_fixture.CaCertPath);
        var clientCert = X509Certificate2.CreateFromPemFile(_fixture.ClientCertPath, _fixture.ClientKeyPath);

        var tlsConfig = ClientTlsConfig.FromSslOptions(new SslClientAuthenticationOptions
        {
            TargetHost = "localhost",
            ClientCertificates = new X509Certificate2Collection { clientCert },
            RemoteCertificateValidationCallback = (_, cert, _, _) =>
            {
                if (cert is null) return false;
                using var chain = new X509Chain();
                chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
                chain.ChainPolicy.CustomTrustStore.Add(caCert);
                chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
                return chain.Build(new X509Certificate2(cert));
            },
        });

        var key = TestHelpers.NewKey();
        var payload = "pem-loaded"u8.ToArray();

        await WithKtlsRetryAsync(async () =>
        {
            await using var client = await CeleriantClient.ConnectAsync(
                _fixture.Address,
                tlsConfig: tlsConfig);

            await client.WriteAsync(TestHelpers.SingleEventWrite(key, payload));

            var read = await client.ReadAsync(TestHelpers.ReadAllRequest(key));
            var events = read.EventBatches.SelectMany(b => b.Events).ToArray();
            Assert.Single(events);
            Assert.Equal(payload, events[0].EventValue);
        });
    }
}
