using System.Security.Cryptography;
using Celeriant.Client.Errors;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Transport;

namespace Celeriant.Client.IntegrationTests;

/// <summary>
/// Live-server regression guards for the two behaviour bugs the blind adversarial API-surface program
/// confirmed against a real node (2026-08-29). Harnesses archived under <c>session/harness/</c>.
/// </summary>
[Collection("Server")]
public sealed class AdversarialRegressionTests
{
    private readonly ServerFixture _fixture;

    public AdversarialRegressionTests(ServerFixture fixture) => _fixture = fixture;

    /// <summary>
    /// Block 3: <see cref="TrimIndexOutOfRangeException.CurrentMaxBatchIndex"/> must report the
    /// aggregate's real current version, not 0. The client previously read the wrong server key
    /// (<c>max_event_batch_index</c>; the server sends <c>max_aggregate_version</c>).
    /// </summary>
    [SkippableFact]
    public async Task TrimBeyondMax_CurrentMaxBatchIndex_IsTheRealMax()
    {
        Skip.If(!_fixture.IsAvailable, "Server not running");
        var client = _fixture.Client!;

        var key = new AggregateKey(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());
        for (int i = 1; i <= 3; i++)
        {
            await client.WriteAsync(key, [new AggregateEvent
            {
                ClientSeq = 1,
                EventTimestamp = DateTimeOffset.UtcNow,
                EventTypeMajor = 1,
                EventValue = [(byte)i],
            }], Guid.NewGuid());
        }

        var ex = await Assert.ThrowsAsync<TrimIndexOutOfRangeException>(
            () => client.TrimStartAsync(new TrimStartRequest
            {
                AggregateKey = key,
                ClientId = Guid.NewGuid(),
                KeepFromAggregateVersion = 99,
            }));

        Assert.Equal(99, ex.RequestedTrimIndex);
        Assert.Equal(3, ex.CurrentMaxBatchIndex);
    }

    /// <summary>
    /// Block 5 (the program's headline bug): under client-identity enforcement, a write whose
    /// <c>ClientId</c> is derived with <see cref="CeleriantCrypto.GenerateClientIdentity"/> — exactly
    /// what the guide instructs — must be ACCEPTED. It was rejected before the endianness fix.
    ///
    /// <para>Gated on a dedicated env var because it needs a server started with
    /// <c>--require-client-identity --insecure-allow-plaintext-auth</c>, which the standard fixture is
    /// not. Set <c>CELERIANT_IDENTITY_SERVER_ADDRESS</c> to run it.</para>
    /// </summary>
    [SkippableFact]
    public async Task DerivedClientIdentity_IsAcceptedByAnIdentityEnforcingServer()
    {
        var address = Environment.GetEnvironmentVariable("CELERIANT_IDENTITY_SERVER_ADDRESS");
        Skip.If(string.IsNullOrWhiteSpace(address),
            "Set CELERIANT_IDENTITY_SERVER_ADDRESS to a --require-client-identity server to run this.");

        using var rsa = RSA.Create(2048);
        var pub = Convert.ToBase64String(rsa.ExportSubjectPublicKeyInfo());
        var priv = Convert.ToBase64String(rsa.ExportPkcs8PrivateKey());

        var derived = CeleriantCrypto.GenerateClientIdentity(pub);

        await using var client = await CeleriantClient.ConnectAsync(address!, ct: default);
        var assigned = await client.IdentifyAsync(ClientIdentityConfig.FromRsaKeyPair(pub, priv));

        // The two documented ways to obtain the identity must agree...
        Assert.Equal(assigned, derived);

        // ...and the derived id must actually be accepted as the write ClientId under enforcement.
        var key = new AggregateKey(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());
        var result = await client.WriteAsync(key, [new AggregateEvent
        {
            ClientSeq = 1,
            EventTimestamp = DateTimeOffset.UtcNow,
            EventTypeMajor = 1,
            EventValue = [1],
        }], derived);

        Assert.Equal(1, result.MaxAggregateVersion);
    }
}
