using System.Security.Cryptography;
using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Transport;

namespace Celeriant.Client.Tests;

/// <summary>
/// White-box checks behind the identity and dictionary handshake: each identity error code maps to
/// its own exception type, and every Identify path on a client that shares a <see cref="DictCache"/>
/// advertises the cache's snapshot and teaches the cache what the server confirmed.
///
/// Run: dotnet test Celeriant.Client.Tests --filter FullyQualifiedName~IdentifyDictCacheAndErrorMappingTests
/// </summary>
public class IdentifyDictCacheAndErrorMappingTests
{
    private static readonly TimeSpan Prompt = TimeSpan.FromSeconds(5);

    private static readonly ClientIdentityConfig ApiKey = ClientIdentityConfig.FromApiKey(Convert.ToBase64String(new byte[32]));

    private static string Sha(byte[] bytes) => Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();

    public static TheoryData<uint, Type> IdentityErrorRows() => new()
    {
        { ErrorResponse.IdentifyInvalidNonce, typeof(AuthErrorException) },
        { ErrorResponse.IdentifyInvalidSignature, typeof(AuthErrorException) },
        { ErrorResponse.IdentifyMismatch, typeof(AuthErrorException) },
        { ErrorResponse.IdentifyRequired, typeof(IdentityRequiredException) },
        { ErrorResponse.AuthRequired, typeof(AuthRequiredException) },
        { ErrorResponse.AuthInvalidKey, typeof(AuthInvalidKeyException) },
        { ErrorResponse.AuthInsufficientPermissions, typeof(AuthErrorException) },
    };

    [Theory]
    [MemberData(nameof(IdentityErrorRows))]
    public void EachIdentityErrorCodeMapsToItsOwnExceptionType(uint code, Type expected)
    {
        var error = new ErrorResponse { ErrorCode = code, ErrorMessage = "rejected" };

        Exception mapped = CeleriantClient.CreateException(error);

        Assert.True(mapped.GetType() == expected, $"code {code}: expected {expected.Name}, got {mapped.GetType().Name}");
        ErrorResponse carried = mapped switch
        {
            CeleriantErrorException e => e.Error,
            IdentityRequiredException e => e.Error,
            _ => throw new InvalidOperationException($"code {code}: {mapped.GetType().Name} carries no ErrorResponse"),
        };
        Assert.Same(error, carried);
    }

    public static TheoryData<string> IdentifyPaths() => new() { "explicit IdentifyAsync", "implicit first request" };

    /// <summary>
    /// A client given a cache that already holds dictionary x advertises x, and when the server
    /// ships y instead, the cache holds y afterwards. Covers the explicit path and the unsigned
    /// Identify a client sends ahead of its first request when no identity is configured.
    /// </summary>
    [Theory]
    [MemberData(nameof(IdentifyPaths))]
    public async Task AClientSharingACacheAdvertisesItsSnapshotAndLearnsWhatIsShipped(string path)
    {
        byte[] x = RandomNumberGenerator.GetBytes(1024);
        byte[] y = RandomNumberGenerator.GetBytes(1024);
        // FakeCeleriantServer answers an anonymous Identify itself; this script server sees every frame.
        await using var server = new HandshakeScriptServer(HandshakeReplies.Healthy(y));

        var cache = new DictCache();
        cache.Learn(new CachedDict(Sha(x), x));
        await using var client = await CeleriantClient.ConnectWithDictCacheAsync(
            server.Address, Prompt, tlsConfig: null, cache, CancellationToken.None);

        if (path == "explicit IdentifyAsync")
            await client.IdentifyAsync(ApiKey).WaitAsync(Prompt);
        else
            await client.WriteAsync(OccTestData.Write()).WaitAsync(Prompt);

        string row = $"row '{path}'";
        string?[] advertised = [.. server.Frames
            .Where(f => f.Type == MessageTypes.Requests.Identify)
            .Select(f => HandshakeReplies.Identify(f).KnownDictSha256)];
        Assert.True(advertised is [var only] && only == Sha(x),
            $"{row}: advertised [{string.Join(", ", advertised)}], expected the cached {Sha(x)[..8]}");
        Assert.True(cache.Snapshot().Sha == Sha(y), $"{row}: the cache did not learn the shipped dictionary");
        Assert.Same(client.CurrentDict?.Bytes, cache.Snapshot().Bytes);
    }

    /// <summary>
    /// Identify is only valid as a connection's first frame; the server drops a connection that
    /// sends a second one. A second <c>IdentifyAsync</c>, after either Identify path, fails locally,
    /// sends nothing, and leaves the client usable.
    /// </summary>
    [Theory]
    [MemberData(nameof(IdentifyPaths))]
    public async Task ASecondIdentifyFailsLocallyAndLeavesTheClientUsable(string firstPath)
    {
        await using var server = new HandshakeScriptServer(HandshakeReplies.Healthy());
        await using var client = await CeleriantClient.ConnectAsync(server.Address, connectionTimeout: Prompt);

        if (firstPath == "explicit IdentifyAsync")
            await client.IdentifyAsync(ApiKey).WaitAsync(Prompt);
        else
            await client.WriteAsync(OccTestData.Write()).WaitAsync(Prompt);

        Exception? second = await Record.ExceptionAsync(() => client.IdentifyAsync(ApiKey).WaitAsync(Prompt));
        Exception? next = await Record.ExceptionAsync(() => client.WriteAsync(OccTestData.Write()).WaitAsync(Prompt));

        string row = $"row '{firstPath}'";
        Assert.True(second is InvalidOperationException, $"{row}: second IdentifyAsync threw {second?.GetType().Name ?? "nothing"}");
        Assert.True(next is null, $"{row}: the write after it threw {next?.GetType().Name}: {next?.Message}");
        int identifies = server.Frames.Count(f => f.Type == MessageTypes.Requests.Identify);
        Assert.True(identifies == 1 && server.Accepted == 1,
            $"{row}: {identifies} Identify frames on {server.Accepted} connections, expected 1 on 1");
    }
}
