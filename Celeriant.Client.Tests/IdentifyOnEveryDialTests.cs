using System.Security.Cryptography;
using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Responses;
using static Celeriant.Client.Tests.IdentifyFirstContractTests;

namespace Celeriant.Client.Tests;

/// <summary>
/// The identity handshake on the less common dial paths: a second request after an identity
/// rejection on a pool with an identity configured, leader redirect dials, pool expansion under
/// concurrency, and the re-dial after a broken connection.
/// </summary>
public class IdentifyOnEveryDialTests
{
    private static readonly TimeSpan Prompt = TimeSpan.FromSeconds(5);

    private static ClientIdentityConfig KeyPair()
    {
        using var rsa = RSA.Create(2048);
        return ClientIdentityConfig.FromRsaKeyPair(
            Convert.ToBase64String(rsa.ExportSubjectPublicKeyInfo()),
            Convert.ToBase64String(rsa.ExportPkcs8PrivateKey()));
    }

    /// <summary>
    /// Code, the identity that realistically draws it (flag+keys server and API key only; keys server
    /// and key pair only; plain server and an API key), and the type the caller must see.
    /// </summary>
    public static TheoryData<string> Codes() => new() { "10004 api key only", "10005 key pair only", "10006 api key" };

    private static (uint Code, ClientIdentityConfig Identity, Type Expected) Row(string row) => row switch
    {
        "10004 api key only" => (ErrorResponse.IdentifyRequired, ApiKey, typeof(IdentityRequiredException)),
        "10005 key pair only" => (ErrorResponse.AuthRequired, KeyPair(), typeof(AuthRequiredException)),
        "10006 api key" => (ErrorResponse.AuthInvalidKey, ApiKey, typeof(AuthInvalidKeyException)),
        _ => throw new ArgumentOutOfRangeException(nameof(row)),
    };

    /// <summary>
    /// A pool with an <see cref="CeleriantPoolOptions.IdentityConfig"/> identifies eagerly inside the
    /// node pool's connection factory. A rejected Identify there is an answer from a live node, yet
    /// <c>NodeConnectionPool</c>'s breaker predicate (<c>ex is not ProtocolException</c>) treats every
    /// identity error as a dial failure and opens the 2 s breaker. The caller's next request then gets
    /// <see cref="PoolUnavailableException"/> "circuit breaker open" instead of the typed identity error.
    /// Rust <c>pool.rs</c> <c>is_dial_failure</c> is an allowlist of wire failures and says explicitly
    /// that <c>IdentityRequired</c> and server errors must not arm it ("would hide the real cause behind
    /// 'circuit breaker open'"). The same pool without an identity surfaces the typed error every time.
    /// </summary>
    [Theory]
    [MemberData(nameof(Codes))]
    public async Task ASecondRequestAfterARejectedIdentifyStillSeesTheTypedError(string row)
    {
        var (code, identity, expected) = Row(row);
        await using var server = new HandshakeScriptServer(frame => frame.Type == MessageTypes.Requests.Identify
            ? HandshakeReplies.Rejected(frame, code)
            : HandshakeReplies.Served(frame));
        await using var pool = Pool(server.Address, identity);

        Exception? first = await Record.ExceptionAsync(() => pool.WriteAsync(OccTestData.Write()).WaitAsync(Prompt));
        Exception? second = await Record.ExceptionAsync(() => pool.WriteAsync(OccTestData.Write()).WaitAsync(Prompt));

        Assert.True(first?.GetType() == expected, $"{row}: first write got {first?.GetType().Name}: {first?.Message}");
        Assert.True(second?.GetType() == expected,
            $"{row}: second write got {second?.GetType().Name}: {second?.Message} — the identity rejection armed the dial breaker");
    }

    /// <summary>The same rejection on a pool without an identity, which identifies lazily, is typed on every request.</summary>
    [Fact]
    public async Task ControlAPoolWithoutIdentitySurfacesTheTypedErrorOnEveryRequest()
    {
        await using var server = new HandshakeScriptServer(frame => frame.Type == MessageTypes.Requests.Identify
            ? HandshakeReplies.Rejected(frame, ErrorResponse.AuthRequired)
            : HandshakeReplies.Served(frame));
        await using var pool = Pool(server.Address, null);

        for (int i = 0; i < 3; i++)
        {
            Exception? failure = await Record.ExceptionAsync(() => pool.WriteAsync(OccTestData.Write()).WaitAsync(Prompt));
            Assert.IsType<AuthRequiredException>(failure);
        }
        // One dial per request, no backoff: the pool redials a rejecting node on every call.
        Assert.Equal(3, server.Accepted);
    }

    private static void AssertEveryConnectionIdentifiesFirst(HandshakeScriptServer server, string label)
    {
        foreach (var (id, frames) in server.ByConnection())
        {
            Assert.True(frames[0].Type == MessageTypes.Requests.Identify, $"{label}: connection {id} opened with type {frames[0].Type}");
            Assert.Equal(1, frames.Count(f => f.Type == MessageTypes.Requests.Identify));
        }
        Assert.Empty(server.Faults);
    }

    /// <summary>A NotLeader hint makes the pool dial a new node: that dial must open with Identify too.</summary>
    [Fact]
    public async Task ALeaderRedirectDialOpensWithIdentify()
    {
        await using var leader = new HandshakeScriptServer(HandshakeReplies.Healthy());
        await using var follower = new HandshakeScriptServer(frame => frame.Type == MessageTypes.Requests.Identify
            ? HandshakeReplies.Identified(frame)
            : new HandshakeReply([(MessageTypes.Responses.GenericError, FakeServerProtocol.ErrorFrame(
                ErrorResponse.WriteNotLeader, $"{{\"leader_address\":\"{leader.Address}\"}}",
                WireCodec.Deserialize<Requests.WriteRequest>(frame.Body).CorrelationId))]));
        await using var pool = Pool(follower.Address, null);

        await pool.WriteAsync(OccTestData.Write()).WaitAsync(Prompt);

        Assert.True(leader.Accepted >= 1);
        AssertEveryConnectionIdentifiesFirst(follower, "follower");
        AssertEveryConnectionIdentifiesFirst(leader, "leader");
    }

    /// <summary>Concurrent writes expand the pool to several sockets; each opens with exactly one Identify.</summary>
    [Fact]
    public async Task PoolExpansionUnderConcurrencyIdentifiesEveryNewSocket()
    {
        await using var server = new HandshakeScriptServer(HandshakeReplies.Healthy());
        await using var pool = Pool(server.Address, null);

        await Task.WhenAll(Enumerable.Range(0, 16).Select(_ => pool.WriteAsync(OccTestData.Write()))).WaitAsync(Prompt);

        AssertEveryConnectionIdentifiesFirst(server, "expansion");
    }

    /// <summary>After the server hangs up mid-request, the replacement socket opens with Identify.</summary>
    [Fact]
    public async Task TheRedialAfterABrokenConnectionOpensWithIdentify()
    {
        int writes = 0;
        await using var server = new HandshakeScriptServer(frame => frame.Type == MessageTypes.Requests.Identify
            ? HandshakeReplies.Identified(frame)
            : Interlocked.Increment(ref writes) == 2 ? HandshakeReply.HangUp : HandshakeReplies.Served(frame));
        await using var pool = Pool(server.Address, null);

        await pool.WriteAsync(OccTestData.Write()).WaitAsync(Prompt);
        await Record.ExceptionAsync(() => pool.WriteAsync(OccTestData.Write()).WaitAsync(Prompt));
        await pool.WriteAsync(OccTestData.Write()).WaitAsync(Prompt);

        Assert.Equal(2, server.Accepted);
        AssertEveryConnectionIdentifiesFirst(server, "redial");
    }

    /// <summary>Concurrent first requests on one standalone client race the implicit Identify: exactly one is sent, first.</summary>
    [Fact]
    public async Task ConcurrentFirstRequestsOnOneClientSendOneIdentifyFirst()
    {
        await using var server = new HandshakeScriptServer(HandshakeReplies.Healthy());
        await using var client = await CeleriantClient.ConnectAsync(server.Address, connectionTimeout: Prompt);

        await Task.WhenAll(Enumerable.Range(0, 16).Select(_ => Task.Run(() => client.WriteAsync(OccTestData.Write())))).WaitAsync(Prompt);

        Assert.Equal(1, server.Accepted);
        AssertEveryConnectionIdentifiesFirst(server, "concurrent first requests");
    }
}
