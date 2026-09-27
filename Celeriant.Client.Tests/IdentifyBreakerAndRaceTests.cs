using System.Net;
using System.Net.Sockets;
using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Responses;
using Celeriant.Transport;
using static Celeriant.Client.Tests.IdentifyFirstContractTests;

namespace Celeriant.Client.Tests;

/// <summary>
/// Two rules around the eager handshake. Every way a dead or broken node fails it lands on an
/// exception type that opens the node's circuit breaker, while a node that answered leaves the
/// breaker closed. And a second IdentifyAsync fails locally, including when it races the implicit
/// Identify of a first request.
/// </summary>
public class IdentifyBreakerAndRaceTests
{
    private static readonly TimeSpan Short = TimeSpan.FromMilliseconds(300);
    private static readonly TimeSpan Prompt = TimeSpan.FromSeconds(5);

    private static NodeConnectionPool Node(string address, ClientIdentityConfig? identity, ClientTlsConfig? tls = null)
        => new(address, new CeleriantPoolOptions
        {
            Address = address,
            IdentityConfig = identity,
            MaxConnections = 2,
            ConnectionTimeout = Short,
            RequestTimeout = Short,
            TlsConfig = tls,
        }, new DictCache());

    private static string ClosedPort()
    {
        var l = new TcpListener(IPAddress.Loopback, 0);
        l.Start();
        string addr = $"127.0.0.1:{((IPEndPoint)l.LocalEndpoint).Port}";
        l.Stop();
        return addr;
    }

    public static TheoryData<string> DeadNodes() => new()
    {
        "connect refused",
        "identify stalls",
        "identify hang-up",
        "tls to a plain peer that hangs up",
        "tls handshake stalls",
    };

    /// <summary>Every dead-node shape fails the eager handshake with an allowlisted type and arms the breaker.</summary>
    [Theory]
    [MemberData(nameof(DeadNodes))]
    public async Task ADeadNodeStillOpensTheBreaker(string shape)
    {
        await using var server = new HandshakeScriptServer(frame => shape switch
        {
            "identify stalls" => HandshakeReply.Silence,
            _ => HandshakeReply.HangUp,
        });
        var raw = new TcpListener(IPAddress.Loopback, 0);
        raw.Start();
        var rawAccepts = Task.Run(async () =>
        {
            try
            {
                while (true)
                {
                    var c = await raw.AcceptTcpClientAsync();
                    if (shape == "tls to a plain peer that hangs up")
                    {
                        await c.GetStream().WriteAsync(new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 });
                        c.Dispose();
                    }
                    // "tls handshake stalls": hold the socket open and say nothing.
                }
            }
            catch { }
        });
        string rawAddr = $"127.0.0.1:{((IPEndPoint)raw.LocalEndpoint).Port}";

        (string addr, ClientTlsConfig? tls) = shape switch
        {
            "connect refused" => (ClosedPort(), null),
            "tls to a plain peer that hangs up" or "tls handshake stalls" => (rawAddr, ClientTlsConfig.Create("localhost")),
            _ => (server.Address, (ClientTlsConfig?)null),
        };

        await using var node = Node(addr, ApiKey, tls);
        Exception? error = await Record.ExceptionAsync(() => node.GetConnectionAsync(CancellationToken.None).WaitAsync(Prompt));
        raw.Stop();

        Assert.True(error is ConnectionFailedException or ConnectionTimeoutException,
            $"{shape}: got {error?.GetType().Name}: {error?.Message}");
        Assert.True(node.IsCircuitOpen, $"{shape}: {error?.GetType().Name} did not open the breaker");
    }

    /// <summary>A node that answered Identify with an identity error, a protocol error, or a malformed reply leaves the breaker closed.</summary>
    [Theory]
    [InlineData("10005")]
    [InlineData("protocol error")]
    public async Task ANodeThatAnsweredLeavesTheBreakerClosed(string answer)
    {
        await using var server = new HandshakeScriptServer(frame => answer switch
        {
            "10005" => HandshakeReplies.Rejected(frame, ErrorResponse.AuthRequired),
            _ => HandshakeReplies.ProtocolError(frame, echo: true, close: true),
        });
        await using var node = Node(server.Address, ApiKey);
        Exception? error = await Record.ExceptionAsync(() => node.GetConnectionAsync(CancellationToken.None).WaitAsync(Prompt));
        Assert.NotNull(error);
        Assert.False(node.IsCircuitOpen, $"{answer}: {error.GetType().Name} opened the breaker");
    }

    /// <summary>
    /// A first write and an explicit IdentifyAsync start together on a fresh client. Whichever
    /// wins the identify lock, exactly one Identify reaches the wire, it is the first frame, the write
    /// succeeds, and IdentifyAsync either succeeds or throws InvalidOperationException.
    /// </summary>
    [Fact]
    public async Task ARacingFirstWriteAndIdentifySendExactlyOneIdentify()
    {
        await using var server = new HandshakeScriptServer(HandshakeReplies.Healthy());
        int identifyWon = 0, writeWon = 0;
        for (int i = 0; i < 200; i++)
        {
            await using var client = await CeleriantClient.ConnectAsync(server.Address, Prompt);
            client.WithTimeout(Prompt);
            // Stagger the identify side by a varying spin so both orders occur.
            int spin = (i % 20) * 2000;
            using var go = new ManualResetEventSlim();
            var write = Task.Run(() => { go.Wait(); return client.WriteAsync(OccTestData.Write()); });
            var identify = Task.Run(() => { go.Wait(); Thread.SpinWait(spin); return client.IdentifyAsync(ApiKey); });
            go.Set();
            await write.WaitAsync(Prompt);
            Exception? idError = await Record.ExceptionAsync(() => identify.WaitAsync(Prompt));
            if (idError is null) identifyWon++;
            else if (idError is InvalidOperationException) writeWon++;
            else Assert.Fail($"iteration {i}: IdentifyAsync threw {idError.GetType().Name}: {idError.Message}");
        }

        var byConn = server.ByConnection();
        Assert.Equal(200, byConn.Count);
        foreach (var (conn, frames) in byConn)
        {
            Assert.Equal(MessageTypes.Requests.Identify, frames[0].Type);
            Assert.Equal(1, frames.Count(f => f.Type == MessageTypes.Requests.Identify));
        }
        Assert.Empty(server.Faults);
        Assert.Equal(200, identifyWon + writeWon);
        Assert.True(identifyWon > 0 && writeWon > 0, $"race did not exercise both orders: identify={identifyWon} write={writeWon}");
    }
}
