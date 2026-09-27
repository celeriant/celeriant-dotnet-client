using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Watch;

namespace Celeriant.Client.Tests;

/// <summary>
/// The server closes any connection whose first frame is not Identify, so
/// every connection the client opens, of every kind, with or without an identity configured, sends
/// Identify (type 14) first and exactly once.
///
/// Run: dotnet test Celeriant.Client.Tests --filter FullyQualifiedName~IdentifyFirstContractTests
/// </summary>
public class IdentifyFirstContractTests
{
    private static readonly TimeSpan Prompt = TimeSpan.FromSeconds(5);

    internal static readonly ClientIdentityConfig ApiKey = ClientIdentityConfig.FromApiKey(Convert.ToBase64String(new byte[32]));

    internal static WatchRequest AnyWatch() => new() { Aggregates = [Guid.NewGuid()] };

    internal static CeleriantPool Pool(string address, ClientIdentityConfig? identity) => new(new CeleriantPoolOptions
    {
        Address = address,
        IdentityConfig = identity,
        MaxConnections = 4,
        ConnectionTimeout = Prompt,
        RequestTimeout = Prompt,
    });

    internal static WatchOptions Watch(ClientIdentityConfig? identity, long? maxShardHint = null) => new()
    {
        IdentityConfig = identity,
        ConnectionTimeout = Prompt,
        MaxShardHint = maxShardHint,
    };

    /// <summary>Each connection kind, driven through one successful operation against <c>address</c>.</summary>
    private static readonly Dictionary<string, Func<string, Task>> Kinds = new()
    {
        ["pool, no identity, write"] = async address =>
        {
            await using var pool = Pool(address, null);
            await pool.WriteAsync(OccTestData.Write());
        },
        ["pool, API key, write"] = async address =>
        {
            await using var pool = Pool(address, ApiKey);
            await pool.WriteAsync(OccTestData.Write());
        },
        ["pool.WatchAsync, no identity"] = async address =>
        {
            await using var pool = Pool(address, null);
            await using var watch = await pool.WatchAsync(AnyWatch());
        },
        ["pool.WatchAsync, API key"] = async address =>
        {
            await using var pool = Pool(address, ApiKey);
            await using var watch = await pool.WatchAsync(AnyWatch());
        },
        ["WatchConnection, no identity, one shard"] = async address =>
        {
            await using var watch = await WatchConnection.ConnectAsync(address, AnyWatch(), Watch(null));
        },
        ["WatchConnection, no identity, two shards"] = async address =>
        {
            await using var watch = await WatchConnection.ConnectAsync(address, AnyWatch(), Watch(null, maxShardHint: 2));
        },
        ["WatchConnection, API key"] = async address =>
        {
            await using var watch = await WatchConnection.ConnectAsync(address, AnyWatch(), Watch(ApiKey));
        },
        ["CeleriantClient, first request"] = async address =>
        {
            await using var client = await CeleriantClient.ConnectAsync(address, connectionTimeout: Prompt);
            await client.WriteAsync(OccTestData.Write());
        },
        ["CeleriantClient, explicit IdentifyAsync then request"] = async address =>
        {
            await using var client = await CeleriantClient.ConnectAsync(address, connectionTimeout: Prompt);
            await client.IdentifyAsync(ApiKey);
            await client.WriteAsync(OccTestData.Write());
        },
    };

    public static TheoryData<string> KindNames() => new(Kinds.Keys);

    [Theory]
    [MemberData(nameof(KindNames))]
    public async Task EveryConnectionSendsIdentifyFirstAndExactlyOnce(string kind)
    {
        await using var server = new HandshakeScriptServer(HandshakeReplies.Healthy());

        Exception? failure = await Record.ExceptionAsync(() => Kinds[kind](server.Address).WaitAsync(Prompt * 2));

        var connections = server.ByConnection();
        string wire = string.Join("; ", connections.Select(c => $"conn {c.Key}: [{string.Join(",", c.Value.Select(f => f.Type))}]"));
        Assert.True(connections.Count > 0, $"row '{kind}': no frame reached the server");
        foreach (var (id, frames) in connections)
        {
            Assert.True(frames[0].Type == MessageTypes.Requests.Identify,
                $"row '{kind}': connection {id} opened with type {frames[0].Type}, not Identify (14). Wire: {wire}");
            int identifies = frames.Count(f => f.Type == MessageTypes.Requests.Identify);
            Assert.True(identifies == 1, $"row '{kind}': connection {id} sent {identifies} Identify frames. Wire: {wire}");
        }
        Assert.True(failure is null, $"row '{kind}': the operation failed against a healthy server: {failure}");
        Assert.Contains(server.Frames, f => f.Type != MessageTypes.Requests.Identify);
        Assert.Empty(server.Faults);
    }
}
