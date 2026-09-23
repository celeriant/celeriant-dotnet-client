using System.Collections.Concurrent;
using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Streaming;

namespace Celeriant.Client.Tests;

/// <summary>
/// Shard ids are non-negative (invariants.md "Sharding and Concurrency"), so a negative StartShard
/// is invalid input and must be rejected before anything reaches the wire, on the list path as the
/// watch path already does.
/// </summary>
public class NegativeStartShardBlackBoxTests
{
    [Fact]
    public async Task List_StartShardNegative_IsRejectedClientSide()
    {
        var receivedShardIds = new ConcurrentQueue<long>();

        // A server that behaves like the real one: an out-of-range shard id is answered with the
        // 9002 (IncompatibleFilters) shard-routing error (connection_handler::validate_shard_id).
        await using var server = FakeCeleriantServer.Start((session, _, body) =>
        {
            var request = WireCodec.Deserialize<ListAggregatesRequest>(body);
            receivedShardIds.Enqueue(request.ShardId);
            return session.SendFrameAsync(
                MessageTypes.Responses.GenericError,
                FakeServerProtocol.ErrorFrame(
                    ErrorResponse.ShardRoutingIncompatibleFilters,
                    $"shard_id {request.ShardId} out of range {{\"num_shards\":2}}",
                    request.CorrelationId));
        });

        await using var client = await CeleriantClient.ConnectAsync(server.Address);

        // The client rejects the option up front, before any request naming shard -1 is sent.
        var argument = Assert.Throws<ArgumentOutOfRangeException>(() =>
            client.ListAggregatesAsync(options: new ListOptions { StartShard = -1 }));

        Assert.Equal(nameof(ListOptions.StartShard), argument.ParamName);
        Assert.Empty(receivedShardIds);
    }
}
