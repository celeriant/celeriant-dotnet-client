using System.Collections.Concurrent;
using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Watch;

namespace Celeriant.Client.Tests;

/// <summary>
/// White-box companions to <see cref="WatchConnectParityTests"/>, covering two things the
/// black-box tests cannot see because both concern how connect hands work to the readers:
///
/// <list type="bullet">
/// <item>a subscription ack may already carry events, produced before any reader existed. Nothing
/// else will ever deliver them, so dropping them is a silent gap spanning exactly the moment the
/// watch was established — invisible to a caller, who sees a healthy subscription;</item>
/// <item>the shard count can come from the server, and a value past <see cref="int"/> range that is
/// cast unchecked wraps to a small positive number, leaving the caller watching a handful of shards
/// while believing it covers every one the server named.</item>
/// </list>
/// </summary>
public class WatchConnectAckAndShardCountTests
{
    private static readonly TimeSpan HangBudget = TimeSpan.FromSeconds(5);

    private static readonly byte[] WatchAck =
        FakeServerSession.BuildFrame(MessageTypes.Responses.Watch, WireCodec.Serialize(new WatchResponse()));

    /// <summary>
    /// The ack is the only frame a shard sends before its reader starts, so an event riding on it
    /// has exactly one chance to be kept.
    /// </summary>
    [Fact]
    public async Task EventsCarriedOnTheSubscriptionAck_AreDeliveredRatherThanDropped()
    {
        var onAck = Guid.NewGuid();

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            // Only shard 1 answers with a populated ack; the rest ack empty and stay silent, so the
            // only event this watch will ever see is the one riding on that subscription reply.
            long? shard = WireCodec.Deserialize<WatchRequest>(body).ShardId;
            await session.SendRawAsync(shard is 1 ? EventFrame(onAck) : WatchAck);
        });

        await using var watch = await WatchConnection.ConnectAsync(
            server.Address,
            new WatchRequest(),
            new WatchOptions { ConnectionTimeout = TimeSpan.FromSeconds(5), MaxShardHint = 2 });

        var next = watch.NextAsync(CancellationToken.None);
        var finished = await Task.WhenAny(next, Task.Delay(HangBudget));
        Assert.True(
            ReferenceEquals(finished, next),
            "the event arrived on the subscription ack, before any reader was running. Nothing else "
            + "will ever deliver it, so a caller that waits for it waits forever while holding what "
            + "looks like a healthy subscription");

        Assert.Equal(onAck, Assert.Single((await next).Events).AggregateId);
    }

    /// <summary>
    /// A shard count past <see cref="int"/> range is nonsense, but it must be rejected loudly. Cast
    /// unchecked it wraps: 2^32 + 2 becomes 2, and the caller silently watches two shards.
    /// </summary>
    [Fact]
    public async Task AShardCountPastIntRange_IsRejectedRatherThanWrappingToAFewShards()
    {
        const long wrapsToTwo = 4294967298L; // 2^32 + 2
        var subscribed = new ConcurrentQueue<long>();

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            var request = WireCodec.Deserialize<WatchRequest>(body);
            if (request.ShardId is null)
            {
                await session.SendFrameAsync(
                    MessageTypes.Responses.GenericError,
                    FakeServerProtocol.ErrorFrame(
                        ErrorResponse.ShardRoutingMultipleShards,
                        $"watch filter spans shards {{\"num_shards\":{wrapsToTwo}}}",
                        request.CorrelationId));
                return;
            }

            subscribed.Enqueue(request.ShardId.Value);
            await session.SendRawAsync(WatchAck);
        });

        var failure = await Record.ExceptionAsync(() => WatchConnection.ConnectAsync(
            server.Address,
            new WatchRequest(),
            new WatchOptions { ConnectionTimeout = TimeSpan.FromSeconds(5) }));

        Assert.True(
            failure is CeleriantClientException,
            $"the server named {wrapsToTwo} shards, which cannot be subscribed. Connecting anyway "
            + $"subscribed shard(s) [{string.Join(", ", subscribed.Order())}] and handed back a watch "
            + $"covering those alone: got {failure?.GetType().Name ?? "no exception"}");
    }

    /// <summary>
    /// An error frame answering a watch request already throws on the way out of the transport. A
    /// well-formed reply of the wrong kind does not: it decodes cleanly, and without this check the
    /// shard is treated as subscribed. The caller then holds a watch over a shard that never
    /// acknowledged the subscription and will never push anything to it.
    /// </summary>
    [Fact]
    public async Task AShardAnsweringItsSubscriptionWithTheWrongResponseType_FailsTheConnect()
    {
        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            var request = WireCodec.Deserialize<WatchRequest>(body);
            if (request.ShardId is 1)
            {
                // Well-formed, decodes fine, and is not an acknowledgement of anything.
                await session.SendFrameAsync(
                    MessageTypes.Responses.AggregateDetails,
                    WireCodec.Serialize(new AggregateDetailsResponse
                    {
                        CorrelationId = request.CorrelationId,
                        MaxAggregateVersion = 1,
                    }));
                return;
            }

            await session.SendRawAsync(WatchAck);
        });

        var failure = await Record.ExceptionAsync(() => WatchConnection.ConnectAsync(
            server.Address,
            new WatchRequest(),
            new WatchOptions { ConnectionTimeout = TimeSpan.FromSeconds(5), MaxShardHint = 2 }));

        Assert.True(
            failure is CeleriantClientException,
            "shard 1 never acknowledged its subscription, so the watch does not cover it. Returning "
            + $"a connection anyway leaves the caller blind to that shard with nothing to say so: "
            + $"got {failure?.GetType().Name ?? "no exception"}");
    }

    private static byte[] EventFrame(Guid aggregateId)
        => FakeServerSession.BuildFrame(
            MessageTypes.Responses.Watch,
            WireCodec.Serialize(new WatchResponse
            {
                Events =
                [
                    new WatchResponseEvent
                    {
                        OrgId = Guid.NewGuid(),
                        AggregateTypeId = Guid.NewGuid(),
                        AggregateId = aggregateId,
                        Operation = WatchOperationType.Write,
                        FromAggregateVersion = 1,
                        ToAggregateVersion = 1,
                    },
                ],
            }));
}
