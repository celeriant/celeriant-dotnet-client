using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Tests;

/// <summary>
/// Once the request bytes are on the wire the client no longer knows whether the write happened. A
/// write lost after it was sent is a distinct type and is never sent to a second node: re-sending
/// it is how one write becomes two events.
/// </summary>
public class PostSendContractTests
{
    /// <summary>
    /// The leader reads the whole write and then dies without answering. The request may or may not
    /// have been applied, so the only safe report is "unknown". The seed, standing by with a hint
    /// back at the leader, must never be dialled, and the leader must see exactly one copy.
    /// </summary>
    [Fact]
    public async Task AWriteLostAfterItWasSent_IsReportedAsUnknown_AndNeverSentAgain()
    {
        int leaderRequests = 0;
        FakeCeleriantServer? leader = null;

        leader = FakeCeleriantServer.Start((session, _, _) =>
        {
            Interlocked.Increment(ref leaderRequests);
            session.Close();
            return Task.CompletedTask;
        });

        await using (leader)
        {
            await using var seed = FakeCeleriantServer.Start(
                (session, _, _) => LeaderRoutingFakes.NotLeaderAsync(session, leader!.Address));

            await using var pool = new CeleriantPool(
                LeaderRoutingFakes.Options(leader.Address, seed.Address));

            var failure = await LeaderRoutingFakes.FailureAsync(
                () => pool.WriteAsync(LeaderRoutingFakes.NewWrite()),
                "a write whose leader closed the socket after reading it");

            Assert.IsType<RequestOutcomeUnknownException>(failure);
            Assert.True(
                leaderRequests == 1,
                $"the leader received the write {leaderRequests} times. It had already read the "
                + "first copy: a second one is a duplicate event, not a retry");
            Assert.True(
                seed.ConnectionsAccepted == 0,
                $"the write was handed to another node ({seed.ConnectionsAccepted} connections to "
                + "the seed) after the leader had already read it. Nothing about an unanswered "
                + "request says it was not applied");
        }
    }

    /// <summary>
    /// The contrast: the response arrived and could not be decoded. The stream is unusable, but the
    /// exchange completed, so this stays a <see cref="ProtocolException"/> and is not re-sent.
    /// </summary>
    [Fact]
    public async Task AWriteAnsweredWithAMalformedFrame_IsAProtocolError_AndNotResent()
    {
        int leaderRequests = 0;

        await using var leader = FakeCeleriantServer.Start((session, _, _) =>
        {
            Interlocked.Increment(ref leaderRequests);
            // A well-framed response whose body is not MessagePack: 0xC1 is the one byte the
            // format never emits.
            return session.SendFrameAsync(MessageTypes.Responses.Write, [0xC1, 0xC1, 0xC1]);
        });

        await using var seed = FakeCeleriantServer.Start(
            (session, _, _) => LeaderRoutingFakes.NotLeaderAsync(session, leader.Address));

        await using var pool = new CeleriantPool(LeaderRoutingFakes.Options(leader.Address, seed.Address));

        var failure = await LeaderRoutingFakes.FailureAsync(
            () => pool.WriteAsync(LeaderRoutingFakes.NewWrite()),
            "a write answered with an undecodable frame");

        Assert.IsType<ProtocolException>(failure);
        Assert.True(
            leaderRequests == 1 && seed.ConnectionsAccepted == 0,
            $"the leader saw {leaderRequests} copies of the write and the seed {seed.ConnectionsAccepted} "
            + "connections: a reply that arrived and failed to decode is still a reply, so the "
            + "request has been served once and must not be served again");
    }
}
