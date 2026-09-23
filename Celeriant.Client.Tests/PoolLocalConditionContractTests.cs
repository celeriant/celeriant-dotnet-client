using Celeriant.Client.Errors;

namespace Celeriant.Client.Tests;

/// <summary>
/// A condition inside this client is not a verdict about the cluster. The local type is distinct
/// from <see cref="ConnectionFailedException"/> and not derived from it, because "this pool
/// declined to dial" and "the node is unreachable" lead callers to different actions.
/// </summary>
public class PoolLocalConditionContractTests
{
    /// <summary>
    /// A refused connect opens the node's breaker, and the next write inside that window never
    /// reaches a socket. Reporting it as a connection failure tells the caller the node is down on
    /// the strength of a decision this client made, so the second write must fail with the local
    /// type, naming the address and the reason.
    /// </summary>
    [Fact]
    public async Task AnOpenBreaker_IsALocalConditionWithItsOwnType_NotAConnectionFailure()
    {
        var (primary, _) = LeaderRoutingFakes.DeadAddress();
        await using var pool = new CeleriantPool(LeaderRoutingFakes.Options(primary));

        // Opens the breaker on the primary: a real connect, really refused.
        await LeaderRoutingFakes.FailureAsync(
            () => pool.WriteAsync(LeaderRoutingFakes.NewWrite()),
            "the write that refuses at the primary");

        var failure = await LeaderRoutingFakes.FailureAsync(
            () => pool.WriteAsync(LeaderRoutingFakes.NewWrite()),
            "the write issued while the breaker is still open");

        Assert.False(
            failure is ConnectionFailedException,
            $"the breaker refused to dial, and the caller was told \"{failure!.Message}\" as a "
            + "ConnectionFailedException: a local back-off is being reported as a statement about "
            + "the node, which no retry policy can tell apart from a node that is genuinely gone");
        Assert.IsType<PoolUnavailableException>(failure);
        Assert.True(
            LeaderRoutingFakes.Names(failure, primary),
            $"\"{failure.Message}\" does not name {primary}: the caller cannot tell which node this "
            + "pool is backing off from");
    }

    /// <summary>
    /// The local failure belongs to the caller at once. Nothing else in the cluster is asked, though
    /// the seed here would happily answer, and the cached leader is still the leader afterwards:
    /// once the address is listening again, the next write goes straight back to it.
    ///
    /// <para>
    /// The seed is asked once while the breaker is being opened, since that write is a real walk
    /// over a refused primary. What this pins is that the local exception adds no contact after it.
    /// </para>
    /// </summary>
    [Fact]
    public async Task ALocalFailureAtTheLeader_ContactsNoOtherNode_AndLeavesTheLeaderCached()
    {
        var (leaderAddress, port) = LeaderRoutingFakes.DeadAddress();
        int seedRequests = 0;

        await using var seed = FakeCeleriantServer.Start((session, _, _) =>
        {
            Interlocked.Increment(ref seedRequests);
            return LeaderRoutingFakes.NotLeaderAsync(session, leaderAddress);
        });

        await using var pool = new CeleriantPool(LeaderRoutingFakes.Options(leaderAddress, seed.Address));

        await LeaderRoutingFakes.FailureAsync(
            () => pool.WriteAsync(LeaderRoutingFakes.NewWrite()),
            "the write that refuses at the leader and walks to the seed");

        int seedRequestsBefore = Volatile.Read(ref seedRequests);
        int seedConnectionsBefore = seed.ConnectionsAccepted;

        var failure = await LeaderRoutingFakes.FailureAsync(
            () => pool.WriteAsync(LeaderRoutingFakes.NewWrite()),
            "the write issued while the leader's breaker is open");

        Assert.IsType<PoolUnavailableException>(failure);
        Assert.True(
            Volatile.Read(ref seedRequests) == seedRequestsBefore
                && seed.ConnectionsAccepted == seedConnectionsBefore,
            $"the seed went from {seedRequestsBefore} requests on {seedConnectionsBefore} connections "
            + $"to {Volatile.Read(ref seedRequests)} on {seed.ConnectionsAccepted}. This pool declining "
            + "to dial its own leader says nothing about who the leader is, so the write must come "
            + "back to the caller rather than being handed to another node");

        await Task.Delay(LeaderRoutingFakes.PastTheBreakerWindow);

        int leaderRequests = 0;
        await using var revivedLeader = FakeCeleriantServer.Start(
            (session, _, _) =>
            {
                Interlocked.Increment(ref leaderRequests);
                return LeaderRoutingFakes.WriteOkAsync(session);
            },
            port);

        var response = await LeaderRoutingFakes.WithinBudgetAsync(
            pool.WriteAsync(LeaderRoutingFakes.NewWrite()),
            "the write after the leader came back");

        Assert.Equal(1, response.MaxAggregateVersion);
        Assert.True(
            leaderRequests == 1 && Volatile.Read(ref seedRequests) == seedRequestsBefore,
            $"after the leader came back it served {leaderRequests} requests and the seed was asked "
            + $"{Volatile.Read(ref seedRequests) - seedRequestsBefore} more times: the local failure "
            + "moved the cached leader, so a write now pays a redirect to relearn what the pool "
            + "already knew");
    }
}
