using Celeriant.Client.Errors;
namespace Celeriant.Client.Tests;

/// <summary>
/// Routing behaviour LeaderRoutingContractTests does not reach: the post-send request timeout,
/// seed order, the checkout preflight, and the read walk's handling of local refusals and lost
/// responses. Drives the scripted cluster in <see cref="LeaderRoutingFakes"/>.
/// </summary>
public class LeaderRoutingWalkTests
{
    /// <summary>
    /// The leader reads the whole write and then says nothing until the request timeout. The
    /// write may have been applied, the same as a lost connection, so it must report an unknown
    /// outcome rather than a timeout the walk would answer by re-sending. The seed is never dialled.
    /// </summary>
    [Fact]
    public async Task ARequestTimeoutWaitingForTheResponse_IsUnknownOutcome_AndNeverReachesTheSeed()
    {
        var silence = new TaskCompletionSource();
        await using var leader = FakeCeleriantServer.Start((_, _, _) => silence.Task);
        await using var seed = FakeCeleriantServer.Start(
            (session, _, _) => LeaderRoutingFakes.WriteOkAsync(session));

        await using var pool = new CeleriantPool(
            LeaderRoutingFakes.Options(leader.Address, seed.Address));

        var failure = await LeaderRoutingFakes.FailureAsync(
            () => pool.WriteAsync(LeaderRoutingFakes.NewWrite()),
            "a write whose leader never answers");
        silence.SetResult();

        Assert.IsType<RequestOutcomeUnknownException>(failure);
        Assert.Equal(0, seed.ConnectionsAccepted);
    }

    /// <summary>
    /// The server answers one write, then closes every socket it accepted, as a restart or an idle
    /// close would. A write into the dead pooled connection's send buffer still succeeds, so without
    /// the checkout preflight the failure lands past the send boundary and the caller is told the
    /// outcome is unknown for a request no node ever read. The next write must get a fresh
    /// connection and succeed, and the server must see exactly two.
    /// </summary>
    [Fact]
    public async Task AWriteOnAPooledConnectionTheServerClosed_GetsAFreshOneAndSucceeds()
    {
        int requests = 0;
        await using var node = FakeCeleriantServer.Start(async (session, _, _) =>
        {
            int n = Interlocked.Increment(ref requests);
            await LeaderRoutingFakes.WriteOkAsync(session);
            if (n == 1)
                session.Close();
        });

        await using var pool = new CeleriantPool(LeaderRoutingFakes.Options(node.Address));

        await LeaderRoutingFakes.WithinBudgetAsync(
            pool.WriteAsync(LeaderRoutingFakes.NewWrite()), "the first write");

        // Let the FIN land before the pooled connection is leased again.
        await Task.Delay(300);

        var response = await LeaderRoutingFakes.WithinBudgetAsync(
            pool.WriteAsync(LeaderRoutingFakes.NewWrite()),
            "the write after the server closed the pooled connection");

        Assert.Equal(1, response.MaxAggregateVersion);
        Assert.Equal(2, Volatile.Read(ref requests));
    }

    /// <summary>
    /// The residual case: the server reads the second request and only then dies. The preflight
    /// could not have known and the write may have been applied, so this stays an unknown outcome
    /// and is not re-sent.
    /// </summary>
    [Fact]
    public async Task AConnectionClosedAfterTheRequestWasRead_IsStillAnUnknownOutcome()
    {
        int requests = 0;
        await using var node = FakeCeleriantServer.Start(async (session, _, _) =>
        {
            if (Interlocked.Increment(ref requests) == 1)
            {
                await LeaderRoutingFakes.WriteOkAsync(session);
                return;
            }
            session.Close();
        });
        await using var seed = FakeCeleriantServer.Start(
            (session, _, _) => LeaderRoutingFakes.WriteOkAsync(session));

        await using var pool = new CeleriantPool(
            LeaderRoutingFakes.Options(node.Address, seed.Address));

        await LeaderRoutingFakes.WithinBudgetAsync(
            pool.WriteAsync(LeaderRoutingFakes.NewWrite()), "the first write");

        var failure = await LeaderRoutingFakes.FailureAsync(
            () => pool.WriteAsync(LeaderRoutingFakes.NewWrite()),
            "the write the server read and never answered");

        Assert.IsType<RequestOutcomeUnknownException>(failure);
        Assert.Equal(0, seed.ConnectionsAccepted);
    }

    /// <summary>
    /// A read is always safe to re-issue, so the two failures the write walk returns at once are
    /// just broken candidates here: the next node serves the read instead.
    /// </summary>
    [Fact]
    public async Task AReadWhoseResponseIsLostAfterSend_IsServedByTheNextCandidate()
    {
        await using var leader = FakeCeleriantServer.Start((session, _, _) =>
        {
            session.Close();
            return Task.CompletedTask;
        });
        await using var follower = FakeCeleriantServer.Start(FakeServerProtocol.EchoDetails());

        await using var pool = new CeleriantPool(
            LeaderRoutingFakes.Options(leader.Address, follower.Address));

        var key = FakeServerProtocol.NewKey();
        var details = await LeaderRoutingFakes.WithinBudgetAsync(
            pool.AggregateDetailsAsync(FakeServerProtocol.Details(key)),
            "a read past a leader that loses the response");

        Assert.Equal(key.AggregateId, details.LastClientId);
    }

    /// <summary>
    /// The same for a candidate this client has locally written off: an open breaker stops the
    /// dial, it does not stop the read.
    /// </summary>
    [Fact]
    public async Task AReadPastANodeWhoseBreakerIsOpen_IsServedByTheNextCandidate()
    {
        var (dead, _) = LeaderRoutingFakes.DeadAddress();
        await using var follower = FakeCeleriantServer.Start(FakeServerProtocol.EchoDetails());

        await using var pool = new CeleriantPool(LeaderRoutingFakes.Options(dead, follower.Address));

        // The first read refuses the connect and opens the breaker; the second finds it open.
        var key = FakeServerProtocol.NewKey();
        await LeaderRoutingFakes.WithinBudgetAsync(
            pool.AggregateDetailsAsync(FakeServerProtocol.Details(key)), "the read that opens the breaker");
        var details = await LeaderRoutingFakes.WithinBudgetAsync(
            pool.AggregateDetailsAsync(FakeServerProtocol.Details(key)), "the read past the open breaker");

        Assert.Equal(key.AggregateId, details.LastClientId);
    }

    /// <summary>
    /// Seeds are walked in the order the caller configured them, not in whatever order a hash map
    /// happens to enumerate: a caller that lists its nodes nearest-first gets them tried that way,
    /// and the walk is reproducible between runs.
    /// </summary>
    [Fact]
    public async Task TheWalkVisitsSeedsInOptionsOrder()
    {
        var arrivals = new List<string>();
        void Record(string name) { lock (arrivals) arrivals.Add(name); }

        var (primary, _) = LeaderRoutingFakes.DeadAddress();
        FakeCeleriantServer? firstRef = null;
        await using var first = FakeCeleriantServer.Start((session, _, _) =>
        {
            Record("first");
            // Hinting at itself: refused, so the walk has only the seed cursor to go on.
            return LeaderRoutingFakes.NotLeaderAsync(session, firstRef!.Address);
        });
        firstRef = first;
        await using var second = FakeCeleriantServer.Start((session, _, _) =>
        {
            Record("second");
            return LeaderRoutingFakes.WriteOkAsync(session);
        });

        await using var pool = new CeleriantPool(
            LeaderRoutingFakes.Options(primary, first.Address, second.Address));

        await LeaderRoutingFakes.WithinBudgetAsync(
            pool.WriteAsync(LeaderRoutingFakes.NewWrite()), "a write walking both seeds");

        Assert.Equal(["first", "second"], arrivals);
    }

    /// <summary>
    /// A request/response connection owes nothing between exchanges, so an idle pooled socket that
    /// is readable at all is unfit, not only one readable with zero bytes. Bytes waiting on it are
    /// either a graceful peer's TLS close_notify or unsolicited data the next checkout would read
    /// as its own response.
    /// </summary>
    [Fact]
    public async Task APooledConnectionLeftWithBytesOnIt_IsRetiredNotReused()
    {
        int requests = 0;
        await using var node = FakeCeleriantServer.Start(async (session, _, _) =>
        {
            int n = Interlocked.Increment(ref requests);
            await LeaderRoutingFakes.WriteOkAsync(session);
            if (n == 1)
                await session.SendRawAsync(new byte[] { 0x42 });   // stray byte, socket stays open
        });

        await using var pool = new CeleriantPool(LeaderRoutingFakes.Options(node.Address));

        await LeaderRoutingFakes.WithinBudgetAsync(
            pool.WriteAsync(LeaderRoutingFakes.NewWrite()), "the first write");

        // Let the stray byte land before the pooled connection is leased again.
        await Task.Delay(300);

        await LeaderRoutingFakes.WithinBudgetAsync(
            pool.WriteAsync(LeaderRoutingFakes.NewWrite()),
            "the second write, on the connection the server left bytes on");

        Assert.Equal(2, Volatile.Read(ref requests));
        Assert.True(
            node.ConnectionsAccepted == 2,
            $"the server accepted {node.ConnectionsAccepted} connection(s); a pooled connection "
            + "with bytes waiting on it must be retired and the write sent on a fresh one");
    }

    /// <summary>
    /// Hops have their own cap inside the shared attempt budget. Without it a chain of live
    /// redirects spends the whole budget and the configured seed that would have taken the write
    /// is never dialled.
    /// </summary>
    [Fact]
    public async Task AChainOfLiveHints_DoesNotStarveTheConfiguredSeedThatIsLeader()
    {
        var chain = new FakeCeleriantServer[4];
        string NextAddress(int i) => chain[i + 1].Address;

        await using var leaderSeed = FakeCeleriantServer.Start(
            (session, _, _) => LeaderRoutingFakes.WriteOkAsync(session));

        // The last hop hints at the seed, which the hop cap has already reached by then.
        chain[3] = FakeCeleriantServer.Start(
            (session, _, _) => LeaderRoutingFakes.NotLeaderAsync(session, leaderSeed.Address));
        for (int i = 2; i >= 0; i--)
        {
            int self = i;
            chain[i] = FakeCeleriantServer.Start(
                (session, _, _) => LeaderRoutingFakes.NotLeaderAsync(session, NextAddress(self)));
        }

        try
        {
            // The primary hints into the chain; the only other configured node is the real leader.
            await using var primary = FakeCeleriantServer.Start(
                (session, _, _) => LeaderRoutingFakes.NotLeaderAsync(session, chain[0].Address));

            await using var pool = new CeleriantPool(
                LeaderRoutingFakes.Options(primary.Address, leaderSeed.Address));

            var response = await LeaderRoutingFakes.WithinBudgetAsync(
                pool.WriteAsync(LeaderRoutingFakes.NewWrite()),
                "a write whose redirects run out before the seed list does");

            Assert.Equal(1, response.MaxAggregateVersion);
        }
        finally
        {
            foreach (var node in chain)
                await node.DisposeAsync();
        }
    }
}
