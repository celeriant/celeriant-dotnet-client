using System.Net;
using System.Net.Sockets;
using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Watch;

namespace Celeriant.Client.Tests;

/// <summary>
/// Edge cases of the watch address and teardown surface: <c>WatchConnection.Address</c>,
/// the poison that stops sibling shard readers and the single-owner-per-socket rule that goes with
/// it, and the cached-leader reset on a failed watch dial.
///
/// Every test is aimed at one of two states: a caller subscribed and blind, or a caller blind and
/// not told. Where a test can only reach a resource leak rather than a blindness, it says so.
/// </summary>
public class WatchAddressEdgeCaseTests
{
    private static readonly TimeSpan HangBudget = TimeSpan.FromSeconds(5);

    private static readonly byte[] WatchAck =
        FakeServerSession.BuildFrame(MessageTypes.Responses.Watch, WireCodec.Serialize(new WatchResponse()));

    // =====================================================================
    // A. The cached-leader reset
    // =====================================================================

    /// <summary>
    /// The Rust <c>clear_leader()</c> this mirrors has two arms, and the second is the one that
    /// matters here:
    /// <code>
    /// fn clear_leader(&amp;self) {
    ///     let mut guard = self.leader_address.write().unwrap();
    ///     if guard.is_some() { *guard = None; }
    ///     else if let Some(seed) = self.options.seed_addresses.first() { *guard = Some(seed.clone()); }
    /// }
    /// </code>
    /// (celeriant_client_tokio/src/pool.rs, <c>clear_leader</c>). Rust's <c>None</c> means "no
    /// cached leader, fall back to the primary", so its two arms are: a cached leader that is not
    /// the primary is dropped back to the primary; a leader that IS the primary — the case Rust
    /// reaches when the primary itself just failed — moves to the first seed, explicitly "to avoid
    /// retrying the dead primary".
    ///
    /// The .NET reset implements the first arm only. When the node that refused the watch dial is
    /// the configured primary, reverting to the configured primary is a no-op, so the pool stays
    /// pinned to the node it has just proved dead and every later operation re-pays that dial.
    /// </summary>
    [Fact]
    public async Task Pool_WhenTheNodeThatRefusedTheWatchDialIsTheConfiguredPrimary_ThePoolStaysPinnedToIt()
    {
        // Accepts the socket and hangs up before acking: a dial that reaches the node and still
        // fails, which is what makes it countable from the server side.
        await using var deadPrimary = FakeCeleriantServer.Start((session, _, _) =>
        {
            session.Close();
            return Task.CompletedTask;
        });
        await using var healthy = FakeCeleriantServer.Start((session, _, _) => session.SendRawAsync(WatchAck));

        await using var pool = new CeleriantPool(new CeleriantPoolOptions
        {
            Address = deadPrimary.Address,
            SeedAddresses = [healthy.Address],
            ConnectionTimeout = TimeSpan.FromMilliseconds(500),
            RequestTimeout = TimeSpan.FromSeconds(10),
        });

        // No SetLeaderForTesting: the leader is the configured primary, which is the state a pool
        // is in until a write redirects it. This is the common case, not an exotic one.
        Assert.Equal(deadPrimary.Address, pool.GetWatchAddress());

        var first = pool.WatchAsync(new WatchRequest());
        await AwaitCompletionAsync(first, "the first pool.WatchAsync failing over from a dead primary");
        await using var firstWatch = await first;
        Assert.Equal(healthy.Address, firstWatch.Address);

        int dialsDuringFailover = deadPrimary.ConnectionsAccepted;
        Assert.True(
            dialsDuringFailover >= 1,
            $"the test says nothing about a primary that was never dialled ({deadPrimary.Address})");

        var second = pool.WatchAsync(new WatchRequest());
        await AwaitCompletionAsync(second, "the second pool.WatchAsync after the first had failed over");
        await using var secondWatch = await second;

        Assert.True(
            deadPrimary.ConnectionsAccepted == dialsDuringFailover,
            $"the watch dial to {deadPrimary.Address} failed and the pool fell through to "
            + $"{healthy.Address}, but the primary is still the first candidate "
            + $"(pool.GetWatchAddress() = {pool.GetWatchAddress()}) and was dialled again "
            + $"({deadPrimary.ConnectionsAccepted} dials in total, {dialsDuringFailover} of them "
            + "before the second call). Rust's clear_leader moves to the first seed in exactly this "
            + "case; reverting to Options.Address is a no-op when Options.Address is what failed");
    }

    /// <summary>
    /// The routing half of the same defect, without counting sockets: after a watch has failed over
    /// away from the primary, the primary must not still be named as where the next operation goes
    /// first. This is the same contract ReadRoutingOrderTests asserts — it only ever exercises it
    /// with a cached leader that is not the primary.
    /// </summary>
    [Fact]
    public async Task Pool_AfterAWatchFailsOverFromTheConfiguredPrimary_ThePrimaryIsNoLongerTheFirstCandidate()
    {
        var deadPrimary = ReservedAddressThatRefusesConnections();
        await using var healthy = FakeCeleriantServer.Start((session, _, _) => session.SendRawAsync(WatchAck));

        await using var pool = new CeleriantPool(new CeleriantPoolOptions
        {
            Address = deadPrimary,
            SeedAddresses = [healthy.Address],
            ConnectionTimeout = TimeSpan.FromMilliseconds(500),
            RequestTimeout = TimeSpan.FromSeconds(10),
        });

        var connect = pool.WatchAsync(new WatchRequest());
        await AwaitCompletionAsync(connect, "pool.WatchAsync failing over from an unreachable primary");
        await using var watch = await connect;

        Assert.True(
            pool.GetWatchAddress() != deadPrimary,
            $"the dial to the primary {deadPrimary} was refused and the watch landed on "
            + $"{healthy.Address}, yet the pool still sends the next operation to the refused node "
            + "first: the failure taught it nothing");
    }

    /// <summary>
    /// The reset is a blind write, not a compare-and-set against the candidate that failed. The
    /// candidate list is snapshotted at the top of WatchAsync, so a leader learned from a
    /// NotLeaderException redirect while the stale candidate's dial was still in flight is
    /// overwritten by the failure of a node that is no longer the leader — and the pool goes back
    /// to a node the cluster has already told it is not in charge.
    ///
    /// The dial here is bounded by ConnectionTimeout rather than raced against a wall clock: the
    /// redirect is applied while the connect is provably still outstanding.
    /// </summary>
    [Fact]
    public async Task Pool_ALeaderLearnedWhileAStaleWatchDialWasInFlight_IsDiscardedByThatDialsFailure()
    {
        // Accepts the socket and never acks: the dial is in flight until ConnectionTimeout.
        var reachedServer = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var staleLeader = FakeCeleriantServer.Start((session, _, _) =>
        {
            reachedServer.TrySetResult();
            return Task.CompletedTask;
        });
        await using var primary = FakeCeleriantServer.Start((session, _, _) => session.SendRawAsync(WatchAck));
        await using var newLeader = FakeCeleriantServer.Start((session, _, _) => session.SendRawAsync(WatchAck));

        await using var pool = new CeleriantPool(new CeleriantPoolOptions
        {
            Address = primary.Address,
            SeedAddresses = [staleLeader.Address, newLeader.Address],
            ConnectionTimeout = TimeSpan.FromMilliseconds(500),
            RequestTimeout = TimeSpan.FromSeconds(10),
        });

        pool.SetLeaderForTesting(staleLeader.Address);

        var connect = pool.WatchAsync(new WatchRequest());
        await AwaitCompletionAsync(reachedServer.Task, "the watch dial reaching the stale leader");

        // What a concurrent write does when a follower redirects it: this is the pool's freshest
        // knowledge of who is in charge, learned while the watch dial above is still outstanding.
        pool.SetLeaderForTesting(newLeader.Address);

        await AwaitCompletionAsync(connect, "pool.WatchAsync failing over from the unresponsive stale leader");
        await using var watch = await connect;

        Assert.True(
            pool.GetWatchAddress() == newLeader.Address,
            $"the pool had just learned {newLeader.Address} was the leader; the failure of a dial to "
            + $"{staleLeader.Address}, a node the snapshot named before that was known, reset the "
            + $"leader to {pool.GetWatchAddress()}. The reset overwrites rather than retracting the "
            + "address it dialled, so fresher leader knowledge is lost and writes go back to a node "
            + "the cluster has already redirected away from");
    }

    /// <summary>
    /// The reset lives in two catch arms — a refused dial and a dial that timed out. Only the
    /// refused arm is exercised anywhere: deleting the reset from the timeout arm leaves the whole
    /// landed suite green. A node that accepts the socket and never answers is the more common
    /// shape of a dying node than one that refuses outright, so this is the arm that matters most.
    /// </summary>
    [Fact]
    public async Task Pool_AfterAWatchDialToTheCachedLeaderTimesOut_ThePoolIsNoLongerPinnedToIt()
    {
        // Accepts the socket and never acks: the dial ends in a timeout, not a refusal.
        await using var unresponsiveLeader = FakeCeleriantServer.Start((session, _, _) => Task.CompletedTask);
        await using var healthy = FakeCeleriantServer.Start((session, _, _) => session.SendRawAsync(WatchAck));

        await using var pool = new CeleriantPool(new CeleriantPoolOptions
        {
            Address = healthy.Address,
            SeedAddresses = [unresponsiveLeader.Address],
            ConnectionTimeout = TimeSpan.FromMilliseconds(500),
            RequestTimeout = TimeSpan.FromSeconds(10),
        });

        pool.SetLeaderForTesting(unresponsiveLeader.Address);

        var connect = pool.WatchAsync(new WatchRequest());
        await AwaitCompletionAsync(connect, "pool.WatchAsync failing over from a leader that never acks");
        await using var watch = await connect;
        Assert.Equal(healthy.Address, watch.Address);

        Assert.True(
            pool.GetWatchAddress() != unresponsiveLeader.Address,
            $"the watch dial to the cached leader {unresponsiveLeader.Address} timed out and the pool "
            + $"fell through to {healthy.Address}, yet the timed-out node is still where the next "
            + "operation goes first — every later call re-pays the same timeout before failing over");
    }

    /// <summary>
    /// With follower routing on, the leader is the LAST candidate, so candidate zero is a follower.
    /// A follower refusing a watch dial says nothing about who is in charge, and the reset must not
    /// fire — otherwise one unreachable follower discards leader knowledge the cluster gave the
    /// pool, and the write path goes back to a node that has already redirected it away.
    ///
    /// Both guards on the reset — the routing-mode guard and the candidate-zero guard — are
    /// currently unpinned: removing the routing-mode guard leaves the whole suite green.
    /// </summary>
    [Fact]
    public async Task Pool_WithFollowerRouting_AFollowerRefusingTheWatchDial_DoesNotClearTheLeader()
    {
        var deadFollowerA = ReservedAddressThatRefusesConnections();
        var deadFollowerB = ReservedAddressThatRefusesConnections();
        await using var leader = FakeCeleriantServer.Start((session, _, _) => session.SendRawAsync(WatchAck));

        await using var pool = new CeleriantPool(new CeleriantPoolOptions
        {
            Address = deadFollowerA,
            SeedAddresses = [deadFollowerB],
            RouteReadsToFollowers = true,
            ConnectionTimeout = TimeSpan.FromMilliseconds(500),
            RequestTimeout = TimeSpan.FromSeconds(10),
        });

        // The leader the cluster told this pool about: neither of the two followers, and the last
        // candidate a follower-routed watch will try.
        pool.SetLeaderForTesting(leader.Address);

        var connect = pool.WatchAsync(new WatchRequest());
        await AwaitCompletionAsync(connect, "pool.WatchAsync falling through two dead followers to the leader");
        await using var watch = await connect;
        Assert.Equal(leader.Address, watch.Address);

        var candidates = pool.GetReadNodeAddresses();
        Assert.True(
            candidates[^1] == leader.Address,
            $"two followers refused the watch dial and the pool forgot that {leader.Address} is the "
            + $"leader: it now believes the leader is {candidates[^1]}. A follower's connect failure "
            + "carries no information about the leader, so writes are now aimed at a node the "
            + "cluster has already redirected away from");
    }

    // =====================================================================
    // B. Single ownership per shard socket: is there a socket nobody closes?
    // =====================================================================

    /// <summary>
    /// The MaxShards bound exists so an absurd shard count is refused before the client tries to
    /// honour it. If it were checked after the fan-out it would bound nothing: the sockets are the
    /// resource, and every one of them is opened by a task whose reader may never run.
    /// </summary>
    [Fact]
    public async Task MultiShard_AShardCountPastTheClientBound_OpensNoSocketsAtAll()
    {
        await using var server = FakeCeleriantServer.Start((session, _, _) => session.SendRawAsync(WatchAck));

        var failure = await Record.ExceptionAsync(() => WatchConnection.ConnectAsync(
            server.Address,
            new WatchRequest(),
            new WatchOptions { ConnectionTimeout = TimeSpan.FromSeconds(5), MaxShardHint = 2000 }));

        // The 2000 is the caller's own MaxShardHint, so it is a programmer error no retry or
        // failover can fix — an argument exception, not something a catch around connect for
        // client errors should be swallowing alongside the failovers.
        Assert.True(
            failure is ArgumentOutOfRangeException,
            $"expected an argument error naming the caller's own bound, got {Describe(failure)}");
        Assert.Equal(nameof(WatchOptions.MaxShardHint), ((ArgumentOutOfRangeException)failure!).ParamName);
        Assert.True(
            server.ConnectionsAccepted == 0,
            $"the range was rejected, but {server.ConnectionsAccepted} sockets were opened first: "
            + "the bound is being applied after the fan-out it exists to prevent");
    }

    /// <summary>
    /// The same bound reached through the 9001 fallback, where a probe connection is already open
    /// when the server names the shard count. That probe must not survive the rejection: it is a
    /// socket whose only owner — the reader that would have closed it — never starts.
    /// </summary>
    [Fact]
    public async Task ShardRoutingFallback_AShardCountPastTheClientBound_ReleasesTheProbeSocket()
    {
        var sessionsEnded = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int ended = 0;

        await using var server = FakeCeleriantServer.Start(
            async (session, _, body) =>
            {
                var request = WireCodec.Deserialize<WatchRequest>(body);
                if (request.ShardId is null)
                {
                    await session.SendFrameAsync(
                        MessageTypes.Responses.GenericError,
                        FakeServerProtocol.ErrorFrame(
                            ErrorResponse.ShardRoutingMultipleShards,
                            "watch filter spans shards {\"num_shards\":5000}",
                            request.CorrelationId));
                    return;
                }

                await session.SendRawAsync(WatchAck);
            },
            _ =>
            {
                if (Interlocked.Increment(ref ended) >= 1)
                    sessionsEnded.TrySetResult();
            });

        var failure = await Record.ExceptionAsync(() => WatchConnection.ConnectAsync(
            server.Address,
            new WatchRequest(),
            new WatchOptions { ConnectionTimeout = TimeSpan.FromSeconds(5) }));

        Assert.True(failure is CeleriantClientException, $"expected a client error, got {Describe(failure)}");
        Assert.True(
            server.ConnectionsAccepted == 1,
            $"the rejected fan-out still opened sockets: {server.ConnectionsAccepted} accepted where "
            + "only the probe should have been");
        await AwaitCompletionAsync(sessionsEnded.Task, "the probe socket of a rejected fallback being released");
    }

    /// <summary>
    /// A shard that cannot subscribe fails the connect. The shards that DID subscribe are then
    /// sockets with no reader — the reader is what owns and closes a shard socket, and these
    /// readers never start — so the failed connect has to close them itself or they leak one
    /// socket per shard, per attempt, forever.
    /// </summary>
    [SkippableFact]
    public async Task MultiShard_AConnectThatFailsOnOneShard_ReleasesEverySocketItOpened()
    {
        const int shardCount = 4;
        var everySocketEnded = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int ended = 0;

        await using var server = FakeCeleriantServer.Start(
            async (session, _, body) =>
            {
                // Shard 3 answers with a well-formed frame of the wrong kind: the connect fails
                // after the earlier shards have already subscribed and own sockets.
                if (ShardOf(body) is 3)
                {
                    await session.SendFrameAsync(
                        MessageTypes.Responses.Read,
                        WireCodec.Serialize(new ReadResponse()));
                    return;
                }

                await session.SendRawAsync(WatchAck);
            },
            _ =>
            {
                if (Interlocked.Increment(ref ended) >= shardCount)
                    everySocketEnded.TrySetResult();
            });

        var gcBefore = GC.CollectionCount(0);
        var failure = await Record.ExceptionAsync(() => ConnectAsync(server.Address, maxShardHint: shardCount));
        Assert.True(failure is CeleriantClientException, $"expected a client error, got {Describe(failure)}");

        await AssertReleasedByTheClientAsync(
            everySocketEnded.Task,
            gcBefore,
            () => $"all {shardCount} shard sockets of a failed multi-shard connect being released "
                + $"({Volatile.Read(ref ended)} of {shardCount} had been released)");
    }

    /// <summary>
    /// The same question with the connect cancelled rather than refused: the caller's token fires
    /// while one shard's ack is still outstanding. Every socket opened so far has to go with it.
    /// </summary>
    [SkippableFact]
    public async Task MultiShard_AConnectCancelledMidFanOut_ReleasesEverySocketItOpened()
    {
        const int shardCount = 3;
        var everySocketEnded = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var lastShardReached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int ended = 0;

        await using var server = FakeCeleriantServer.Start(
            async (session, _, body) =>
            {
                // Shard 2 is never acked: the read loop stays reading, so it still notices the
                // client closing the socket. Parking inside the handler would hide that.
                if (ShardOf(body) is 2)
                {
                    lastShardReached.TrySetResult();
                    return;
                }

                await session.SendRawAsync(WatchAck);
            },
            _ =>
            {
                if (Interlocked.Increment(ref ended) >= shardCount)
                    everySocketEnded.TrySetResult();
            });

        using var cancel = new CancellationTokenSource();
        var connect = WatchConnection.ConnectAsync(
            server.Address,
            new WatchRequest(),
            new WatchOptions { ConnectionTimeout = TimeSpan.FromSeconds(30), MaxShardHint = shardCount },
            cancel.Token);

        await AwaitCompletionAsync(lastShardReached.Task, "the server receiving every shard's watch request");
        var gcBefore = GC.CollectionCount(0);
        await cancel.CancelAsync();

        var failure = await Record.ExceptionAsync(() => connect);
        Assert.True(
            failure is OperationCanceledException,
            $"the caller cancelled the connect and got {Describe(failure)}");

        await AssertReleasedByTheClientAsync(
            everySocketEnded.Task,
            gcBefore,
            () => $"all {shardCount} shard sockets of a cancelled multi-shard connect being released "
                + $"({Volatile.Read(ref ended)} of {shardCount} had been released)");
    }

    // =====================================================================
    // C. The poison cancels the readers from inside a reader's own finally
    // =====================================================================

    /// <summary>
    /// A reader that fails publishes its reason, then cancels the token it is itself running under,
    /// then closes the channel, then closes its own socket — all inside its <c>finally</c>, and all
    /// while a caller may be in <c>DisposeAsync</c> awaiting that same task. Run the race often
    /// enough to catch a teardown that can wedge.
    /// </summary>
    [Fact]
    public async Task MultiShard_DisposeRacingThePoison_NeverWedgesAndAlwaysReleasesEverySocket()
    {
        const int shardCount = 3;
        const int rounds = 40;

        for (int round = 0; round < rounds; round++)
        {
            var kill = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var everySocketEnded = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            int ended = 0;

            await using var server = FakeCeleriantServer.Start(
                async (session, _, body) =>
                {
                    await session.SendRawAsync(WatchAck);
                    if (ShardOf(body) is not 1)
                        return;

                    await kill.Task;
                    session.Close();
                },
                _ =>
                {
                    if (Interlocked.Increment(ref ended) >= shardCount)
                        everySocketEnded.TrySetResult();
                });

            var watch = await ConnectAsync(server.Address, maxShardHint: shardCount);

            // Poison and dispose start together: the failing reader is inside its finally, cancelling
            // the token, at the moment DisposeAsync begins waiting on it.
            var disposeSoon = Task.Run(async () =>
            {
                kill.SetResult();
                await watch.DisposeAsync();
            });

            await AwaitCompletionAsync(disposeSoon, $"DisposeAsync racing the poison on round {round}");
            await disposeSoon;
            await AwaitCompletionAsync(
                everySocketEnded.Task,
                $"round {round}: all {shardCount} shard sockets being released "
                + $"({Volatile.Read(ref ended)} of {shardCount} released)");
        }
    }

    /// <summary>
    /// Two callers disposing at once, on top of a poison. The teardown claims itself atomically, so
    /// the loser returns immediately — which means it can return BEFORE the readers are gone. That
    /// is a documented shape here, not a defect; what must hold is that neither call hangs.
    /// </summary>
    [Fact]
    public async Task MultiShard_TwoConcurrentDisposalsOfAPoisonedWatch_BothComplete()
    {
        const int shardCount = 3;
        var kill = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            await session.SendRawAsync(WatchAck);
            if (ShardOf(body) is not 1)
                return;

            await kill.Task;
            session.Close();
        });

        var watch = await ConnectAsync(server.Address, maxShardHint: shardCount);
        kill.SetResult();

        var next = watch.NextAsync(CancellationToken.None);
        await AwaitCompletionAsync(next, "NextAsync surfacing the poison");
        await Record.ExceptionAsync(() => next);

        var a = watch.DisposeAsync().AsTask();
        var b = watch.DisposeAsync().AsTask();
        await AwaitCompletionAsync(Task.WhenAll(a, b), "two concurrent DisposeAsync calls on a poisoned watch");
        await Task.WhenAll(a, b);
    }

    /// <summary>
    /// A caller parked in <c>NextAsync</c> while another thread disposes must be released, and with
    /// something it can act on. Parked-forever here is the invariant's exact failure mode: still
    /// subscribed as far as the caller knows, receiving nothing, never told.
    /// </summary>
    [Fact]
    public async Task MultiShard_DisposingWhileACallerIsParkedInNextAsync_ReleasesThatCaller()
    {
        const int shardCount = 3;

        await using var server = FakeCeleriantServer.Start((session, _, _) => session.SendRawAsync(WatchAck));

        var watch = await ConnectAsync(server.Address, maxShardHint: shardCount);
        var next = watch.NextAsync(CancellationToken.None);

        // Give the reader a moment to be genuinely parked on the socket rather than still starting.
        Assert.Null(await watch.NextAsync(TimeSpan.FromMilliseconds(100)));

        var dispose = watch.DisposeAsync().AsTask();
        await AwaitCompletionAsync(dispose, "DisposeAsync while a caller is parked in NextAsync");
        await dispose;

        await AwaitCompletionAsync(next, "the parked NextAsync being released by the disposal");
        var failure = await Record.ExceptionAsync(() => next);
        Assert.True(failure is not null, "the parked caller was released with no exception and no event");
    }

    // =====================================================================
    // D. What the caller is told when the poison lands
    // =====================================================================

    /// <summary>
    /// The poison cancels the siblings, so every sibling reader exits through the cancellation arm
    /// carrying the fail-closed preset "stopped without reporting a cause". The caller must still
    /// be handed the FIRST, real failure: the generic preset names no cause and would tell a caller
    /// nothing about which node or shard went.
    /// </summary>
    [Fact]
    public async Task MultiShard_ThePoisonReportsTheFailingShardsCause_NotASiblingsGenericStop()
    {
        const int shardCount = 5;
        var kill = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            await session.SendRawAsync(WatchAck);
            if (ShardOf(body) is not 3)
                return;

            await kill.Task;
            session.Close();
        });

        await using var watch = await ConnectAsync(server.Address, maxShardHint: shardCount);
        var next = watch.NextAsync(CancellationToken.None);
        kill.SetResult();
        await AwaitCompletionAsync(next, "NextAsync surfacing the poison");

        var failure = await Record.ExceptionAsync(() => next);
        Assert.True(failure is CeleriantClientException, $"expected a client error, got {Describe(failure)}");
        Assert.False(
            failure!.Message.Contains("without reporting a cause", StringComparison.Ordinal),
            "the caller was handed a sibling reader's generic fail-closed placeholder instead of the "
            + $"real failure that killed the subscription: {failure.Message}");
    }

    /// <summary>
    /// Events the subscription had already accepted must survive the poison. They rode in on the
    /// subscription acks, before any reader existed, so nothing else will ever deliver them; a
    /// poison that discards them loses events the client had in hand and reports only the failure.
    /// </summary>
    [Fact]
    public async Task MultiShard_EventsAcceptedBeforeTheFailure_StillDrainAheadOfIt()
    {
        const int shardCount = 3;
        var aggregateId = Guid.NewGuid();
        var kill = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            // Shard 0's ack carries an event: it is in the client's hands before any reader runs,
            // which removes the race between "delivered" and "poisoned" from this test.
            if (ShardOf(body) is 0)
            {
                await session.SendRawAsync(EventFrame(aggregateId));
                return;
            }

            await session.SendRawAsync(WatchAck);
            if (ShardOf(body) is not 1)
                return;

            await kill.Task;
            session.Close();
        });

        await using var watch = await ConnectAsync(server.Address, maxShardHint: shardCount);

        kill.SetResult();

        // Let the poison land first, so this asserts a drain from an already-dead subscription
        // rather than winning a race against it.
        await Task.Delay(300);

        var first = watch.NextAsync(TimeSpan.FromSeconds(2));
        await AwaitCompletionAsync(first, "NextAsync draining the event accepted before the poison");
        var buffered = await Record.ExceptionAsync(async () => Assert.NotNull(await first));
        Assert.True(
            buffered is null,
            "an event the subscription had already accepted was discarded by the poisoning and the "
            + $"caller saw the failure instead: {Describe(buffered)}");
        Assert.Equal(aggregateId, Assert.Single((await first)!.Events).AggregateId);

        var then = watch.NextAsync(CancellationToken.None);
        await AwaitCompletionAsync(then, "NextAsync reporting the poison once the buffer has drained");
        Assert.True(
            await Record.ExceptionAsync(() => then) is CeleriantClientException,
            "the buffer drained but the failure behind it never surfaced");
    }

    /// <summary>
    /// The same drain question for events delivered mid-stream rather than on the ack: an event
    /// written into the channel by a healthy shard before a sibling died must still reach the
    /// caller. Losing it would be an event the client received, acknowledged nothing about, and
    /// discarded — blind on data it already held.
    /// </summary>
    [Fact]
    public async Task MultiShard_AnEventDeliveredBeforeASiblingDied_StillReachesTheCaller()
    {
        const int shardCount = 3;
        var aggregateId = Guid.NewGuid();
        var deliver = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var kill = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            await session.SendRawAsync(WatchAck);
            switch (ShardOf(body))
            {
                case 0:
                    await deliver.Task;
                    await session.SendRawAsync(EventFrame(aggregateId));
                    return;
                case 1:
                    await kill.Task;
                    session.Close();
                    return;
                default:
                    return;
            }
        });

        await using var watch = await ConnectAsync(server.Address, maxShardHint: shardCount);

        deliver.SetResult();
        // The event is in the channel well before the poison starts, so this is a drain, not a race.
        await Task.Delay(300);
        kill.SetResult();
        await Task.Delay(300);

        var first = watch.NextAsync(TimeSpan.FromSeconds(2));
        await AwaitCompletionAsync(first, "NextAsync draining the event delivered before the poison");
        var dropped = await Record.ExceptionAsync(async () => Assert.NotNull(await first));
        Assert.True(
            dropped is null,
            "an event already delivered into the subscription was lost when a sibling shard died: "
            + $"the caller saw {Describe(dropped)} instead");
        Assert.Equal(aggregateId, Assert.Single((await first)!.Events).AggregateId);
    }

    /// <summary>
    /// A poisoned watch polled on a deadline must report the failure, never a null. Null means
    /// "nothing arrived yet, still subscribed", and a caller polling on a short deadline would read
    /// a dead subscription as a quiet one for as long as it kept polling.
    /// </summary>
    [Fact]
    public async Task MultiShard_PollingAPoisonedWatchOnADeadline_NeverReportsItAsMerelyQuiet()
    {
        const int shardCount = 3;
        var kill = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            await session.SendRawAsync(WatchAck);
            if (ShardOf(body) is not 1)
                return;

            await kill.Task;
            session.Close();
        });

        await using var watch = await ConnectAsync(server.Address, maxShardHint: shardCount);
        var next = watch.NextAsync(CancellationToken.None);
        kill.SetResult();
        await AwaitCompletionAsync(next, "NextAsync surfacing the poison");
        await Record.ExceptionAsync(() => next);

        for (int poll = 0; poll < 5; poll++)
        {
            var failure = await Record.ExceptionAsync(async () =>
                Assert.Null(await watch.NextAsync(TimeSpan.FromMilliseconds(1))));
            Assert.True(
                failure is CeleriantClientException,
                $"poll {poll} of a poisoned watch on a 1ms deadline reported {Describe(failure)}; "
                + "a null there tells a blind caller its subscription is merely quiet");
        }
    }

    /// <summary>
    /// The failure a dead watch reports is stable: the same object every time, and a stack trace
    /// that does not lengthen with each read. A caller that polls after the failure — the natural
    /// shape, since the subscription can never recover — otherwise grows the exception without
    /// bound.
    /// </summary>
    [Fact]
    public async Task MultiShard_ReReadingADeadWatch_ReplaysTheSameFailureWithoutGrowingIt()
    {
        const int shardCount = 2;
        var kill = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            await session.SendRawAsync(WatchAck);
            if (ShardOf(body) is not 1)
                return;

            await kill.Task;
            session.Close();
        });

        await using var watch = await ConnectAsync(server.Address, maxShardHint: shardCount);
        var next = watch.NextAsync(CancellationToken.None);
        kill.SetResult();
        await AwaitCompletionAsync(next, "NextAsync surfacing the poison");

        var first = await Record.ExceptionAsync(() => next);
        Assert.NotNull(first);

        int firstLength = first!.StackTrace?.Length ?? 0;
        for (int read = 0; read < 6; read++)
        {
            var again = await Record.ExceptionAsync(() => watch.NextAsync(CancellationToken.None));
            Assert.True(ReferenceEquals(first, again), $"read {read} produced a different exception object");
            int length = again!.StackTrace?.Length ?? 0;
            Assert.True(
                length == firstLength,
                $"the stack trace grew from {firstLength} to {length} characters over {read + 1} "
                + "re-reads of one dead watch");
        }
    }

    /// <summary>
    /// The degenerate multi-shard watch: one shard, so exactly one reader, and that reader is the
    /// only thing that can ever complete the channel. Everything the poison does — publish, cancel
    /// the siblings it does not have, close the channel, close its socket — happens in one task
    /// with no second reader behind it to finish the job if it stops early.
    /// </summary>
    [Fact]
    public async Task MultiShard_ASingleShardFanOutThatDies_TellsTheCallerRatherThanGoingQuiet()
    {
        var kill = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var socketEnded = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var server = FakeCeleriantServer.Start(
            async (session, _, _) =>
            {
                await session.SendRawAsync(WatchAck);
                await kill.Task;
                session.Close();
            },
            _ => socketEnded.TrySetResult());

        await using var watch = await ConnectAsync(server.Address, maxShardHint: 1);
        Assert.Equal(1, server.ConnectionsAccepted);

        var next = watch.NextAsync(CancellationToken.None);
        kill.SetResult();
        await AwaitCompletionAsync(
            next,
            "NextAsync on a one-shard fan-out whose only socket died — with no sibling reader to "
            + "close the channel, a caller left waiting here is subscribed, blind and never told");

        var failure = await Record.ExceptionAsync(() => next);
        Assert.True(failure is CeleriantClientException, $"expected a client error, got {Describe(failure)}");
        await AwaitCompletionAsync(socketEnded.Task, "the only shard socket being released by the poison");
    }

    // =====================================================================
    // E. Address
    // =====================================================================

    /// <summary>
    /// Address is read exactly when the subscription is over — after a failure, when the caller is
    /// deciding whether the node it lands on next is the same one. A value that stops being
    /// readable at the moment it is needed answers the question nobody asks.
    /// </summary>
    [Fact]
    public async Task Address_SurvivesThePoisonAndTheDisposal()
    {
        const int shardCount = 3;
        var kill = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            await session.SendRawAsync(WatchAck);
            if (ShardOf(body) is not 1)
                return;

            await kill.Task;
            session.Close();
        });

        var watch = await ConnectAsync(server.Address, maxShardHint: shardCount);
        var next = watch.NextAsync(CancellationToken.None);
        kill.SetResult();
        await AwaitCompletionAsync(next, "NextAsync surfacing the poison");
        await Record.ExceptionAsync(() => next);

        Assert.Equal(server.Address, watch.Address);
        await watch.DisposeAsync();
        Assert.Equal(server.Address, watch.Address);
    }

    /// <summary>
    /// The 9001 fallback tears the probe down and redials the same node once per shard. The address
    /// has to describe the node the surviving subscription is attached to, not the connection that
    /// was thrown away.
    /// </summary>
    [Fact]
    public async Task Address_OnThe9001FallbackPath_NamesTheNodeTheShardsLandedOn()
    {
        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            var request = WireCodec.Deserialize<WatchRequest>(body);
            if (request.ShardId is null)
            {
                await session.SendFrameAsync(
                    MessageTypes.Responses.GenericError,
                    FakeServerProtocol.ErrorFrame(
                        ErrorResponse.ShardRoutingMultipleShards,
                        "watch filter spans shards {\"num_shards\":3}",
                        request.CorrelationId));
                return;
            }

            await session.SendRawAsync(WatchAck);
        });

        await using var watch = await ConnectAsync(server.Address, maxShardHint: null);

        Assert.Equal(server.Address, watch.Address);
        Assert.Equal(4, server.ConnectionsAccepted);
    }

    /// <summary>Address is read from other threads while the subscription is live; it must not tear.</summary>
    [Fact]
    public async Task Address_ReadConcurrentlyWithNextAsync_IsAlwaysTheSameValue()
    {
        await using var server = FakeCeleriantServer.Start((session, _, _) => session.SendRawAsync(WatchAck));
        await using var watch = await ConnectAsync(server.Address, maxShardHint: 3);

        using var stop = new CancellationTokenSource();
        var readers = Enumerable.Range(0, 4).Select(_ => Task.Run(() =>
        {
            while (!stop.IsCancellationRequested)
                Assert.Equal(server.Address, watch.Address);
        })).ToArray();

        Assert.Null(await watch.NextAsync(TimeSpan.FromMilliseconds(200)));
        await stop.CancelAsync();
        await Task.WhenAll(readers);
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

    private static Task<WatchConnection> ConnectAsync(string address, long? maxShardHint)
        => WatchConnection.ConnectAsync(
            address,
            new WatchRequest(),
            new WatchOptions
            {
                ConnectionTimeout = TimeSpan.FromSeconds(5),
                MaxShardHint = maxShardHint,
            },
            CancellationToken.None);

    /// <summary>The shard a received watch request subscribes to, or null for the unsharded probe.</summary>
    private static long? ShardOf(byte[] requestBody)
        => WireCodec.Deserialize<WatchRequest>(requestBody).ShardId;

    private static string Describe(Exception? failure)
        => failure is null ? "no exception at all" : failure.GetType().Name;

    // =====================================================================
    // helpers
    // =====================================================================

    /// <summary>
    /// A "host:port" nothing is listening on: bound to claim the port, then released, so the dial
    /// is refused immediately instead of waiting out a TCP timeout.
    /// </summary>
    private static string ReservedAddressThatRefusesConnections()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        int port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return $"127.0.0.1:{port}";
    }

    /// <summary>
    /// A socket the client leaks is still closed eventually — the finalizer gets to it — so a plain
    /// "did the server see the socket go" assertion goes green on a leak given enough time and a
    /// garbage collection. This waits on a budget too short for that and then refuses to conclude
    /// anything if a gen-0 collection happened inside the window: a pass here means the CLIENT
    /// closed the socket, not the GC.
    /// </summary>
    private static async Task AssertReleasedByTheClientAsync(
        Task everySocketEnded,
        int gcBefore,
        Func<string> whatLeaked)
    {
        var finished = await Task.WhenAny(everySocketEnded, Task.Delay(TimeSpan.FromMilliseconds(750)));
        bool collected = GC.CollectionCount(0) != gcBefore;

        Assert.True(
            ReferenceEquals(finished, everySocketEnded),
            $"{whatLeaked()} did not happen: the connect failed and left sockets open that nothing "
            + "owns. Each shard socket's only owner is the reader task that never started, so a "
            + "connect that fails after subscribing leaks one socket per subscribed shard");

        // Inconclusive, not wrong. Gen-0 counts are process-wide and xUnit runs test classes in
        // parallel, so an unrelated test allocating is enough to land a collection inside this
        // window — failing here would make an honest pass indistinguishable from someone else's
        // garbage. A skip keeps the run truthful about what was and was not established.
        Skip.If(
            collected,
            $"INCONCLUSIVE: {whatLeaked()} was observed, but a garbage collection ran inside the "
            + "window, so a finalizer could have closed the sockets instead of the client. Re-run.");
    }

    /// <summary>Fail loudly on a hang instead of deadlocking the run.</summary>
    private static async Task AwaitCompletionAsync(Task task, string whatWouldHang)
    {
        var finished = await Task.WhenAny(task, Task.Delay(HangBudget));
        Assert.True(
            ReferenceEquals(finished, task),
            $"{whatWouldHang} was still running after {HangBudget.TotalSeconds:0}s: it hangs where "
            + "the contract requires it to complete");
    }
}
