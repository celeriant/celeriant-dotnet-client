using System.Collections.Concurrent;
using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Watch;
using Celeriant.Transport;

namespace Celeriant.Client.Tests;

/// <summary>
/// White-box pins for two rules shared with the Rust client.
///
/// <para>
/// <b>Connection retirement</b> — <see cref="NodeConnectionPool"/> retires a connection only when
/// the error left the stream desynchronised (<c>LeavesConnectionDirty</c>: connection, timeout,
/// protocol). A server-decoded application error arrives as a complete response frame, so the
/// exchange finished and the socket is clean: it has to be reused. The OCC retry loop is the case
/// that makes retiring expensive — one discarded-and-redialled connection per attempt.
/// </para>
///
/// <para>
/// <b>Shard-range rejection</b> — all three shard-range rejections in
/// <c>WatchConnection.ConnectMultiShardAsync</c> pick their exception by ORIGIN: a bound the
/// server reported through the 9001 <c>num_shards</c> fallback is a runtime condition
/// (<see cref="ProtocolException"/>); a bound the caller supplied in
/// <see cref="WatchOptions.MaxShardHint"/> is a programmer error
/// (<see cref="ArgumentOutOfRangeException"/> naming <c>MaxShardHint</c>).
/// </para>
///
/// <para>
/// Every await that could stall runs under <see cref="HangBudget"/>: a hang reports nothing, a
/// failed assertion reports the defect.
/// </para>
/// </summary>
public class ConnectionRetirementAndShardRangeTests
{
    private static readonly TimeSpan HangBudget = TimeSpan.FromSeconds(10);

    private static readonly byte[] WatchAck =
        FakeServerSession.BuildFrame(MessageTypes.Responses.Watch, WireCodec.Serialize(new WatchResponse()));

    // =====================================================================
    // Retire only on dirty-stream errors
    // =====================================================================

    /// <summary>
    /// The contrast to an OCC conflict, which keeps its connection
    /// (<see cref="GuardedWriteConflictTests"/>). A reply carrying another request's correlation id
    /// means this stream is a reply behind: <c>CeleriantClient.VerifyCorrelation</c> raises a
    /// <see cref="ProtocolException"/>, which <c>LeavesConnectionDirty</c> keeps in the retire set.
    /// The next request must not be served on that socket.
    ///
    /// <para>
    /// Note on what this can and cannot prove: the transport also poisons the connection on this
    /// path, and <c>ConnectionPool.ReturnConnectionAsync</c> discards a poisoned connection on its
    /// own. So the new socket is over-determined and this test cannot isolate the
    /// <c>MarkBroken</c> call — every dirty-stream error in the client also poisons. What it does
    /// pin is the half of the rule that the OCC test cannot: narrowing the retire set must
    /// not have let a genuinely desynchronised connection back into the pool.
    /// </para>
    /// </summary>
    [Fact]
    public async Task CorrelationMismatch_LeavesTheStreamDirty_SoTheNextRequestGetsANewConnection()
    {
        var servedOn = new ConcurrentQueue<int>();

        // Every reply is well-formed and carries a correlation id belonging to nobody.
        await using var server = FakeCeleriantServer.Start((session, _, _) =>
        {
            servedOn.Enqueue(session.ConnectionId);
            return session.SendFrameAsync(
                MessageTypes.Responses.AggregateDetails,
                WireCodec.Serialize(new AggregateDetailsResponse
                {
                    CorrelationId = Guid.NewGuid(),
                    MaxAggregateVersion = 1,
                }));
        });

        await using var pool = SingleConnectionNodePool(server);

        var first = await FailureWithinBudgetAsync(
            () => pool.ExecuteRequestAsync(Details(), CancellationToken.None),
            "the request answered with a foreign correlation id");
        Assert.IsType<ProtocolException>(first);
        Assert.Equal(1, server.ConnectionsAccepted);

        var second = await FailureWithinBudgetAsync(
            () => pool.ExecuteRequestAsync(Details(), CancellationToken.None),
            "the request after a correlation mismatch");
        Assert.IsType<ProtocolException>(second);

        Assert.True(
            server.ConnectionsAccepted == 2,
            "the first reply belonged to another request, so that stream is one frame behind and "
            + $"everything read from it afterwards would be too. The pool accepted "
            + $"{server.ConnectionsAccepted} connections, meaning the desynchronised one was handed "
            + "straight back out");
        Assert.True(
            servedOn.Distinct().Count() == 2,
            $"both requests were served on connections [{string.Join(", ", servedOn)}]: the poisoned "
            + "connection was reused");
    }

    /// <summary>
    /// <c>NotLeader</c> is a server-decoded error: the node answered, completely, with a redirect.
    /// The connection to it is fine — the pool's routing layer is what moves on — so retiring it
    /// throws away a working socket to a node this client will keep talking to for reads.
    /// </summary>
    [Fact]
    public async Task NotLeaderError_IsServerDecoded_SoTheConnectionIsKept()
        => await AssertServerDecodedErrorKeepsTheConnectionAsync(
            ErrorResponse.WriteNotLeader,
            "{\"leader_address\":\"127.0.0.1:9999\"}",
            failure =>
            {
                var notLeader = Assert.IsType<NotLeaderException>(failure);
                Assert.Equal("127.0.0.1:9999", notLeader.LeaderAddress);
            });

    /// <summary>
    /// <c>ServerBusy</c> is the same shape and the worst one to retire on: it is the server asking
    /// for a retry, and the retry then pays for a fresh connection to a node that just said it is
    /// short of capacity.
    /// </summary>
    [Fact]
    public async Task ServerBusyError_IsServerDecoded_SoTheConnectionIsKept()
        => await AssertServerDecodedErrorKeepsTheConnectionAsync(
            ErrorResponse.ServerBusy,
            "{}",
            failure => Assert.IsType<ServerBusyException>(failure));

    // =====================================================================
    // Shard-range rejections blame the origin of the bound
    // =====================================================================
    //
    // Already covered elsewhere, and deliberately not repeated here:
    //   * caller MaxShardHint past the 1024 MaxShards bound -> ArgumentOutOfRangeException with
    //     ParamName MaxShardHint and zero sockets opened, in
    //     WatchAddressEdgeCaseTests.MultiShard_AShardCountPastTheClientBound_OpensNoSocketsAtAll.
    //   * caller MaxShardHint below StartShard (the inverted range) -> ArgumentOutOfRangeException,
    //     in WatchConnectEdgeCaseTests.RejectedRange_IsAnArgumentErrorFromTheCallerAndAClientErrorFromTheServer.
    //
    // The server-origin twins below ARE repeated in shape but not in strength: the existing tests
    // assert only the CeleriantClientException base, which a ShardRoutingException or a
    // WatchErrorException would also satisfy. The implementation names ProtocolException
    // specifically, so that is what these assert.

    /// <summary>
    /// Site 1, caller origin, with the default <c>StartShard</c>: <c>MaxShardHint = 0</c> is the
    /// natural way to write this bug (a shard count that has not been filled in yet), and it names
    /// the empty range [0, 0). The blamed parameter must be <c>MaxShardHint</c>: blaming
    /// <c>StartShard</c> sends the reader to the one value in the pair they did not set.
    /// </summary>
    [Fact]
    public async Task EmptyRangeFromTheCaller_BlamesMaxShardHint_AndOpensNoSockets()
    {
        await using var server = FakeCeleriantServer.Start((session, _, _) => session.SendRawAsync(WatchAck));

        var failure = await ConnectFailureAsync(server, new WatchOptions
        {
            ConnectionTimeout = HangBudget,
            MaxShardHint = 0,
        });

        var argument = Assert.IsType<ArgumentOutOfRangeException>(failure);
        Assert.Equal(nameof(WatchOptions.MaxShardHint), argument.ParamName);
        Assert.Equal(0L, Assert.IsType<long>(argument.ActualValue));
        Assert.Equal(0, server.ConnectionsAccepted);
    }

    /// <summary>
    /// Site 1, server origin: the same empty range, with the upper bound reported by the server's
    /// <c>num_shards</c>. The caller's options are fine — the same <c>StartShard</c> works against
    /// a larger cluster — so this belongs inside the client's hierarchy where a connect-time catch
    /// can see it, and specifically as a protocol error: the server named a shard count that
    /// contradicts the request it just answered.
    /// </summary>
    [Fact]
    public async Task EmptyRangeFromTheServer_IsAProtocolException()
    {
        await using var server = FakeCeleriantServer.Start(ShardRoutingFallback(numShards: "3"));

        var failure = await ConnectFailureAsync(server, new WatchOptions
        {
            ConnectionTimeout = HangBudget,
            StartShard = 6,
        });

        Assert.IsType<ProtocolException>(failure);
        Assert.Contains("The server reported 3 shards", failure!.Message, StringComparison.Ordinal);
        Assert.Equal(1, server.ConnectionsAccepted);
    }

    /// <summary>
    /// Site 2, caller origin: a hint large enough to overflow <c>checked((int)(numShards -
    /// startShard))</c>. Blaming the server here would tell a caller passing a garbage
    /// <c>long</c> that their own cluster had misbehaved.
    /// </summary>
    [Fact]
    public async Task OverflowingHintFromTheCaller_BlamesMaxShardHint_AndOpensNoSockets()
    {
        await using var server = FakeCeleriantServer.Start((session, _, _) => session.SendRawAsync(WatchAck));

        var failure = await ConnectFailureAsync(server, new WatchOptions
        {
            ConnectionTimeout = HangBudget,
            MaxShardHint = long.MaxValue,
        });

        var argument = Assert.IsType<ArgumentOutOfRangeException>(failure);
        Assert.Equal(nameof(WatchOptions.MaxShardHint), argument.ParamName);
        Assert.Equal(long.MaxValue, Assert.IsType<long>(argument.ActualValue));
        Assert.False(
            failure is CeleriantClientException,
            "the caller's own constant cannot be fixed by a retry or a failover, so it must stay "
            + "outside the client's hierarchy rather than being swallowed by a catch written for a "
            + "node dying");
        Assert.Equal(0, server.ConnectionsAccepted);
    }

    /// <summary>
    /// Site 2, server origin: a <c>num_shards</c> past <see cref="int"/> range. The
    /// <see cref="OverflowException"/> is kept as the inner exception, because without it the
    /// message alone does not say which of the three rejections fired.
    /// </summary>
    [Fact]
    public async Task OverflowingNumShardsFromTheServer_IsAProtocolException()
    {
        await using var server = FakeCeleriantServer.Start(
            ShardRoutingFallback(numShards: long.MaxValue.ToString()));

        var failure = await ConnectFailureAsync(server, new WatchOptions { ConnectionTimeout = HangBudget });

        var protocolError = Assert.IsType<ProtocolException>(failure);
        Assert.Contains($"The server reported {long.MaxValue} shards", protocolError.Message, StringComparison.Ordinal);
        Assert.IsType<OverflowException>(protocolError.InnerException);

        // Only the probe. The fan-out never starts, so nothing else is dialled.
        Assert.Equal(1, server.ConnectionsAccepted);
    }

    /// <summary>
    /// Site 3, server origin: a shard count inside <see cref="int"/> range but past the 1024 the
    /// client will open connections for. Its caller-origin twin is already covered (see the note
    /// above this region); this one pins the specific type, where the existing test asserts only
    /// the base class.
    /// </summary>
    [Fact]
    public async Task ShardCountPastTheClientBoundFromTheServer_IsAProtocolException()
    {
        await using var server = FakeCeleriantServer.Start(ShardRoutingFallback(numShards: "5000"));

        var failure = await ConnectFailureAsync(server, new WatchOptions { ConnectionTimeout = HangBudget });

        Assert.IsType<ProtocolException>(failure);
        Assert.Contains("The server reported 5000 shards", failure!.Message, StringComparison.Ordinal);
        Assert.Contains("1024", failure.Message, StringComparison.Ordinal);
        Assert.Equal(1, server.ConnectionsAccepted);
    }

    /// <summary>
    /// The three sites, read as one rule rather than three: for a given bound, whether it came
    /// from the caller or from the server is the only thing that changes, and it changes the
    /// exception every time. One server, one bound value per site, both origins.
    /// </summary>
    [Theory]
    [InlineData(3L, 6L)]              // site 1: empty range
    [InlineData(long.MaxValue, 0L)]   // site 2: the checked cast overflows
    [InlineData(5000L, 0L)]           // site 3: past MaxShards
    public async Task EveryShardRangeRejection_PicksItsExceptionByOrigin(long bound, long startShard)
    {
        await using var server = FakeCeleriantServer.Start(
            ShardRoutingFallback(numShards: bound.ToString()));

        // Site 1's server-origin form needs a StartShard above the reported count; the fallback
        // path takes StartShard from the same options, so both origins use the same value.
        var fromCaller = await ConnectFailureAsync(server, new WatchOptions
        {
            ConnectionTimeout = HangBudget,
            StartShard = startShard,
            MaxShardHint = bound,
        });

        var argument = Assert.IsType<ArgumentOutOfRangeException>(fromCaller);
        Assert.Equal(nameof(WatchOptions.MaxShardHint), argument.ParamName);

        var fromServer = await ConnectFailureAsync(server, new WatchOptions
        {
            ConnectionTimeout = HangBudget,
            StartShard = startShard,
        });

        Assert.IsType<ProtocolException>(fromServer);
    }

    // =====================================================================
    // helpers
    // =====================================================================

    /// <summary>
    /// A node pool capped at one connection, so reuse and re-dialling are distinguishable by the
    /// server's accepted-connection count alone. No identity config: the factory then skips the
    /// Identify handshake, and every socket the server sees is one this test asked for.
    /// </summary>
    private static NodeConnectionPool SingleConnectionNodePool(FakeCeleriantServer server)
        => new(
            server.Address,
            new CeleriantPoolOptions
            {
                Address = server.Address,
                MaxConnections = 1,
                ConnectionTimeout = TimeSpan.FromSeconds(5),
                RequestTimeout = TimeSpan.FromSeconds(10),
            },
            new DictCache());

    /// <summary>
    /// The correlation id every request in the retirement tests carries. Set by the caller rather than
    /// filled in by the transport, so the fake server can echo it without decoding a body that may
    /// have been dictionary-compressed.
    /// </summary>
    private static readonly Guid FixedCorrelationId = Guid.Parse("00000000-0000-0000-0000-0000000000c1");

    private static ClientRequest OccWrite()
        => new ClientRequest.Write(new WriteRequest
        {
            CorrelationId = FixedCorrelationId,
            ClientId = Guid.NewGuid(),
            Writes = new Dictionary<AggregateKey, SingleAggregateWrite>
            {
                [FakeServerProtocol.NewKey()] = new SingleAggregateWrite
                {
                    Events = [new AggregateEvent { EventTimestamp = DateTimeOffset.UtcNow, EventTypeMajor = 1, EventValue = "x"u8.ToArray() }],
                    ExpectedVersion = 1,
                },
            },
        });

    private static ClientRequest Details()
        => new ClientRequest.AggregateDetails(
            FakeServerProtocol.Details(FakeServerProtocol.NewKey(), FixedCorrelationId));

    /// <summary>
    /// Answer every request with <paramref name="errorCode"/>, then assert that four attempts all
    /// shared one connection: a complete error frame leaves the stream at a frame boundary.
    /// </summary>
    private static async Task AssertServerDecodedErrorKeepsTheConnectionAsync(
        uint errorCode,
        string errorMessage,
        Action<Exception?> assertFailure)
    {
        const int attempts = 4;

        await using var server = FakeCeleriantServer.Start((session, _, _) =>
            session.SendFrameAsync(
                MessageTypes.Responses.GenericError,
                FakeServerProtocol.ErrorFrame(errorCode, errorMessage, FixedCorrelationId)));

        await using var pool = SingleConnectionNodePool(server);

        for (int attempt = 0; attempt < attempts; attempt++)
        {
            var failure = await FailureWithinBudgetAsync(
                () => pool.ExecuteRequestAsync(OccWrite(), CancellationToken.None),
                $"request {attempt + 1} answered with error {errorCode}");

            assertFailure(failure);
        }

        Assert.True(
            server.ConnectionsAccepted == 1,
            $"error {errorCode} arrived as a complete response frame {attempts} times and the pool "
            + $"opened {server.ConnectionsAccepted} connections. The server decoded the request and "
            + "answered it: nothing about that stream is desynchronised");
    }

    /// <summary>
    /// A server that answers the unsharded probe with a 9001 naming <paramref name="numShards"/>,
    /// which is how a shard count reaches <c>ConnectMultiShardAsync</c> with a SERVER origin. Any
    /// sharded request is acked normally, so a fan-out that should not have started is visible as
    /// extra accepted connections rather than as a hang.
    /// </summary>
    private static FakeCeleriantServer.RequestHandler ShardRoutingFallback(string numShards)
        => async (session, _, body) =>
        {
            var request = WireCodec.Deserialize<WatchRequest>(body);
            if (request.ShardId is null)
            {
                await session.SendFrameAsync(
                    MessageTypes.Responses.GenericError,
                    FakeServerProtocol.ErrorFrame(
                        ErrorResponse.ShardRoutingMultipleShards,
                        $"watch filter spans shards {{\"num_shards\":{numShards}}}",
                        request.CorrelationId));
                return;
            }

            await session.SendRawAsync(WatchAck);
        };

    /// <summary>
    /// Connect and return whatever it failed with, under the hang budget. The connect runs on the
    /// pool because a rejected fan-out that regresses is a fan-out: an unbudgeted await here stops
    /// the run producing output at all, and a hang reports nothing.
    /// </summary>
    private static async Task<Exception?> ConnectFailureAsync(FakeCeleriantServer server, WatchOptions options)
    {
        var connect = Task.Run(() => WatchConnection.ConnectAsync(server.Address, new WatchRequest(), options));

        // Observe whatever the task carries even if the budget below expires, and close a
        // connection this was not supposed to get.
        _ = connect.ContinueWith(
            t => { _ = t.Exception; if (t.Status == TaskStatus.RanToCompletion) _ = t.Result.DisposeAsync(); },
            TaskScheduler.Default);

        await AwaitCompletionAsync(connect, $"a connect with MaxShardHint {options.MaxShardHint?.ToString() ?? "unset"}");

        var failure = await Record.ExceptionAsync(() => connect);
        Assert.True(failure is not null, "the shard range names no subscribable set of shards, so the "
            + "connect must fail rather than hand back a watch bound to something else");
        return failure;
    }

    /// <summary>Run <paramref name="action"/> under the hang budget and return what it threw.</summary>
    private static async Task<Exception?> FailureWithinBudgetAsync(Func<Task> action, string whatWouldHang)
    {
        var running = Task.Run(action);
        await AwaitCompletionAsync(running, whatWouldHang);

        var failure = await Record.ExceptionAsync(() => running);
        Assert.True(failure is not null, $"{whatWouldHang} was expected to fail and did not");
        return failure;
    }

    private static async Task AwaitCompletionAsync(Task task, string whatWouldHang)
    {
        var finished = await Task.WhenAny(task, Task.Delay(HangBudget));
        Assert.True(
            ReferenceEquals(finished, task),
            $"{whatWouldHang} was still pending after {HangBudget.TotalSeconds:0}s");
    }
}
