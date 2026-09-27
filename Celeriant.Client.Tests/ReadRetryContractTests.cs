using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;

namespace Celeriant.Client.Tests;

/// <summary>
/// A read changes nothing on the server, so a read whose reply was lost after it was sent is safe to
/// send again. The pool re-sends it once on another connection, including when the node that lost it
/// is the only one it knows. Writes keep the opposite rule (see <see cref="PostSendContractTests"/>).
/// </summary>
public class ReadRetryContractTests
{
    /// <summary>
    /// One node. It reads the first copy of the read and closes the socket without answering, then
    /// answers the second copy. The caller gets the answer and never sees the lost reply.
    /// </summary>
    [Fact]
    public async Task AReadLostAfterItWasSent_IsSentAgain_AndTheCallerGetsTheAnswer()
    {
        int reads = 0;
        await using var node = FakeCeleriantServer.Start(async (session, messageType, body) =>
        {
            Assert.Equal(MessageTypes.Requests.Read, messageType);
            if (Interlocked.Increment(ref reads) == 1)
            {
                session.Close();
                return;
            }
            var request = WireCodec.Deserialize<ReadRequest>(body);
            await session.SendFrameAsync(MessageTypes.Responses.Read, Page(request, [1], next: null));
        });
        await using var pool = new CeleriantPool(LeaderRoutingFakes.Options(node.Address));

        var response = await pool.ReadAsync(NewRead(from: 1));

        Assert.Equal([1L], response.EventBatches.Select(b => b.AggregateVersion));
        Assert.True(reads == 2, $"the node received the read {reads} times; expected the lost copy plus one re-send");
    }

    /// <summary>
    /// The re-send is bounded. A node that loses every reply is reported as an unknown outcome after
    /// the second copy, not retried until the caller's patience runs out.
    /// </summary>
    [Fact]
    public async Task AReadThatKeepsLosingItsReply_IsSentTwice_ThenReportedAsUnknown()
    {
        int reads = 0;
        await using var node = FakeCeleriantServer.Start((session, _, _) =>
        {
            Interlocked.Increment(ref reads);
            session.Close();
            return Task.CompletedTask;
        });
        await using var pool = new CeleriantPool(LeaderRoutingFakes.Options(node.Address));

        var failure = await LeaderRoutingFakes.FailureAsync(
            () => pool.ReadAsync(NewRead(from: 1)),
            "a read whose node closes the socket after every request");

        Assert.IsType<RequestOutcomeUnknownException>(failure);
        Assert.True(reads == 2, $"the node received the read {reads} times; expected exactly 2");
    }

    /// <summary>
    /// Two nodes that lose every reply. The walk moves past the first after one send, and only the
    /// last, with nowhere left to go, gets the re-send.
    /// </summary>
    [Fact]
    public async Task OnlyTheLastCandidateGetsTheResend()
    {
        int firstReads = 0, lastReads = 0;
        await using var first = FakeCeleriantServer.Start((session, _, _) =>
        {
            Interlocked.Increment(ref firstReads);
            session.Close();
            return Task.CompletedTask;
        });
        await using var last = FakeCeleriantServer.Start((session, _, _) =>
        {
            Interlocked.Increment(ref lastReads);
            session.Close();
            return Task.CompletedTask;
        });
        await using var pool = new CeleriantPool(LeaderRoutingFakes.Options(first.Address, last.Address));

        var failure = await LeaderRoutingFakes.FailureAsync(
            () => pool.ReadAsync(NewRead(from: 1)),
            "a read whose every node closes the socket after each request");

        Assert.IsType<RequestOutcomeUnknownException>(failure);
        Assert.True((firstReads, lastReads) == (1, 2),
            $"first node read {firstReads} times, last {lastReads}; expected the first once and the last twice");
    }

    /// <summary>
    /// A streaming read whose second page loses every reply. The page is asked for exactly twice, the
    /// batches of the first page still arrive, and the stream then ends with the unknown outcome.
    /// </summary>
    [Fact]
    public async Task AStreamingReadWhosePageKeepsLosingItsReply_AsksTwice_ThenFails()
    {
        var requestedFrom = new List<long>();
        await using var node = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            var request = WireCodec.Deserialize<ReadRequest>(body);
            long from = request.Filters.FromAggregateVersion;
            lock (requestedFrom) requestedFrom.Add(from);
            if (from == 1)
                await session.SendFrameAsync(MessageTypes.Responses.Read, Page(request, [1, 2], next: 3));
            else
                session.Close();
        });
        await using var pool = new CeleriantPool(LeaderRoutingFakes.Options(node.Address));

        var versions = new List<long>();
        var failure = await LeaderRoutingFakes.FailureAsync(async () =>
        {
            await foreach (var batch in pool.ReadAllAsync(FakeServerProtocol.NewKey(), ReadFilters.From(1)))
                versions.Add(batch.AggregateVersion);
        }, "a stream whose second page is always lost");

        Assert.IsType<RequestOutcomeUnknownException>(failure);
        Assert.Equal([1L, 2], versions);
        Assert.Equal([1L, 3, 3], requestedFrom);
    }

    /// <summary>
    /// A streaming read loses its second page. The stream resumes from that page's cursor on another
    /// connection, so every batch arrives exactly once and in order.
    /// </summary>
    [Fact]
    public async Task AStreamingReadThatLosesAPage_ResumesFromThatPage_WithNoBatchRepeatedOrSkipped()
    {
        var requestedFrom = new List<long>();
        int secondPageAttempts = 0;
        await using var node = FakeCeleriantServer.Start(async (session, messageType, body) =>
        {
            Assert.Equal(MessageTypes.Requests.Read, messageType);
            var request = WireCodec.Deserialize<ReadRequest>(body);
            long from = request.Filters.FromAggregateVersion;
            lock (requestedFrom) requestedFrom.Add(from);
            switch (from)
            {
                case 1:
                    await session.SendFrameAsync(MessageTypes.Responses.Read, Page(request, [1, 2], next: 3));
                    return;
                case 3 when Interlocked.Increment(ref secondPageAttempts) == 1:
                    session.Close();
                    return;
                case 3:
                    await session.SendFrameAsync(MessageTypes.Responses.Read, Page(request, [3, 4], next: null));
                    return;
                default:
                    throw new InvalidOperationException($"unexpected page cursor {from}");
            }
        });
        await using var pool = new CeleriantPool(LeaderRoutingFakes.Options(node.Address));

        var versions = new List<long>();
        await foreach (var batch in pool.ReadAllAsync(FakeServerProtocol.NewKey(), ReadFilters.From(1)))
            versions.Add(batch.AggregateVersion);

        Assert.Equal([1L, 2, 3, 4], versions);
        Assert.Equal([1L, 3, 3], requestedFrom);
    }

    /// <summary>
    /// With follower routing the pool rotates across nodes, but a stream stays on the node that
    /// served its first page: a cursor sent to a replica that is further behind ends the stream
    /// early instead of failing. The lost page is re-sent to that same node.
    /// </summary>
    [Fact]
    public async Task AStreamingReadThatLosesAPage_ResendsItToTheSameNode()
    {
        var served = new List<(string Node, long From)>();
        int lostPages = 0;
        FakeCeleriantServer.RequestHandler Node(string name) => async (session, _, body) =>
        {
            var request = WireCodec.Deserialize<ReadRequest>(body);
            long from = request.Filters.FromAggregateVersion;
            lock (served) served.Add((name, from));
            if (from == 3 && Interlocked.Increment(ref lostPages) == 1)
            {
                session.Close();
                return;
            }
            await session.SendFrameAsync(MessageTypes.Responses.Read,
                from == 1 ? Page(request, [1, 2], next: 3) : Page(request, [3, 4], next: null));
        };
        // The primary is the presumed leader, so two seeds are the followers the pool rotates across.
        await using var leader = FakeCeleriantServer.Start(Node("leader"));
        await using var b = FakeCeleriantServer.Start(Node("b"));
        await using var c = FakeCeleriantServer.Start(Node("c"));
        await using var pool = new CeleriantPool(new CeleriantPoolOptions
        {
            Address = leader.Address,
            SeedAddresses = [b.Address, c.Address],
            RouteReadsToFollowers = true,
            MaxConnections = 1,
            ConnectionTimeout = TimeSpan.FromSeconds(1),
            RequestTimeout = TimeSpan.FromSeconds(2),
        });

        var versions = new List<long>();
        await foreach (var batch in pool.ReadAllAsync(FakeServerProtocol.NewKey(), ReadFilters.From(1)))
            versions.Add(batch.AggregateVersion);

        Assert.Equal([1L, 2, 3, 4], versions);
        Assert.True(served.Select(s => s.Node).Distinct().Count() == 1,
            $"one stream was served by more than one node: {string.Join(", ", served.Select(s => $"{s.Node}@{s.From}"))}");
    }

    private static ReadRequest NewRead(long from) => new()
    {
        AggregateKey = FakeServerProtocol.NewKey(),
        Filters = ReadFilters.From(from),
    };

    private static byte[] Page(ReadRequest request, long[] versions, long? next)
        => WireCodec.Serialize(new ReadResponse
        {
            CorrelationId = request.CorrelationId,
            EventBatches = versions.Select(v => new AggregateEventBatch
            {
                AggregateVersion = v,
                ServerTimestamp = DateTimeOffset.UnixEpoch,
            }).ToArray(),
            NextAggregateVersion = next,
        });
}
