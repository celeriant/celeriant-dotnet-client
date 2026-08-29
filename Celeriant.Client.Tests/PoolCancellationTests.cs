using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;

namespace Celeriant.Client.Tests;

/// <summary>
/// A connection whose request/response exchange did not complete must never be reused.
///
/// <para>
/// Cancellation is driven by a fake server that reads the request frame and then deliberately
/// refuses to answer, so the client is provably parked on the response read when the token fires.
/// No wall-clock race against a real reply.
/// </para>
/// </summary>
public class PoolCancellationTests
{
    private sealed record Arrival(int Connection, Guid Aggregate);

    private sealed record Crosstalk(
        IReadOnlyList<Arrival> Arrivals,
        AggregateKey NextKey,
        AggregateDetailsResponse NextAnswer);

    [Fact]
    public async Task CallerCancelledRequest_DoesNotLeakItsResponseToTheNextRequest()
    {
        var result = await RunCancelledThenNextAsync(truncateResponseHeader: false);

        Assert.Equal(result.NextKey.AggregateId, result.NextAnswer.LastClientId);
    }

    [Fact]
    public async Task CallerCancelledRequest_ConnectionIsNotReusedByTheNextRequest()
    {
        var result = await RunCancelledThenNextAsync(truncateResponseHeader: false);

        Assert.Equal(3, result.Arrivals.Count);
        Assert.NotEqual(result.Arrivals[1].Connection, result.Arrivals[2].Connection);
    }

    [Fact]
    public async Task CancellationInsideTheFrameRead_DoesNotLeakThePartialFrameToTheNextRequest()
    {
        var result = await RunCancelledThenNextAsync(truncateResponseHeader: true);

        Assert.Equal(result.NextKey.AggregateId, result.NextAnswer.LastClientId);
    }

    /// <summary>
    /// A lease discarded as broken must wake a caller parked at the connection cap; releasing the
    /// semaphore without writing to the idle channel leaves that caller asleep for ever.
    /// </summary>
    [Fact]
    public async Task PoolWaiterAtTheCap_IsWokenWhenACancelledLeaseIsDiscarded()
    {
        var requestOne = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int seen = 0;

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            var request = FakeServerProtocol.DecodeDetails(body);
            byte[] answer = FakeServerProtocol.DetailsAnswer(request);

            // Request 1 warms the pool. Request 2 is parked and then cancelled.
            if (Interlocked.Increment(ref seen) != 2)
            {
                await session.SendFrameAsync(MessageTypes.Responses.AggregateDetails, answer);
                return;
            }

            requestOne.SetResult();
            await release.Task;
            await session.SendFrameAsync(MessageTypes.Responses.AggregateDetails, answer);
        });

        await using var pool = new CeleriantPool(FakeServerProtocol.SingleConnectionPool(server));

        // Warm: one connection exists and the per-node cap (1) is fully consumed.
        await pool.AggregateDetailsAsync(FakeServerProtocol.Details(FakeServerProtocol.NewKey()));

        using var cts = new CancellationTokenSource();
        var parkedOnRead = pool.AggregateDetailsAsync(
            FakeServerProtocol.Details(FakeServerProtocol.NewKey()), cts.Token);
        await requestOne.Task;

        // Second caller: pool is at the cap, so this parks inside GetConnectionAsync.
        var waiter = pool.AggregateDetailsAsync(FakeServerProtocol.Details(FakeServerProtocol.NewKey()));
        Assert.False(waiter.IsCompleted, "the waiter must be parked at the cap for this test to mean anything");

        await cts.CancelAsync();
        Assert.NotNull(await Record.ExceptionAsync(() => parkedOnRead));
        release.SetResult();

        var finished = await Task.WhenAny(waiter, Task.Delay(TimeSpan.FromSeconds(5)));
        Assert.True(
            ReferenceEquals(finished, waiter),
            "a caller waiting on the connection cap must be released when the lease ahead of it is "
            + "discarded; discarding returns a _totalSem permit but writes nothing to the idle "
            + "channel, so the waiter sleeps forever");
        await waiter;
    }

    /// <summary>
    /// A completed exchange must leave the connection reusable; losing the dirty-flag clear in
    /// <c>SendAsync</c> retires a healthy connection after every single request.
    /// </summary>
    [Fact]
    public async Task ACompletedExchange_LeavesTheSameConnectionInThePool()
    {
        var arrivals = new List<int>();
        await using var server = FakeCeleriantServer.Start((session, _, body) =>
        {
            var request = FakeServerProtocol.DecodeDetails(body);
            lock (arrivals)
                arrivals.Add(session.ConnectionId);
            return session.SendFrameAsync(
                MessageTypes.Responses.AggregateDetails, FakeServerProtocol.DetailsAnswer(request));
        });

        await using var pool = new CeleriantPool(FakeServerProtocol.SingleConnectionPool(server));

        for (int i = 0; i < 3; i++)
            await pool.AggregateDetailsAsync(FakeServerProtocol.Details(FakeServerProtocol.NewKey()));

        Assert.Equal(3, arrivals.Count);
        Assert.Equal(new[] { arrivals[0], arrivals[0], arrivals[0] }, arrivals);
        Assert.Equal(1, server.ConnectionsAccepted);
    }

    // -------------------------------------------------------------------------
    // Scenario
    // -------------------------------------------------------------------------

    /// <summary>
    /// Warm the pool, cancel a request that is parked on its response read, then issue a second
    /// request of the same type on a pool capped at one connection per node.
    ///
    /// <para>
    /// With <paramref name="truncateResponseHeader"/> the server first delivers 8 bytes of the
    /// 17-byte response header, so the cancellation lands part-way through a header rather than one
    /// whole frame behind. Whether the client has already consumed those 8 bytes or they are still
    /// in the socket when the token fires does not change the outcome: either way the exchange did
    /// not complete and the stream is mid-frame.
    /// </para>
    /// </summary>
    private static async Task<Crosstalk> RunCancelledThenNextAsync(bool truncateResponseHeader)
    {
        var arrivals = new List<Arrival>();
        var parked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int seen = 0;

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            var request = FakeServerProtocol.DecodeDetails(body);
            lock (arrivals)
                arrivals.Add(new Arrival(session.ConnectionId, request.AggregateKey.AggregateId));

            var frame = FakeServerSession.BuildFrame(
                MessageTypes.Responses.AggregateDetails,
                FakeServerProtocol.DetailsAnswer(request));

            // Request 1 warms the pool, request 2 is the one the test cancels.
            if (Interlocked.Increment(ref seen) != 2)
            {
                await session.SendRawAsync(frame);
                return;
            }

            if (truncateResponseHeader)
                await session.SendRawAsync(frame.AsMemory(0, 8));

            // The request frame is fully read, so the client's write has completed and it is
            // parked on the response read. Stay silent until the test says otherwise.
            parked.SetResult();
            await release.Task;
            await session.SendRawAsync(truncateResponseHeader ? frame.AsMemory(8) : frame);
        });

        await using var pool = new CeleriantPool(FakeServerProtocol.SingleConnectionPool(server));

        // Warm the pool, so the cancelled attempt starts at the write and not at connect.
        await pool.AggregateDetailsAsync(FakeServerProtocol.Details(FakeServerProtocol.NewKey()));

        using var cts = new CancellationTokenSource();
        var inflight = pool.AggregateDetailsAsync(
            FakeServerProtocol.Details(FakeServerProtocol.NewKey()), cts.Token);

        await parked.Task;
        await cts.CancelAsync();
        Assert.NotNull(await Record.ExceptionAsync(() => inflight));

        // Let the abandoned response land in the socket the pool may have taken back.
        release.SetResult();

        var nextKey = FakeServerProtocol.NewKey();
        var answer = await pool.AggregateDetailsAsync(FakeServerProtocol.Details(nextKey));

        return new Crosstalk(arrivals, nextKey, answer);
    }
}
