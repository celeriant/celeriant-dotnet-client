using System.Threading.Channels;
using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Watch;

namespace Celeriant.Client.Tests;

/// <summary>
/// Edge cases of the fail-closed watch contract. Every test here is a claim about the
/// invariant "a caller holding a live WatchConnection is either receiving every matching event or
/// being told it is not".
/// </summary>
public class WatchFailClosedEdgeCaseTests
{
    private static readonly TimeSpan HangBudget = TimeSpan.FromSeconds(5);

    private static readonly byte[] WatchAck =
        FakeServerSession.BuildFrame(MessageTypes.Responses.Watch, WireCodec.Serialize(new WatchResponse()));

    // =====================================================================
    // 1. The poison exception is one shared, mutable instance.
    // =====================================================================

    /// <summary>
    /// A poisoned multi-shard watch parks one exception object on the channel and
    /// <c>ExceptionDispatchInfo.Capture(...).Throw()</c> hands that same object to every later
    /// <c>NextAsync</c>. Rethrowing an instance restores and then re-appends its stack trace, so
    /// each read mutates the object the previous read already gave the caller. Two callers holding
    /// the same exception see it change under them, and a caller that logs it twice gets two
    /// different, ever-longer traces for one failure.
    ///
    /// The single-shard path re-derives the failure from the socket and hands back a fresh instance
    /// each time, so D3 unified the exception TYPE across the two paths while leaving their
    /// identity and mutability semantics different.
    /// </summary>
    [Fact]
    public async Task PoisonedMultiShardWatch_ASecondRead_DoesNotMutateTheFirstReadsException()
    {
        await using var watch = await ConnectThenDropEveryShardAsync(maxShardHint: 2);

        var first = await Record.ExceptionAsync(() => watch.NextAsync(CancellationToken.None));
        Assert.NotNull(first);
        int firstTraceLength = first.StackTrace?.Length ?? 0;

        var second = await Record.ExceptionAsync(() => watch.NextAsync(CancellationToken.None));

        // A poisoned watch keeps telling every reader — that is the fail-closed contract — and
        // re-throwing one shared instance appends a stack trace per throw. So the object the first
        // caller is still holding grows underneath it, driven by a second caller it never sees.
        Assert.NotNull(second);
        Assert.Equal(firstTraceLength, first.StackTrace?.Length ?? 0);
    }

    /// <summary>
    /// The consequence of the above for the caller the goal explicitly protects: one that polls
    /// <c>NextAsync(TimeSpan)</c> in a loop. Once the subscription is poisoned every iteration
    /// re-throws the same object and grows it, so the exception a long-lived poller holds grows
    /// without bound for as long as it keeps polling.
    /// </summary>
    [Fact]
    public async Task PoisonedMultiShardWatch_PolledInALoop_DoesNotGrowTheExceptionWithoutBound()
    {
        await using var watch = await ConnectThenDropEveryShardAsync(maxShardHint: 2);

        var lengths = new List<int>();
        for (int i = 0; i < 50; i++)
        {
            var failure = await Record.ExceptionAsync(async () =>
                await watch.NextAsync(TimeSpan.FromMilliseconds(50)));
            Assert.NotNull(failure);
            lengths.Add(failure!.StackTrace?.Length ?? 0);
        }

        Assert.True(
            lengths[^1] <= lengths[0] * 2,
            "a poisoned watch polled in a loop must not grow the exception it keeps re-throwing; "
            + $"stack trace went from {lengths[0]} to {lengths[^1]} characters over 50 polls "
            + $"(+{lengths[^1] - lengths[0]}, ~{(lengths[^1] - lengths[0]) / 49} per poll, unbounded)");
    }

    // =====================================================================
    // 2. Short-deadline polling must not drop events.
    // =====================================================================

    /// <summary>
    /// A caller polling on a short deadline abandons a channel read on every empty tick. If an
    /// abandoned read could race a concurrent shard write and swallow the item, the caller would be
    /// blind to that event and never told — the exact failure the watch contract forbids, and one
    /// that only shows up under a poll loop running for a long time.
    /// </summary>
    [Fact]
    public async Task MultiShard_PollingOnAShortDeadline_DropsNoEvents()
    {
        const int eventCount = 3000;

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            await session.SendRawAsync(WatchAck);
            if (ShardOf(body) is not 0)
                return;

            for (int i = 1; i <= eventCount; i++)
                await session.SendRawAsync(EventFrame(Guid.NewGuid(), version: i));
        });

        await using var watch = await ConnectAsync(server, maxShardHint: 2);

        var seen = new List<long?>();
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        while (seen.Count < eventCount && DateTime.UtcNow < deadline)
        {
            WatchResponse? next = await watch.NextAsync(TimeSpan.FromMilliseconds(1));
            if (next is not null)
                seen.Add(next.Events[0].FromAggregateVersion);
        }

        Assert.Equal(eventCount, seen.Count);
        Assert.Equal(Enumerable.Range(1, eventCount).Select(i => (long?)i), seen);
    }

    // =====================================================================
    // 3. Events already delivered into the channel before a sibling failure.
    // =====================================================================

    /// <summary>
    /// A shard failure poisons the whole watch, but events that already crossed into the channel
    /// were genuinely observed and must still reach the caller before the error does. Losing them
    /// would be a gap the caller is told about only as "something failed", with no way to know
    /// which events it never saw.
    /// </summary>
    [Fact]
    public async Task MultiShard_EventsBufferedBeforeASiblingShardDies_AreStillDelivered()
    {
        var shardZeroDelivered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var dropShardOne = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            await session.SendRawAsync(WatchAck);
            if (ShardOf(body) is 0)
            {
                await session.SendRawAsync(EventFrame(Guid.NewGuid(), version: 1));
                await session.SendRawAsync(EventFrame(Guid.NewGuid(), version: 2));
                shardZeroDelivered.SetResult();
                return;
            }

            await dropShardOne.Task;
            session.Close();
        });

        await using var watch = await ConnectAsync(server, maxShardHint: 2);

        await shardZeroDelivered.Task;
        // Give the reader time to move both frames into the channel before poisoning it.
        await Task.Delay(200);
        dropShardOne.SetResult();

        var versions = new List<long?>();
        Exception? failure = null;
        for (int i = 0; i < 5 && failure is null; i++)
        {
            failure = await Record.ExceptionAsync(async () =>
            {
                WatchResponse? next = await watch.NextAsync(HangBudget);
                if (next is not null)
                    versions.Add(next.Events[0].FromAggregateVersion);
            });
        }

        Assert.Equal([(long?)1L, 2L], versions);
        Assert.IsAssignableFrom<CeleriantClientException>(failure);
    }

    // =====================================================================
    // 4. Disposal robustness.
    // =====================================================================

    /// <summary>
    /// Disposal cancels the shard readers and then waits for them while they are parked mid-frame.
    /// If the teardown faulted a reader or lost the cancellation, that wait would fault or hang.
    ///
    /// Written when DisposeAsync disposed the token source before awaiting the readers that held its
    /// token; the ordering was fixed, and this stayed as the guard against it coming back.
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData(2L)]
    public async Task DisposeAsync_WhileAShardReaderIsParkedMidFrame_CompletesWithoutFaulting(long? maxShardHint)
    {
        await using var server = FakeCeleriantServer.Start(async (session, _, _) =>
        {
            await session.SendRawAsync(WatchAck);
            // Half a frame header, never completed: the reader parks inside a frame.
            await session.SendRawAsync(WatchAck[..8]);
        });

        var watch = await ConnectAsync(server, maxShardHint);
        await Task.Delay(200);

        var dispose = watch.DisposeAsync().AsTask();
        await AwaitCompletionAsync(dispose, "DisposeAsync with a shard reader parked mid-frame");
        Assert.Null(await Record.ExceptionAsync(() => dispose));
    }

    /// <summary>
    /// Two threads racing into disposal must not both walk the teardown. This reproduced a
    /// <c>NullReferenceException</c> when the guard was an unsynchronised bool and the loser walked
    /// fields the winner had already nulled; the guard is now an atomic claim and the fields are no
    /// longer nulled, and this is what holds that in place.
    ///
    /// NOTE: this is a race detector, not a stable assertion. It reproduces on roughly three runs in
    /// four on this machine; a green run is not evidence the race is gone.
    /// </summary>
    [Fact]
    public async Task DisposeAsync_RacedFromTwoThreads_DoesNotThrow()
    {
        for (int attempt = 0; attempt < 1000; attempt++)
        {
            await using var server = FakeCeleriantServer.Start((session, _, _) => session.SendRawAsync(WatchAck));
            var watch = await ConnectAsync(server, maxShardHint: 2);

            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            Task Race() => Task.Run(async () =>
            {
                await gate.Task;
                await watch.DisposeAsync();
            });

            var a = Race();
            var b = Race();
            gate.SetResult();

            var failure = await Record.ExceptionAsync(() => Task.WhenAll(a, b));
            Assert.True(
                failure is null,
                $"attempt {attempt}: two concurrent DisposeAsync calls surfaced {failure}");
        }
    }

    /// <summary>
    /// A caller already past <c>ThrowIfDisposed</c> inside <c>NextAsync</c> when disposal begins
    /// must come back with <c>ObjectDisposedException</c> or a client exception — never a
    /// <c>NullReferenceException</c> out of the library's own teardown. This reproduced one when
    /// disposal nulled the connection fields underneath a live reader.
    ///
    /// NOTE: this is a race detector, not a stable assertion. It reproduces on roughly three runs in
    /// four on this machine; a green run is not evidence the race is gone.
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData(2L)]
    public async Task NextAsyncRacingDisposal_NeverSurfacesANullReferenceException(long? maxShardHint)
    {
        for (int attempt = 0; attempt < 3000; attempt++)
        {
            await using var server = FakeCeleriantServer.Start((session, _, _) => session.SendRawAsync(WatchAck));
            var watch = await ConnectAsync(server, maxShardHint);

            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var reader = Task.Run(async () =>
            {
                await gate.Task;
                for (int i = 0; i < 20; i++)
                {
                    try { await watch.NextAsync(TimeSpan.FromMilliseconds(1)); }
                    catch (NullReferenceException) { throw; }
                    catch { return; }
                }
            });
            var disposer = Task.Run(async () => { await gate.Task; await watch.DisposeAsync(); });

            gate.SetResult();
            var failure = await Record.ExceptionAsync(() => Task.WhenAll(reader, disposer));

            Assert.True(
                failure is not NullReferenceException,
                $"attempt {attempt}: NextAsync racing DisposeAsync surfaced {failure}");
        }
    }

    // =====================================================================
    // Helpers
    // =====================================================================

    private static async Task<WatchConnection> ConnectThenDropEveryShardAsync(long? maxShardHint)
    {
        var drop = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var server = FakeCeleriantServer.Start(async (session, _, _) =>
        {
            await session.SendRawAsync(WatchAck);
            await drop.Task;
            session.Close();
        });

        WatchConnection watch = await ConnectAsync(server, maxShardHint);
        drop.SetResult();

        // Park until the subscription is poisoned, so the tests below start from a dead watch.
        for (int i = 0; i < 50; i++)
        {
            if (await Record.ExceptionAsync(async () => await watch.NextAsync(TimeSpan.FromMilliseconds(100))) is not null)
                return watch;
        }

        Assert.Fail("the watch never failed after every shard connection was dropped");
        return watch;
    }

    private static Task<WatchConnection> ConnectAsync(
        FakeCeleriantServer server,
        long? maxShardHint,
        CancellationToken ct = default)
        => WatchConnection.ConnectAsync(
            server.Address,
            new WatchRequest(),
            new WatchOptions
            {
                ConnectionTimeout = TimeSpan.FromSeconds(5),
                MaxShardHint = maxShardHint,
            },
            ct);

    private static long? ShardOf(byte[] requestBody)
        => WireCodec.Deserialize<WatchRequest>(requestBody).ShardId;

    private static byte[] EventFrame(Guid aggregateId, long version)
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
                        FromAggregateVersion = version,
                        ToAggregateVersion = version,
                    },
                ],
            }));

    private static async Task AwaitCompletionAsync(Task task, string whatWouldHang)
    {
        var finished = await Task.WhenAny(task, Task.Delay(HangBudget));
        Assert.True(
            ReferenceEquals(finished, task),
            $"{whatWouldHang} was still running after {HangBudget.TotalSeconds:0}s");
    }
}
