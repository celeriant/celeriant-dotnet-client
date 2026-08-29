using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Watch;

namespace Celeriant.Client.Tests;

/// <summary>
/// White-box companions to <see cref="WatchFailClosedTests"/>, pinning two behaviours the
/// black-box tests cannot tell apart:
///
/// <list type="bullet">
/// <item>suppressing a shard reader's reason on disposal — without this, a watch the caller shut
/// down reports itself as having gone blind, and a caller that logs or alerts on that is told its
/// subscription broke every time it closes one cleanly;</item>
/// <item>which reason a caller is handed when several shard readers fail at once — the sibling that
/// merely noticed the channel was closed must never be the reason reported, because it names the
/// plumbing instead of the failure.</item>
/// </list>
/// </summary>
public class WatchFailClosedImplementationTests
{
    private static readonly TimeSpan HangBudget = TimeSpan.FromSeconds(5);

    private static readonly byte[] WatchAck =
        FakeServerSession.BuildFrame(MessageTypes.Responses.Watch, WireCodec.Serialize(new WatchResponse()));

    /// <summary>
    /// Disposal is the caller ending the subscription, not the subscription dying. A parked read is
    /// the only place the two are distinguishable, because every later call is refused before it
    /// reaches the channel.
    /// </summary>
    [Fact]
    public async Task DisposeWhileParked_ReportsDisposalRatherThanTheFailClosedReason()
    {
        await using var server = FakeCeleriantServer.Start((session, _, _) => session.SendRawAsync(WatchAck));

        var watch = await ConnectAsync(server, maxShardHint: 2);
        var parked = watch.NextAsync(CancellationToken.None);

        await watch.DisposeAsync();

        var failure = await CaptureAsync(parked, "the read parked when the watch was disposed");
        Assert.True(
            failure is ObjectDisposedException,
            "the caller disposed this watch, so the parked read must say so; reporting the "
            + "fail-closed reason instead tells a caller shutting down cleanly that it went blind "
            + $"and may have missed events. Got {failure?.GetType().Name ?? "no exception"}");
    }

    /// <summary>
    /// Every shard reader races to the same exit when one of them completes the channel: the
    /// siblings' next write throws <c>ChannelClosedException</c>. The caller must be handed the
    /// failure that actually happened, never a sibling's report of the plumbing closing.
    /// </summary>
    [Fact]
    public async Task WhenEveryShardFailsAtOnce_TheCallerGetsTheRealFailureNotAChannelClosedException()
    {
        var drop = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var server = FakeCeleriantServer.Start(async (session, _, _) =>
        {
            await session.SendRawAsync(WatchAck);
            await drop.Task;
            session.Close();
        });

        await using var watch = await ConnectAsync(server, maxShardHint: 4);

        var next = watch.NextAsync(CancellationToken.None);
        drop.SetResult();

        var failure = await CaptureAsync(next, "NextAsync after every shard lost its connection");
        Assert.True(
            failure is ConnectionFailedException,
            "four shards lost their sockets at once; the caller must be told the connection failed, "
            + $"not handed whichever sibling noticed the channel had closed. Got {Describe(failure)}");
    }

    /// <summary>
    /// The failure is captured once and replayed, so a caller polling a dead watch in a retry loop
    /// must keep getting the same answer rather than a different exception each time.
    /// </summary>
    [Fact]
    public async Task APoisonedWatchReportsTheIdenticalFailureOnEveryRead()
    {
        var drop = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var server = FakeCeleriantServer.Start(async (session, _, _) =>
        {
            await session.SendRawAsync(WatchAck);
            await drop.Task;
            session.Close();
        });

        await using var watch = await ConnectAsync(server, maxShardHint: 2);

        var next = watch.NextAsync(CancellationToken.None);
        drop.SetResult();
        var first = await CaptureAsync(next, "the first read after the shards died");

        for (int i = 2; i <= 5; i++)
        {
            var again = await CaptureAsync(
                watch.NextAsync(CancellationToken.None), $"read {i} on a poisoned watch");

            Assert.True(
                again?.GetType() == first?.GetType(),
                $"read {i} of a dead watch changed the answer: first {Describe(first)}, "
                + $"now {Describe(again)}. A caller retrying must see a stable failure");
            Assert.Equal(first?.Message, again?.Message);
        }
    }

    private static Task<WatchConnection> ConnectAsync(FakeCeleriantServer server, long? maxShardHint)
        => WatchConnection.ConnectAsync(
            server.Address,
            new WatchRequest(),
            new WatchOptions
            {
                ConnectionTimeout = TimeSpan.FromSeconds(5),
                MaxShardHint = maxShardHint,
            });

    /// <summary>Fail loudly on a hang instead of deadlocking the run.</summary>
    private static async Task<Exception?> CaptureAsync(Task task, string whatWouldHang)
    {
        var finished = await Task.WhenAny(task, Task.Delay(HangBudget));
        Assert.True(
            ReferenceEquals(finished, task),
            $"{whatWouldHang} was still running after {HangBudget.TotalSeconds:0}s: it hangs where "
            + "the contract requires it to complete");

        return await Record.ExceptionAsync(() => task);
    }

    private static string Describe(Exception? failure)
        => failure is null ? "no exception at all" : $"{failure.GetType().Name}";
}
