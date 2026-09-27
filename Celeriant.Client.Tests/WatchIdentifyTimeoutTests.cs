using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Watch;

namespace Celeriant.Client.Tests;

/// <summary>
/// The identify half of the connect-time bounding contract.
///
/// <para>
/// A node that is up enough to accept a socket and read a request frame, and dead enough never to
/// answer it, is the ordinary shape of a half-failed node. On the watch path with an
/// <see cref="WatchOptions.IdentityConfig"/> set, the identify exchange happens inside the connect,
/// so an unbounded read there parks <c>ConnectAsync</c> forever — and <c>CeleriantPool.WatchAsync</c>
/// with it, since routing can only reach the next candidate node on a connect that returns.
/// </para>
///
/// <para>
/// The contract: the connect must fail with <see cref="ConnectionTimeoutException"/> — the
/// failover-class exception the pool routes on — inside a bound derived from
/// <see cref="WatchOptions.ConnectionTimeout"/>, and the socket the client opened must be closed on
/// the way out rather than left half-open against a node that is already sick.
/// </para>
/// </summary>
public class WatchIdentifyTimeoutTests
{
    /// <summary>How long a call gets before the test calls it hung. Never used as a race timer.</summary>
    private static readonly TimeSpan HangBudget = TimeSpan.FromSeconds(5);

    /// <summary>Comfortably larger than <see cref="ConnectionTimeout"/>, far below a hang.</summary>
    private static readonly TimeSpan ConnectBudget = TimeSpan.FromSeconds(3);

    private static readonly TimeSpan ConnectionTimeout = TimeSpan.FromMilliseconds(300);

    [Fact]
    public async Task Identify_ServerReadsTheRequestAndNeverAnswers_FailsWithConnectionTimeoutAndClosesTheSocket()
    {
        var firstFrameArrived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var sessionEnded = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        uint firstFrameType = 0;

        await using var server = FakeCeleriantServer.Start(
            (_, messageType, _) =>
            {
                // A node that reads the request and is never going to answer it. The write comes
                // first: TrySetResult releases the awaiter, and a reader that got there before the
                // write would print "type-0" in the message where the real type matters most.
                Volatile.Write(ref firstFrameType, messageType);
                firstFrameArrived.TrySetResult();
                return Task.CompletedTask;
            },
            _ => sessionEnded.TrySetResult());

        var connect = Task.Run(() => WatchConnection.ConnectAsync(
            server.Address,
            new WatchRequest(),
            new WatchOptions
            {
                ConnectionTimeout = ConnectionTimeout,
                IdentityConfig = ClientIdentityConfig.FromClientId(Guid.NewGuid()),
            }));

        // Never let a hung or unexpectedly-successful connect fault the run from a finalizer.
        _ = connect.ContinueWith(
            t =>
            {
                _ = t.Exception;
                if (t.Status == TaskStatus.RanToCompletion)
                    _ = t.Result.DisposeAsync();
            },
            TaskScheduler.Default);

        await AwaitCompletionAsync(
            firstFrameArrived.Task,
            "the connect's first request frame reaching a server that accepts the connection");

        var settled = await Task.WhenAny(connect, Task.Delay(ConnectBudget));
        Assert.True(
            ReferenceEquals(settled, connect),
            $"ConnectionTimeout is {ConnectionTimeout.TotalMilliseconds:0}ms, the server read the "
            + $"connect's {DescribeFrame(Volatile.Read(ref firstFrameType))} request and answered "
            + $"nothing, and ConnectAsync was still running {ConnectBudget.TotalSeconds:0}s later. "
            + "Nothing bounds the identify read, so one half-dead node parks the connect indefinitely "
            + "and CeleriantPool.WatchAsync never reaches the next candidate node");

        // Pin the phase, not just the type. The subscription acknowledgement is bounded the same
        // way and raises the same exception against this same never-answering server, so type
        // alone is satisfied by a build where the identify bound does not exist at all.
        Assert.Equal(MessageTypes.Requests.Identify, Volatile.Read(ref firstFrameType));

        var failure = await Record.ExceptionAsync(() => connect);
        Assert.True(
            failure is ConnectionTimeoutException,
            "a node that accepts the socket and never answers the identify request must come back as "
            + "ConnectionTimeoutException, the failover-class exception routing retries on; got "
            + $"{Describe(failure)}");
        Assert.Contains("identity handshake", failure!.Message, StringComparison.Ordinal);

        await AwaitCompletionAsync(
            sessionEnded.Task,
            "the server observing the client close the socket it opened for the timed-out identify");
    }

    /// <summary>Fail loudly on a hang instead of deadlocking the run.</summary>
    private static async Task AwaitCompletionAsync(Task task, string whatWouldHang)
    {
        var finished = await Task.WhenAny(task, Task.Delay(HangBudget));
        Assert.True(
            ReferenceEquals(finished, task),
            $"{whatWouldHang} was still pending after {HangBudget.TotalSeconds:0}s: it hangs where the "
            + "contract requires it to complete");
    }

    private static string Describe(Exception? failure)
        => failure is null ? "a connection and no exception at all" : failure.GetType().Name;

    private static string DescribeFrame(uint messageType) => messageType switch
    {
        MessageTypes.Requests.Identify => "Identify",
        MessageTypes.Requests.Watch => "Watch",
        _ => $"type-{messageType}",
    };
}
