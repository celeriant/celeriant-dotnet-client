using System.Reflection;
using Celeriant.Client.Errors;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Moq;

namespace Celeriant.Client.Tests;

/// <summary>
/// The leader walk must terminate in bounded time, and must not grow the node map without bound,
/// when a server keeps redirecting to a never-ending stream of fresh leader addresses.
/// </summary>
public class LeaderRedirectBoundTests
{
    private static readonly AggregateKey TestKey = new(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());

    private static WriteRequest MakeWriteRequest() => new()
    {
        ClientId = Guid.NewGuid(),
        Writes = new Dictionary<AggregateKey, SingleAggregateWrite>
        {
            [TestKey] = new()
            {
                AllowCreate = true,
                Events =
                [
                    new AggregateEvent
                    {
                        EventTypeMajor = 1, EventTypeMinor = 0, EventValue = [1], ClientSeq = 1,
                    }
                ],
            }
        }
    };

    [Fact]
    public async Task LeaderRedirectLoop_MustTerminateUnderFreshLeaderStream()
    {
        int counter = 0;
        var distinctAddresses = new List<string>();
        var lockObj = new object();

        await using var pool = new CeleriantPool(
            new CeleriantPoolOptions { Address = "node-0:10000" },
            (addr, _, _) =>
            {
                lock (lockObj) distinctAddresses.Add(addr);
                var mock = new Mock<INodeConnectionPool>();
                mock.Setup(p => p.Address).Returns(addr);
                mock.Setup(p => p.DisposeAsync()).Returns(ValueTask.CompletedTask);
                mock.Setup(p => p.ExecuteRequestAsync(It.IsAny<ClientRequest>(), It.IsAny<CancellationToken>()))
                    .Returns(async (ClientRequest _, CancellationToken ct) =>
                    {
                        // Simulate a network round-trip; also honours cancellation so the
                        // runaway loop can be stopped once the observation window closes.
                        await Task.Delay(1, ct).ConfigureAwait(false);
                        var next = $"node-{Interlocked.Increment(ref counter)}:10000";
                        throw new NotLeaderException(
                            new ErrorResponse
                            {
                                ErrorCode = ErrorResponse.WriteNotLeader,
                                ErrorMessage = $"{{\"leader_address\":\"{next}\"}}",
                            },
                            next);
                    });
                return mock.Object;
            });

        using var cts = new CancellationTokenSource();
        var writeTask = pool.WriteAsync(MakeWriteRequest(), cts.Token);

        var winner = await Task.WhenAny(writeTask, Task.Delay(TimeSpan.FromSeconds(5)));

        if (winner != writeTask)
        {
            cts.Cancel();
            try { await writeTask; } catch { /* OperationCanceledException on shutdown */ }
            int hungPoolCount = ReadNodePoolCount(pool);
            int hungDistinct;
            lock (lockObj) hungDistinct = distinctAddresses.Count;
            Assert.Fail(
                $"WriteAsync did NOT terminate within 5s under a stream of fresh leader addresses. " +
                $"Redirects={counter}, distinctAddresses={hungDistinct}, _nodePools.Count={hungPoolCount}. " +
                $"The failover loop is unbounded and the node pool grows without bound.");
        }

        // Terminated in bounded time: it must have thrown, not succeeded, and the node
        // pool must not have grown without bound.
        await Assert.ThrowsAsync<ConnectionFailedException>(() => writeTask);

        int poolCount = ReadNodePoolCount(pool);
        Assert.True(poolCount <= 17, $"node pool grew without bound: _nodePools.Count={poolCount}");
    }

    private static int ReadNodePoolCount(CeleriantPool pool)
    {
        var field = typeof(CeleriantPool).GetField("_nodePools",
            BindingFlags.NonPublic | BindingFlags.Instance)!;
        var dict = field.GetValue(pool);
        var countProp = dict!.GetType().GetProperty("Count")!;
        return (int)countProp.GetValue(dict)!;
    }
}