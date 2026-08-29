using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Transport;
using Moq;

namespace Celeriant.Client.Tests;

/// <summary>
/// Regression guards for the fixes found by the blind adversarial API-surface program (2026-08-29).
/// Each test pins a defect a black-box attacker reached from the public surface; the harnesses that
/// found them are archived under <c>session/harness/</c>. See <c>session/findings/</c> for the write-ups.
/// </summary>
public class AdversarialRegressionTests
{
    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(10);

    // =====================================================================
    // Block 3: ReadFilters.FromAggregateVersion clamps <1 to 1
    // =====================================================================

    /// <summary>
    /// The doc-comment promises "a value below 1 reads as 1". A struct default leaves
    /// <see cref="ReadFilters.FromAggregateVersion"/> at 0, and an explicit 0 is the natural bug; both
    /// must read (and serialize) as 1, or a default <c>new ReadFilters { ... }</c> is rejected by the
    /// server with a trim-shaped error on an untrimmed aggregate.
    /// </summary>
    [Theory]
    [InlineData(0L, 1L)]
    [InlineData(-5L, 1L)]
    [InlineData(1L, 1L)]
    [InlineData(7L, 7L)]
    public void ReadFilters_FromAggregateVersion_ClampsBelowOne(long set, long expected)
    {
        var filters = new ReadFilters { FromAggregateVersion = set };
        Assert.Equal(expected, filters.FromAggregateVersion);
    }

    [Fact]
    public void ReadFilters_StructDefault_ReadsAsOne()
        => Assert.Equal(1L, new ReadFilters().FromAggregateVersion);

    [Fact]
    public void ReadFilters_ClampedValue_IsWhatSerializes()
    {
        // The wire must carry the clamped value, not the raw 0 the server rejects.
        var round = WireCodec.Deserialize<ReadFilters>(
            WireCodec.Serialize(new ReadFilters { FromAggregateVersion = 0 }));
        Assert.Equal(1L, round.FromAggregateVersion);
    }

    // =====================================================================
    // Block 1: a disposed pool throws ObjectDisposedException (as documented),
    // not the ConnectionFailedException it throws when the cluster is down.
    // =====================================================================

    [Fact]
    public async Task DisposedPool_ReadWriteWatch_ThrowObjectDisposedException()
    {
        var pool = CreateMockedPool();
        await pool.DisposeAsync();

        var key = NewKey();
        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => pool.ReadAsync(new ReadRequest { AggregateKey = key, Filters = ReadFilters.From(1) }));
        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => pool.WriteAsync(key, [SampleEvent()], Guid.NewGuid()));
        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => pool.WatchAsync(new WatchRequest()));
    }

    // =====================================================================
    // Block 2: a null EventValue is rejected client-side (ArgumentException)
    // before it can desynchronise the connection.
    // =====================================================================

    [Fact]
    public async Task NullEventValue_ThrowsArgumentException_BeforeSend_AndKeepsConnectionUsable()
    {
        int framesSeen = 0;
        await using var server = FakeCeleriantServer.Start((session, _, body) =>
        {
            Interlocked.Increment(ref framesSeen);
            // Echo the correlation id so the client's response binding accepts the reply.
            var req = WireCodec.Deserialize<WriteRequest>(body);
            return session.SendFrameAsync(
                MessageTypes.Responses.Write,
                WireCodec.Serialize(new WriteResponse { CorrelationId = req.CorrelationId }));
        });

        await using var client = await CeleriantClient.ConnectAsync(server.Address, ct: default);

        var key = NewKey();
        var bad = new WriteRequest
        {
            ClientId = Guid.NewGuid(),
            Writes = new Dictionary<AggregateKey, SingleAggregateWrite>
            {
                [key] = new SingleAggregateWrite { Events = [new AggregateEvent { ClientSeq = 1, EventValue = null! }] },
            },
        };

        var ex = await RunWithinBudget(() => client.WriteAsync(bad));
        Assert.IsType<ArgumentException>(ex);
        Assert.Equal(0, Volatile.Read(ref framesSeen)); // guard fired before any frame reached the server

        // The connection is still usable: a well-formed write goes through.
        var ok = await client.WriteAsync(new WriteRequest
        {
            ClientId = Guid.NewGuid(),
            Writes = new Dictionary<AggregateKey, SingleAggregateWrite>
            {
                [key] = new SingleAggregateWrite { Events = [SampleEvent()] },
            },
        });
        Assert.NotNull(ok);
        Assert.Equal(1, Volatile.Read(ref framesSeen));
    }

    // =====================================================================
    // Round 2 — Block: RegisterSchema "cannot accept writes" (2027) must be a
    // not-leader condition so the pool fails over, not a fatal internal error.
    // =====================================================================

    [Fact]
    public void RegisterSchemaCannotAcceptWrites_IsNotLeader_AndMapsToNotLeaderException()
    {
        var error = new ErrorResponse
        {
            ErrorCode = ErrorResponse.RegisterSchemaCannotAcceptWrites,
            ErrorMessage = "{\"leader_address\":\"leader-host:10000\"}",
        };

        Assert.True(error.IsNotLeader);
        var mapped = CeleriantClient.CreateException(error);
        var notLeader = Assert.IsType<NotLeaderException>(mapped);
        Assert.Equal("leader-host:10000", notLeader.LeaderAddress);
    }

    // =====================================================================
    // Round 2 — Block: empty events and a pre-epoch EventTimestamp are rejected
    // client-side (ArgumentException) instead of an opaque server error / silent
    // far-future corruption.
    // =====================================================================

    [Fact]
    public async Task EmptyEvents_ThrowsArgumentException_BeforeSend()
        => await AssertWriteRejectedBeforeSend(new SingleAggregateWrite { Events = [] });

    [Fact]
    public async Task PreEpochEventTimestamp_ThrowsArgumentException_BeforeSend()
        => await AssertWriteRejectedBeforeSend(new SingleAggregateWrite
        {
            // valid type + value; only the timestamp is bad (default MinValue = pre-epoch)
            Events = [new AggregateEvent { ClientSeq = 1, EventTypeMajor = 1, EventValue = [1] }],
        });

    [Fact]
    public async Task ZeroEventTypeMajor_ThrowsArgumentException_BeforeSend()
        => await AssertWriteRejectedBeforeSend(new SingleAggregateWrite
        {
            Events = [new AggregateEvent { ClientSeq = 1, EventTimestamp = DateTimeOffset.UtcNow, EventValue = [1] }], // EventTypeMajor = 0
        });

    [Fact]
    public async Task NegativeExpectedVersion_ThrowsArgumentException_BeforeSend()
        => await AssertWriteRejectedBeforeSend(new SingleAggregateWrite
        {
            ExpectedVersion = -5,
            Events = [SampleEvent()],
        });

    [Fact]
    public async Task DuplicateClientSeqUnderIdempotency_ThrowsArgumentException_BeforeSend()
        => await AssertWriteRejectedBeforeSend(new SingleAggregateWrite
        {
            EnforceClientIdempotency = true,
            Events = [SampleEventSeq(1), SampleEventSeq(1)], // two events reuse seq 1
        });

    private async Task AssertWriteRejectedBeforeSend(SingleAggregateWrite write)
    {
        int framesSeen = 0;
        await using var server = FakeCeleriantServer.Start((session, _, body) =>
        {
            Interlocked.Increment(ref framesSeen);
            var req = WireCodec.Deserialize<WriteRequest>(body);
            return session.SendFrameAsync(
                MessageTypes.Responses.Write, WireCodec.Serialize(new WriteResponse { CorrelationId = req.CorrelationId }));
        });
        await using var client = await CeleriantClient.ConnectAsync(server.Address, ct: default);

        var request = new WriteRequest
        {
            ClientId = Guid.NewGuid(),
            Writes = new Dictionary<AggregateKey, SingleAggregateWrite> { [NewKey()] = write },
        };

        var ex = await RunWithinBudget(() => client.WriteAsync(request));
        Assert.IsType<ArgumentException>(ex);
        Assert.Equal(0, Volatile.Read(ref framesSeen)); // guard fired before any frame reached the server
    }

    // =====================================================================
    // helpers
    // =====================================================================

    private static CeleriantPool CreateMockedPool()
        => new(
            new CeleriantPoolOptions { Address = "leader:1", SeedAddresses = ["follower:1"] },
            (addr, _, _) =>
            {
                var mock = new Mock<INodeConnectionPool>();
                mock.Setup(p => p.Address).Returns(addr);
                mock.Setup(p => p.DisposeAsync()).Returns(ValueTask.CompletedTask);
                return mock.Object;
            });

    private static AggregateKey NewKey() => new(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());

    private static AggregateEvent SampleEvent() => SampleEventSeq(1);

    private static AggregateEvent SampleEventSeq(long clientSeq) => new()
    {
        ClientSeq = clientSeq,
        EventTimestamp = DateTimeOffset.UtcNow,
        EventTypeMajor = 1,
        EventValue = "x"u8.ToArray(),
    };

    private static async Task<Exception?> RunWithinBudget(Func<Task> action)
    {
        var running = Task.Run(action);
        var finished = await Task.WhenAny(running, Task.Delay(Budget));
        Assert.True(ReferenceEquals(finished, running), "call did not complete within the budget");
        return await Record.ExceptionAsync(() => running);
    }
}
