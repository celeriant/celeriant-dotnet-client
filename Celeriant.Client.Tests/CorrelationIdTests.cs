using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;

namespace Celeriant.Client.Tests;

/// <summary>
/// Every response must be bound to the request that was sent, and the binding must be checked
/// before the response is interpreted. <c>CorrelationId</c> is a user-facing application tag, so
/// the transport fills it only when the caller left it null.
/// </summary>
public class CorrelationIdTests
{
    [Fact]
    public async Task TransportFillsCorrelationId_WhenTheCallerLeavesItNull()
    {
        var wire = new List<Guid?>();
        await using var server = FakeCeleriantServer.Start(RecordCorrelationIds(wire));
        await using var pool = new CeleriantPool(FakeServerProtocol.SingleConnectionPool(server));

        await pool.AggregateDetailsAsync(FakeServerProtocol.Details(FakeServerProtocol.NewKey()));

        Assert.NotNull(Assert.Single(wire));
    }

    [Fact]
    public async Task CallerSuppliedCorrelationId_ReachesTheWireUnchanged()
    {
        var supplied = Guid.NewGuid();
        var wire = new List<Guid?>();
        await using var server = FakeCeleriantServer.Start(RecordCorrelationIds(wire));
        await using var pool = new CeleriantPool(FakeServerProtocol.SingleConnectionPool(server));

        await pool.AggregateDetailsAsync(
            FakeServerProtocol.Details(FakeServerProtocol.NewKey(), supplied));

        Guid? sent = Assert.Single(wire);
        Assert.Equal(supplied, sent);
    }

    /// <summary>
    /// The production shape: a stale 7001 error frame belonging to some earlier request. The echo
    /// check has to run ahead of the point where a server error response becomes a thrown
    /// exception, or this walks straight through as the current request's domain failure.
    /// </summary>
    [Fact]
    public async Task StaleErrorFrameForAnotherRequest_DoesNotSurfaceAsThisRequestsFailure()
    {
        await using var server = FakeCeleriantServer.Start((session, _, _) =>
            session.SendFrameAsync(
                MessageTypes.Responses.GenericError,
                FakeServerProtocol.ErrorFrame(
                    ErrorResponse.ExistsAggregateNotExists, "{}", Guid.NewGuid())));

        await using var pool = new CeleriantPool(FakeServerProtocol.SingleConnectionPool(server));

        var ex = await Record.ExceptionAsync(() =>
            pool.AggregateDetailsAsync(FakeServerProtocol.Details(FakeServerProtocol.NewKey())));

        Assert.NotNull(ex);
        Assert.False(
            ex is CeleriantErrorException error
                && error.Error.ErrorCode == ErrorResponse.ExistsAggregateNotExists,
            "an error frame whose correlation id belongs to a different request must be rejected "
            + $"before it is read as this request's failure, but it surfaced as {ex.GetType().Name}");
    }

    /// <summary>
    /// The same binding holds for the implicit Identify: an error that does not echo it is a
    /// protocol failure that poisons the connection before any mutation is sent.
    /// </summary>
    [Fact]
    public async Task IdentifyErrorForAnotherRequest_IsAProtocolError_AndNothingIsSent()
    {
        await using var server = new RecordingFrameServer(_ => Task.FromResult(new FrameReply(
            MessageTypes.Responses.GenericError,
            FakeServerProtocol.ErrorFrame(ErrorResponse.AuthInvalidKey, "Invalid API key.", Guid.Empty))));
        await using var client = await CeleriantClient.ConnectAsync(server.Address);

        var failure = await Record.ExceptionAsync(() => client.WriteAsync(OccTestData.Write()));

        Assert.IsType<ProtocolException>(failure);
        Assert.True(client.IsPoisoned);
        Assert.DoesNotContain(server.Frames, frame => frame.Type is 3 or 5);
    }

    /// <summary>
    /// Detecting a mismatch and then handing the same connection back is worse than not detecting
    /// it, because the stream stays offset and every later borrower inherits the offset.
    /// </summary>
    [Fact]
    public async Task CorrelationIdMismatch_RetiresTheConnection()
    {
        var arrivals = new List<int>();
        int seen = 0;

        await using var server = FakeCeleriantServer.Start((session, _, body) =>
        {
            var request = FakeServerProtocol.DecodeDetails(body);
            lock (arrivals)
                arrivals.Add(session.ConnectionId);

            // Request 1 warms the pool; request 2 gets an answer stamped with a foreign id.
            Guid? foreign = Interlocked.Increment(ref seen) == 2 ? Guid.NewGuid() : null;
            return session.SendFrameAsync(
                MessageTypes.Responses.AggregateDetails,
                FakeServerProtocol.DetailsAnswer(request, foreign));
        });

        await using var pool = new CeleriantPool(FakeServerProtocol.SingleConnectionPool(server));

        await pool.AggregateDetailsAsync(FakeServerProtocol.Details(FakeServerProtocol.NewKey()));

        var ex = await Record.ExceptionAsync(() =>
            pool.AggregateDetailsAsync(FakeServerProtocol.Details(FakeServerProtocol.NewKey())));
        Assert.NotNull(ex);

        await pool.AggregateDetailsAsync(FakeServerProtocol.Details(FakeServerProtocol.NewKey()));

        Assert.Equal(3, arrivals.Count);
        Assert.NotEqual(arrivals[1], arrivals[2]);
    }

    /// <summary>
    /// Regression guard, not a currently-failing case: <see cref="WatchResponse"/> has no
    /// correlation id field at all, so echo validation must not make watch unusable.
    /// </summary>
    [Fact]
    public async Task WatchResponseCarriesNoCorrelationId_AndWatchStillWorks()
    {
        var orgId = Guid.NewGuid();
        await using var server = FakeCeleriantServer.Start((session, _, _) =>
            session.SendFrameAsync(
                MessageTypes.Responses.Watch,
                WireCodec.Serialize(new WatchResponse
                {
                    Events =
                    [
                        new WatchResponseEvent
                        {
                            OrgId = orgId,
                            AggregateTypeId = Guid.NewGuid(),
                            AggregateId = Guid.NewGuid(),
                            Operation = WatchOperationType.Write,
                        }
                    ]
                })));

        await using var pool = new CeleriantPool(FakeServerProtocol.SingleConnectionPool(server));
        await using var watch = await pool.WatchAsync(new WatchRequest());

        var response = await watch.NextAsync(TimeSpan.FromSeconds(5));

        Assert.NotNull(response);
        Assert.Equal(orgId, Assert.Single(response.Events).OrgId);
    }

    /// <summary>
    /// Regression guard, not a currently-failing case: a server that could not decode the request
    /// has no correlation id to echo, so a null id on a protocol error must not be reported as a
    /// mismatch instead of the protocol error it is.
    /// </summary>
    [Fact]
    public async Task ProtocolErrorWithNoCorrelationId_StillSurfacesAsAProtocolError()
    {
        await using var server = FakeCeleriantServer.Start((session, _, _) =>
            session.SendFrameAsync(
                MessageTypes.Responses.ProtocolError,
                FakeServerProtocol.ProtocolErrorFrame(
                    ErrorResponse.ShardRoutingMultipleShards, "{}", correlationId: null)));

        await using var pool = new CeleriantPool(FakeServerProtocol.SingleConnectionPool(server));

        await Assert.ThrowsAnyAsync<ProtocolException>(() =>
            pool.AggregateDetailsAsync(FakeServerProtocol.Details(FakeServerProtocol.NewKey())));
    }

    /// <summary>
    /// A mismatch must retire the connection itself and not only the lease: a raw lease taken from
    /// the pool has no wrapper that marks it broken, so a poison call that does nothing hands the
    /// offset stream straight to the next borrower.
    /// </summary>
    [Fact]
    public async Task CorrelationMismatchOnARawLease_RetiresTheConnection()
    {
        var arrivals = new List<int>();
        int seen = 0;

        await using var server = FakeCeleriantServer.Start((session, _, body) =>
        {
            var request = FakeServerProtocol.DecodeDetails(body);
            lock (arrivals)
                arrivals.Add(session.ConnectionId);

            Guid? foreign = Interlocked.Increment(ref seen) == 1 ? Guid.NewGuid() : null;
            return session.SendFrameAsync(
                MessageTypes.Responses.AggregateDetails,
                FakeServerProtocol.DetailsAnswer(request, foreign));
        });

        await using var pool = new CeleriantPool(FakeServerProtocol.SingleConnectionPool(server));

        // A raw lease: no ExecuteRequestAsync wrapper, so nothing else marks the lease broken.
        await using (var conn = await pool.GetConnectionAsync())
        {
            Assert.NotNull(await Record.ExceptionAsync(() => conn.Client.SendRequestAsync(
                new ClientRequest.AggregateDetails(FakeServerProtocol.Details(FakeServerProtocol.NewKey())))));
        }

        await pool.AggregateDetailsAsync(FakeServerProtocol.Details(FakeServerProtocol.NewKey()));

        Assert.Equal(2, arrivals.Count);
        Assert.NotEqual(arrivals[0], arrivals[1]);
    }

    /// <summary>
    /// Making the request DTOs records so the transport can copy one to fill its correlation id
    /// must not move a byte on the wire, against a class with the identical key layout.
    /// </summary>
    [Fact]
    public void RecordDtoSerializesByteIdenticallyToItsClassTwin()
    {
        var key = FakeServerProtocol.NewKey();
        var id = Guid.NewGuid();
        var filters = new ReadFilters();

        byte[] asRecord = WireCodec.Serialize(
            new ReadRequest { CorrelationId = id, AggregateKey = key, Filters = filters });
        byte[] asClass = WireCodec.Serialize(
            new ReadRequestClassTwin { CorrelationId = id, AggregateKey = key, Filters = filters });

        Assert.Equal(asClass, asRecord);
    }

    /// <summary>
    /// The copy that fills the correlation id must carry every other member through untouched: the
    /// bytes must match a request built with the id set from the start.
    /// </summary>
    [Fact]
    public void WithCorrelationIdCopyIsWireIdenticalApartFromTheId()
    {
        var original = new WriteRequest
        {
            ClientId = Guid.NewGuid(),
            UserId = Guid.NewGuid(),
            Writes = new Dictionary<AggregateKey, SingleAggregateWrite>
            {
                [FakeServerProtocol.NewKey()] = new SingleAggregateWrite
                {
                    Events = [new AggregateEvent { EventValue = "x"u8.ToArray() }],
                },
            },
        };

        var id = Guid.NewGuid();
        var copied = original with { CorrelationId = id };
        var built = new WriteRequest
        {
            CorrelationId = id,
            ClientId = original.ClientId,
            UserId = original.UserId,
            Writes = original.Writes,
        };

        Assert.Equal(WireCodec.Serialize(built), WireCodec.Serialize(copied));
    }

    /// <summary>
    /// Filling the correlation id must cost the same whatever the request carries: a deep copy here
    /// would make every request pay for its own payload size on the hot path.
    /// </summary>
    [Fact]
    public void TheCorrelationIdCopyIsConstantCostInPayloadSize()
    {
        static WriteRequest Build(int aggregates)
        {
            var writes = new Dictionary<AggregateKey, SingleAggregateWrite>();
            for (int i = 0; i < aggregates; i++)
                writes[FakeServerProtocol.NewKey()] = new SingleAggregateWrite
                {
                    Events = [new AggregateEvent { EventValue = new byte[256] }],
                };
            return new WriteRequest { ClientId = Guid.NewGuid(), Writes = writes };
        }

        static long CopyCost(WriteRequest request)
        {
            var id = Guid.NewGuid();
            _ = new ClientRequest.Write(request with { CorrelationId = id });   // JIT warm
            long before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < 100; i++)
                _ = new ClientRequest.Write(request with { CorrelationId = id });
            return (GC.GetAllocatedBytesForCurrentThread() - before) / 100;
        }

        long small = CopyCost(Build(1));
        long large = CopyCost(Build(256));

        Assert.Equal(small, large);
        Assert.True(large < 256, $"the copy must be O(1), measured {large} bytes/op");
    }

    /// <summary>
    /// A hand-written class carrying the identical <c>[Key]</c> layout and formatters as
    /// <see cref="ReadRequest"/>, to compare the record's wire bytes against.
    /// </summary>
    [MessagePack.MessagePackObject]
    public sealed class ReadRequestClassTwin
    {
        [MessagePack.Key(0)]
        [MessagePack.MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
        public Guid? CorrelationId { get; init; }

        [MessagePack.Key(1)]
        public required AggregateKey AggregateKey { get; init; }

        [MessagePack.Key(2)]
        public required ReadFilters Filters { get; init; }
    }

    private static FakeCeleriantServer.RequestHandler RecordCorrelationIds(List<Guid?> wire) =>
        (session, _, body) =>
        {
            var request = FakeServerProtocol.DecodeDetails(body);
            lock (wire)
                wire.Add(request.CorrelationId);
            return session.SendFrameAsync(
                MessageTypes.Responses.AggregateDetails,
                FakeServerProtocol.DetailsAnswer(request));
        };
}
