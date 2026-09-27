using System.Buffers;
using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Streaming;
using Celeriant.Transport;
using MessagePack;

namespace Celeriant.Client.Tests;

/// <summary>
/// Where the transport hardening (dictionary miss, send lock, size caps, StartShard) meets leader
/// routing. Each test states one interaction contract; a failure names the defect.
/// </summary>
public class TransportGuardRoutingTests
{
    private const string ShaUnderTest = "sha-the-client-never-cached";

    /// <summary>Set by the test so a fake can echo it without decoding the request body.</summary>
    private static readonly Guid CorrelationId = Guid.Parse("00000000-0000-0000-0000-0000000000e5");

    private static readonly TimeSpan Budget = TimeSpan.FromSeconds(2);

    // -------------------------------------------------------------------------
    // Dict-miss at Identify vs the pool's dial-failure breaker
    // -------------------------------------------------------------------------

    /// <summary>
    /// A sha-only confirmation the client cannot resolve throws out of Identify, which runs inside
    /// the node pool's connection factory. The factory opened the socket and nothing else will
    /// close it, so the failure must close it before it propagates.
    /// </summary>
    [SkippableFact]
    public async Task DictShaMissAtIdentify_ClosesTheSocketTheFactoryOpened()
    {
        var socketEnded = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var node = FakeCeleriantServer.Start(
            ShaOnlyIdentifyThenWriteOk(),
            _ => socketEnded.TrySetResult());

        await using var pool = new CeleriantPool(IdentifiedOptions(node.Address));

        int gcBefore = GC.CollectionCount(0);
        var failure = await Record.ExceptionAsync(() => pool.WriteAsync(NewWrite()));
        Assert.True(
            failure is ProtocolException,
            $"a dictionary sha the client cannot resolve must fail the handshake; got {Describe(failure)}");

        var finished = await Task.WhenAny(socketEnded.Task, Task.Delay(TimeSpan.FromMilliseconds(750)));
        bool collected = GC.CollectionCount(0) != gcBefore;

        Assert.True(
            ReferenceEquals(finished, socketEnded.Task),
            "Identify threw inside NodeConnectionPool.CreateConnectionAsync, which never disposes "
            + "the CeleriantClient it had already connected: the socket is still open and only a "
            + "finalizer will ever close it, so every failed handshake leaks one.");

        Skip.If(
            collected,
            "INCONCLUSIVE: the socket was released, but a garbage collection ran inside the "
            + "window, so a finalizer could have closed it instead of the factory. Re-run.");
    }

    /// <summary>
    /// The breaker exists so a swarm of callers does not each time out on a dead node. A sha-only
    /// confirmation is the node answering, so it must not arm the breaker and fast-fail every
    /// other caller for the cooldown.
    /// </summary>
    [Fact]
    public async Task DictShaMissAtIdentify_DoesNotArmTheDialFailureBreaker()
    {
        await using var node = FakeCeleriantServer.Start(ShaOnlyIdentifyThenWriteOk());
        await using var pool = new CeleriantPool(IdentifiedOptions(node.Address));

        await Record.ExceptionAsync(() => pool.WriteAsync(NewWrite()));
        var second = await Record.ExceptionAsync(() => pool.WriteAsync(NewWrite()));

        Assert.True(
            second is not PoolUnavailableException,
            "a handshake the node answered armed the connect breaker, so the next caller is "
            + $"refused locally for the cooldown; got {Describe(second)}");
    }

    // -------------------------------------------------------------------------
    // Dispose under the send lock vs the send boundary
    // -------------------------------------------------------------------------

    /// <summary>
    /// A caller parked on the send lock wrote nothing. When the connection is disposed under it,
    /// it must get the pre-send classification a walk can act on: never ObjectDisposedException
    /// from the semaphore, and never RequestOutcomeUnknown, which would forbid a retry for a
    /// request that never existed.
    /// </summary>
    [Fact]
    public async Task DisposeWhileQueuedOnTheSendLock_GivesThePreSendClassification()
    {
        await using var node = FakeCeleriantServer.Start(
            async (_, _, _) => await Task.Delay(Budget));       // never answers

        var client = await CeleriantClient.ConnectAsync(node.Address, TimeSpan.FromSeconds(5));

        var inflight = client.SendRequestAsync(Details());       // holds the send lock
        await Task.Delay(200);
        var queued = client.SendRequestAsync(Details());         // parked on the send lock
        await Task.Delay(200);

        await client.DisposeAsync();

        var failure = await Record.ExceptionAsync(() => queued);

        Assert.True(
            failure is ConnectionFailedException,
            "a caller queued on the send lock of a disposed connection wrote no bytes, so it must "
            + $"see a walkable ConnectionFailed; got {Describe(failure)}");
        Assert.False(
            failure is RequestOutcomeUnknownException,
            "a queued caller never reached the socket; classifying it as an unknown outcome "
            + "forbids a retry the caller is entitled to");

        await Record.ExceptionAsync(() => inflight);
    }

    // -------------------------------------------------------------------------
    // Response length guard vs post-send handling on a write
    // -------------------------------------------------------------------------

    /// <summary>
    /// An oversized response header arrives after the write is on the wire. It is a decode-class
    /// failure, so it goes back to the caller as ProtocolException, the request is never re-sent,
    /// and the connection, mid-frame with a body nothing will read, is retired.
    /// </summary>
    [Fact]
    public async Task AnOversizedResponseToAWrite_IsProtocolAndIsNeverReSent()
    {
        int writes = 0;
        await using var node = FakeCeleriantServer.Start(async (session, messageType, body) =>
        {
            if (messageType != MessageTypes.Requests.Write)
                return;

            if (Interlocked.Increment(ref writes) == 1)
            {
                await session.SendRawAsync(OversizedHeader());   // header only: body never comes
                return;
            }

            await WriteOkAsync(session);
        });

        await using var pool = new CeleriantPool(new CeleriantPoolOptions
        {
            Address = node.Address,
            MaxConnections = 1,
            ConnectionTimeout = TimeSpan.FromSeconds(2),
            RequestTimeout = TimeSpan.FromSeconds(2),
            MaxResponseSize = 1024,
        });

        var failure = await Record.ExceptionAsync(() => pool.WriteAsync(NewWrite()));

        Assert.True(
            failure is ProtocolException,
            $"an oversized response page must surface as ProtocolException; got {Describe(failure)}");
        Assert.Equal(1, Volatile.Read(ref writes));

        // The poisoned connection must not come back out of the pool.
        await pool.WriteAsync(NewWrite());
        Assert.True(
            node.ConnectionsAccepted >= 2,
            "the connection that read an oversized header was reused; it is mid-frame and must be retired");
        Assert.Equal(2, Volatile.Read(ref writes));
    }

    // -------------------------------------------------------------------------
    // Request size check vs the walk
    // -------------------------------------------------------------------------

    /// <summary>
    /// An oversized request is the caller's own payload error. It is knowable without a node, so
    /// it must not cost a dial, and the walk must not convert it into a routing failure.
    /// </summary>
    [Fact]
    public async Task AnOversizedRequest_ThrowsBeforeAnyNodeIsDialled()
    {
        await using var node = FakeCeleriantServer.Start((session, _, _) => WriteOkAsync(session));
        await using var pool = new CeleriantPool(new CeleriantPoolOptions
        {
            Address = node.Address,
            MaxConnections = 1,
            ConnectionTimeout = TimeSpan.FromSeconds(2),
            RequestTimeout = TimeSpan.FromSeconds(2),
            MaxRequestSize = 4096,
        });

        var failure = await Record.ExceptionAsync(() => pool.WriteAsync(NewWrite(payloadBytes: 64 * 1024)));

        Assert.True(
            failure is ArgumentException,
            $"an oversized request must be refused client-side; got {Describe(failure)}");
        Assert.True(
            node.ConnectionsAccepted == 0,
            $"the size guard sits behind the pool checkout, so a payload no node could accept "
            + $"still dialled {node.ConnectionsAccepted} node(s) and ran a handshake");
    }

    /// <summary>The same payload error must not be masked by the state of the cluster.</summary>
    [Fact]
    public async Task AnOversizedRequest_IsNotMaskedByAnUnreachableNode()
    {
        var (dead, _) = LeaderRoutingFakes.DeadAddress();
        await using var pool = new CeleriantPool(new CeleriantPoolOptions
        {
            Address = dead,
            MaxConnections = 1,
            ConnectionTimeout = TimeSpan.FromSeconds(1),
            RequestTimeout = TimeSpan.FromSeconds(2),
            MaxRequestSize = 4096,
        });

        var failure = await Record.ExceptionAsync(() => pool.WriteAsync(NewWrite(payloadBytes: 64 * 1024)));

        Assert.True(
            failure is ArgumentException,
            $"an oversized request is refused without a node, so an unreachable cluster must not "
            + $"change what the caller is told; got {Describe(failure)}");
    }

    // -------------------------------------------------------------------------
    // Watch caps vs watch failover
    // -------------------------------------------------------------------------

    /// <summary>
    /// The pool's caps bound every candidate the watch dials, including the one it fails over to.
    /// </summary>
    [Fact]
    public async Task WatchCaps_ApplyToTheCandidateReachedByFailover()
    {
        await using var live = FakeCeleriantServer.Start(async (session, _, _) =>
        {
            await session.SendRawAsync(FakeServerSession.BuildFrame(
                MessageTypes.Responses.Watch, WireCodec.Serialize(new WatchResponse())));
            await session.SendRawAsync(OversizedWatchEvent());
        });

        var (dead, _) = LeaderRoutingFakes.DeadAddress();
        await using var pool = new CeleriantPool(new CeleriantPoolOptions
        {
            Address = dead,
            SeedAddresses = [live.Address],
            ConnectionTimeout = TimeSpan.FromSeconds(1),
            MaxResponseSize = 1024,
        });

        await using var watch = await pool.WatchAsync(new WatchRequest());

        Assert.Equal(live.Address, watch.Address);

        var failure = await Record.ExceptionAsync(() => watch.NextAsync(CancellationToken.None));

        Assert.True(
            failure is ProtocolException,
            $"the pool's MaxResponseSize must bound the watch connection reached by failover; "
            + $"got {Describe(failure)}");
    }

    // -------------------------------------------------------------------------
    // StartShard validation vs the pool checkout
    // -------------------------------------------------------------------------

    /// <summary>
    /// A negative StartShard is knowable without a node. Validating it only inside the pool's
    /// iterator core puts the checkout first, so the cluster's state decides what the caller is
    /// told about their own argument.
    /// </summary>
    [Fact]
    public async Task NegativeStartShardOnThePool_IsNotMaskedByTheCheckout()
    {
        var (dead, _) = LeaderRoutingFakes.DeadAddress();
        await using var pool = new CeleriantPool(new CeleriantPoolOptions
        {
            Address = dead,
            ConnectionTimeout = TimeSpan.FromSeconds(1),
        });

        var failure = await Record.ExceptionAsync(async () =>
        {
            await foreach (var _ in pool.ListOrgsAsync(new ListOptions { StartShard = -1 }))
            {
            }
        });

        Assert.True(
            failure is ArgumentOutOfRangeException,
            $"a negative StartShard must be rejected before the pool checkout; got {Describe(failure)}");
    }

    /// <summary>And it must cost no connection even when the cluster is healthy.</summary>
    [Fact]
    public async Task NegativeStartShardOnThePool_DialsNoNode()
    {
        await using var node = FakeCeleriantServer.Start(FakeServerProtocol.EchoDetails());
        await using var pool = new CeleriantPool(new CeleriantPoolOptions
        {
            Address = node.Address,
            ConnectionTimeout = TimeSpan.FromSeconds(2),
        });

        await Record.ExceptionAsync(async () =>
        {
            await foreach (var _ in pool.ListOrgsAsync(new ListOptions { StartShard = -1 }))
            {
            }
        });

        Assert.True(
            node.ConnectionsAccepted == 0,
            $"rejecting a negative StartShard dialled {node.ConnectionsAccepted} node(s)");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static CeleriantPoolOptions IdentifiedOptions(string address) => new()
    {
        Address = address,
        MaxConnections = 1,
        ConnectionTimeout = TimeSpan.FromSeconds(2),
        RequestTimeout = TimeSpan.FromSeconds(2),
        IdentityConfig = ClientIdentityConfig.FromClientId(Guid.NewGuid()),
    };

    private static FakeCeleriantServer.RequestHandler ShaOnlyIdentifyThenWriteOk()
        => async (session, messageType, body) =>
        {
            if (messageType == MessageTypes.Requests.Identify)
            {
                await session.SendFrameAsync(MessageTypes.Responses.Identify, ShaOnlyIdentifyBody(WireCodec.Deserialize<IdentifyRequest>(body).CorrelationId));
                return;
            }

            await WriteOkAsync(session);
        };

    private static Task WriteOkAsync(FakeServerSession session)
        => session.SendFrameAsync(
            MessageTypes.Responses.Write,
            WireCodec.Serialize(new WriteResponse { CorrelationId = CorrelationId, MaxAggregateVersion = 1 }));

    private static ClientRequest Details()
        => new ClientRequest.AggregateDetails(FakeServerProtocol.Details(FakeServerProtocol.NewKey()));

    private static WriteRequest NewWrite(int payloadBytes = 8) => new()
    {
        CorrelationId = CorrelationId,
        ClientId = Guid.NewGuid(),
        Writes = new Dictionary<AggregateKey, SingleAggregateWrite>
        {
            [FakeServerProtocol.NewKey()] = new SingleAggregateWrite
            {
                Events = [new AggregateEvent
                {
                    EventTimestamp = DateTimeOffset.UtcNow,
                    EventTypeMajor = 1,
                    EventValue = new byte[payloadBytes],
                }],
                AllowCreate = true,
            },
        },
    };

    /// <summary>A 17-byte header promising far more body than the configured cap allows.</summary>
    private static byte[] OversizedHeader()
    {
        var header = new byte[WireHeader.Size];
        WireHeader
            .ForRequest(WireHeader.ProtocolVersionV5, MessageTypes.Responses.Write, 2_000_000)
            .WriteTo(header);
        return header;
    }

    private static byte[] OversizedWatchEvent()
        => FakeServerSession.BuildFrame(
            MessageTypes.Responses.Watch,
            WireCodec.Serialize(new WatchResponse
            {
                Events = Enumerable.Range(0, 200)
                    .Select(_ => new WatchResponseEvent
                    {
                        OrgId = Guid.NewGuid(),
                        AggregateTypeId = Guid.NewGuid(),
                        AggregateId = Guid.NewGuid(),
                        Operation = WatchOperationType.Write,
                        FromAggregateVersion = 1,
                        ToAggregateVersion = 1,
                    })
                    .ToArray(),
            }));

    private static byte[] ShaOnlyIdentifyBody(Guid? correlation)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new MessagePackWriter(buffer);
        writer.WriteArrayHeader(5);
        CeleriantNullableGuidFormatter.Instance.Serialize(ref writer, correlation, WireCodec.Options);
        writer.WriteNil();               // client_id
        writer.WriteNil();               // access_level
        writer.Write(ShaUnderTest);      // compression_dict_sha256
        writer.WriteNil();               // compression_dict_bytes
        writer.Flush();
        return buffer.WrittenSpan.ToArray();
    }

    private static string Describe(Exception? failure)
        => failure is null ? "no exception at all" : $"{failure.GetType().Name}: {failure.Message}";
}
