using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;

namespace Celeriant.Client.Tests;

/// <summary>
/// A pool's <see cref="CeleriantPoolOptions.MaxResponseSize"/> bounds every connection it hands out,
/// and a watch connection is no exception: it is a dedicated connection the pool dials, so a watch
/// response page past the configured cap must throw <see cref="ProtocolException"/> exactly as a
/// pooled read does. The watch path builds its own <see cref="Watch.WatchOptions"/>, so this is the
/// one place a caller's memory bound could be silently dropped in favour of the client's 64 MB
/// default.
/// </summary>
public class WatchMaxResponseSizeTests
{
    /// <summary>The server's subscription ack: a watch frame carrying no events.</summary>
    private static readonly byte[] WatchAck =
        FakeServerSession.BuildFrame(MessageTypes.Responses.Watch, WireCodec.Serialize(new WatchResponse()));

    /// <summary>A watch event frame whose body is well past a 1 KiB cap, so the cap is what rejects it.</summary>
    private static readonly byte[] OversizedEvent = FakeServerSession.BuildFrame(
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

    [Fact]
    public async Task Pool_ConfiguredMaxResponseSize_IsHonoredOnWatchConnections()
    {
        await using var server = FakeCeleriantServer.Start(async (session, _, _) =>
        {
            await session.SendRawAsync(WatchAck);
            await session.SendRawAsync(OversizedEvent);
        });

        await using var pool = new CeleriantPool(new CeleriantPoolOptions
        {
            Address = server.Address,
            ConnectionTimeout = TimeSpan.FromSeconds(5),
            MaxResponseSize = 1024,
        });

        await using var watch = await pool.WatchAsync(new WatchRequest());

        var failure = await Record.ExceptionAsync(() => watch.NextAsync(CancellationToken.None));

        Assert.True(
            failure is ProtocolException,
            $"a watch response past the pool's 1 KiB MaxResponseSize must throw ProtocolException, "
            + $"not deliver the event; got {Describe(failure)}");
    }

    private static string Describe(Exception? failure)
        => failure is null ? "no exception at all" : failure.GetType().Name;
}