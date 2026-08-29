using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Transport;

namespace Celeriant.Client.Tests;

/// <summary>
/// A response is bound to the KIND of request, not only to its id.
///
/// <para>
/// The correlation id cannot cover every case on its own: <c>CarriesCorrelationId</c> exempts
/// <c>Watch</c>, whose frame has no correlation field on the wire at all, so without a type check
/// a Watch frame answering a read passes every other check and is handed back as a successful
/// read of a different shape. The <c>ProtocolException</c> a caller would eventually see comes
/// from <c>CeleriantPool</c>'s response mapper, one layer above
/// <c>NodeConnectionPool.ExecuteRequestAsync</c> and therefore AFTER its <c>await using</c> has
/// handed the connection back — too late for the retire predicate or <c>IsPoisoned</c> to stop
/// the desynchronised connection being reused. <c>VerifyResponseType</c> is the .NET counterpart
/// of the second thing that arms <c>stream_dirty</c> in the Rust client.
/// </para>
/// </summary>
public class ResponseTypeBindingTests
{
    private static readonly TimeSpan HangBudget = TimeSpan.FromSeconds(10);

    private static readonly Guid Correlation = Guid.Parse("00000000-0000-0000-0000-0000000000e1");

    /// <summary>
    /// The worst shape: a reply whose type carries NO correlation id at all, so the correlation
    /// check returns early and cannot see it. The reply must still be refused, and the connection
    /// retired — a frame this far out of step means every frame after it is too.
    /// </summary>
    [Fact]
    public async Task AWatchFrameAnsweringADetailsRequest_IsRefusedAndRetiresTheConnection()
    {
        int served = 0;

        await using var server = FakeCeleriantServer.Start((session, _, body) =>
        {
            // First request gets a Watch frame — wrong type, and a type with no correlation id.
            if (Interlocked.Increment(ref served) == 1)
                return session.SendFrameAsync(
                    MessageTypes.Responses.Watch,
                    WireCodec.Serialize(new WatchResponse()));

            var request = FakeServerProtocol.DecodeDetails(body);
            return session.SendFrameAsync(
                MessageTypes.Responses.AggregateDetails,
                FakeServerProtocol.DetailsAnswer(request));
        });

        await using var pool = SingleConnectionNodePool(server);

        var failure = await WithinBudgetAsync(
            () => Record.ExceptionAsync(() => pool.ExecuteRequestAsync(Details(), CancellationToken.None)),
            "the request answered with a Watch frame");

        Assert.IsType<ProtocolException>(failure);

        ClientResponse second = await WithinBudgetAsync(
            () => pool.ExecuteRequestAsync(Details(), CancellationToken.None),
            "the request after the mistyped reply");
        Assert.IsType<ClientResponse.AggregateDetails>(second);

        // A second TCP connection: the desynchronised one was retired rather than handed to the
        // next borrower, which would have read this request's answer off the previous reply.
        Assert.Equal(2, server.ConnectionsAccepted);
    }

    /// <summary>
    /// The same binding with a reply type that DOES carry a correlation id, echoed correctly. A
    /// right id on the wrong kind of frame is still a stream out of step, so the correct echo must
    /// not buy it a pass.
    /// </summary>
    [Fact]
    public async Task AWriteFrameWithTheRightCorrelationId_IsStillRefusedForADetailsRequest()
    {
        await using var server = FakeCeleriantServer.Start((session, _, _) =>
            session.SendFrameAsync(
                MessageTypes.Responses.Write,
                WireCodec.Serialize(new WriteResponse { CorrelationId = Correlation })));

        await using var pool = SingleConnectionNodePool(server);

        var failure = await WithinBudgetAsync(
            () => Record.ExceptionAsync(() => pool.ExecuteRequestAsync(Details(), CancellationToken.None)),
            "a details request answered with a Write response");

        Assert.IsType<ProtocolException>(failure);
    }

    private static NodeConnectionPool SingleConnectionNodePool(FakeCeleriantServer server)
        => new(
            server.Address,
            new CeleriantPoolOptions
            {
                Address = server.Address,
                MaxConnections = 1,
                ConnectionTimeout = TimeSpan.FromSeconds(5),
                RequestTimeout = TimeSpan.FromSeconds(10),
            },
            new DictCache());

    private static ClientRequest Details()
        => new ClientRequest.AggregateDetails(
            FakeServerProtocol.Details(FakeServerProtocol.NewKey(), Correlation));

    private static async Task<T> WithinBudgetAsync<T>(Func<Task<T>> action, string whatWouldHang)
    {
        var running = Task.Run(action);
        var finished = await Task.WhenAny(running, Task.Delay(HangBudget));
        Assert.True(
            ReferenceEquals(finished, running),
            $"{whatWouldHang} was still pending after {HangBudget.TotalSeconds:0}s");
        return await running;
    }
}
