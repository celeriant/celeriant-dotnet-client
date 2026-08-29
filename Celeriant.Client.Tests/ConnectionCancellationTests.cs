using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;

namespace Celeriant.Client.Tests;

/// <summary>
/// A caller token that fires between the request write and the response read leaves the stream
/// framing indeterminate. The connection must report itself unhealthy afterwards, so the pool
/// discards it instead of handing it to the next borrower.
/// </summary>
public class ConnectionCancellationTests
{
    [Fact]
    public async Task SendRequestAsync_CallerTokenCancelledOnTheResponseRead_PoisonsTheConnection()
    {
        var parked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            var request = FakeServerProtocol.DecodeDetails(body);
            parked.SetResult();
            await release.Task;
            await session.SendFrameAsync(
                MessageTypes.Responses.AggregateDetails,
                FakeServerProtocol.DetailsAnswer(request));
        });

        await using var client = await CeleriantClient.ConnectAsync(server.Address, connectionTimeout: TimeSpan.FromSeconds(5));

        using var cts = new CancellationTokenSource();
        var inflight = client.SendRequestAsync(
            new ClientRequest.AggregateDetails(FakeServerProtocol.Details(FakeServerProtocol.NewKey())),
            cts.Token);

        await parked.Task;
        await cts.CancelAsync();
        Assert.NotNull(await Record.ExceptionAsync(() => inflight));

        Assert.True(
            client.IsPoisoned,
            "a connection abandoned between the request write and the response read still has the "
            + "undrained response on the wire, so it must not report itself healthy");

        release.SetResult();
    }

    [Fact]
    public async Task IdentifyAsync_CallerTokenCancelledOnTheResponseRead_PoisonsTheConnection()
    {
        var parked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var server = FakeCeleriantServer.Start(async (_, _, _) =>
        {
            parked.SetResult();
            await release.Task;
        });

        await using var client = await CeleriantClient.ConnectAsync(server.Address, connectionTimeout: TimeSpan.FromSeconds(5));

        using var cts = new CancellationTokenSource();
        var inflight = client.IdentifyAsync(ClientIdentityConfig.FromClientId(Guid.NewGuid()), cts.Token);

        await parked.Task;
        await cts.CancelAsync();
        Assert.NotNull(await Record.ExceptionAsync(() => inflight));

        Assert.True(
            client.IsPoisoned,
            "an abandoned Identify exchange leaves the same undrained response on the wire");

        release.SetResult();
    }

    /// <summary>
    /// The synchronous send path must gate on the same health check as the async one; gating on the
    /// raw poison field alone lets it write onto a stream the async guard refuses and read back the
    /// abandoned request's reply.
    /// </summary>
    [Fact]
    public Task SynchronousSendRequest_RefusesADirtyStream() =>
        WithAbandonedStreamAsync(scenario =>
        {
            var nextKey = FakeServerProtocol.NewKey();
            ClientResponse? got = null;
            var thrown = Record.Exception(() => got = scenario.Client.SendRequest(
                new ClientRequest.AggregateDetails(FakeServerProtocol.Details(nextKey))));

            if (thrown is null)
            {
                var details = Assert.IsType<ClientResponse.AggregateDetails>(got);
                Assert.Fail(
                    "the synchronous path wrote onto a stream the async guard refuses and read the "
                    + $"abandoned request's reply: LastClientId {details.Value.LastClientId} is the "
                    + $"CANCELLED request's aggregate {scenario.AbandonedKey.AggregateId}, not the one just "
                    + $"asked for ({nextKey.AggregateId})");
            }

            return Task.CompletedTask;
        });

    /// <summary>
    /// The synchronous send path must refuse a dirty stream before it writes anything, because
    /// correlation validation only catches the crosstalk after the server has already executed the
    /// request — which for a write is a side effect that cannot be taken back.
    /// </summary>
    [Fact]
    public Task SynchronousSendRequest_OnADirtyStream_SendsNothingToTheServer() =>
        WithAbandonedStreamAsync(async scenario =>
        {
            Assert.NotNull(Record.Exception(() => scenario.Client.SendRequest(
                new ClientRequest.AggregateDetails(FakeServerProtocol.Details(FakeServerProtocol.NewKey())))));

            await Task.Delay(100);
            Assert.Equal(1, scenario.RequestsReceived());
        });

    /// <summary>
    /// A directly owned (unpooled) client must refuse to send on a stream it abandoned, and refuse
    /// up front: a refusal that arrives only after the write has gone out is too late for a server
    /// that has already acted on it.
    /// </summary>
    [Fact]
    public Task UnpooledClient_RefusesToSendOnAStreamItAbandoned() =>
        WithAbandonedStreamAsync(async scenario =>
        {
            Assert.NotNull(await Record.ExceptionAsync(() => scenario.Client.SendRequestAsync(
                new ClientRequest.AggregateDetails(FakeServerProtocol.Details(FakeServerProtocol.NewKey())))));

            await Task.Delay(100);
            Assert.Equal(1, scenario.RequestsReceived());
        });

    // -------------------------------------------------------------------------
    // Scenario
    // -------------------------------------------------------------------------

    private sealed record AbandonedStream(
        CeleriantClient Client,
        AggregateKey AbandonedKey,
        Func<int> RequestsReceived);

    /// <summary>
    /// Cancel a request parked on its response read, let the server put the abandoned reply on the
    /// wire exactly as a real server would, then hand <paramref name="run"/> the still-open client
    /// and a live count of the request frames the server has received.
    /// </summary>
    private static async Task WithAbandonedStreamAsync(Func<AbandonedStream, Task> run)
    {
        var parked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var abandonedReplySent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        int arrived = 0;

        await using var server = FakeCeleriantServer.Start(async (session, _, body) =>
        {
            var request = FakeServerProtocol.DecodeDetails(body);
            byte[] answer = FakeServerProtocol.DetailsAnswer(request);

            if (Interlocked.Increment(ref arrived) == 1)
            {
                parked.SetResult();
                await release.Task;
                await session.SendFrameAsync(MessageTypes.Responses.AggregateDetails, answer);
                abandonedReplySent.SetResult();
                return;
            }

            await session.SendFrameAsync(MessageTypes.Responses.AggregateDetails, answer);
        });

        await using var client = await CeleriantClient.ConnectAsync(
            server.Address, connectionTimeout: TimeSpan.FromSeconds(5));

        var abandonedKey = FakeServerProtocol.NewKey();
        using var cts = new CancellationTokenSource();
        var inflight = client.SendRequestAsync(
            new ClientRequest.AggregateDetails(FakeServerProtocol.Details(abandonedKey)), cts.Token);

        await parked.Task;
        await cts.CancelAsync();
        Assert.NotNull(await Record.ExceptionAsync(() => inflight));

        // Put the abandoned reply on the wire and give it time to reach the client's socket.
        release.SetResult();
        await abandonedReplySent.Task;
        await Task.Delay(50);

        await run(new AbandonedStream(client, abandonedKey, () => Volatile.Read(ref arrived)));
    }
}
