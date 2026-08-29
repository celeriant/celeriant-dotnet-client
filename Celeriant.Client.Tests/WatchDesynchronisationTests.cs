using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Watch;

namespace Celeriant.Client.Tests;

/// <summary>
/// <see cref="WatchConnection.NextAsync(TimeSpan, CancellationToken)"/> returns null for both a
/// deadline that expired on an empty socket and one that expired half way through a frame, and
/// that contract does not change. <see cref="WatchConnection.IsDesynchronised"/> is the only thing
/// that separates the two, so it must stay false for the quiet case and go true for the truncated
/// one; either half alone is satisfiable by a constant.
/// </summary>
public class WatchDesynchronisationTests
{
    /// <summary>The server's subscription ack: a watch frame carrying no events.</summary>
    private static readonly byte[] WatchAck =
        FakeServerSession.BuildFrame(MessageTypes.Responses.Watch, WireCodec.Serialize(new WatchResponse()));

    /// <summary>Part of a frame header, and nothing that ever completes it.</summary>
    private static readonly byte[] TruncatedFrame = WatchAck[..8];

    [Fact]
    public async Task NextAsync_DeadlineExpiresOnAQuietSocket_IsNotDesynchronised()
    {
        await using var server = FakeCeleriantServer.Start((session, _, _) => session.SendRawAsync(WatchAck));
        await using var watch = await ConnectWatchAsync(server);

        Assert.Null(await watch.NextAsync(TimeSpan.FromMilliseconds(150)));

        Assert.False(
            watch.IsDesynchronised,
            "a deadline expiring with nothing on the wire consumed no bytes, so the stream is still "
            + "on a message boundary and the subscription is usable");
    }

    [Fact]
    public async Task NextAsync_DeadlineExpiresPartWayThroughAFrame_IsDesynchronised()
    {
        var truncatedFrameSent = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        await using var server = FakeCeleriantServer.Start(async (session, _, _) =>
        {
            await session.SendRawAsync(WatchAck);
            await session.SendRawAsync(TruncatedFrame);
            truncatedFrameSent.SetResult();
        });

        await using var watch = await ConnectWatchAsync(server);

        // The partial bytes are on the wire before the read that consumes them is ever issued, and
        // the rest of that frame is never sent. The deadline therefore decides only WHEN the read
        // is abandoned, never how many bytes it had taken by then: there is nothing to race.
        await truncatedFrameSent.Task;

        Assert.Null(await watch.NextAsync(TimeSpan.FromSeconds(1)));

        Assert.True(
            watch.IsDesynchronised,
            "the read was abandoned half way through a frame, so the next one would start mid-message "
            + "and hand the caller garbage; a null return alone cannot tell a caller that");
    }

    private static Task<WatchConnection> ConnectWatchAsync(FakeCeleriantServer server)
        => WatchConnection.ConnectAsync(
            server.Address,
            new WatchRequest(),
            new WatchOptions { ConnectionTimeout = TimeSpan.FromSeconds(5) });
}
