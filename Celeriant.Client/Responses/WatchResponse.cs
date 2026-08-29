using MessagePack;

namespace Celeriant.Client.Responses;

/// <summary>One batch of watch notifications, as returned by <c>WatchConnection.NextAsync</c>. Only
/// batches with at least one event are surfaced; empty keep-alive frames are consumed internally.</summary>
[MessagePackObject]
public sealed class WatchResponse
{
    /// <summary>The change notifications in this batch, oldest first.</summary>
    [Key(0)]
    public WatchResponseEvent[] Events { get; init; } = [];
}
