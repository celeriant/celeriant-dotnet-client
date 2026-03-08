using MessagePack;

namespace Celeriant.Client.Responses;

[MessagePackObject]
public sealed class WatchResponse
{
    [Key(0)]
    public WatchResponseEvent[] Events { get; init; } = [];
}
