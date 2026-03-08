namespace Celeriant.Client.Responses;

/// <summary>
/// The type of operation that triggered a watch event.
/// Matches Rust <c>AggregateWatchEventOperation</c> discriminants.
/// </summary>
public enum WatchOperationType : byte
{
    Delete = 0,
    Write = 1,
    Read = 2,
    TrimStart = 3,
    Details = 4,
    Create = 5,
}
