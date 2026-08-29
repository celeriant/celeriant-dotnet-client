namespace Celeriant.Client.Responses;

/// <summary>
/// The type of operation that triggered a watch event.
/// Matches Rust <c>AggregateWatchEventOperation</c> discriminants.
/// </summary>
public enum WatchOperationType : byte
{
    /// <summary>The aggregate was deleted.</summary>
    Delete = 0,
    /// <summary>Events were appended.</summary>
    Write = 1,
    /// <summary>A read occurred (not normally surfaced to watchers).</summary>
    Read = 2,
    /// <summary>The aggregate was trimmed.</summary>
    TrimStart = 3,
    /// <summary>Aggregate details were requested (not normally surfaced to watchers).</summary>
    Details = 4,
    /// <summary>The aggregate was created. Note the first write to a new aggregate emits <b>two</b>
    /// notifications — a <see cref="Create"/> (with no version range) followed by a <see cref="Write"/>
    /// for the first batch — so a subscription to both operation types sees the aggregate's creation
    /// twice.</summary>
    Create = 5,
}
