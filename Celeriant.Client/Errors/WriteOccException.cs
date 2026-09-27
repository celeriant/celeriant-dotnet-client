using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when a write is rejected due to an optimistic concurrency violation (error 2003).
/// The aggregate has been modified since you last read it.
/// Re-read from <see cref="CurrentAggregateVersion"/>, re-validate your domain logic, and retry.
/// </summary>
public class WriteOccException : WriteErrorException
{
    /// <summary>
    /// The aggregate version you expected the aggregate to be at.
    /// </summary>
    public long ExpectedVersion { get; }

    public IReadOnlyList<AggregateConflict> Conflicts { get; } = Array.Empty<AggregateConflict>();

    /// <summary>
    /// The aggregate version the aggregate is actually at on the server.
    /// Re-read from this version to catch up before retrying.
    /// </summary>
    public long CurrentAggregateVersion { get; }

    internal WriteOccException(ErrorResponse error, IReadOnlyList<AggregateConflict> conflicts) : base(error)
    {
        Conflicts = conflicts;
        if (conflicts.Count == 0) return;
        ExpectedVersion = conflicts[0].Expected;
        CurrentAggregateVersion = conflicts[0].Current;
    }

    public WriteOccException(ErrorResponse error) : base(error)
    {
        ExpectedVersion = error.GetLong("expected_version") ?? 0;
        CurrentAggregateVersion = error.GetLong("current_aggregate_version") ?? 0;
    }
}
