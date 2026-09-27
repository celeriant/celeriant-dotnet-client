using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when a delete is rejected due to an optimistic concurrency violation (error 4002).
/// The aggregate has been modified since you last read it.
/// Re-read from <see cref="CurrentAggregateVersion"/>, re-validate, and retry.
/// </summary>
public class DeleteOccException : DeleteErrorException
{
    /// <summary>
    /// The aggregate version you expected the aggregate to be at.
    /// </summary>
    public long ExpectedVersion { get; }

    public IReadOnlyList<AggregateConflict> Conflicts { get; } = Array.Empty<AggregateConflict>();

    /// <summary>
    /// The aggregate version the aggregate is actually at on the server.
    /// </summary>
    public long CurrentAggregateVersion { get; }

    internal DeleteOccException(ErrorResponse error, IReadOnlyList<AggregateConflict> conflicts) : base(error)
    {
        Conflicts = conflicts;
        if (conflicts.Count == 0) return;
        ExpectedVersion = conflicts[0].Expected;
        CurrentAggregateVersion = conflicts[0].Current;
    }

    public DeleteOccException(ErrorResponse error) : base(error)
    {
        ExpectedVersion = error.GetLong("expected_version") ?? 0;
        CurrentAggregateVersion = error.GetLong("current_aggregate_version") ?? 0;
    }
}
