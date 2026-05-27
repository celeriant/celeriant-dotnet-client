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

    /// <summary>
    /// The aggregate version the aggregate is actually at on the server.
    /// </summary>
    public long CurrentAggregateVersion { get; }

    public DeleteOccException(ErrorResponse error) : base(error)
    {
        ExpectedVersion = error.GetLong("expected_version") ?? 0;
        CurrentAggregateVersion = error.GetLong("current_aggregate_version") ?? 0;
    }
}
