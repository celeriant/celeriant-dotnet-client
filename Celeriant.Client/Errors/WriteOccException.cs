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

    /// <summary>
    /// The aggregate version the aggregate is actually at on the server.
    /// Re-read from this version to catch up before retrying.
    /// </summary>
    public long CurrentAggregateVersion { get; }

    public WriteOccException(ErrorResponse error) : base(error)
    {
        ExpectedVersion = error.GetLong("expected_version") ?? 0;
        CurrentAggregateVersion = error.GetLong("current_aggregate_version") ?? 0;
    }
}
