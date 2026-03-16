using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when a write is rejected due to an optimistic concurrency violation (error 2003).
/// The aggregate has been modified since you last read it.
/// Re-read from <see cref="CurrentBatchIndex"/>, re-validate your domain logic, and retry.
/// </summary>
public class WriteOccException : WriteErrorException
{
    /// <summary>
    /// The batch index you expected the aggregate to be at.
    /// </summary>
    public long ExpectedBatchIndex { get; }

    /// <summary>
    /// The batch index the aggregate is actually at on the server.
    /// Re-read from this index to catch up before retrying.
    /// </summary>
    public long CurrentBatchIndex { get; }

    public WriteOccException(ErrorResponse error) : base(error)
    {
        ExpectedBatchIndex = error.GetLong("expected_event_batch_index") ?? 0;
        CurrentBatchIndex = error.GetLong("current_event_batch_index") ?? 0;
    }
}
