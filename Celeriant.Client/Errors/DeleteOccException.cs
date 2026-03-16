using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when a delete is rejected due to an optimistic concurrency violation (error 4002).
/// The aggregate has been modified since you last read it.
/// Re-read from <see cref="CurrentBatchIndex"/>, re-validate, and retry.
/// </summary>
public class DeleteOccException : DeleteErrorException
{
    /// <summary>
    /// The batch index you expected the aggregate to be at.
    /// </summary>
    public long ExpectedBatchIndex { get; }

    /// <summary>
    /// The batch index the aggregate is actually at on the server.
    /// </summary>
    public long CurrentBatchIndex { get; }

    public DeleteOccException(ErrorResponse error) : base(error)
    {
        ExpectedBatchIndex = error.GetLong("expected_event_batch_index") ?? 0;
        CurrentBatchIndex = error.GetLong("current_event_batch_index") ?? 0;
    }
}
