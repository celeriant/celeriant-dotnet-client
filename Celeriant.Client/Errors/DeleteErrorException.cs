using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when the server returns an error during a delete operation.
/// </summary>
public class DeleteErrorException : CeleriantErrorException
{
    /// <summary>
    /// The expected event batch index (error 4002 optimistic concurrency violation).
    /// </summary>
    public long? ExpectedEventBatchIndex { get; }

    /// <summary>
    /// The current event batch index on the server (error 4002 optimistic concurrency violation).
    /// </summary>
    public long? CurrentEventBatchIndex { get; }

    public DeleteErrorException(ErrorResponse error) : base(error)
    {
        ExpectedEventBatchIndex = error.GetLong("expected_event_batch_index");
        CurrentEventBatchIndex = error.GetLong("current_event_batch_index");
    }
}
