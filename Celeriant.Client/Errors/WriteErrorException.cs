using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when the server returns an error during a write operation.
/// </summary>
public class WriteErrorException : CeleriantErrorException
{
    /// <summary>
    /// The client event index of the offending event (error 2001 zero event type).
    /// </summary>
    public long? ClientEventIndex { get; }

    /// <summary>
    /// The last accepted client event index (error 2002 idempotency violation).
    /// </summary>
    public long? LastClientEventIndex { get; }

    /// <summary>
    /// The client event index that was attempted (error 2002 idempotency violation).
    /// </summary>
    public long? AttemptedClientEventIndex { get; }

    /// <summary>
    /// The expected event batch index (error 2003 optimistic concurrency violation).
    /// </summary>
    public long? ExpectedEventBatchIndex { get; }

    /// <summary>
    /// The current event batch index on the server (error 2003 optimistic concurrency violation).
    /// </summary>
    public long? CurrentEventBatchIndex { get; }

    public WriteErrorException(ErrorResponse error) : base(error)
    {
        ClientEventIndex = error.GetLong("client_event_index");
        LastClientEventIndex = error.GetLong("last_client_event_index");
        AttemptedClientEventIndex = error.GetLong("attempted_client_event_index");
        ExpectedEventBatchIndex = error.GetLong("expected_event_batch_index");
        CurrentEventBatchIndex = error.GetLong("current_event_batch_index");
    }
}
