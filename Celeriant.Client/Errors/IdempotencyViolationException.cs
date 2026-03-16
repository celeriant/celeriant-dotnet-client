using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when a write is rejected because the client event index has already been accepted (error 2002).
/// This means the event was already written — the write is a duplicate and was safely rejected.
/// No action is needed; the original write succeeded.
/// </summary>
public class IdempotencyViolationException : WriteErrorException
{
    /// <summary>
    /// The highest client event index the server has already accepted for this client.
    /// Events up to and including this index have been durably written.
    /// </summary>
    public long LastAcceptedClientEventIndex { get; }

    /// <summary>
    /// The client event index that was attempted in this (rejected) write.
    /// </summary>
    public long AttemptedIndex { get; }

    public IdempotencyViolationException(ErrorResponse error) : base(error)
    {
        LastAcceptedClientEventIndex = error.GetLong("last_client_event_index") ?? 0;
        AttemptedIndex = error.GetLong("attempted_client_event_index") ?? 0;
    }
}
