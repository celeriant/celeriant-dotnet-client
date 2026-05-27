using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when a write is rejected because the client seq has already been accepted (error 2002).
/// This means the event was already written — the write is a duplicate and was safely rejected.
/// No action is needed; the original write succeeded.
/// </summary>
public class IdempotencyViolationException : WriteErrorException
{
    /// <summary>
    /// The highest client seq the server has already accepted for this client.
    /// Events up to and including this seq have been durably written.
    /// </summary>
    public long LastAcceptedClientSeq { get; }

    /// <summary>
    /// The client seq that was attempted in this (rejected) write.
    /// </summary>
    public long AttemptedClientSeq { get; }

    public IdempotencyViolationException(ErrorResponse error) : base(error)
    {
        LastAcceptedClientSeq = error.GetLong("last_client_seq") ?? 0;
        AttemptedClientSeq = error.GetLong("attempted_client_seq") ?? 0;
    }
}
