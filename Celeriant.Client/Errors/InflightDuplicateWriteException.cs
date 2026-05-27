using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when a write is rejected because an identical write (same client seq) is fsynced
/// but its replication is not yet confirmed (error 2013).
///
/// <para>
/// Unlike <see cref="IdempotencyViolationException"/>, the prior write is <b>not yet durable</b>:
/// treating this as success would risk a false acknowledgement if that write later rolls back.
/// Hold the <c>client_seq</c> constant and retry after a short backoff; the write becomes either a
/// confirmed success or an <see cref="IdempotencyViolationException"/> once it is durable.
/// </para>
/// </summary>
public class InflightDuplicateWriteException : WriteErrorException
{
    /// <summary>
    /// The highest client seq the server currently knows about for this client.
    /// </summary>
    public long LastClientSeq { get; }

    /// <summary>
    /// The client seq that was attempted in this (rejected) write.
    /// </summary>
    public long AttemptedClientSeq { get; }

    public InflightDuplicateWriteException(ErrorResponse error) : base(error)
    {
        LastClientSeq = error.GetLong("last_client_seq") ?? 0;
        AttemptedClientSeq = error.GetLong("attempted_client_seq") ?? 0;
    }
}
