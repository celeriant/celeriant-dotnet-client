using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when a write is rejected because the client seq has already been accepted (error 2002).
///
/// <para>
/// This is safe to ignore <b>only</b> when you are retrying the identical write — the earlier
/// attempt landed. It is <b>not</b> safe to ignore when a client seq may have been reused for
/// different data, or when a <c>ClientId</c> is shared across concurrent writers: in those cases the
/// event you just attempted was rejected and is <b>not</b> stored. The exception cannot tell the two
/// apart. When in doubt, point-read the seq (<c>ReadFilters</c> with <c>MinClientSeq</c>/
/// <c>MaxClientSeq</c> and <c>IncludeClientId</c>) and compare the <see cref="AggregateEvent.EventId"/>:
/// yours means the prior attempt landed, a sibling's means your event never landed — re-derive and retry.
/// </para>
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
