namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when a request was fully written to the socket and no response came back: the peer
/// closed or reset, or the read failed or timed out.
///
/// <para>
/// The node may or may not have applied the request. The client cannot know whether the operation
/// was idempotent, so it never re-sends it: the leader walk returns this to the caller instead of
/// trying another node, and the connection is discarded. Only the caller can decide whether
/// re-issuing is safe.
/// </para>
///
/// <para>
/// Reads are the exception, because a read changes nothing. The pool moves a lost read on to the next
/// node, and when no node is left it sends the read once more on another connection. A pool
/// <c>ReadAllAsync</c> stream sends a lost page once more to the same node and carries on. A read
/// still ends in this exception when the second attempt is lost too. A bare
/// <see cref="CeleriantClient"/> and the pool's list streams do not re-send.
/// </para>
/// </summary>
public class RequestOutcomeUnknownException : CeleriantClientException
{
    public RequestOutcomeUnknownException(string message)
        : base(message) { }

    public RequestOutcomeUnknownException(string message, Exception innerException)
        : base(message, innerException) { }
}
