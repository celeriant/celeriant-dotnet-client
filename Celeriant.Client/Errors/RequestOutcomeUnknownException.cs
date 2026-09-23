namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when a request was fully written to the socket and no response came back: the peer
/// closed or reset, or the read failed or timed out.
///
/// <para>
/// The node may or may not have applied the request. The client cannot know whether the operation
/// was idempotent, so it never re-sends it: the leader walk returns this to the caller instead of
/// trying another node, and the connection is discarded. Only the caller can decide whether
/// re-issuing is safe. A read is always safe to retry, so read routing treats it as just another
/// broken candidate.
/// </para>
/// </summary>
public class RequestOutcomeUnknownException : CeleriantClientException
{
    public RequestOutcomeUnknownException(string message)
        : base(message) { }

    public RequestOutcomeUnknownException(string message, Exception innerException)
        : base(message, innerException) { }
}
