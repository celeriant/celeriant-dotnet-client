namespace Celeriant.Transport;

/// <summary>
/// Lets each product keep its own exception hierarchy while sharing the transport. The connection
/// throws transport-level failures through this factory, so a storage caller still sees
/// <c>CeleriantTimeoutException</c> and a queue caller still sees <c>QueueTimeoutException</c>.
/// </summary>
public interface ITransportExceptionFactory
{
    /// <summary>A request on an established connection exceeded its timeout.</summary>
    Exception Timeout(string message);

    /// <summary>Connection establishment (dial, TLS handshake) exceeded its timeout.
    /// Failover-class for callers, unlike a request timeout.</summary>
    Exception ConnectTimeout(string message);

    /// <summary>The connection could not be established, was closed mid-operation, or failed I/O.</summary>
    Exception ConnectionFailed(string message, Exception? inner = null);

    /// <summary>The peer violated the wire protocol (bad frame, unexpected type, undecodable body).</summary>
    Exception Protocol(string message, Exception? inner = null);

    /// <summary>
    /// The pool itself refused before any node was contacted: its circuit breaker is open, or it
    /// has been disposed. Purely local, so it says nothing about the node's health and a caller
    /// must not read it as "this node is down".
    /// </summary>
    Exception PoolUnavailable(string address, string reason)
        => ConnectionFailed($"Connection pool for {address} is unavailable: {reason}.");

    /// <summary>
    /// The request was fully written and no response came back. The node may or may not have
    /// applied it, so it must never be re-sent.
    /// </summary>
    Exception RequestOutcomeUnknown(string message, Exception? inner = null)
        => ConnectionFailed(message, inner);
}
