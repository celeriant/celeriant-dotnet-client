namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when the client's own connection pool for a node refused before any node was contacted:
/// its circuit breaker is open after a recent failed connection attempt, or the pool has been disposed.
///
/// <para>
/// Distinct from <see cref="ConnectionFailedException"/> on purpose. No byte left this process, so
/// it carries no evidence about the node's health: routing never retires a cached leader on it,
/// the write path returns it to the caller rather than walking to a follower that cannot serve a
/// write anyway, and retrying it can never duplicate a request.
/// </para>
/// </summary>
public class PoolUnavailableException : CeleriantClientException
{
    /// <summary>The node address whose pool refused.</summary>
    public string Address { get; }

    /// <summary>Why the pool refused, in the form "its circuit breaker is open".</summary>
    public string Reason { get; }

    public PoolUnavailableException(string address, string reason)
        : base($"Connection pool for {address} is unavailable: {reason}.")
    {
        Address = address;
        Reason = reason;
    }
}
