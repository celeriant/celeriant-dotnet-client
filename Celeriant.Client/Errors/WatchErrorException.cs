using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when the server returns an error during a watch operation.
/// </summary>
public class WatchErrorException : CeleriantErrorException
{
    /// <summary>
    /// The requested latency in milliseconds (error 8001 latency too high).
    /// </summary>
    public long? RequestedMs { get; }

    /// <summary>
    /// The maximum allowed latency in milliseconds (error 8001 latency too high).
    /// </summary>
    public long? MaxMs { get; }

    /// <summary>
    /// The current number of active subscribers (error 8005 too many subscribers).
    /// </summary>
    public long? ActiveSubscribers { get; }

    /// <summary>
    /// The maximum allowed subscribers (error 8005 too many subscribers).
    /// </summary>
    public long? MaxSubscribers { get; }

    public WatchErrorException(ErrorResponse error) : base(error)
    {
        RequestedMs = error.GetLong("requested_ms");
        MaxMs = error.GetLong("max_ms");
        ActiveSubscribers = error.GetLong("active_subscribers");
        MaxSubscribers = error.GetLong("max_subscribers");
    }
}
