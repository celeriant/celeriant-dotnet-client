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

    public WatchErrorException(ErrorResponse error) : base(error)
    {
        RequestedMs = error.GetLong("requested_ms");
        MaxMs = error.GetLong("max_ms");
    }
}
