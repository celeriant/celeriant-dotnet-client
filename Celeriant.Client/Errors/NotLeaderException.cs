using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when the server responds that it is not the current leader for a write,
/// trim, or delete operation. The <see cref="LeaderAddress"/> may point to the
/// current leader if the server included it in the error response.
/// </summary>
public class NotLeaderException : CeleriantClientException
{
    /// <summary>
    /// The address of the current leader, if provided by the server.
    /// Format is "host:port". May be null if the server did not include it.
    /// </summary>
    public string? LeaderAddress { get; }

    /// <summary>
    /// The raw error response from the server.
    /// </summary>
    public ErrorResponse Error { get; }

    public NotLeaderException(ErrorResponse error, string? leaderAddress)
        : base($"Server is not the leader. Leader address: {leaderAddress ?? "unknown"}")
    {
        Error = error;
        LeaderAddress = leaderAddress;
    }
}
