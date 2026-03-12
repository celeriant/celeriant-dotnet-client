using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when the server returns an internal error (replication failure, fsync error,
/// cache error, disk read error, etc.). These typically indicate a server-side problem
/// that the client cannot resolve.
/// </summary>
public class ServerInternalErrorException : CeleriantErrorException
{
    /// <summary>
    /// Diagnostic detail from the server, if available.
    /// </summary>
    public string? Detail { get; }

    public ServerInternalErrorException(ErrorResponse error) : base(error)
    {
        Detail = error.GetString("detail");
    }
}
