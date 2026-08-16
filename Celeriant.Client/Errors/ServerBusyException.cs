using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when the server is too busy to handle the request.
/// The client should retry after a brief backoff or try another node.
/// </summary>
public class ServerBusyException : CeleriantClientException
{
    /// <summary>
    /// The raw error response from the server.
    /// </summary>
    public ErrorResponse Error { get; }

    public ServerBusyException(ErrorResponse error)
        : base("Server busy: retry after backoff")
    {
        Error = error;
    }
}
