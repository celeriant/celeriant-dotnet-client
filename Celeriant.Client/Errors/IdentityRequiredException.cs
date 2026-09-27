using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when the server requires a verified client identity (code 10004). Identity is fixed when
/// a connection opens: configure it with <c>CeleriantPoolOptions.IdentityConfig</c> or
/// <c>WatchOptions.IdentityConfig</c>, or call <c>IdentifyAsync</c> on a fresh
/// <c>CeleriantClient</c> before its first request. The server's own text is in <see cref="Error"/>.
/// </summary>
public class IdentityRequiredException : CeleriantClientException
{
    /// <summary>
    /// The raw error response from the server.
    /// </summary>
    public ErrorResponse Error { get; }

    public IdentityRequiredException(ErrorResponse error)
        : base("Server requires a verified client identity. Configure one when connecting: "
               + "CeleriantPoolOptions.IdentityConfig, WatchOptions.IdentityConfig, or IdentifyAsync "
               + "on a fresh CeleriantClient before its first request.")
    {
        Error = error;
    }
}
