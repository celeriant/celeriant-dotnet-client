using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when the server requires client identity verification before
/// processing the request. Call <c>IdentifyAsync</c> with a
/// <c>ClientIdentityConfig</c> to authenticate.
/// </summary>
public class IdentityRequiredException : CeleriantClientException
{
    /// <summary>
    /// The raw error response from the server.
    /// </summary>
    public ErrorResponse Error { get; }

    public IdentityRequiredException(ErrorResponse error)
        : base("Server requires identity verification. Call IdentifyAsync before sending requests.")
    {
        Error = error;
    }
}
