using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when the server returns a generic application-level error response
/// that is not handled by a more specific exception type.
/// </summary>
public class CeleriantErrorException : CeleriantClientException
{
    /// <summary>
    /// The error response from the server containing the error code and message.
    /// </summary>
    public ErrorResponse Error { get; }

    public CeleriantErrorException(ErrorResponse error)
        : base($"Server returned error {error.ErrorCode}: {error.ErrorMessage}")
    {
        Error = error;
    }
}
