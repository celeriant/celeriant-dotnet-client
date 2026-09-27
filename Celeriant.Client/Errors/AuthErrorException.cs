using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when the server returns an authentication or authorization error.
/// Covers AUTH_INSUFFICIENT_PERMISSIONS (10007) and IDENTIFY_* errors (10001-10003); AUTH_REQUIRED (10005)
/// and AUTH_INVALID_KEY (10006) surface as the subclasses <see cref="AuthRequiredException"/> and
/// <see cref="AuthInvalidKeyException"/>.
/// </summary>
public class AuthErrorException : CeleriantErrorException
{
    public AuthErrorException(ErrorResponse error) : base(error)
    {
    }
}
