using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when the server returns an authentication or authorization error.
/// Covers AUTH_REQUIRED (10005), AUTH_INVALID_KEY (10006), AUTH_INSUFFICIENT_PERMISSIONS (10007),
/// and IDENTIFY_* errors (10001-10003).
/// </summary>
public class AuthErrorException : CeleriantErrorException
{
    public AuthErrorException(ErrorResponse error) : base(error)
    {
    }
}
