using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when the server rejects the presented API key (error 10006, AUTH_INVALID_KEY):
/// the key is unknown, or the server has no API keys configured at all.
/// The server closes the connection after this reply.
/// </summary>
public class AuthInvalidKeyException(ErrorResponse error) : AuthErrorException(error);
