using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when the server has an API keys file and the client did not present an API key
/// (error 10005, AUTH_REQUIRED). Configure <c>ClientIdentityConfig</c> with an API key.
/// The server closes the connection after this reply.
/// </summary>
public class AuthRequiredException(ErrorResponse error) : AuthErrorException(error);
