using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when the server returns a generic application-level error response
/// that is not handled by a more specific exception type.
/// </summary>
/// <remarks>
/// <see cref="NotLeaderException"/>, <see cref="ServerBusyException"/> and
/// <see cref="IdentityRequiredException"/> also represent server error responses but extend
/// <see cref="CeleriantClientException"/> directly, not this class — they are handled specially
/// (failover, backpressure retry, and identity respectively). Catching <c>CeleriantErrorException</c>
/// alone will not catch those three; to also absorb backpressure, catch <see cref="ServerBusyException"/>
/// explicitly, or catch <see cref="CeleriantClientException"/> for every server-side error at once.
/// </remarks>
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
