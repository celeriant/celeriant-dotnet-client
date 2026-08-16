namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when connection establishment (dial, TLS handshake) exceeds the configured timeout.
/// Failover-class: routing treats it like <see cref="ConnectionFailedException"/> and tries the
/// next node, unlike a <see cref="CeleriantTimeoutException"/> on an established connection.
/// Mirrors the Rust client's ConnectionTimeout / RequestTimeout split.
/// </summary>
public class ConnectionTimeoutException : CeleriantTimeoutException
{
    public ConnectionTimeoutException(string message)
        : base(message) { }

    public ConnectionTimeoutException(string message, Exception innerException)
        : base(message, innerException) { }
}
