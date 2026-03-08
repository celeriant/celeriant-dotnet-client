namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when a TCP connection to the Celeriant server cannot be established
/// or is lost during an active request.
/// </summary>
public class ConnectionFailedException : CeleriantClientException
{
    public ConnectionFailedException(string message)
        : base(message) { }

    public ConnectionFailedException(string message, Exception innerException)
        : base(message, innerException) { }
}
