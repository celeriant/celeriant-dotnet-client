namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when a request or connection attempt exceeds the configured timeout.
/// </summary>
public class CeleriantTimeoutException : CeleriantClientException
{
    public CeleriantTimeoutException(string message)
        : base(message) { }

    public CeleriantTimeoutException(string message, Exception innerException)
        : base(message, innerException) { }
}
