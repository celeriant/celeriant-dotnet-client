namespace Celeriant.Client.Errors;

/// <summary>
/// Base exception for all Celeriant client errors.
/// </summary>
public class CeleriantClientException : Exception
{
    public CeleriantClientException() { }

    public CeleriantClientException(string message)
        : base(message) { }

    public CeleriantClientException(string message, Exception innerException)
        : base(message, innerException) { }
}
