namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when a wire protocol violation is detected, such as an unexpected
/// message type, unsupported protocol version, or MessagePack deserialization failure.
/// </summary>
public class ProtocolException : CeleriantClientException
{
    public ProtocolException(string message)
        : base(message) { }

    public ProtocolException(string message, Exception innerException)
        : base(message, innerException) { }
}
