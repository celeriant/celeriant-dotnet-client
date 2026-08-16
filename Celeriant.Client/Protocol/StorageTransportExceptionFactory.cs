using Celeriant.Client.Errors;
using Celeriant.Transport;

namespace Celeriant.Client.Protocol;

/// <summary>
/// Surfaces shared-transport failures as the storage client's own exception types, so callers keep
/// catching <see cref="CeleriantTimeoutException"/>, <see cref="ConnectionFailedException"/>, and
/// <see cref="ProtocolException"/>.
/// </summary>
internal sealed class StorageTransportExceptionFactory : ITransportExceptionFactory
{
    public static readonly StorageTransportExceptionFactory Instance = new();

    private StorageTransportExceptionFactory() { }

    public Exception Timeout(string message)
        => new CeleriantTimeoutException(message);

    public Exception ConnectTimeout(string message)
        => new ConnectionTimeoutException(message);

    public Exception ConnectionFailed(string message, Exception? inner = null)
        => inner is null ? new ConnectionFailedException(message) : new ConnectionFailedException(message, inner);

    public Exception Protocol(string message, Exception? inner = null)
        => inner is null ? new ProtocolException(message) : new ProtocolException(message, inner);
}
