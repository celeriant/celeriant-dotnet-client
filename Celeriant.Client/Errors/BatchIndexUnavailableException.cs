using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when a read requests an aggregate version that is no longer available (error 1000).
/// This typically happens after a trim operation has removed older event batches.
/// Re-read starting from <see cref="MinimumAvailableVersion"/>.
/// </summary>
public class BatchIndexUnavailableException : ReadErrorException
{
    /// <summary>
    /// The aggregate version that was requested but is no longer available.
    /// </summary>
    public long RequestedVersion { get; }

    /// <summary>
    /// The earliest aggregate version still available on the server.
    /// Start reading from here instead.
    /// </summary>
    public long MinimumAvailableVersion { get; }

    public BatchIndexUnavailableException(ErrorResponse error) : base(error)
    {
        RequestedVersion = error.GetLong("requested") ?? 0;
        MinimumAvailableVersion = error.GetLong("minimum_available") ?? 0;
    }
}
