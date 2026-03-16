using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when a read requests a batch index that is no longer available (error 1000).
/// This typically happens after a trim operation has removed older event batches.
/// Re-read starting from <see cref="MinimumAvailableBatchIndex"/>.
/// </summary>
public class BatchIndexUnavailableException : ReadErrorException
{
    /// <summary>
    /// The batch index that was requested but is no longer available.
    /// </summary>
    public long RequestedBatchIndex { get; }

    /// <summary>
    /// The earliest batch index still available on the server.
    /// Start reading from here instead.
    /// </summary>
    public long MinimumAvailableBatchIndex { get; }

    public BatchIndexUnavailableException(ErrorResponse error) : base(error)
    {
        RequestedBatchIndex = error.GetLong("requested") ?? 0;
        MinimumAvailableBatchIndex = error.GetLong("minimum_available") ?? 0;
    }
}
