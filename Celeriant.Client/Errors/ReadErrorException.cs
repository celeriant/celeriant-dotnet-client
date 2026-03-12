using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when the server returns an error during a read or aggregate-details operation.
/// </summary>
public class ReadErrorException : CeleriantErrorException
{
    /// <summary>
    /// The batch index that was requested but is no longer available (error 1000).
    /// </summary>
    public long? RequestedBatchIndex { get; }

    /// <summary>
    /// The minimum available batch index (error 1000).
    /// </summary>
    public long? MinimumAvailable { get; }

    public ReadErrorException(ErrorResponse error) : base(error)
    {
        RequestedBatchIndex = error.GetLong("requested");
        MinimumAvailable = error.GetLong("minimum_available");
    }
}
