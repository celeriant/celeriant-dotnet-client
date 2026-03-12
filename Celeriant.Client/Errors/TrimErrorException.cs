using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when the server returns an error during a trim-start operation.
/// </summary>
public class TrimErrorException : CeleriantErrorException
{
    /// <summary>
    /// The trim index that was requested (error 3004 index out of range).
    /// </summary>
    public long? RequestedIndex { get; }

    /// <summary>
    /// The maximum event batch index (error 3004 index out of range).
    /// </summary>
    public long? MaxEventBatchIndex { get; }

    public TrimErrorException(ErrorResponse error) : base(error)
    {
        RequestedIndex = error.GetLong("requested");
        MaxEventBatchIndex = error.GetLong("max_event_batch_index");
    }
}
