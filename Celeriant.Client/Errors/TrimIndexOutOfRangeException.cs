using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when a trim operation specifies an index beyond the aggregate's current range (error 3004).
/// </summary>
public class TrimIndexOutOfRangeException : TrimErrorException
{
    /// <summary>
    /// The trim index that was requested.
    /// </summary>
    public long RequestedTrimIndex { get; }

    /// <summary>
    /// The aggregate's current maximum event batch index.
    /// You cannot trim beyond this point.
    /// </summary>
    public long CurrentMaxBatchIndex { get; }

    public TrimIndexOutOfRangeException(ErrorResponse error) : base(error)
    {
        RequestedTrimIndex = error.GetLong("requested") ?? 0;
        CurrentMaxBatchIndex = error.GetLong("max_event_batch_index") ?? 0;
    }
}
