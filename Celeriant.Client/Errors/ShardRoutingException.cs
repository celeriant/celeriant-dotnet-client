using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when a multi-aggregate write targets aggregates that map to different shards (error 9001).
/// All aggregates in a single <c>WriteRequest</c> must belong to the same shard.
/// Check <see cref="NumShards"/> for the number of shards the aggregates were distributed across.
/// </summary>
public class ShardRoutingException : CeleriantErrorException
{
    /// <summary>
    /// The number of distinct shards the request's aggregates mapped to.
    /// </summary>
    public long? NumShards { get; }

    public ShardRoutingException(ErrorResponse error) : base(error)
    {
        NumShards = error.GetLong("num_shards");
    }
}
