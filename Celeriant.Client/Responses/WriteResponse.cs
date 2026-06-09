using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Responses;

[MessagePackObject]
public sealed class WriteResponse
{
    [Key(0)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? CorrelationId { get; init; }

    /// <summary>
    /// Highest aggregate version committed by this request. Populated only when the
    /// request wrote exactly one aggregate; <c>null</c> for multi-aggregate writes.
    /// </summary>
    [Key(1)]
    [MessagePackFormatter(typeof(NullableUInt64AsInt64Formatter))]
    public long? MaxAggregateVersion { get; init; }
}
