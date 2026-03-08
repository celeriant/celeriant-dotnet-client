using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Requests;

/// <summary>
/// Uniquely identifies an aggregate (event stream) in Celeriant.
/// Maps 1:1 to the Rust <c>AggregateKey</c> struct.
///
/// Serialized as a 3-element MessagePack array: [org_id, aggregate_type_id, aggregate_id].
/// </summary>
[MessagePackObject]
public readonly record struct AggregateKey
{
    [Key(0)]
    [MessagePackFormatter(typeof(CeleriantGuidFormatter))]
    public Guid OrgId { get; init; }

    [Key(1)]
    [MessagePackFormatter(typeof(CeleriantGuidFormatter))]
    public Guid AggregateTypeId { get; init; }

    [Key(2)]
    [MessagePackFormatter(typeof(CeleriantGuidFormatter))]
    public Guid AggregateId { get; init; }

    public AggregateKey(Guid orgId, Guid aggregateTypeId, Guid aggregateId)
    {
        OrgId = orgId;
        AggregateTypeId = aggregateTypeId;
        AggregateId = aggregateId;
    }
}
