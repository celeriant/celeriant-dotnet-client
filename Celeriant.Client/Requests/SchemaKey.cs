using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Requests;

/// <summary>
/// Identifies a schema registered for a specific event type within an aggregate type.
/// Maps 1:1 to the Rust <c>SchemaKey</c> struct.
///
/// Serialized as a 4-element MessagePack array: [org_id, aggregate_type_id, event_type_major, event_type_minor].
/// </summary>
[MessagePackObject]
public readonly record struct SchemaKey
{
    [Key(0)]
    [MessagePackFormatter(typeof(CeleriantGuidFormatter))]
    public Guid OrgId { get; init; }

    [Key(1)]
    [MessagePackFormatter(typeof(CeleriantGuidFormatter))]
    public Guid AggregateTypeId { get; init; }

    [Key(2)]
    [MessagePackFormatter(typeof(UInt64AsInt64Formatter))]
    public long EventTypeMajor { get; init; }

    [Key(3)]
    [MessagePackFormatter(typeof(UInt64AsInt64Formatter))]
    public long EventTypeMinor { get; init; }

    public SchemaKey(Guid orgId, Guid aggregateTypeId, long eventTypeMajor, long eventTypeMinor)
    {
        OrgId = orgId;
        AggregateTypeId = aggregateTypeId;
        EventTypeMajor = eventTypeMajor;
        EventTypeMinor = eventTypeMinor;
    }
}
