using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Responses;

[MessagePackObject]
public sealed class OrgListItem
{
    [Key(0)]
    [MessagePackFormatter(typeof(CeleriantGuidFormatter))]
    public Guid OrgId { get; init; }
}
