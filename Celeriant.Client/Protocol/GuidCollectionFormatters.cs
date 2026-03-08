using MessagePack;
using MessagePack.Formatters;

namespace Celeriant.Client.Protocol;

/// <summary>
/// Serializes a <c>HashSet&lt;Guid&gt;</c> as a MessagePack array of 16-byte binary values.
/// Each element is encoded using <see cref="CeleriantGuidFormatter"/> (raw in-memory bytes).
/// </summary>
public sealed class GuidHashSetFormatter : IMessagePackFormatter<HashSet<Guid>>
{
    public static readonly GuidHashSetFormatter Instance = new();

    private GuidHashSetFormatter() { }

    public void Serialize(ref MessagePackWriter writer, HashSet<Guid> value, MessagePackSerializerOptions options)
    {
        writer.WriteArrayHeader(value.Count);
        foreach (Guid g in value)
            CeleriantGuidFormatter.Instance.Serialize(ref writer, g, options);
    }

    public HashSet<Guid> Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
    {
        int count = reader.ReadArrayHeader();
        HashSet<Guid> result = new(count);
        for (int i = 0; i < count; i++)
            result.Add(CeleriantGuidFormatter.Instance.Deserialize(ref reader, options));
        return result;
    }
}

/// <summary>
/// Serializes a nullable <c>HashSet&lt;Guid&gt;</c> as either nil or an array.
/// </summary>
public sealed class NullableGuidHashSetFormatter : IMessagePackFormatter<HashSet<Guid>?>
{
    public static readonly NullableGuidHashSetFormatter Instance = new();

    private NullableGuidHashSetFormatter() { }

    public void Serialize(ref MessagePackWriter writer, HashSet<Guid>? value, MessagePackSerializerOptions options)
    {
        if (value is null)
        {
            writer.WriteNil();
            return;
        }
        GuidHashSetFormatter.Instance.Serialize(ref writer, value, options);
    }

    public HashSet<Guid>? Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
    {
        if (reader.TryReadNil())
            return null;
        return GuidHashSetFormatter.Instance.Deserialize(ref reader, options);
    }
}
