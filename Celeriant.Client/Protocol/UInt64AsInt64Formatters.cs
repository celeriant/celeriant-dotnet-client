using MessagePack;
using MessagePack.Formatters;

namespace Celeriant.Client.Protocol;

/// <summary>
/// Serializes a <see cref="long"/> as a <c>ulong</c> on the wire to match Rust's u64 encoding.
/// </summary>
public sealed class UInt64AsInt64Formatter : IMessagePackFormatter<long>
{
    public static readonly UInt64AsInt64Formatter Instance = new();

    public void Serialize(ref MessagePackWriter writer, long value, MessagePackSerializerOptions options)
    {
        writer.Write((ulong)value);
    }

    public long Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
    {
        return (long)reader.ReadUInt64();
    }
}

/// <summary>
/// Serializes a nullable <see cref="long"/> as a nullable <c>ulong</c> on the wire.
/// </summary>
public sealed class NullableUInt64AsInt64Formatter : IMessagePackFormatter<long?>
{
    public static readonly NullableUInt64AsInt64Formatter Instance = new();

    public void Serialize(ref MessagePackWriter writer, long? value, MessagePackSerializerOptions options)
    {
        if (value is null)
        {
            writer.WriteNil();
            return;
        }
        writer.Write((ulong)value.Value);
    }

    public long? Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
    {
        if (reader.TryReadNil())
            return null;
        return (long)reader.ReadUInt64();
    }
}

/// <summary>
/// Serializes a <c>long[]?</c> as a <c>ulong[]?</c> on the wire.
/// </summary>
public sealed class NullableUInt64ArrayAsInt64ArrayFormatter : IMessagePackFormatter<long[]?>
{
    public static readonly NullableUInt64ArrayAsInt64ArrayFormatter Instance = new();

    public void Serialize(ref MessagePackWriter writer, long[]? value, MessagePackSerializerOptions options)
    {
        if (value is null)
        {
            writer.WriteNil();
            return;
        }
        writer.WriteArrayHeader(value.Length);
        foreach (long item in value)
            writer.Write((ulong)item);
    }

    public long[]? Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
    {
        if (reader.TryReadNil())
            return null;

        int count = reader.ReadArrayHeader();
        var result = new long[count];
        for (int i = 0; i < count; i++)
            result[i] = (long)reader.ReadUInt64();
        return result;
    }
}
