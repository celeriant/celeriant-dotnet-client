using MessagePack;
using MessagePack.Formatters;

namespace Celeriant.Client.Protocol;

/// <summary>
/// Serializes a <see cref="DateTimeOffset"/> as a <c>ulong</c> epoch milliseconds value
/// on the wire, matching the Rust server's timestamp format.
/// </summary>
public sealed class EpochMillisFormatter : IMessagePackFormatter<DateTimeOffset>
{
    public static readonly EpochMillisFormatter Instance = new();

    public void Serialize(ref MessagePackWriter writer, DateTimeOffset value, MessagePackSerializerOptions options)
    {
        writer.Write((ulong)value.ToUnixTimeMilliseconds());
    }

    public DateTimeOffset Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
    {
        ulong epochMs = reader.ReadUInt64();
        return DateTimeOffset.FromUnixTimeMilliseconds((long)epochMs);
    }
}

/// <summary>
/// Serializes a nullable <see cref="DateTimeOffset"/> as a nullable <c>ulong</c> epoch milliseconds.
/// Maps <c>null</c> ↔ msgpack nil.
/// </summary>
public sealed class NullableEpochMillisFormatter : IMessagePackFormatter<DateTimeOffset?>
{
    public static readonly NullableEpochMillisFormatter Instance = new();

    public void Serialize(ref MessagePackWriter writer, DateTimeOffset? value, MessagePackSerializerOptions options)
    {
        if (value is null)
        {
            writer.WriteNil();
            return;
        }
        writer.Write((ulong)value.Value.ToUnixTimeMilliseconds());
    }

    public DateTimeOffset? Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
    {
        if (reader.TryReadNil())
            return null;
        ulong epochMs = reader.ReadUInt64();
        return DateTimeOffset.FromUnixTimeMilliseconds((long)epochMs);
    }
}

/// <summary>
/// Serializes a nullable <see cref="DateTimeOffset"/> as a non-nullable <c>ulong</c> on the wire,
/// treating 0 as "no data" (null). Used for response types where the server sends 0 when
/// no timestamp is available (e.g., aggregate list items with no events).
/// </summary>
public sealed class ZeroAsNullEpochMillisFormatter : IMessagePackFormatter<DateTimeOffset?>
{
    public static readonly ZeroAsNullEpochMillisFormatter Instance = new();

    public void Serialize(ref MessagePackWriter writer, DateTimeOffset? value, MessagePackSerializerOptions options)
    {
        if (value is null)
        {
            writer.Write(0UL);
            return;
        }
        writer.Write((ulong)value.Value.ToUnixTimeMilliseconds());
    }

    public DateTimeOffset? Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
    {
        ulong epochMs = reader.ReadUInt64();
        if (epochMs == 0)
            return null;
        return DateTimeOffset.FromUnixTimeMilliseconds((long)epochMs);
    }
}

/// <summary>
/// Serializes a nullable <see cref="TimeSpan"/> as a nullable <c>ulong</c> milliseconds value.
/// </summary>
public sealed class NullableMillisTimeSpanFormatter : IMessagePackFormatter<TimeSpan?>
{
    public static readonly NullableMillisTimeSpanFormatter Instance = new();

    public void Serialize(ref MessagePackWriter writer, TimeSpan? value, MessagePackSerializerOptions options)
    {
        if (value is null)
        {
            writer.WriteNil();
            return;
        }
        writer.Write((ulong)value.Value.TotalMilliseconds);
    }

    public TimeSpan? Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
    {
        if (reader.TryReadNil())
            return null;
        ulong ms = reader.ReadUInt64();
        return TimeSpan.FromMilliseconds(ms);
    }
}
