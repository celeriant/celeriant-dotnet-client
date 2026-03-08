using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Responses;

namespace Celeriant.Client.Protocol;

/// <summary>
/// MessagePack formatter for <see cref="AccessLevel"/>?.
/// Rust serializes this enum as its variant name string ("ReadWrite", "ReadOnly")
/// via rmp_serde's default enum serialization.
/// </summary>
internal sealed class NullableAccessLevelFormatter : IMessagePackFormatter<AccessLevel?>
{
    public static readonly NullableAccessLevelFormatter Instance = new();

    public void Serialize(ref MessagePackWriter writer, AccessLevel? value, MessagePackSerializerOptions options)
    {
        if (value is null)
        {
            writer.WriteNil();
            return;
        }

        string name = value.Value switch
        {
            AccessLevel.ReadWrite => "ReadWrite",
            AccessLevel.ReadOnly => "ReadOnly",
            _ => throw new MessagePackSerializationException($"Unknown AccessLevel: {value.Value}"),
        };
        writer.Write(name);
    }

    public AccessLevel? Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
    {
        if (reader.TryReadNil())
            return null;

        var type = reader.NextMessagePackType;

        if (type == MessagePackType.String)
        {
            string name = reader.ReadString()!;
            return name switch
            {
                "ReadWrite" => AccessLevel.ReadWrite,
                "ReadOnly" => AccessLevel.ReadOnly,
                _ => throw new MessagePackSerializationException($"Unknown AccessLevel variant: '{name}'"),
            };
        }

        if (type == MessagePackType.Integer)
        {
            byte val = reader.ReadByte();
            return val switch
            {
                1 => AccessLevel.ReadWrite,
                2 => AccessLevel.ReadOnly,
                _ => throw new MessagePackSerializationException($"Unknown AccessLevel discriminant: {val}"),
            };
        }

        throw new MessagePackSerializationException(
            $"Expected string or integer for AccessLevel, got {type}");
    }
}
