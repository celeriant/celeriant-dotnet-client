using System.Buffers;
using MessagePack;
using MessagePack.Formatters;

namespace Celeriant.Client.Protocol;

/// <summary>
/// Converts a <see cref="Guid"/> to/from 16 big-endian bytes that match
/// Rust's <c>rmp_serde</c> wire format for <c>u128</c>.
///
/// Rust serializes u128 as msgpack bin8 (c4 10) with 16 big-endian bytes
/// (most-significant byte first). A .NET Guid's <c>ToByteArray()</c> uses
/// mixed endianness: Data1 (4 bytes) and Data2/Data3 (2 bytes each) are
/// stored little-endian, while Data4 (8 bytes) is already big-endian.
///
/// These helpers re-order the mixed-endian groups so that the final byte
/// array represents the UUID in network byte order (big-endian u128), which
/// maps directly to Rust's serialization.
/// </summary>
internal static class GuidEndianHelper
{
    /// <summary>
    /// Converts a <see cref="Guid"/> to 16 big-endian bytes (Rust u128 to_be_bytes order).
    /// </summary>
    public static byte[] GuidToBigEndianBytes(Guid value)
    {
        byte[] bytes = value.ToByteArray(); // mixed-endian layout
        // Reverse Data1 (bytes 0-3): little-endian -> big-endian
        (bytes[0], bytes[3]) = (bytes[3], bytes[0]);
        (bytes[1], bytes[2]) = (bytes[2], bytes[1]);
        // Reverse Data2 (bytes 4-5): little-endian -> big-endian
        (bytes[4], bytes[5]) = (bytes[5], bytes[4]);
        // Reverse Data3 (bytes 6-7): little-endian -> big-endian
        (bytes[6], bytes[7]) = (bytes[7], bytes[6]);
        // Data4 (bytes 8-15) is already big-endian: no change needed
        return bytes;
    }

    /// <summary>
    /// Converts 16 big-endian bytes (Rust u128 to_be_bytes order) back to a <see cref="Guid"/>.
    /// </summary>
    public static Guid BigEndianBytesToGuid(byte[] bytes)
    {
        byte[] copy = (byte[])bytes.Clone();
        // Reverse Data1
        (copy[0], copy[3]) = (copy[3], copy[0]);
        (copy[1], copy[2]) = (copy[2], copy[1]);
        // Reverse Data2
        (copy[4], copy[5]) = (copy[5], copy[4]);
        // Reverse Data3
        (copy[6], copy[7]) = (copy[7], copy[6]);
        return new Guid(copy);
    }
}

/// <summary>
/// Serializes a <see cref="Guid"/> as a 16-byte MessagePack binary value using
/// big-endian byte order, matching Rust's <c>rmp_serde</c> wire format for <c>u128</c>.
///
/// Wire format: msgpack bin8 <c>c4 10</c> followed by 16 bytes most-significant-byte first.
/// </summary>
public sealed class CeleriantGuidFormatter : IMessagePackFormatter<Guid>
{
    public static readonly CeleriantGuidFormatter Instance = new();

    private CeleriantGuidFormatter() { }

    public void Serialize(ref MessagePackWriter writer, Guid value, MessagePackSerializerOptions options)
    {
        byte[] buf = GuidEndianHelper.GuidToBigEndianBytes(value);
        writer.Write(buf);
    }

    public Guid Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
    {
        ReadOnlySequence<byte>? bytes = reader.ReadBytes();
        if (bytes is null)
            return Guid.Empty;

        byte[] buf = new byte[16];
        bytes.Value.CopyTo(buf);
        return GuidEndianHelper.BigEndianBytesToGuid(buf);
    }
}

/// <summary>
/// Serializes a nullable <see cref="Guid"/> as either nil or a 16-byte binary value.
/// </summary>
public sealed class CeleriantNullableGuidFormatter : IMessagePackFormatter<Guid?>
{
    public static readonly CeleriantNullableGuidFormatter Instance = new();

    private CeleriantNullableGuidFormatter() { }

    public void Serialize(ref MessagePackWriter writer, Guid? value, MessagePackSerializerOptions options)
    {
        if (value is null)
        {
            writer.WriteNil();
            return;
        }
        CeleriantGuidFormatter.Instance.Serialize(ref writer, value.Value, options);
    }

    public Guid? Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
    {
        if (reader.TryReadNil())
            return null;
        return CeleriantGuidFormatter.Instance.Deserialize(ref reader, options);
    }
}
