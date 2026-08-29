using System.Buffers;
using MessagePack;
using MessagePack.Formatters;

namespace Celeriant.Client.Protocol;

/// <summary>
/// Reads a byte array that may arrive as msgpack <c>bin</c> OR as an <c>array</c> of integers.
///
/// <para>
/// Nearly every byte field in this protocol is annotated <c>#[serde(with = "…_bytes")]</c> on the
/// server, which routes it through <c>serde_bytes</c> and puts <c>bin</c> on the wire — event
/// payloads, IVs, u128 ids. <c>IdentifyResponse.compression_dict_bytes</c> is a plain
/// <c>Option&lt;Vec&lt;u8&gt;&gt;</c> with no such annotation, and <c>rmp_serde</c>'s compact
/// encoding writes a bare <c>Vec&lt;u8&gt;</c> as an ARRAY of integers instead. Rust talking to
/// Rust never notices — it is symmetric — but a reader expecting <c>bin</c> fails outright, and
/// the server ships those bytes on the first Identify of every connection that supplies an
/// identity.
/// </para>
///
/// <para>
/// So this accepts both: the array form the server sends today, and the <c>bin</c> form it would
/// send if the annotation is ever added. Writing always uses <c>bin</c>, which is what the client
/// sends elsewhere and what the server's own deserializer accepts for this field either way.
/// </para>
/// </summary>
public sealed class SeqOrBinBytesFormatter : IMessagePackFormatter<byte[]?>
{
    public static readonly SeqOrBinBytesFormatter Instance = new();

    public void Serialize(ref MessagePackWriter writer, byte[]? value, MessagePackSerializerOptions options)
    {
        if (value is null)
            writer.WriteNil();
        else
            writer.Write(value);
    }

    public byte[]? Deserialize(ref MessagePackReader reader, MessagePackSerializerOptions options)
    {
        if (reader.TryReadNil())
            return null;

        if (reader.NextMessagePackType is not MessagePackType.Array)
            return reader.ReadBytes() is { } bin ? BuffersExtensions.ToArray(bin) : null;

        int count = reader.ReadArrayHeader();
        var bytes = new byte[count];
        for (int i = 0; i < count; i++)
            bytes[i] = reader.ReadByte();

        return bytes;
    }
}
