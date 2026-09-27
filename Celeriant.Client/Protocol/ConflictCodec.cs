using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using MessagePack;

namespace Celeriant.Client.Protocol;

internal static class ConflictCodec
{
    internal static (Guid? CorrelationId, IReadOnlyList<AggregateConflict> Conflicts) Decode(ReadOnlyMemory<byte> body)
    {
        var reader = new MessagePackReader(body);
        if (reader.ReadArrayHeader() != 2) throw new MessagePackSerializationException("Conflict response must contain two fields.");
        Guid? correlation = CeleriantNullableGuidFormatter.Instance.Deserialize(ref reader, WireCodec.Options);
        int count = reader.ReadArrayHeader();
        if (count > (body.Length - reader.Consumed) / 58)
            throw new MessagePackSerializationException("Invalid conflict count.");
        var conflicts = new AggregateConflict[count];
        for (int i = 0; i < count; i++)
        {
            if (reader.ReadArrayHeader() != 3 || reader.ReadArrayHeader() != 3)
                throw new MessagePackSerializationException("Invalid conflict or aggregate key shape.");
            var key = new AggregateKey(ReadKey(ref reader), ReadKey(ref reader), ReadKey(ref reader));
            long expected = checked((long)reader.ReadUInt64());
            long current = checked((long)reader.ReadUInt64());
            conflicts[i] = new AggregateConflict(key, expected, current);
        }
        if (!reader.End) throw new MessagePackSerializationException("Trailing conflict response bytes.");
        return (correlation, Array.AsReadOnly(conflicts));
    }

    private static Guid ReadKey(ref MessagePackReader reader)
    {
        if (reader.NextMessagePackType != MessagePackType.Binary)
            throw new MessagePackSerializationException("Aggregate identifiers must be 16-byte binary values.");
        return CeleriantGuidFormatter.Instance.Deserialize(ref reader, WireCodec.Options);
    }
}
