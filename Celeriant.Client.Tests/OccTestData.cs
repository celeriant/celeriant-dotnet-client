using System.Buffers;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using MessagePack;

namespace Celeriant.Client.Tests;

/// <summary>
/// Requests, keys and binary conflict bodies for guarded-write tests, plus the success replies a
/// <see cref="RecordingFrameServer"/> gives when a test does not script one.
/// </summary>
internal static class OccTestData
{
    internal static readonly Guid Correlation = Guid.Parse("ffffffff-ffff-ffff-ffff-ffffffffffff");
    internal static readonly Guid WriterId = Guid.Parse("00000000-0000-0000-0000-000000000031");

    /// <summary>Sorted by big-endian key bytes, straddling each byte boundary of every id.</summary>
    internal static readonly AggregateKey[] Keys =
    [
        Key(1, 1, 255), Key(1, 1, 256), Key(1, 256, 0), Key(255, 0, 0), Key(256, 0, 0),
        new(Guid.Parse("80000000-0000-0000-0000-000000000000"), Guid.Empty, Guid.Empty),
    ];

    internal static AggregateKey Key(ulong org, ulong type, ulong id) => new(Id(org), Id(type), Id(id));
    private static Guid Id(ulong value) => Guid.Parse($"00000000-0000-0000-{value >> 48:x4}-{value & 0xffffffffffff:x12}");

    internal static SingleAggregateWrite Append(long? expected) => new()
    {
        ExpectedVersion = expected,
        Events = [new() { ClientSeq = 1, EventTimestamp = DateTimeOffset.UnixEpoch,
            EventTypeMajor = 1, EventValue = [42] }],
    };
    internal static WriteRequest Write(Dictionary<AggregateKey, SingleAggregateWrite>? writes = null) => new()
    {
        CorrelationId = Correlation, ClientId = WriterId,
        Writes = writes ?? Keys.Reverse().ToDictionary(k => k, _ => Append(10)),
    };
    internal static DeleteRequest Delete(AggregateKey[] keys, long? expected = 10) => new()
    {
        CorrelationId = Correlation, ClientId = WriterId,
        Deletes = keys.Reverse().ToDictionary(k => k, _ => new SingleAggregateDelete { ExpectedVersion = expected }),
    };
    internal static CeleriantPool Pool(string address, long ceiling = 64 * 1024 * 1024) => new(new()
    {
        Address = address, MaxConnections = 1, MaxResponseSize = ceiling,
        ConnectionTimeout = TimeSpan.FromSeconds(2), RequestTimeout = TimeSpan.FromSeconds(2),
    });

    /// <summary>A V5 conflict body; entry i carries expected + i and current + i unless pinned at the max.</summary>
    internal static byte[] ConflictBody(Guid correlation, AggregateKey[] keys, ulong expected = 10, ulong current = 20)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new MessagePackWriter(buffer);
        writer.WriteArrayHeader(2);
        writer.Write(correlation.ToByteArray(bigEndian: true));
        writer.WriteArrayHeader(keys.Length);
        for (var i = 0; i < keys.Length; i++)
        {
            writer.WriteArrayHeader(3);
            writer.WriteArrayHeader(3);
            writer.Write(keys[i].OrgId.ToByteArray(bigEndian: true));
            writer.Write(keys[i].AggregateTypeId.ToByteArray(bigEndian: true));
            writer.Write(keys[i].AggregateId.ToByteArray(bigEndian: true));
            writer.Write(expected == ulong.MaxValue ? expected : expected + (ulong)i);
            writer.Write(current == ulong.MaxValue ? current : current + (ulong)i);
        }
        writer.Flush();
        return buffer.WrittenSpan.ToArray();
    }

    internal static FrameReply Identify(RecordedFrame frame, uint version = 5) => new(16,
        WireCodec.Serialize(new IdentifyResponse
        {
            CorrelationId = WireCodec.Deserialize<IdentifyRequest>(frame.Body).CorrelationId,
        }), version);

    internal static FrameReply Success(RecordedFrame frame, uint version = 5) => frame.Type switch
    {
        14 => Identify(frame, version),
        3 => new(3, WireCodec.Serialize(new WriteResponse
        {
            CorrelationId = WireCodec.Deserialize<WriteRequest>(frame.Body).CorrelationId,
            MaxAggregateVersion = 1,
        }), version),
        5 => new(5, WireCodec.Serialize(new SuccessResponse
        {
            CorrelationId = WireCodec.Deserialize<DeleteRequest>(frame.Body).CorrelationId,
        }), version),
        _ => throw new InvalidOperationException($"Unexpected request type {frame.Type}"),
    };
}
