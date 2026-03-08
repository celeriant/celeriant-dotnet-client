using System.Buffers;
using Celeriant.Client.Protocol;
using Celeriant.Client.Responses;
using MessagePack;
using MessagePack.Formatters;

namespace Celeriant.Client.Tests;

/// <summary>
/// Tests for custom MessagePack formatters that handle Rust wire format compatibility.
/// </summary>
public class FormatterTests
{
    private static readonly MessagePackSerializerOptions Options = WireCodec.Options;

    private static byte[] SerializeWith<T>(IMessagePackFormatter<T> formatter, T value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new MessagePackWriter(buffer);
        formatter.Serialize(ref writer, value, Options);
        writer.Flush();
        return buffer.WrittenSpan.ToArray();
    }

    private static T DeserializeWith<T>(IMessagePackFormatter<T> formatter, byte[] bytes)
    {
        var reader = new MessagePackReader(bytes);
        return formatter.Deserialize(ref reader, Options);
    }

    private static T RoundTrip<T>(IMessagePackFormatter<T> formatter, T value)
    {
        var bytes = SerializeWith(formatter, value);
        return DeserializeWith(formatter, bytes);
    }

    // -----------------------------------------------------------------------
    // GuidFormatter
    // -----------------------------------------------------------------------

    [Fact]
    public void GuidFormatter_RoundTrip_PreservesValue()
    {
        var guid = Guid.Parse("01020304-0506-0708-090a-0b0c0d0e0f10");
        var result = RoundTrip(CeleriantGuidFormatter.Instance, guid);
        Assert.Equal(guid, result);
    }

    [Fact]
    public void GuidFormatter_EmptyGuid_RoundTrips()
    {
        var result = RoundTrip(CeleriantGuidFormatter.Instance, Guid.Empty);
        Assert.Equal(Guid.Empty, result);
    }

    [Fact]
    public void GuidFormatter_RandomGuids_AllRoundTrip()
    {
        for (int i = 0; i < 10; i++)
        {
            var guid = Guid.NewGuid();
            Assert.Equal(guid, RoundTrip(CeleriantGuidFormatter.Instance, guid));
        }
    }

    [Fact]
    public void GuidFormatter_SerializesAs16BigEndianBytes()
    {
        var guid = Guid.Parse("01020304-0506-0708-090a-0b0c0d0e0f10");
        var bytes = SerializeWith(CeleriantGuidFormatter.Instance, guid);

        // msgpack bin8 (0xc4) + length (0x10 = 16) + 16 big-endian bytes
        Assert.Equal(18, bytes.Length);
        Assert.Equal(0xc4, bytes[0]); // bin8
        Assert.Equal(16, bytes[1]);   // length

        // Verify big-endian byte order
        var payload = bytes.AsSpan(2);
        Assert.Equal(0x01, payload[0]);
        Assert.Equal(0x02, payload[1]);
        Assert.Equal(0x03, payload[2]);
        Assert.Equal(0x04, payload[3]);
        Assert.Equal(0x05, payload[4]);
        Assert.Equal(0x06, payload[5]);
        Assert.Equal(0x07, payload[6]);
        Assert.Equal(0x08, payload[7]);
    }

    // -----------------------------------------------------------------------
    // NullableGuidFormatter
    // -----------------------------------------------------------------------

    [Fact]
    public void NullableGuidFormatter_NonNull_RoundTrips()
    {
        Guid? value = Guid.NewGuid();
        Assert.Equal(value, RoundTrip(CeleriantNullableGuidFormatter.Instance, value));
    }

    [Fact]
    public void NullableGuidFormatter_Null_RoundTrips()
    {
        Assert.Null(RoundTrip(CeleriantNullableGuidFormatter.Instance, (Guid?)null));
    }

    // -----------------------------------------------------------------------
    // GuidHashSetFormatter
    // -----------------------------------------------------------------------

    [Fact]
    public void GuidHashSetFormatter_RoundTrip_PreservesElements()
    {
        var set = new HashSet<Guid> { Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid() };
        Assert.Equal(set, RoundTrip(GuidHashSetFormatter.Instance, set));
    }

    [Fact]
    public void GuidHashSetFormatter_EmptySet_RoundTrips()
    {
        Assert.Empty(RoundTrip(GuidHashSetFormatter.Instance, new HashSet<Guid>()));
    }

    [Fact]
    public void NullableGuidHashSetFormatter_Null_RoundTrips()
    {
        Assert.Null(RoundTrip(NullableGuidHashSetFormatter.Instance, (HashSet<Guid>?)null));
    }

    [Fact]
    public void NullableGuidHashSetFormatter_NonNull_RoundTrips()
    {
        HashSet<Guid>? value = [Guid.NewGuid()];
        var result = RoundTrip(NullableGuidHashSetFormatter.Instance, value);
        Assert.Equal(value, result);
    }

    // -----------------------------------------------------------------------
    // EpochMillisFormatter
    // -----------------------------------------------------------------------

    [Fact]
    public void EpochMillisFormatter_RoundTrip_PreservesMillisecondPrecision()
    {
        var now = DateTimeOffset.UtcNow;
        var truncated = DateTimeOffset.FromUnixTimeMilliseconds(now.ToUnixTimeMilliseconds());
        Assert.Equal(truncated, RoundTrip(EpochMillisFormatter.Instance, truncated));
    }

    [Fact]
    public void EpochMillisFormatter_UnixEpoch_RoundTrips()
    {
        Assert.Equal(DateTimeOffset.UnixEpoch, RoundTrip(EpochMillisFormatter.Instance, DateTimeOffset.UnixEpoch));
    }

    [Fact]
    public void NullableEpochMillisFormatter_Null_RoundTrips()
    {
        Assert.Null(RoundTrip(NullableEpochMillisFormatter.Instance, (DateTimeOffset?)null));
    }

    [Fact]
    public void NullableEpochMillisFormatter_NonNull_RoundTrips()
    {
        DateTimeOffset? value = DateTimeOffset.FromUnixTimeMilliseconds(1700000000000);
        Assert.Equal(value, RoundTrip(NullableEpochMillisFormatter.Instance, value));
    }

    // -----------------------------------------------------------------------
    // ZeroAsNullEpochMillisFormatter
    // -----------------------------------------------------------------------

    [Fact]
    public void ZeroAsNullEpochMillis_Null_SerializesAsZero_DeserializesAsNull()
    {
        Assert.Null(RoundTrip(ZeroAsNullEpochMillisFormatter.Instance, (DateTimeOffset?)null));
    }

    [Fact]
    public void ZeroAsNullEpochMillis_NonNull_RoundTrips()
    {
        DateTimeOffset? value = DateTimeOffset.FromUnixTimeMilliseconds(1700000000000);
        Assert.Equal(value, RoundTrip(ZeroAsNullEpochMillisFormatter.Instance, value));
    }

    [Fact]
    public void ZeroAsNullEpochMillis_Null_WritesIntegerNotNil()
    {
        var bytes = SerializeWith(ZeroAsNullEpochMillisFormatter.Instance, (DateTimeOffset?)null);
        var reader = new MessagePackReader(bytes);
        // Should be a ulong 0, not msgpack nil (0xc0)
        Assert.Equal(MessagePackType.Integer, reader.NextMessagePackType);
    }

    // -----------------------------------------------------------------------
    // NullableMillisTimeSpanFormatter
    // -----------------------------------------------------------------------

    [Fact]
    public void NullableMillisTimeSpan_Null_RoundTrips()
    {
        Assert.Null(RoundTrip(NullableMillisTimeSpanFormatter.Instance, (TimeSpan?)null));
    }

    [Fact]
    public void NullableMillisTimeSpan_NonNull_RoundTrips()
    {
        TimeSpan? value = TimeSpan.FromMilliseconds(12345);
        Assert.Equal(value, RoundTrip(NullableMillisTimeSpanFormatter.Instance, value));
    }

    // -----------------------------------------------------------------------
    // UInt64AsInt64Formatter
    // -----------------------------------------------------------------------

    [Theory]
    [InlineData(0L)]
    [InlineData(42L)]
    [InlineData(long.MaxValue)]
    public void UInt64AsInt64_RoundTrip(long value)
    {
        Assert.Equal(value, RoundTrip(UInt64AsInt64Formatter.Instance, value));
    }

    [Fact]
    public void UInt64AsInt64_SerializesAsUInt64()
    {
        var bytes = SerializeWith(UInt64AsInt64Formatter.Instance, 42L);
        var reader = new MessagePackReader(bytes);
        // Should deserialize as ulong (Rust compatibility)
        Assert.Equal(42UL, reader.ReadUInt64());
    }

    [Fact]
    public void NullableUInt64AsInt64_Null_RoundTrips()
    {
        Assert.Null(RoundTrip(NullableUInt64AsInt64Formatter.Instance, (long?)null));
    }

    [Fact]
    public void NullableUInt64AsInt64_NonNull_RoundTrips()
    {
        Assert.Equal(99L, RoundTrip(NullableUInt64AsInt64Formatter.Instance, (long?)99));
    }

    // -----------------------------------------------------------------------
    // NullableUInt64ArrayAsInt64ArrayFormatter
    // -----------------------------------------------------------------------

    [Fact]
    public void NullableInt64Array_Null_RoundTrips()
    {
        Assert.Null(RoundTrip(NullableUInt64ArrayAsInt64ArrayFormatter.Instance, (long[]?)null));
    }

    [Fact]
    public void NullableInt64Array_Empty_RoundTrips()
    {
        var result = RoundTrip(NullableUInt64ArrayAsInt64ArrayFormatter.Instance, Array.Empty<long>());
        Assert.NotNull(result);
        Assert.Empty(result);
    }

    [Fact]
    public void NullableInt64Array_WithValues_RoundTrips()
    {
        long[] value = [1, 2, long.MaxValue, 0];
        Assert.Equal(value, RoundTrip(NullableUInt64ArrayAsInt64ArrayFormatter.Instance, value));
    }

    // -----------------------------------------------------------------------
    // NullableAccessLevelFormatter
    // -----------------------------------------------------------------------

    [Theory]
    [InlineData(AccessLevel.ReadWrite)]
    [InlineData(AccessLevel.ReadOnly)]
    public void AccessLevelFormatter_RoundTrip_PreservesValue(AccessLevel level)
    {
        Assert.Equal(level, RoundTrip(NullableAccessLevelFormatter.Instance, (AccessLevel?)level));
    }

    [Fact]
    public void AccessLevelFormatter_Null_RoundTrips()
    {
        Assert.Null(RoundTrip(NullableAccessLevelFormatter.Instance, (AccessLevel?)null));
    }

    [Theory]
    [InlineData(AccessLevel.ReadWrite, "ReadWrite")]
    [InlineData(AccessLevel.ReadOnly, "ReadOnly")]
    public void AccessLevelFormatter_SerializesAsString(AccessLevel level, string expected)
    {
        var bytes = SerializeWith(NullableAccessLevelFormatter.Instance, (AccessLevel?)level);
        var reader = new MessagePackReader(bytes);
        Assert.Equal(expected, reader.ReadString());
    }

    [Fact]
    public void AccessLevelFormatter_DeserializesIntegerDiscriminant()
    {
        // Write byte 1 (ReadWrite) directly and verify formatter can read it
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new MessagePackWriter(buffer);
        writer.Write((byte)1);
        writer.Flush();
        var bytes = buffer.WrittenSpan.ToArray();

        var reader = new MessagePackReader(bytes);
        var result = NullableAccessLevelFormatter.Instance.Deserialize(ref reader, Options);
        Assert.Equal(AccessLevel.ReadWrite, result);
    }

    [Fact]
    public void AccessLevelFormatter_UnknownString_Throws()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new MessagePackWriter(buffer);
        writer.Write("InvalidLevel");
        writer.Flush();
        var bytes = buffer.WrittenSpan.ToArray();

        Assert.Throws<MessagePackSerializationException>(() =>
            DeserializeWith(NullableAccessLevelFormatter.Instance, bytes));
    }

    [Fact]
    public void AccessLevelFormatter_UnknownDiscriminant_Throws()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new MessagePackWriter(buffer);
        writer.Write((byte)99);
        writer.Flush();
        var bytes = buffer.WrittenSpan.ToArray();

        Assert.Throws<MessagePackSerializationException>(() =>
            DeserializeWith(NullableAccessLevelFormatter.Instance, bytes));
    }

    // -----------------------------------------------------------------------
    // GuidEndianHelper
    // -----------------------------------------------------------------------

    [Fact]
    public void GuidEndianHelper_RoundTrip_PreservesGuid()
    {
        var guid = Guid.NewGuid();
        var bigEndian = GuidEndianHelper.GuidToBigEndianBytes(guid);
        Assert.Equal(guid, GuidEndianHelper.BigEndianBytesToGuid(bigEndian));
    }

    [Fact]
    public void GuidEndianHelper_BigEndianBytes_AreNetworkOrder()
    {
        var guid = Guid.Parse("01020304-0506-0708-090a-0b0c0d0e0f10");
        var bytes = GuidEndianHelper.GuidToBigEndianBytes(guid);

        Assert.Equal(0x01, bytes[0]);
        Assert.Equal(0x02, bytes[1]);
        Assert.Equal(0x03, bytes[2]);
        Assert.Equal(0x04, bytes[3]);
        Assert.Equal(0x05, bytes[4]);
        Assert.Equal(0x06, bytes[5]);
        Assert.Equal(0x07, bytes[6]);
        Assert.Equal(0x08, bytes[7]);
        Assert.Equal(0x09, bytes[8]);
        Assert.Equal(0x0a, bytes[9]);
    }
}
