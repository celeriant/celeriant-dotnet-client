using System.Buffers.Binary;
using System.Runtime.InteropServices;

namespace Celeriant.Transport;

/// <summary>
/// 17-byte little-endian wire header shared by every Celeriant product (storage engine and
/// queue both reuse celeriant_wire's framing). Only the protocol version (which selects the
/// body codec) and the message-type id namespace differ per product.
///
/// Layout:
///   offset 0  : uint32  version              (V2 = bincode, V3 = MessagePack)
///   offset 4  : uint32  message_type
///   offset 8  : uint32  compressed_length
///   offset 12 : uint32  uncompressed_length
///   offset 16 : uint8   compression_type
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 1)]
public readonly struct WireHeader
{
    public const int Size = 17;
    public const uint ProtocolVersionV2 = 2;
    public const uint ProtocolVersionV3 = 3;

    public readonly uint Version;             // offset 0
    public readonly uint MessageType;         // offset 4
    public readonly uint CompressedLength;    // offset 8
    public readonly uint UncompressedLength;  // offset 12
    public readonly byte CompressionType;     // offset 16

    public WireHeader(
        uint version,
        uint messageType,
        uint compressedLength,
        uint uncompressedLength,
        byte compressionType)
    {
        Version = version;
        MessageType = messageType;
        CompressedLength = compressedLength;
        UncompressedLength = uncompressedLength;
        CompressionType = compressionType;
    }

    /// <summary>Construct a header with no compression for the given protocol version.</summary>
    public static WireHeader ForRequest(uint version, uint messageType, uint length) =>
        new(version, messageType, length, length, 0);

    /// <summary>Construct a compressed header for the given protocol version.</summary>
    public static WireHeader ForCompressedRequest(
        uint version,
        uint messageType,
        uint compressedLength,
        uint uncompressedLength,
        CompressionType compression) =>
        new(version, messageType, compressedLength, uncompressedLength, (byte)compression);

    /// <summary>Parse a 17-byte header from a buffer.</summary>
    public static WireHeader ParseFrom(ReadOnlySpan<byte> buf)
    {
        uint version          = BinaryPrimitives.ReadUInt32LittleEndian(buf[0..]);
        uint messageType      = BinaryPrimitives.ReadUInt32LittleEndian(buf[4..]);
        uint compressedLength = BinaryPrimitives.ReadUInt32LittleEndian(buf[8..]);
        uint uncompressedLen  = BinaryPrimitives.ReadUInt32LittleEndian(buf[12..]);
        byte compressionType  = buf[16];
        return new WireHeader(version, messageType, compressedLength, uncompressedLen, compressionType);
    }

    /// <summary>Write the 17-byte header into the given buffer starting at offset 0.</summary>
    public void WriteTo(Span<byte> buf)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(buf[0..], Version);
        BinaryPrimitives.WriteUInt32LittleEndian(buf[4..], MessageType);
        BinaryPrimitives.WriteUInt32LittleEndian(buf[8..], CompressedLength);
        BinaryPrimitives.WriteUInt32LittleEndian(buf[12..], UncompressedLength);
        buf[16] = CompressionType;
    }
}
