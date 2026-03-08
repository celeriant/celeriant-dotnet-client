using System.Buffers.Binary;
using System.Runtime.InteropServices;

namespace Celeriant.Client.Protocol;

/// <summary>
/// 17-byte little-endian wire header for the Celeriant V3 (MessagePack) protocol.
///
/// Layout:
///   offset 0  : uint32  version
///   offset 4  : uint32  message_type
///   offset 8  : uint32  compressed_length
///   offset 12 : uint32  uncompressed_length
///   offset 16 : uint8   compression_type
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 1)]
internal readonly struct WireHeader
{
    public const int Size = 17;
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

    /// <summary>
    /// Construct a V3 header with no compression.
    /// compressed_length and uncompressed_length are both set to <paramref name="length"/>.
    /// </summary>
    public static WireHeader ForRequest(uint messageType, uint length) =>
        new(ProtocolVersionV3, messageType, length, length, 0);

    /// <summary>
    /// Construct a V3 header with compression.
    /// </summary>
    public static WireHeader ForCompressedRequest(
        uint messageType,
        uint compressedLength,
        uint uncompressedLength,
        Protocol.CompressionType compression) =>
        new(ProtocolVersionV3, messageType, compressedLength, uncompressedLength, (byte)compression);

    /// <summary>
    /// Parse a 17-byte header from a buffer.
    /// </summary>
    public static WireHeader ParseFrom(ReadOnlySpan<byte> buf)
    {
        uint version          = BinaryPrimitives.ReadUInt32LittleEndian(buf[0..]);
        uint messageType      = BinaryPrimitives.ReadUInt32LittleEndian(buf[4..]);
        uint compressedLength = BinaryPrimitives.ReadUInt32LittleEndian(buf[8..]);
        uint uncompressedLen  = BinaryPrimitives.ReadUInt32LittleEndian(buf[12..]);
        byte compressionType  = buf[16];
        return new WireHeader(version, messageType, compressedLength, uncompressedLen, compressionType);
    }

    /// <summary>
    /// Write the 17-byte header into the given buffer starting at offset 0.
    /// </summary>
    public void WriteTo(Span<byte> buf)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(buf[0..], Version);
        BinaryPrimitives.WriteUInt32LittleEndian(buf[4..], MessageType);
        BinaryPrimitives.WriteUInt32LittleEndian(buf[8..], CompressedLength);
        BinaryPrimitives.WriteUInt32LittleEndian(buf[12..], UncompressedLength);
        buf[16] = CompressionType;
    }

}
