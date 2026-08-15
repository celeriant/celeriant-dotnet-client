using Celeriant.Transport;

namespace Celeriant.Transport.Tests;

public class WireHeaderTests
{
    private const uint V3 = WireHeader.ProtocolVersionV3;

    private static WireHeader RoundTrip(WireHeader header)
    {
        Span<byte> buf = stackalloc byte[WireHeader.Size];
        header.WriteTo(buf);
        return WireHeader.ParseFrom(buf);
    }

    [Fact]
    public void RoundTrip_BasicHeader_AllFieldsPreserved()
    {
        var header = new WireHeader(
            version: 3,
            messageType: 2,
            compressedLength: 512,
            uncompressedLength: 1024,
            compressionType: 1);

        var read = RoundTrip(header);

        Assert.Equal(3u, read.Version);
        Assert.Equal(2u, read.MessageType);
        Assert.Equal(512u, read.CompressedLength);
        Assert.Equal(1024u, read.UncompressedLength);
        Assert.Equal(1, read.CompressionType);
    }

    [Fact]
    public void RoundTrip_ForRequest_NoneCompression()
    {
        var header = WireHeader.ForRequest(V3, messageType: 7, length: 256);
        var read = RoundTrip(header);

        Assert.Equal(WireHeader.ProtocolVersionV3, read.Version);
        Assert.Equal(7u, read.MessageType);
        Assert.Equal(256u, read.CompressedLength);
        Assert.Equal(256u, read.UncompressedLength);
        Assert.Equal(0, read.CompressionType);
    }

    [Fact]
    public void RoundTrip_ForCompressedRequest_AllCompressionTypes()
    {
        var compressionTypes = new[]
        {
            CompressionType.None,
            CompressionType.ZstdDict,
        };

        foreach (var ct in compressionTypes)
        {
            var header = WireHeader.ForCompressedRequest(
                V3,
                messageType: 3,
                compressedLength: 100,
                uncompressedLength: 400,
                compression: ct);

            var read = RoundTrip(header);

            Assert.Equal(WireHeader.ProtocolVersionV3, read.Version);
            Assert.Equal(3u, read.MessageType);
            Assert.Equal(100u, read.CompressedLength);
            Assert.Equal(400u, read.UncompressedLength);
            Assert.Equal((byte)ct, read.CompressionType);
        }
    }

    [Fact]
    public void RoundTrip_V2_BincodeVersionPreserved()
    {
        var header = WireHeader.ForRequest(WireHeader.ProtocolVersionV2, messageType: 3, length: 64);
        var read = RoundTrip(header);
        Assert.Equal(2u, read.Version);
        Assert.Equal(3u, read.MessageType);
    }

    [Fact]
    public void RoundTrip_MaxMessageType_Preserved()
    {
        var header = WireHeader.ForRequest(V3, messageType: uint.MaxValue, length: 0);
        var read = RoundTrip(header);
        Assert.Equal(uint.MaxValue, read.MessageType);
    }

    [Fact]
    public void RoundTrip_Version3_IsProtocolVersionV3()
    {
        Assert.Equal(3u, WireHeader.ProtocolVersionV3);

        var header = WireHeader.ForRequest(V3, messageType: 5, length: 42);
        var read = RoundTrip(header);
        Assert.Equal(3u, read.Version);
    }

    [Fact]
    public void Size_IsSeventeenBytes()
    {
        Assert.Equal(17, WireHeader.Size);
    }

    [Fact]
    public void WriteTo_ProducesExactly17Bytes()
    {
        var header = WireHeader.ForRequest(V3, messageType: 1, length: 100);
        var buf = new byte[WireHeader.Size];
        header.WriteTo(buf);

        Assert.Equal(3u, System.Buffers.Binary.BinaryPrimitives.ReadUInt32LittleEndian(buf.AsSpan(0)));
    }

    [Fact]
    public void ForRequest_SetsCompressedAndUncompressedEqual()
    {
        var header = WireHeader.ForRequest(V3, messageType: 2, length: 999);
        Assert.Equal(header.CompressedLength, header.UncompressedLength);
        Assert.Equal(999u, header.CompressedLength);
        Assert.Equal(0, header.CompressionType);
    }

    [Fact]
    public void ForCompressedRequest_SetsDistinctLengths()
    {
        var header = WireHeader.ForCompressedRequest(
            V3,
            messageType: 3,
            compressedLength: 100,
            uncompressedLength: 400,
            compression: CompressionType.ZstdDict);

        Assert.Equal(100u, header.CompressedLength);
        Assert.Equal(400u, header.UncompressedLength);
        Assert.NotEqual(header.CompressedLength, header.UncompressedLength);
        Assert.Equal((byte)CompressionType.ZstdDict, header.CompressionType);
    }
}
