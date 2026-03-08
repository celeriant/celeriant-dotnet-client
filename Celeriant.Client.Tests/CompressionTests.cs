using Celeriant.Client.Protocol;

namespace Celeriant.Client.Tests;

/// <summary>
/// Round-trip compression tests for all 5 compression algorithms.
/// </summary>
public class CompressionTests
{
    private static readonly byte[] SmallPayload  = [0x48, 0x65, 0x6C, 0x6C, 0x6F, 0x20, 0x77, 0x6F, 0x72, 0x6C, 0x64];  // "Hello world"
    private static readonly byte[] EmptyPayload  = [];

    /// <summary>Generate a 1MB payload with repeating-pattern data (compressible).</summary>
    private static byte[] MakeLargePayload(int size = 1_048_576)
    {
        var data = new byte[size];
        for (int i = 0; i < size; i++)
            data[i] = (byte)(i % 251); // prime modulus for varied but compressible data
        return data;
    }

    // -----------------------------------------------------------------------
    // None
    // -----------------------------------------------------------------------

    [Fact]
    public void None_RoundTrip_Small()
    {
        VerifyRoundTrip(SmallPayload, CompressionType.None);
    }

    [Fact]
    public void None_RoundTrip_Empty()
    {
        VerifyRoundTrip(EmptyPayload, CompressionType.None);
    }

    [Fact]
    public void None_Compress_ReturnsSameArray()
    {
        // For None, Compress should return the original array object unchanged.
        var compressed = WireCodec.Compress(SmallPayload, CompressionType.None);
        Assert.Same(SmallPayload, compressed);
    }

    // -----------------------------------------------------------------------
    // Zstd
    // -----------------------------------------------------------------------

    [Fact]
    public void Zstd_RoundTrip_Small()
    {
        VerifyRoundTrip(SmallPayload, CompressionType.Zstd);
    }

    [Fact]
    public void Zstd_RoundTrip_Large()
    {
        VerifyRoundTrip(MakeLargePayload(), CompressionType.Zstd);
    }

    [Fact]
    public void Zstd_CompressedSmallerThanOriginal_ForLargeRepetitivePayload()
    {
        var payload    = MakeLargePayload();
        var compressed = WireCodec.Compress(payload, CompressionType.Zstd);
        Assert.True(compressed.Length < payload.Length,
            $"Expected compressed size ({compressed.Length}) < original ({payload.Length})");
    }

    // -----------------------------------------------------------------------
    // Snappy
    // -----------------------------------------------------------------------

    [Fact]
    public void Snappy_RoundTrip_Small()
    {
        VerifyRoundTrip(SmallPayload, CompressionType.Snappy);
    }

    [Fact]
    public void Snappy_RoundTrip_Large()
    {
        VerifyRoundTrip(MakeLargePayload(), CompressionType.Snappy);
    }

    // -----------------------------------------------------------------------
    // Brotli
    // -----------------------------------------------------------------------

    [Fact]
    public void Brotli_RoundTrip_Small()
    {
        VerifyRoundTrip(SmallPayload, CompressionType.Brotli);
    }

    [Fact]
    public void Brotli_RoundTrip_Large()
    {
        VerifyRoundTrip(MakeLargePayload(), CompressionType.Brotli);
    }

    [Fact]
    public void Brotli_CompressedSmallerThanOriginal_ForLargeRepetitivePayload()
    {
        var payload    = MakeLargePayload();
        var compressed = WireCodec.Compress(payload, CompressionType.Brotli);
        Assert.True(compressed.Length < payload.Length,
            $"Expected compressed size ({compressed.Length}) < original ({payload.Length})");
    }

    // -----------------------------------------------------------------------
    // Gzip
    // -----------------------------------------------------------------------

    [Fact]
    public void Gzip_RoundTrip_Small()
    {
        VerifyRoundTrip(SmallPayload, CompressionType.Gzip);
    }

    [Fact]
    public void Gzip_RoundTrip_Large()
    {
        VerifyRoundTrip(MakeLargePayload(), CompressionType.Gzip);
    }

    [Fact]
    public void Gzip_CompressedSmallerThanOriginal_ForLargeRepetitivePayload()
    {
        var payload    = MakeLargePayload();
        var compressed = WireCodec.Compress(payload, CompressionType.Gzip);
        Assert.True(compressed.Length < payload.Length,
            $"Expected compressed size ({compressed.Length}) < original ({payload.Length})");
    }

    // -----------------------------------------------------------------------
    // All algorithms — parametrised sweep
    // -----------------------------------------------------------------------

    [Theory]
    [InlineData(CompressionType.None)]
    [InlineData(CompressionType.Zstd)]
    [InlineData(CompressionType.Snappy)]
    [InlineData(CompressionType.Brotli)]
    [InlineData(CompressionType.Gzip)]
    public void AllAlgorithms_RoundTrip_SmallPayload(CompressionType ct)
    {
        VerifyRoundTrip(SmallPayload, ct);
    }

    [Theory]
    [InlineData(CompressionType.None)]
    [InlineData(CompressionType.Zstd)]
    [InlineData(CompressionType.Snappy)]
    [InlineData(CompressionType.Brotli)]
    [InlineData(CompressionType.Gzip)]
    public void AllAlgorithms_RoundTrip_LargePayload(CompressionType ct)
    {
        VerifyRoundTrip(MakeLargePayload(), ct);
    }

    // -----------------------------------------------------------------------
    // Unknown compression type
    // -----------------------------------------------------------------------

    [Fact]
    public void Compress_UnknownType_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            WireCodec.Compress(SmallPayload, (CompressionType)99));
    }

    [Fact]
    public void Decompress_UnknownType_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            WireCodec.Decompress(SmallPayload, (CompressionType)99, (uint)SmallPayload.Length));
    }

    // -----------------------------------------------------------------------
    // Helper
    // -----------------------------------------------------------------------

    private static void VerifyRoundTrip(byte[] original, CompressionType ct)
    {
        var compressed   = WireCodec.Compress(original, ct);
        var decompressed = WireCodec.Decompress(compressed, ct, (uint)original.Length);

        Assert.Equal(original.Length, decompressed.Length);
        Assert.Equal(original, decompressed);
    }
}
