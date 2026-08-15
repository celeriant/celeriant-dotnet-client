using Celeriant.Transport;

namespace Celeriant.Transport.Tests;

/// <summary>
/// Round-trip tests for dictionary-based zstd wire compression (the only algorithm the
/// protocol now supports besides <see cref="CompressionType.None"/>).
/// </summary>
public class CompressionTests
{
    // zstd accepts arbitrary bytes as a raw-content dictionary; this stands in for the
    // cluster-shipped dictionary for round-trip purposes.
    private static readonly byte[] Dict = MakeDict();
    private static readonly byte[] SmallPayload = "Hello world"u8.ToArray();
    private static readonly byte[] EmptyPayload = [];

    private static byte[] MakeDict()
    {
        var data = new byte[16 * 1024];
        for (int i = 0; i < data.Length; i++)
            data[i] = (byte)(i % 64);
        return data;
    }

    /// <summary>Generate a payload with repeating-pattern data (compressible).</summary>
    private static byte[] MakeLargePayload(int size = 1_048_576)
    {
        var data = new byte[size];
        for (int i = 0; i < size; i++)
            data[i] = (byte)(i % 251); // prime modulus for varied but compressible data
        return data;
    }

    [Fact]
    public void RoundTrip_Small() => VerifyRoundTrip(SmallPayload);

    [Fact]
    public void RoundTrip_Empty() => VerifyRoundTrip(EmptyPayload);

    [Fact]
    public void RoundTrip_Large() => VerifyRoundTrip(MakeLargePayload());

    [Fact]
    public void Compressed_SmallerThanOriginal_ForLargeRepetitivePayload()
    {
        var payload = MakeLargePayload();
        var compressed = DictCompression.CompressWithDict(payload, Dict);
        Assert.True(compressed.Length < payload.Length,
            $"Expected compressed size ({compressed.Length}) < original ({payload.Length})");
    }

    private static void VerifyRoundTrip(byte[] original)
    {
        var compressed = DictCompression.CompressWithDict(original, Dict);
        var decompressed = DictCompression.DecompressWithDict(compressed, (uint)original.Length, Dict);

        Assert.Equal(original.Length, decompressed.Length);
        Assert.Equal(original, decompressed);
    }
}
