using System.Text;

namespace Celeriant.Transport.Tests;

/// <summary>
/// The embedded built-in is a working zstd dictionary, not just the right bytes: a payload compressed
/// against it round-trips, and web-event JSON compresses smaller with it than without it.
///
/// Run: dotnet test Celeriant.Transport.Tests --filter FullyQualifiedName~BuiltinDictionaryTests
/// </summary>
public class BuiltinDictionaryTests
{
    private static readonly byte[] WebEvent = Encoding.UTF8.GetBytes(
        """{"event_type":"page_view","timestamp":"2026-09-27T10:15:00Z","user_id":"u-1842","session_id":"s-99a1","url":"https://example.com/products/42","referrer":"https://example.com/","user_agent":"Mozilla/5.0 (X11; Linux x86_64)","country":"AU"}""");

    [Fact]
    public void AReplyCompressedWithTheBuiltinRoundTrips()
    {
        byte[] dict = BuiltinDictionary.Dict.Bytes;

        byte[] compressed = DictCompression.CompressWithDict(WebEvent, dict);
        byte[] restored = DictCompression.DecompressWithDict(compressed, (uint)WebEvent.Length, dict);

        Assert.Equal(WebEvent, restored);
    }

    [Fact]
    public void TheBuiltinShrinksAWebEventBelowPlainZstd()
    {
        byte[] withDict = DictCompression.CompressWithDict(WebEvent, BuiltinDictionary.Dict.Bytes);
        using var plain = new ZstdSharp.Compressor();
        int withoutDict = plain.Wrap(WebEvent).Length;

        Assert.True(withDict.Length < withoutDict,
            $"with the built-in: {withDict.Length} bytes, without: {withoutDict} bytes");
    }
}
