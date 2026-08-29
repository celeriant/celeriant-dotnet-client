using System.Buffers;
using Celeriant.Client.Protocol;
using Celeriant.Client.Responses;
using MessagePack;

namespace Celeriant.Client.Tests;

/// <summary>
/// The compression dictionary arrives as a msgpack ARRAY of integers, not as <c>bin</c>.
///
/// <para>
/// Every other byte field in this protocol is annotated on the server to route through
/// <c>serde_bytes</c>, which writes <c>bin</c> — event payloads, IVs, the u128 ids.
/// <c>IdentifyResponse.compression_dict_bytes</c> is a plain <c>Option&lt;Vec&lt;u8&gt;&gt;</c>
/// with no such annotation, and <c>rmp_serde</c>'s compact encoding writes a bare
/// <c>Vec&lt;u8&gt;</c> as an array. Rust talking to Rust never notices because it is symmetric.
/// A reader expecting <c>bin</c> throws — and since the server ships the dictionary on the first
/// Identify of every connection that supplies an identity, that reader cannot connect at all.
/// </para>
/// </summary>
public class IdentifyDictBytesWireTests
{
    /// <summary>The shape the server actually puts on the wire today.</summary>
    [Fact]
    public void DictBytesAsAMsgpackArray_AreRead()
    {
        byte[] dict = [0x28, 0xB5, 0x2F, 0xFD, 0x00, 0xFF, 0x7F, 0x01];
        byte[] frame = BuildIdentifyFrame(dict, asBin: false);

        var decoded = WireCodec.Deserialize<IdentifyResponse>(frame);

        Assert.Equal(dict, decoded.CompressionDictBytes);
        Assert.Equal("sha-under-test", decoded.CompressionDictSha256);
    }

    /// <summary>The shape it would take if the annotation is ever added. Both must work.</summary>
    [Fact]
    public void DictBytesAsMsgpackBin_AreStillRead()
    {
        byte[] dict = [0x28, 0xB5, 0x2F, 0xFD, 0x00, 0xFF, 0x7F, 0x01];
        byte[] frame = BuildIdentifyFrame(dict, asBin: true);

        var decoded = WireCodec.Deserialize<IdentifyResponse>(frame);

        Assert.Equal(dict, decoded.CompressionDictBytes);
    }

    [Fact]
    public void AbsentDictBytes_AreNull()
    {
        byte[] frame = BuildIdentifyFrame(null, asBin: false);

        Assert.Null(WireCodec.Deserialize<IdentifyResponse>(frame).CompressionDictBytes);
    }

    /// <summary>
    /// A five-element msgpack array in the field order of the server's <c>IdentifyResponse</c>,
    /// with the dictionary written either way.
    /// </summary>
    private static byte[] BuildIdentifyFrame(byte[]? dict, bool asBin)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new MessagePackWriter(buffer);

        writer.WriteArrayHeader(5);
        writer.WriteNil();                       // correlation_id
        writer.WriteNil();                       // client_id
        writer.WriteNil();                       // access_level
        writer.Write("sha-under-test");           // compression_dict_sha256

        if (dict is null)
        {
            writer.WriteNil();
        }
        else if (asBin)
        {
            writer.Write(dict);
        }
        else
        {
            writer.WriteArrayHeader(dict.Length);
            foreach (byte b in dict)
                writer.Write(b);
        }

        writer.Flush();
        return buffer.WrittenSpan.ToArray();
    }
}
