using MessagePack;

namespace Celeriant.Client.Protocol;

/// <summary>
/// MessagePack (V3) body serialization for the Celeriant storage wire protocol. Framing and
/// zstd-dictionary compression live in the shared transport layer; this only turns typed
/// request/response payloads into bytes and back.
/// </summary>
internal static class WireCodec
{
    /// <summary>MessagePack serializer options using the Celeriant resolver.</summary>
    public static MessagePackSerializerOptions Options => CeleriantResolver.Options;

    /// <summary>Serialize a value to MessagePack bytes.</summary>
    public static byte[] Serialize<T>(T value)
        => MessagePackSerializer.Serialize(value, Options);

    /// <summary>Deserialize MessagePack bytes to a value.</summary>
    public static T Deserialize<T>(ReadOnlyMemory<byte> data)
        => MessagePackSerializer.Deserialize<T>(data, Options);
}
