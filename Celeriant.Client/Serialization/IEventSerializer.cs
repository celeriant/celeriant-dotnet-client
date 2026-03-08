namespace Celeriant.Client.Serialization;

/// <summary>
/// Serializes and deserializes event payloads to and from byte arrays.
/// Implement this interface to plug in any wire format (JSON, Avro, Protobuf, MessagePack, etc.).
/// </summary>
public interface IEventSerializer
{
    /// <summary>Serialize <paramref name="value"/> to a byte array.</summary>
    byte[] Serialize<T>(T value);

    /// <summary>Deserialize a byte array back to <typeparamref name="T"/>.</summary>
    T Deserialize<T>(ReadOnlySpan<byte> data);
}
