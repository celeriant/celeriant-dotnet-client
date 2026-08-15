namespace Celeriant.Transport;

/// <summary>
/// The per-product seam over the shared transport. Supplies the protocol version stamped on
/// every frame, the Identify message-type ids, and how to (de)serialize the Identify body — which
/// is the only typed payload the connection itself touches. All other request/response bodies pass
/// through the connection as opaque bytes; the product encodes/decodes them with the same codec.
/// </summary>
public interface IConnectionCodec
{
    /// <summary>Protocol version written to every outbound <see cref="WireHeader"/> (V2=bincode, V3=MessagePack).</summary>
    uint ProtocolVersion { get; }

    /// <summary>Message-type id of the Identify request (celeriant_msg type 14 for all products).</summary>
    uint IdentifyRequestType { get; }

    /// <summary>Message-type id of a successful Identify response (celeriant_msg type 16).</summary>
    uint IdentifyResponseType { get; }

    /// <summary>Serialize the Identify request body.</summary>
    byte[] EncodeIdentify(in IdentifyParams identity);

    /// <summary>Deserialize a successful Identify response body.</summary>
    IdentifyResult DecodeIdentify(ReadOnlySpan<byte> body);

    /// <summary>
    /// If <paramref name="messageType"/>/<paramref name="body"/> is a server error frame, return the
    /// product exception to throw; otherwise return null. Called by the connection when an Identify
    /// exchange comes back as something other than <see cref="IdentifyResponseType"/>.
    /// </summary>
    Exception? TryMapErrorFrame(uint messageType, ReadOnlySpan<byte> body);
}
