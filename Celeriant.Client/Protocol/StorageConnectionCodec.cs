using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Transport;

namespace Celeriant.Client.Protocol;

/// <summary>
/// Binds the shared transport to the storage engine's V5 (MessagePack) protocol: the Identify
/// body codec, the celeriant_msg type ids, and how a failed Identify error frame maps to a typed
/// <see cref="Errors.CeleriantClientException"/>.
/// </summary>
internal sealed class StorageConnectionCodec : IConnectionCodec
{
    public static readonly StorageConnectionCodec Instance = new();

    private StorageConnectionCodec() { }

    public uint ProtocolVersion => WireHeader.ProtocolVersionV5;
    public bool RequireIdentifyCorrelation => true;
    public uint IdentifyRequestType => MessageTypes.Requests.Identify;
    public uint IdentifyResponseType => MessageTypes.Responses.Identify;

    public byte[] EncodeIdentify(in IdentifyParams identity)
        => WireCodec.Serialize(new IdentifyRequest
        {
            CorrelationId = identity.CorrelationId,
            PublicKey = identity.PublicKey,
            Nonce = identity.Nonce,
            Signature = identity.Signature,
            ApiKey = identity.ApiKey,
            KnownDictSha256 = identity.KnownDictSha256,
        });

    public IdentifyResult DecodeIdentify(ReadOnlySpan<byte> body)
    {
        var r = WireCodec.Deserialize<IdentifyResponse>(body.ToArray());
        return new IdentifyResult(
            r.ClientId,
            r.AccessLevel is { } al ? (byte)al : null,
            r.CompressionDictSha256,
            r.CompressionDictBytes,
            r.CorrelationId);
    }

    public Exception? TryMapIdentifyErrorFrame(uint messageType, ReadOnlySpan<byte> body, Guid? correlationId)
    {
        if (messageType != MessageTypes.Responses.GenericError) return null;
        var error = WireCodec.Deserialize<ErrorResponse>(body.ToArray());
        if (error.CorrelationId != correlationId)
            return new Errors.ProtocolException("Identify error correlation id mismatch.");
        return CeleriantClient.CreateException(error);
    }

    public Exception? TryMapErrorFrame(uint messageType, ReadOnlySpan<byte> body)
        => messageType == MessageTypes.Responses.GenericError
            ? CeleriantClient.CreateException(WireCodec.Deserialize<ErrorResponse>(body.ToArray()))
            : null;
}
