using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;

namespace Celeriant.Client.Tests;

/// <summary>
/// Protocol-level helpers shared by the tests that drive <see cref="FakeCeleriantServer"/>.
/// </summary>
internal static class FakeServerProtocol
{
    /// <summary>
    /// A pool capped at one connection per node, so a second request is forced onto the same
    /// physical connection whenever the pool decides to reuse it.
    /// </summary>
    public static CeleriantPoolOptions SingleConnectionPool(FakeCeleriantServer server) => new()
    {
        Address = server.Address,
        MaxConnections = 1,
        ConnectionTimeout = TimeSpan.FromSeconds(5),
        RequestTimeout = TimeSpan.FromSeconds(10),
    };

    public static AggregateKey NewKey()
        => new(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());

    public static AggregateDetailsRequest Details(AggregateKey key, Guid? correlationId = null)
        => new() { AggregateKey = key, CorrelationId = correlationId };

    public static AggregateDetailsRequest DecodeDetails(byte[] body)
        => WireCodec.Deserialize<AggregateDetailsRequest>(body);

    /// <summary>
    /// The answer to <paramref name="request"/>. The requested aggregate id is stamped into
    /// <see cref="AggregateDetailsResponse.LastClientId"/> so a test can tell whose answer it got,
    /// and the correlation id is echoed the way the real server echoes it.
    /// </summary>
    public static byte[] DetailsAnswer(AggregateDetailsRequest request, Guid? correlationIdOverride = null)
        => WireCodec.Serialize(new AggregateDetailsResponse
        {
            CorrelationId = correlationIdOverride ?? request.CorrelationId,
            MaxAggregateVersion = 1,
            LastClientId = request.AggregateKey.AggregateId,
        });

    public static byte[] ErrorFrame(uint errorCode, string message, Guid? correlationId)
        => WireCodec.Serialize(new ErrorResponse
        {
            CorrelationId = correlationId,
            ErrorCode = errorCode,
            ErrorMessage = message,
        });

    public static byte[] ProtocolErrorFrame(uint errorCode, string message, Guid? correlationId)
        => WireCodec.Serialize(new ProtocolErrorResponse
        {
            CorrelationId = correlationId,
            ErrorCode = errorCode,
            ErrorMessage = message,
        });

    /// <summary>Answer every request immediately and correctly.</summary>
    public static FakeCeleriantServer.RequestHandler EchoDetails() =>
        (session, _, body) =>
        {
            var request = DecodeDetails(body);
            return session.SendFrameAsync(MessageTypes.Responses.AggregateDetails, DetailsAnswer(request));
        };
}
