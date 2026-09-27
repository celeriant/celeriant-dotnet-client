using System.Buffers;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Transport;
using MessagePack;

namespace Celeriant.Client.Tests;

/// <summary>
/// Dictionary negotiation (invariants.md "Wire Format", docs/reference/wire-protocol.md): when the
/// server confirms a dictionary sha during Identify without resending the bytes, the client must
/// resolve them from what it advertised or the built-in. A sha that is neither must fail Identify and
/// poison the connection, not proceed with no dictionary and die on the first ZstdDict response.
/// </summary>
public class DictShaMissIdentifyTests
{
    private const string ShaUnderTest = "sha-under-test";

    [Fact]
    public async Task ShaOnlyConfirm_OfAShaNotAdvertised_MustNotSilentlyProceedToLateZstdDictFailure()
    {
        // The fake server: confirm Identify with a sha-only response, then answer the first
        // data request with a ZstdDict-compressed frame (compression type 1).
        await using var server = FakeCeleriantServer.Start(async (session, messageType, body) =>
        {
            if (messageType == MessageTypes.Requests.Identify)
            {
                await session.SendFrameAsync(MessageTypes.Responses.Identify, BuildShaOnlyIdentifyBody(WireCodec.Deserialize<IdentifyRequest>(body).CorrelationId));
                return;
            }

            await session.SendRawAsync(BuildZstdDictFrame(MessageTypes.Responses.AggregateDetails));
        });

        // Advertise a dictionary other than the one the server confirms, so resolution misses.
        var cache = new DictCache();
        cache.Learn(new CachedDict("some-other-sha", [1, 2, 3]));
        await using var client = await CeleriantClient.ConnectWithDictCacheAsync(
            server.Address, TimeSpan.FromSeconds(5), tlsConfig: null, cache, CancellationToken.None);

        Exception? identifyFailure = await Record.ExceptionAsync(() =>
            client.IdentifyAsync(ClientIdentityConfig.FromClientId(Guid.NewGuid())));

        // Then drive a data request so the server's ZstdDict-compressed reply is read.
        Exception? requestFailure = await Record.ExceptionAsync(() =>
            client.SendRequestAsync(
                new ClientRequest.AggregateDetails(FakeServerProtocol.Details(FakeServerProtocol.NewKey()))));

        Assert.True(
            identifyFailure is not null,
            "Identify accepted a sha-only dictionary confirmation whose cache lookup missed. "
            + "The connection then silently proceeded without a dictionary and failed later: "
            + $"first compressed response threw {Describe(requestFailure)}. "
            + "The client must instead fail cleanly at Identify time.");

        Assert.Contains("no matching dictionary", Assert.IsType<Celeriant.Client.Errors.ProtocolException>(identifyFailure).Message);
        Assert.True(client.IsPoisoned, "Identify failed but did not poison the connection.");
    }

    [Fact]
    public async Task ShaOnlyConfirm_OfAShaNeitherAdvertisedNorBuiltin_MustPoisonAtIdentifyTime()
    {
        await using var server = FakeCeleriantServer.Start(async (session, messageType, body) =>
        {
            if (messageType == MessageTypes.Requests.Identify)
            {
                await session.SendFrameAsync(MessageTypes.Responses.Identify, BuildShaOnlyIdentifyBody(WireCodec.Deserialize<IdentifyRequest>(body).CorrelationId));
                return;
            }

            await session.SendRawAsync(BuildZstdDictFrame(MessageTypes.Responses.AggregateDetails));
        });

        await using var client = await CeleriantClient.ConnectAsync(
            server.Address, connectionTimeout: TimeSpan.FromSeconds(5));

        Exception? identifyFailure = await Record.ExceptionAsync(() =>
            client.IdentifyAsync(ClientIdentityConfig.FromClientId(Guid.NewGuid())));

        Assert.True(
            identifyFailure is not null,
            "Identify accepted a sha-only confirmation of a dictionary the client never advertised. "
            + "The client must fail cleanly at Identify time.");
        Assert.Contains("no matching dictionary", Assert.IsType<Celeriant.Client.Errors.ProtocolException>(identifyFailure).Message);
        Assert.True(client.IsPoisoned, "Identify failed but did not poison the connection.");
    }

    /// <summary>
    /// A five-element msgpack array in the server's <c>IdentifyResponse</c> field order, with the
    /// dictionary sha present and the bytes absent (sha-only confirmation).
    /// </summary>
    private static byte[] BuildShaOnlyIdentifyBody(Guid? correlation)
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new MessagePackWriter(buffer);
        writer.WriteArrayHeader(5);
        CeleriantNullableGuidFormatter.Instance.Serialize(ref writer, correlation, WireCodec.Options);
        writer.WriteNil();               // client_id
        writer.WriteNil();               // access_level
        writer.Write(ShaUnderTest);      // compression_dict_sha256
        writer.WriteNil();               // compression_dict_bytes
        writer.Flush();
        return buffer.WrittenSpan.ToArray();
    }

    /// <summary>A 17-byte frame header plus a body, marked ZstdDict-compressed.</summary>
    private static byte[] BuildZstdDictFrame(uint messageType)
    {
        byte[] body = [0x28, 0xB5, 0x2F, 0xFD];
        var frame = new byte[WireHeader.Size + body.Length];
        WireHeader
            .ForCompressedRequest(
                WireHeader.ProtocolVersionV5, messageType, (uint)body.Length, (uint)body.Length,
                CompressionType.ZstdDict)
            .WriteTo(frame);
        body.CopyTo(frame, WireHeader.Size);
        return frame;
    }

    private static string Describe(Exception? ex)
        => ex is null ? "no exception (silently succeeded)" : $"{ex.GetType().Name}: {ex.Message}";
}