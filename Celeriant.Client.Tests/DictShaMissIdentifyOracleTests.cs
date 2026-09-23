using System.Buffers;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Transport;
using MessagePack;

namespace Celeriant.Client.Tests;

/// <summary>
/// Dictionary negotiation (invariants.md "Wire Format", docs/reference/wire-protocol.md): when the
/// client advertises a dictionary sha during Identify and the server confirms it without resending
/// the bytes, the client must resolve them from its cache. A miss must fail Identify and poison the
/// connection, not proceed with no dictionary and die on the first ZstdDict response.
/// </summary>
public class DictShaMissIdentifyOracleTests
{
    private const string ShaUnderTest = "sha-under-test";

    [Fact]
    public async Task ShaOnlyConfirm_WithDictLookupMiss_MustNotSilentlyProceedToLateZstdDictFailure()
    {
        // The fake server: confirm Identify with a sha-only response, then answer the first
        // data request with a ZstdDict-compressed frame (compression type 1).
        await using var server = FakeCeleriantServer.Start(async (session, messageType, _) =>
        {
            if (messageType == MessageTypes.Requests.Identify)
            {
                await session.SendFrameAsync(MessageTypes.Responses.Identify, BuildShaOnlyIdentifyBody());
                return;
            }

            await session.SendRawAsync(BuildZstdDictFrame(MessageTypes.Responses.AggregateDetails));
        });

        await using var client = await CeleriantClient.ConnectAsync(
            server.Address, connectionTimeout: TimeSpan.FromSeconds(5));

        // Advertise a known sha, but the cache lookup misses (returns null).
        Exception? identifyFailure = await Record.ExceptionAsync(() =>
            client.IdentifyAsync(
                ClientIdentityConfig.FromClientId(Guid.NewGuid()),
                knownDictSha: ShaUnderTest,
                dictLookup: _ => null));

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

        Assert.True(client.IsPoisoned, "Identify failed but did not poison the connection.");
    }

    [Fact]
    public async Task ShaOnlyConfirm_WithNullDictLookup_MustPoisonAtIdentifyTime()
    {
        await using var server = FakeCeleriantServer.Start(async (session, messageType, _) =>
        {
            if (messageType == MessageTypes.Requests.Identify)
            {
                await session.SendFrameAsync(MessageTypes.Responses.Identify, BuildShaOnlyIdentifyBody());
                return;
            }

            await session.SendRawAsync(BuildZstdDictFrame(MessageTypes.Responses.AggregateDetails));
        });

        await using var client = await CeleriantClient.ConnectAsync(
            server.Address, connectionTimeout: TimeSpan.FromSeconds(5));

        Exception? identifyFailure = await Record.ExceptionAsync(() =>
            client.IdentifyAsync(
                ClientIdentityConfig.FromClientId(Guid.NewGuid()),
                knownDictSha: ShaUnderTest,
                dictLookup: null));

        Assert.True(
            identifyFailure is not null,
            "Identify accepted a sha-only dictionary confirmation with no dictLookup. "
            + "The client must fail cleanly at Identify time.");
        Assert.True(client.IsPoisoned, "Identify failed but did not poison the connection.");
    }

    /// <summary>
    /// A five-element msgpack array in the server's <c>IdentifyResponse</c> field order, with the
    /// dictionary sha present and the bytes absent (sha-only confirmation).
    /// </summary>
    private static byte[] BuildShaOnlyIdentifyBody()
    {
        var buffer = new ArrayBufferWriter<byte>();
        var writer = new MessagePackWriter(buffer);
        writer.WriteArrayHeader(5);
        writer.WriteNil();               // correlation_id
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
                WireHeader.ProtocolVersionV3, messageType, (uint)body.Length, (uint)body.Length,
                CompressionType.ZstdDict)
            .WriteTo(frame);
        body.CopyTo(frame, WireHeader.Size);
        return frame;
    }

    private static string Describe(Exception? ex)
        => ex is null ? "no exception (silently succeeded)" : $"{ex.GetType().Name}: {ex.Message}";
}