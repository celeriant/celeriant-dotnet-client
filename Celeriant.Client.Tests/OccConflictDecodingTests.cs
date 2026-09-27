using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Transport;

namespace Celeriant.Client.Tests;

public sealed class OccConflictDecodingTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Conflict_version_overflow_is_rejected_by_the_codec_before_correlation(bool current)
    {
        var correlation = Guid.NewGuid();
        var key = new AggregateKey(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());
        var valid = ConflictCodec.Decode(FakeServerProtocol.ConflictFrame(correlation, key, long.MaxValue, long.MaxValue));
        Assert.Equal(long.MaxValue, Assert.Single(valid.Conflicts).Expected);
        foreach (var overflow in new[] { (ulong)long.MaxValue + 1, ulong.MaxValue })
        {
            var body = FakeServerProtocol.ConflictFrame(correlation, key, current ? 1 : overflow, current ? overflow : 1);
            Assert.Throws<OverflowException>(() => ConflictCodec.Decode(body));
        }
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task Dictionary_compressed_typed_conflicts_keep_the_socket_reusable(bool delete, bool synchronous)
    {
        byte[] dict = System.Text.Encoding.UTF8.GetBytes("aggregate version conflict dictionary payload repeated dictionary payload");
        var key = new AggregateKey(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());
        var correlation = Guid.NewGuid();
        int mutations = 0;
        await using var server = FakeCeleriantServer.Start(async (session, type, body) =>
        {
            if (type == MessageTypes.Requests.Identify)
            {
                var identity = WireCodec.Deserialize<IdentifyRequest>(body);
                await session.SendFrameAsync(MessageTypes.Responses.Identify, WireCodec.Serialize(new IdentifyResponse
                {
                    CorrelationId = identity.CorrelationId,
                    CompressionDictSha256 = "test-dict",
                    CompressionDictBytes = dict,
                }));
                return;
            }
            Interlocked.Increment(ref mutations);
            var conflict = FakeServerProtocol.ConflictFrame(correlation, key, 1, 2);
            var compressed = DictCompression.CompressWithDict(conflict, dict);
            var response = new byte[WireHeader.Size + compressed.Length];
            WireHeader.ForCompressedRequest(5, delete ? 14u : 13u, (uint)compressed.Length, (uint)conflict.Length,
                CompressionType.ZstdDict).WriteTo(response);
            compressed.CopyTo(response, WireHeader.Size);
            await session.SendRawAsync(response);
        });
        await using var client = await CeleriantClient.ConnectAsync(server.Address);
        client.WithTimeout(TimeSpan.FromSeconds(3));
        await client.IdentifyAsync(ClientIdentityConfig.FromClientId(Guid.NewGuid()));
        ClientRequest request = delete
            ? new ClientRequest.Delete(new DeleteRequest
            {
                CorrelationId = correlation, ClientId = Guid.NewGuid(),
                Deletes = new() { [key] = new SingleAggregateDelete { ExpectedVersion = 1 } },
            })
            : new ClientRequest.Write(new WriteRequest
            {
                CorrelationId = correlation, ClientId = Guid.NewGuid(),
                Writes = new() { [key] = SingleAggregateWrite.Guard(1) },
            });
        for (int i = 0; i < 2; i++)
        {
            var error = synchronous
                ? Record.Exception(() => client.SendRequest(request))
                : await Record.ExceptionAsync(() => client.SendRequestAsync(request));
            IReadOnlyList<AggregateConflict> conflicts = delete
                ? Assert.IsType<DeleteOccException>(error).Conflicts
                : Assert.IsType<WriteOccException>(error).Conflicts;
            Assert.Equal(new AggregateConflict(key, 1, 2), Assert.Single(conflicts));
            Assert.False(client.IsPoisoned);
        }
        Assert.Equal(2, mutations);
        Assert.Equal(1, server.ConnectionsAccepted);
    }

    [Theory]
    [InlineData("uncompressed-length", false)]
    [InlineData("uncompressed-length", true)]
    [InlineData("decompressed-length", false)]
    [InlineData("decompressed-length", true)]
    [InlineData("corrupt-zstd", false)]
    [InlineData("corrupt-zstd", true)]
    public async Task Malformed_conflict_compression_is_a_protocol_failure_and_retires_the_connection(
        string defect, bool synchronous)
    {
        byte[] dict = "guard conflict compression dictionary repeated bytes"u8.ToArray();
        var key = OccTestData.Key(1, 2, 3);
        var correlation = Guid.NewGuid();
        int mutations = 0;
        await using var server = FakeCeleriantServer.Start(async (session, type, body) =>
        {
            if (type == MessageTypes.Requests.Identify)
            {
                var identify = WireCodec.Deserialize<IdentifyRequest>(body);
                await session.SendFrameAsync(MessageTypes.Responses.Identify, WireCodec.Serialize(new IdentifyResponse
                {
                    CorrelationId = identify.CorrelationId,
                    CompressionDictSha256 = "test-dict",
                    CompressionDictBytes = dict,
                }));
                return;
            }
            Interlocked.Increment(ref mutations);
            byte[] plain = OccTestData.ConflictBody(correlation, [key]);
            byte[] payload = defect == "uncompressed-length" ? plain
                : defect == "corrupt-zstd" ? new byte[] { 1, 2, 3, 4 }
                : DictCompression.CompressWithDict(plain, dict);
            var bytes = new byte[WireHeader.Size + payload.Length];
            new WireHeader(5, 13, (uint)payload.Length, (uint)plain.Length + 1,
                defect == "uncompressed-length" ? (byte)0 : (byte)1).WriteTo(bytes);
            payload.CopyTo(bytes, WireHeader.Size);
            await session.SendRawAsync(bytes);
        });
        await using var client = await CeleriantClient.ConnectAsync(server.Address);
        client.WithTimeout(TimeSpan.FromSeconds(3));
        await client.IdentifyAsync(ClientIdentityConfig.FromClientId(Guid.NewGuid()));
        var request = new ClientRequest.Write(new WriteRequest
        {
            CorrelationId = correlation, ClientId = OccTestData.WriterId,
            Writes = new() { [key] = SingleAggregateWrite.Guard(10) },
        });
        var error = synchronous
            ? Record.Exception(() => client.SendRequest(request))
            : await Record.ExceptionAsync(() => client.SendRequestAsync(request));
        Assert.True(error is ProtocolException && client.IsPoisoned,
            $"{defect}: expected poisoned ProtocolException, got {error?.GetType().Name}, poisoned={client.IsPoisoned}");
        Assert.Equal(1, mutations);
        Assert.NotNull(await Record.ExceptionAsync(() => client.SendRequestAsync(request)));
        Assert.Equal(1, mutations);
    }
}
