using System.Text;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;

namespace Celeriant.Client.IntegrationTests;

/// <summary>
/// End-to-end compression tests through the actual server.
/// Unit tests verify the codec in isolation; these verify the full wire round-trip.
/// </summary>
[Collection("Server")]
public sealed class CompressionIntegrationTests
{
    private readonly ServerFixture _fixture;

    public CompressionIntegrationTests(ServerFixture fixture) => _fixture = fixture;

    private CeleriantClient Client
    {
        get
        {
            Skip.If(!_fixture.IsAvailable, "Server not running");
            return _fixture.Client!;
        }
    }

    [SkippableTheory]
    [InlineData(CompressionType.Zstd)]
    [InlineData(CompressionType.Snappy)]
    [InlineData(CompressionType.Brotli)]
    [InlineData(CompressionType.Gzip)]
    public async Task WriteWithCompression_ReadBack_PayloadPreserved(CompressionType compression)
    {
        var key = TestHelpers.NewKey();
        var payload = Encoding.UTF8.GetBytes($"compression-test-{compression}-payload");

        var writeResp = await Client.SendRequestAsync(
            new ClientRequest.Write(TestHelpers.SingleEventWrite(key, payload)),
            compression);
        Assert.IsType<ClientResponse.Write>(writeResp);

        var readResp = await Client.SendRequestAsync(
            new ClientRequest.Read(TestHelpers.ReadAllRequest(key)));
        var read = Assert.IsType<ClientResponse.Read>(readResp);
        var events = read.Value.EventBatches.SelectMany(b => b.Events).ToArray();
        Assert.Single(events);
        Assert.Equal(payload, events[0].EventValue);
    }

    [SkippableTheory]
    [InlineData(CompressionType.Zstd)]
    [InlineData(CompressionType.Snappy)]
    [InlineData(CompressionType.Brotli)]
    [InlineData(CompressionType.Gzip)]
    public async Task LargePayload_CompressedWrite_ReadBack(CompressionType compression)
    {
        var key = TestHelpers.NewKey();
        // 10KB payload — large enough that compression is meaningful
        var payload = new byte[10_000];
        new Random(42).NextBytes(payload);

        var writeResp = await Client.SendRequestAsync(
            new ClientRequest.Write(TestHelpers.SingleEventWrite(key, payload)),
            compression);
        Assert.IsType<ClientResponse.Write>(writeResp);

        var readResp = await Client.SendRequestAsync(
            new ClientRequest.Read(TestHelpers.ReadAllRequest(key)));
        var read = Assert.IsType<ClientResponse.Read>(readResp);
        var events = read.Value.EventBatches.SelectMany(b => b.Events).ToArray();
        Assert.Single(events);
        Assert.Equal(payload, events[0].EventValue);
    }
}
