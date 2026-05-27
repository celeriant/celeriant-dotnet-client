using System.Text;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;

namespace Celeriant.Client.IntegrationTests;

/// <summary>
/// End-to-end write/read round-trips through the actual server.
///
/// Wire compression is now automatic and dictionary-based: the client only compresses when the
/// cluster shipped a dictionary during Identify and the payload clears the size threshold, and the
/// server transparently decompresses. These tests connect without identity (so frames are sent
/// uncompressed) and verify the payload survives the round-trip regardless.
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

    [SkippableFact]
    public async Task SmallPayload_Write_ReadBack_Preserved()
    {
        var key = TestHelpers.NewKey();
        var payload = Encoding.UTF8.GetBytes("compression-roundtrip-payload");

        var writeResp = await Client.SendRequestAsync(
            new ClientRequest.Write(TestHelpers.SingleEventWrite(key, payload)));
        Assert.IsType<ClientResponse.Write>(writeResp);

        var readResp = await Client.SendRequestAsync(
            new ClientRequest.Read(TestHelpers.ReadAllRequest(key)));
        var read = Assert.IsType<ClientResponse.Read>(readResp);
        var events = read.Value.EventBatches.SelectMany(b => b.Events).ToArray();
        Assert.Single(events);
        Assert.Equal(payload, events[0].EventValue);
    }

    [SkippableFact]
    public async Task LargePayload_Write_ReadBack_Preserved()
    {
        var key = TestHelpers.NewKey();
        // 10KB payload — large enough to exercise the compression threshold when a dict is present.
        var payload = new byte[10_000];
        new Random(42).NextBytes(payload);

        var writeResp = await Client.SendRequestAsync(
            new ClientRequest.Write(TestHelpers.SingleEventWrite(key, payload)));
        Assert.IsType<ClientResponse.Write>(writeResp);

        var readResp = await Client.SendRequestAsync(
            new ClientRequest.Read(TestHelpers.ReadAllRequest(key)));
        var read = Assert.IsType<ClientResponse.Read>(readResp);
        var events = read.Value.EventBatches.SelectMany(b => b.Events).ToArray();
        Assert.Single(events);
        Assert.Equal(payload, events[0].EventValue);
    }
}
