using System.Text;
using Celeriant.Client.Errors;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;

namespace Celeriant.Client.IntegrationTests;

/// <summary>
/// Tests that verify specific bugs identified in the code review.
/// These tests are designed to FAIL before the fix and PASS after.
/// </summary>
[Collection("Server")]
public sealed class BugVerificationTests
{
    private readonly ServerFixture _fixture;

    public BugVerificationTests(ServerFixture fixture) => _fixture = fixture;

    private string Address
    {
        get
        {
            Skip.If(!_fixture.IsAvailable, "Server not running");
            return _fixture.Address;
        }
    }

    // =========================================================================
    // BUG-1: Sync and async paths must round-trip payloads identically.
    //
    // The sync and async send paths build the wire header through the same logic,
    // so a payload written via the sync path reads back byte-for-byte. (This
    // previously regressed when the sync path mishandled the compression header.)
    // =========================================================================

    [SkippableFact]
    public async Task Bug1_SyncSendRequest_SmallPayload_RoundTrips()
    {
        var address = Address;
        await using var client = await CeleriantClient.ConnectAsync(address, ct: default);

        var key = TestHelpers.NewKey();
        var payload = Encoding.UTF8.GetBytes("bug1-sync-small-payload");
        var writeReq = TestHelpers.SingleEventWrite(key, payload);

        var response = client.SendRequest(new ClientRequest.Write(writeReq));
        Assert.IsType<ClientResponse.Write>(response);

        // Verify the data was actually written correctly by reading it back
        var readResp = await client.SendRequestAsync(
            new ClientRequest.Read(TestHelpers.ReadAllRequest(key)));
        var read = Assert.IsType<ClientResponse.Read>(readResp);
        var events = read.Value.EventBatches.SelectMany(b => b.Events).ToArray();
        Assert.Single(events);
        Assert.Equal(payload, events[0].EventValue);
    }

    [SkippableFact]
    public async Task Bug1_SyncSendRequest_LargePayload_RoundTrips()
    {
        var address = Address;
        await using var client = await CeleriantClient.ConnectAsync(address, ct: default);

        var key = TestHelpers.NewKey();
        var payload = new byte[10_000];
        new Random(7).NextBytes(payload);
        var writeReq = TestHelpers.SingleEventWrite(key, payload);

        var response = client.SendRequest(new ClientRequest.Write(writeReq));
        Assert.IsType<ClientResponse.Write>(response);

        var readResp = await client.SendRequestAsync(
            new ClientRequest.Read(TestHelpers.ReadAllRequest(key)));
        var read = Assert.IsType<ClientResponse.Read>(readResp);
        var events = read.Value.EventBatches.SelectMany(b => b.Events).ToArray();
        Assert.Single(events);
        Assert.Equal(payload, events[0].EventValue);
    }

    [SkippableFact]
    public async Task Bug1_AsyncSendRequest_LargePayload_RoundTrips()
    {
        var address = Address;
        await using var client = await CeleriantClient.ConnectAsync(address, ct: default);

        var key = TestHelpers.NewKey();
        var payload = new byte[10_000];
        new Random(11).NextBytes(payload);
        var writeReq = TestHelpers.SingleEventWrite(key, payload);

        var response = await client.SendRequestAsync(new ClientRequest.Write(writeReq));
        Assert.IsType<ClientResponse.Write>(response);

        var readResp = await client.SendRequestAsync(
            new ClientRequest.Read(TestHelpers.ReadAllRequest(key)));
        var read = Assert.IsType<ClientResponse.Read>(readResp);
        var events = read.Value.EventBatches.SelectMany(b => b.Events).ToArray();
        Assert.Single(events);
        Assert.Equal(payload, events[0].EventValue);
    }

    // =========================================================================
    // BUG-3: CTS leak in ConnectAsync
    //
    // Can't directly test a CTS leak in a unit test, but we can verify that
    // creating many connections with timeouts doesn't cause issues.
    // =========================================================================

    [SkippableFact]
    public async Task Bug3_ManyConnectionsWithTimeout_NoExceptions()
    {
        var address = Address;

        // Create and dispose many connections with timeouts to stress the CTS path
        for (int i = 0; i < 50; i++)
        {
            await using var client = await CeleriantClient.ConnectAsync(
                address,
                connectionTimeout: TimeSpan.FromSeconds(5),
                tlsConfig: null);

            // Send one request to verify the connection works
            var key = TestHelpers.NewKey();
            // Non-existent aggregate throws — that's fine
            try
            {
                var resp = await client.SendRequestAsync(
                    new ClientRequest.AggregateDetails(
                        TestHelpers.DetailsRequest(key)));
                Assert.IsType<ClientResponse.AggregateDetails>(resp);
            }
            catch (CeleriantErrorException)
            {
                // Expected for non-existent aggregates
            }
        }

        // Force GC to flush any leaked CTS finalizers
        GC.Collect();
        GC.WaitForPendingFinalizers();
    }
}
