using System.Text;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Xunit.Abstractions;

namespace Celeriant.Client.Tests;

/// <summary>
/// Where the benchmark's ~1.8 KB of managed allocation per request comes from. Env-gated like the
/// pool probe: set CELERIANT_ALLOC_PROBE=1 to run.
/// </summary>
public class AllocProbe(ITestOutputHelper output)
{
    private static void Require() => Skip.If(
        Environment.GetEnvironmentVariable("CELERIANT_ALLOC_PROBE") is null,
        "Allocation probe: prints numbers, asserts none. Set CELERIANT_ALLOC_PROBE=1 to run.");

    private static ClientRequest.Write BenchmarkWrite()
    {
        Guid g1 = new(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        return new ClientRequest.Write(new WriteRequest
        {
            ClientId = g1,
            Writes = new Dictionary<AggregateKey, SingleAggregateWrite>
            {
                [new AggregateKey(g1, g1, g1)] = new SingleAggregateWrite
                {
                    Events =
                    [
                        new AggregateEvent
                        {
                            ClientSeq = 3,
                            EventSeq = 0,
                            EventId = g1,
                            EventTimestamp = DateTimeOffset.UnixEpoch,
                            EventTypeMajor = 2,
                            EventTypeMinor = 3,
                            EventValue = Encoding.UTF8.GetBytes("[conn-1] Hello World!"),
                        }
                    ],
                    AllowCreate = true,
                }
            },
        });
    }

    [SkippableFact]
    public void RequestSerialisationAllocation()
    {
        Require();

        var request = BenchmarkWrite();
        byte[] wire = WireCodec.Serialize(request.Value);
        output.WriteLine($"serialized request body: {wire.Length} bytes");

        var response = new WriteResponse { CorrelationId = Guid.NewGuid() };
        output.WriteLine($"serialized response body: {WireCodec.Serialize(response).Length} bytes");

        const int n = 200_000;
        for (int i = 0; i < 5_000; i++) _ = WireCodec.Serialize(request.Value);

        long before = GC.GetTotalAllocatedBytes(precise: true);
        for (int i = 0; i < n; i++) _ = WireCodec.Serialize(request.Value);
        long serializeBytes = GC.GetTotalAllocatedBytes(precise: true) - before;
        output.WriteLine($"WireCodec.Serialize: {serializeBytes / (double)n:F0} B/op");

        // The correlation stamp: a Guid plus a `with` clone of the request record.
        before = GC.GetTotalAllocatedBytes(precise: true);
        for (int i = 0; i < n; i++)
        {
            var req = request.Value with { CorrelationId = Guid.NewGuid() };
            GC.KeepAlive(req);
        }
        long stampBytes = GC.GetTotalAllocatedBytes(precise: true) - before;
        output.WriteLine($"correlation stamp (Guid.NewGuid + record clone): {stampBytes / (double)n:F0} B/op");

        // The response buffer the transport allocates fresh per frame (ReadFrameCoreAsync:460).
        int respLen = WireCodec.Serialize(response).Length;
        before = GC.GetTotalAllocatedBytes(precise: true);
        for (int i = 0; i < n; i++) GC.KeepAlive(new byte[respLen]);
        long respBytes = GC.GetTotalAllocatedBytes(precise: true) - before;
        output.WriteLine($"per-frame response buffer (new byte[{respLen}]): {respBytes / (double)n:F0} B/op");

        output.WriteLine(
            $"ACCOUNTED: {(serializeBytes + stampBytes + respBytes) / (double)n:F0} B/req "
            + "of the ~1900 B/req the benchmark reports. The remainder is async machinery "
            + "(Task + state-machine box per suspended await) and deserialization.");
    }
}
