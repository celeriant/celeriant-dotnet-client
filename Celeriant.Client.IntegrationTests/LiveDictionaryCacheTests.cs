using System.Security.Cryptography;
using System.Text;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Transport;
using Xunit.Abstractions;

namespace Celeriant.Client.IntegrationTests;

/// <summary>
/// A pool downloads a custom dictionary at most once in steady state: under a cold concurrent burst, across the node pools of a multi-node pool, with an identity
/// configured, and with nodes that disagree on the dictionary. Live against the Rust server.
/// </summary>
public sealed class LiveDictionaryCacheTests(ITestOutputHelper output)
{
    private static readonly TimeSpan Deadline = TimeSpan.FromSeconds(10);
    private const int CustomLength = 20_000;
    private const int Burst = 8;

    private static AggregateKey NewKey() => new(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());

    private static AggregateEvent[] Events(int seq) =>
        [new() { ClientSeq = seq, EventTimestamp = DateTimeOffset.UtcNow, EventTypeMajor = 1, EventValue = Encoding.UTF8.GetBytes($"{{\"event_type\":\"page_view\",\"n\":{seq}}}") }];

    private static ReadRequest ReadOf(AggregateKey key) => new() { AggregateKey = key, Filters = ReadFilters.From(1) };

    /// <summary>A read whose only job is to run the handshake on whichever node serves it; an unknown aggregate is fine.</summary>
    private static async Task Touch(Func<Task> read)
    {
        try { await read(); }
        catch (Celeriant.Client.Errors.AggregateNotFoundException) { }
    }

    private static CeleriantPool Pool(string address, int maxConnections, IReadOnlyList<string>? seeds = null,
        bool followers = false, ClientIdentityConfig? identity = null) => new(new CeleriantPoolOptions
    {
        Address = address,
        SeedAddresses = seeds,
        RouteReadsToFollowers = followers,
        IdentityConfig = identity,
        MaxConnections = maxConnections,
        ConnectionTimeout = Deadline,
        RequestTimeout = Deadline,
    });

    /// <summary>Hold <paramref name="count"/> leases at once (each past the idle ones is a new dial) and read through each.</summary>
    private static async Task HoldLeases(CeleriantPool pool, int count, AggregateKey key)
    {
        var held = new List<PooledConnection>();
        try
        {
            for (int i = 0; i < count; i++)
            {
                var lease = await pool.GetConnectionAsync().WaitAsync(Deadline);
                held.Add(lease);
                await Touch(() => lease.Client.ReadAsync(ReadOf(key)));
            }
        }
        finally
        {
            foreach (var lease in held)
                await lease.DisposeAsync();
        }
    }

    /// <summary>
    /// A fresh pool takes <see cref="Burst"/> concurrent first writes. Every dial snapshots the cache
    /// before any of them has learned, so each may be shipped the dictionary (the Rust pool does the
    /// same: no single-flight on the first learn). The count is written to the output. After the
    /// burst, the pool is in steady state: <see cref="Burst"/> further new dials must ship nothing.
    /// </summary>
    [SkippableFact]
    public async Task AColdConcurrentBurstMayShipPerDialButSteadyStateShipsNothing()
    {
        Skip.If(RustServerProcess.SkipReason is not null, RustServerProcess.SkipReason);
        byte[] dict = LiveDictionaryProbe.CustomDictionary(CustomLength);
        await using var server = await RustServerProcess.StartAsync(new RustServerOptions { CustomDictionary = dict });

        long before = await LiveDictionaryProbe.Shipped(server);
        var key = NewKey();
        await using var pool = Pool(server.Address, maxConnections: Burst * 2);

        using var gate = new SemaphoreSlim(0);
        var writers = Enumerable.Range(0, Burst).Select(async i =>
        {
            await gate.WaitAsync();
            await pool.WriteAsync(NewKey(), Events(1), Guid.NewGuid());
        }).ToArray();
        gate.Release(Burst);
        await Task.WhenAll(writers).WaitAsync(Deadline * 3);
        long burstDelta = await LiveDictionaryProbe.ShippedSince(server, before);
        long openAfterBurst = await LiveDictionaryProbe.OpenClientConnections(server);

        long steadyBefore = await LiveDictionaryProbe.Shipped(server);
        await HoldLeases(pool, Burst * 2, key);
        long steadyDelta = await LiveDictionaryProbe.ShippedSince(server, steadyBefore);
        long openSteady = await LiveDictionaryProbe.OpenClientConnections(server);

        output.WriteLine($"cold burst of {Burst}: {burstDelta} bytes = {(double)burstDelta / CustomLength:F2} ships, {openAfterBurst} connections open");
        output.WriteLine($"steady state, {Burst * 2} leases held: {steadyDelta} bytes = {(double)steadyDelta / CustomLength:F2} ships, {openSteady} connections open");

        Assert.True(burstDelta % CustomLength == 0 && burstDelta >= CustomLength && burstDelta <= Burst * CustomLength,
            $"cold burst shipped {burstDelta} bytes; expected 1..{Burst} whole ships");
        Assert.True(openSteady > openAfterBurst, $"harness: steady phase opened no new dial ({openAfterBurst} -> {openSteady})");
        Assert.Equal(0, steadyDelta);
    }

    /// <summary>
    /// One pool over two nodes holding the same custom dictionary: writes go to the leader's node pool,
    /// reads to the follower's. The DictCache is one per <see cref="CeleriantPool"/> across its node
    /// pools, so the pair must ship exactly once in total.
    /// </summary>
    [SkippableFact]
    public async Task TwoNodePoolsOfOnePoolShareOneDownload()
    {
        Skip.If(RustServerProcess.SkipReason is not null, RustServerProcess.SkipReason);
        byte[] dict = LiveDictionaryProbe.CustomDictionary(CustomLength);
        await using var a = await RustServerProcess.StartAsync(new RustServerOptions { CustomDictionary = dict });
        await using var b = await RustServerProcess.StartAsync(new RustServerOptions { CustomDictionary = dict });

        long beforeA = await LiveDictionaryProbe.Shipped(a), beforeB = await LiveDictionaryProbe.Shipped(b);
        await using (var pool = Pool(a.Address, maxConnections: 4, seeds: [b.Address], followers: true))
        {
            await pool.WriteAsync(NewKey(), Events(1), Guid.NewGuid());
            await Touch(() => pool.ReadAsync(ReadOf(NewKey())));
            await pool.WriteAsync(NewKey(), Events(1), Guid.NewGuid());
            await Touch(() => pool.ReadAsync(ReadOf(NewKey())));
            Assert.True(await LiveDictionaryProbe.OpenClientConnections(b) >= 1, "harness: no read reached node B");
        }

        long dA = await LiveDictionaryProbe.ShippedSince(a, beforeA), dB = await LiveDictionaryProbe.ShippedSince(b, beforeB);
        output.WriteLine($"node A shipped {dA} bytes, node B shipped {dB} bytes");
        Assert.Equal(CustomLength, dA + dB);
    }

    /// <summary>With an API key configured, the eager Identify path shares the pool cache too, so the dictionary still downloads once.</summary>
    [SkippableFact]
    public async Task APoolWithAnApiKeyDownloadsACustomDictionaryOnceAcrossManyConnections()
    {
        Skip.If(RustServerProcess.SkipReason is not null, RustServerProcess.SkipReason);
        byte[] dict = LiveDictionaryProbe.CustomDictionary(CustomLength);
        byte[] apiKey = RandomNumberGenerator.GetBytes(32);
        await using var server = await RustServerProcess.StartAsync(new RustServerOptions { CustomDictionary = dict, ReadWriteApiKey = apiKey });

        long before = await LiveDictionaryProbe.Shipped(server);
        var key = NewKey();
        await using (var pool = Pool(server.Address, maxConnections: Burst, identity: ClientIdentityConfig.FromApiKey(Convert.ToBase64String(apiKey))))
        {
            await pool.WriteAsync(key, Events(1), Guid.NewGuid());
            var held = new List<PooledConnection>();
            try
            {
                for (int i = 0; i < Burst; i++)
                {
                    held.Add(await pool.GetConnectionAsync().WaitAsync(Deadline));
                    await Touch(() => held[^1].Client.ReadAsync(ReadOf(key)));
                }
                Assert.Equal(Burst, await LiveDictionaryProbe.AwaitOpenClientConnections(server, Burst, TimeSpan.FromSeconds(2)));
            }
            finally
            {
                foreach (var lease in held)
                    await lease.DisposeAsync();
            }
        }
        Assert.Equal(CustomLength, await LiveDictionaryProbe.ShippedSince(server, before));
    }

    /// <summary>
    /// Documents a known limit, shared with the Rust <c>dict_cache.rs</c>: one pool over a custom node and a built-in node. The cache holds one learned dictionary and a
    /// confirmed built-in is "learned" too, so alternating requests evict each other and re-ship: the
    /// built-in node ships its 14 KiB once the cache holds the custom sha.
    /// </summary>
    [SkippableFact]
    public async Task NodesThatDisagreeOnTheDictionaryReshipAsTheCacheFlaps()
    {
        Skip.If(RustServerProcess.SkipReason is not null, RustServerProcess.SkipReason);
        byte[] dict = LiveDictionaryProbe.CustomDictionary(CustomLength);
        await using var custom = await RustServerProcess.StartAsync(new RustServerOptions { CustomDictionary = dict });
        await using var builtin = await RustServerProcess.StartAsync(new RustServerOptions());

        long beforeC = await LiveDictionaryProbe.Shipped(custom), beforeB = await LiveDictionaryProbe.Shipped(builtin);
        await using (var pool = Pool(custom.Address, maxConnections: 1, seeds: [builtin.Address], followers: true))
        {
            for (int i = 0; i < 3; i++)
            {
                // One connection per node pool (max 1), each new dial only if the pool retired the last.
                await pool.WriteAsync(NewKey(), Events(1), Guid.NewGuid());
                await Touch(() => pool.ReadAsync(ReadOf(NewKey())));
            }
        }
        long dC = await LiveDictionaryProbe.ShippedSince(custom, beforeC), dB = await LiveDictionaryProbe.ShippedSince(builtin, beforeB);
        output.WriteLine($"custom node shipped {dC} bytes ({(double)dC / CustomLength:F2} ships); built-in node shipped {dB} bytes ({(double)dB / BuiltinDictionary.Dict.Bytes.Length:F2} ships)");
        Assert.Equal(CustomLength, dC);
        Assert.Equal(BuiltinDictionary.Dict.Bytes.Length, dB);
    }
}
