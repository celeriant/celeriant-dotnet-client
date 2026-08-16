using Moq;

namespace Celeriant.Client.Tests;

/// <summary>
/// Blind-oracle routing tests for CeleriantPool read routing (session/goal.md).
/// Written against the contract only, without reading the implementation.
/// Mirrors celeriant-db/session/oracle_routing_tests.rs, adapted where the
/// .NET contract diverges (leader starts as Options.Address; no clear_leader;
/// unspecified tail order). Opt-in follows goal.md Amendment 2: rotated
/// followers first, leader LAST as a last resort, never excluded.
/// </summary>
public class OracleRoutingTests
{
    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static CeleriantPoolOptions MakeOptions(
        string address,
        IReadOnlyList<string>? seeds = null,
        bool routeReadsToFollowers = false)
        => new()
        {
            Address = address,
            SeedAddresses = seeds,
            RouteReadsToFollowers = routeReadsToFollowers,
        };

    private static Mock<INodeConnectionPool> MockPool(string address)
    {
        var mock = new Mock<INodeConnectionPool>();
        mock.Setup(p => p.Address).Returns(address);
        mock.Setup(p => p.DisposeAsync()).Returns(ValueTask.CompletedTask);
        return mock;
    }

    private static CeleriantPool CreatePool(CeleriantPoolOptions options)
        => new(options, (addr, _, _) => MockPool(addr).Object);

    // -----------------------------------------------------------------------
    // Default mode (RouteReadsToFollowers == false)
    // -----------------------------------------------------------------------

    [Fact]
    public async Task OracleDefaultNoLeaderPrimaryFirstAllKnownOnce()
    {
        // .NET: _leaderAddress starts as Options.Address, so "no cached leader"
        // collapses into leader-is-primary.
        await using var pool = CreatePool(MakeOptions("p:1", seeds: ["b:1", "c:1"]));

        var addrs = pool.GetReadNodeAddresses();

        Assert.Equal("p:1", addrs[0]);
        Assert.Equal(3, addrs.Length);
        Assert.Equal(3, addrs.Distinct().Count());
        foreach (var a in new[] { "p:1", "b:1", "c:1" })
            Assert.Contains(a, addrs);
    }

    [Fact]
    public async Task OracleDefaultCachedLeaderSeedFirstPrimaryLater()
    {
        await using var pool = CreatePool(MakeOptions("p:1", seeds: ["b:1", "c:1"]));
        pool.SetLeaderForTesting("b:1");

        var addrs = pool.GetReadNodeAddresses();

        Assert.Equal("b:1", addrs[0]);
        // primary is still a fallback candidate, just not first
        Assert.Contains("p:1", addrs.Skip(1));
        Assert.Equal(3, addrs.Length);
        Assert.Equal(3, addrs.Distinct().Count());
    }

    [Fact]
    public async Task OracleDefaultLeaderFirstStableAcrossCalls()
    {
        // Adapted from oracle_default_order_stable_across_calls: .NET promises
        // only index 0 stability (tail order unspecified), so pin leader-first
        // and leader-once on every call, not the full list.
        await using var pool = CreatePool(MakeOptions("p:1", seeds: ["b:1", "c:1"]));
        pool.SetLeaderForTesting("c:1");

        for (var i = 0; i < 6; i++)
        {
            var addrs = pool.GetReadNodeAddresses();
            Assert.Equal("c:1", addrs[0]);
            Assert.Single(addrs, "c:1");
        }
    }

    [Fact]
    public async Task OracleDefaultWatchLeaderElsePrimary()
    {
        await using var pool = CreatePool(MakeOptions("p:1", seeds: ["b:1"]));

        Assert.Equal("p:1", pool.GetWatchAddress());
        pool.SetLeaderForTesting("b:1");
        Assert.Equal("b:1", pool.GetWatchAddress());
    }

    [Fact]
    public async Task OracleDefaultLeaderResetToPrimaryFirst()
    {
        // Adapted from oracle_default_clear_leader_reverts_to_primary_first:
        // .NET has no clear_leader; setting the leader back to the primary is
        // the closest analog.
        await using var pool = CreatePool(MakeOptions("p:1", seeds: ["b:1", "c:1"]));
        pool.SetLeaderForTesting("b:1");
        pool.SetLeaderForTesting("p:1");

        var addrs = pool.GetReadNodeAddresses();
        Assert.Equal("p:1", addrs[0]);
        Assert.Equal("p:1", pool.GetWatchAddress());
    }

    [Fact]
    public async Task OracleDefaultSecondUpdateWins()
    {
        await using var pool = CreatePool(MakeOptions("p:1", seeds: ["b:1", "c:1"]));
        pool.SetLeaderForTesting("b:1");
        pool.SetLeaderForTesting("c:1");

        var addrs = pool.GetReadNodeAddresses();
        Assert.Equal("c:1", addrs[0]);
        Assert.Equal("c:1", pool.GetWatchAddress());
    }

    [Fact]
    public async Task OracleDefaultUnknownLeaderGoesFirstKnownsFollow()
    {
        // SetLeaderForTesting registers a node pool for the address (mirrors
        // discovery), so the former unknown is now a known node; leader-first applies.
        await using var pool = CreatePool(MakeOptions("p:1", seeds: ["b:1"]));
        pool.SetLeaderForTesting("x:9");

        var addrs = pool.GetReadNodeAddresses();
        Assert.Equal("x:9", addrs[0]);
        Assert.Equal(3, addrs.Length);
        foreach (var a in new[] { "x:9", "p:1", "b:1" })
            Assert.Single(addrs, a);
        Assert.Equal("x:9", pool.GetWatchAddress());
    }

    // -----------------------------------------------------------------------
    // Opt-in mode (RouteReadsToFollowers == true)
    // -----------------------------------------------------------------------

    [Fact]
    public async Task OracleOptinLeaderPresentButLast()
    {
        // Amendment 2: the leader is no longer excluded: it is the last-resort
        // candidate. Every follower appears exactly once before it.
        await using var pool = CreatePool(
            MakeOptions("p:1", seeds: ["b:1", "c:1"], routeReadsToFollowers: true));
        pool.SetLeaderForTesting("p:1");

        var addrs = pool.GetReadNodeAddresses();
        Assert.Equal(3, addrs.Length);
        Assert.Equal("p:1", addrs[^1]);
        Assert.Single(addrs, "p:1");
        foreach (var a in new[] { "b:1", "c:1" })
            Assert.Single(addrs.Take(addrs.Length - 1), a);
    }

    [Fact]
    public async Task OracleOptinFreshPoolTreatsPrimaryAsLeader()
    {
        // Adapted from oracle_optin_no_leader_all_known_candidates: .NET has no
        // "no leader" state (_leaderAddress starts as Options.Address), so a fresh
        // opt-in pool treats the primary as the leader: last resort, not excluded.
        await using var pool = CreatePool(
            MakeOptions("p:1", seeds: ["b:1", "c:1"], routeReadsToFollowers: true));

        var addrs = pool.GetReadNodeAddresses();
        Assert.Equal(3, addrs.Length);
        Assert.Equal("p:1", addrs[^1]);
        Assert.Single(addrs, "p:1");
        foreach (var a in new[] { "b:1", "c:1" })
            Assert.Single(addrs.Take(addrs.Length - 1), a);
    }

    [Fact]
    public async Task OracleOptinRotationCoversAllFollowers()
    {
        await using var pool = CreatePool(
            MakeOptions("p:1", seeds: ["b:1", "c:1", "d:1"], routeReadsToFollowers: true));
        pool.SetLeaderForTesting("p:1");

        var firsts = new HashSet<string>();
        for (var i = 0; i < 12; i++)
        {
            var addrs = pool.GetReadNodeAddresses();
            firsts.Add(addrs[0]);
            // leader never leads, but always closes the list
            Assert.NotEqual("p:1", addrs[0]);
            Assert.Equal("p:1", addrs[^1]);
        }

        // load spread: every follower must lead the list eventually
        foreach (var a in new[] { "b:1", "c:1", "d:1" })
            Assert.Contains(a, firsts);
    }

    [Fact]
    public async Task OracleOptinWatchNeverLeaderAndRotates()
    {
        // Still holds under Amendment 2: watch takes the FIRST candidate, and the
        // leader sits last, so it never leads while a follower exists.
        await using var pool = CreatePool(
            MakeOptions("p:1", seeds: ["b:1", "c:1"], routeReadsToFollowers: true));
        pool.SetLeaderForTesting("p:1");

        var seen = new HashSet<string>();
        for (var i = 0; i < 8; i++)
        {
            var w = pool.GetWatchAddress();
            Assert.NotEqual("p:1", w);
            seen.Add(w);
        }
        Assert.Contains("b:1", seen);
        Assert.Contains("c:1", seen);
    }

    [Fact]
    public async Task OracleOptinWatchNoFollowersFallsBack()
    {
        await using var pool = CreatePool(MakeOptions("p:1", routeReadsToFollowers: true));
        pool.SetLeaderForTesting("p:1");

        // no followers: the leader-last list is just [leader], and watch takes
        // the first candidate: a usable address, not a throw
        Assert.Equal("p:1", pool.GetWatchAddress());
    }

    [Fact]
    public async Task OracleOptinSingleNodeYieldsLeaderOnly()
    {
        // Amendment 2: no special case anymore: the general rule (rotated
        // followers, then leader last) with zero followers yields exactly [leader].
        await using var pool = CreatePool(MakeOptions("p:1", routeReadsToFollowers: true));
        pool.SetLeaderForTesting("p:1");

        var addrs = pool.GetReadNodeAddresses();
        Assert.Equal(["p:1"], addrs);
    }

    [Fact]
    public async Task OracleOptinLeaderResetRestoresFollower()
    {
        // Under leader-last the new leader is demoted to the tail, not removed;
        // resetting the leader to the primary promotes it back into the rotated
        // follower section.
        await using var pool = CreatePool(
            MakeOptions("p:1", seeds: ["b:1", "c:1"], routeReadsToFollowers: true));
        pool.SetLeaderForTesting("b:1");
        Assert.Equal("b:1", pool.GetReadNodeAddresses()[^1]);

        pool.SetLeaderForTesting("p:1");
        var addrs = pool.GetReadNodeAddresses();
        Assert.Equal("p:1", addrs[^1]);
        foreach (var a in new[] { "b:1", "c:1" })
            Assert.Single(addrs.Take(addrs.Length - 1), a);
    }

    [Fact]
    public async Task OracleOptinOnlyLatestLeaderLast()
    {
        // Only the latest leader sits last; the prior leader rejoins the
        // rotated followers.
        await using var pool = CreatePool(
            MakeOptions("p:1", seeds: ["b:1", "c:1"], routeReadsToFollowers: true));
        pool.SetLeaderForTesting("b:1");
        pool.SetLeaderForTesting("c:1");

        var addrs = pool.GetReadNodeAddresses();
        Assert.Equal("c:1", addrs[^1]);
        Assert.Single(addrs, "c:1");
        foreach (var a in new[] { "b:1", "p:1" })
            Assert.Single(addrs.Take(addrs.Length - 1), a);
    }

    [Fact]
    public async Task OracleOptinUnknownLeaderLastLikeAnyLeader()
    {
        // In .NET, SetLeaderForTesting registers the address as a known node (mirrors
        // discovery), so the former unknown IS the leader: last resort, with
        // the original knowns rotated ahead of it.
        await using var pool = CreatePool(
            MakeOptions("p:1", seeds: ["b:1"], routeReadsToFollowers: true));
        pool.SetLeaderForTesting("x:9");

        var addrs = pool.GetReadNodeAddresses();
        Assert.Equal(3, addrs.Length);
        Assert.Equal("x:9", addrs[^1]);
        Assert.Single(addrs, "x:9");
        foreach (var a in new[] { "p:1", "b:1" })
            Assert.Single(addrs.Take(addrs.Length - 1), a);
    }

    [Fact]
    public async Task OracleOptinLeaderIsLastResort()
    {
        // Mirrors oracle_optin_leader_is_last_resort: whatever the rotation does,
        // every candidate list starts with a follower and ends with the leader.
        await using var pool = CreatePool(
            MakeOptions("p:1", seeds: ["b:1", "c:1"], routeReadsToFollowers: true));
        pool.SetLeaderForTesting("p:1");

        for (var i = 0; i < 8; i++)
        {
            var addrs = pool.GetReadNodeAddresses();
            Assert.Contains(addrs[0], new[] { "b:1", "c:1" });
            Assert.Equal("p:1", addrs[^1]);
        }
    }

    // Skipped (no .NET analog): Rust empty-primary case: Options.Address is required.
    // Skipped (no .NET analog): Rust clear_leader pin-to-seed quirk: no clear_leader;
    // covered by the SetLeaderForTesting-back-to-primary adaptations above.
}
