using Moq;

namespace Celeriant.Client.Tests;

/// <summary>
/// Reads use the configured leader by default. Follower routing rotates followers
/// first and retains the leader as a last resort.
/// </summary>
public class ReadRoutingOrderTests
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
    public async Task DefaultNoLeaderPrimaryFirstAllKnownOnce()
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
    public async Task DefaultCachedLeaderSeedFirstPrimaryLater()
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
    public async Task DefaultLeaderFirstStableAcrossCalls()
    {
        // .NET promises only index 0 stability (tail order unspecified), so pin
        // leader-first and leader-once on every call, not the full list.
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
    public async Task DefaultWatchLeaderElsePrimary()
    {
        await using var pool = CreatePool(MakeOptions("p:1", seeds: ["b:1"]));

        Assert.Equal("p:1", pool.GetWatchAddress());
        pool.SetLeaderForTesting("b:1");
        Assert.Equal("b:1", pool.GetWatchAddress());
    }

    [Fact]
    public async Task DefaultLeaderResetToPrimaryFirst()
    {
        // This pins the ROUTING consequence only — that a leader back at the primary puts the
        // primary first — by setting it directly. It does not exercise the reset path itself; the tests that do
        // are in WatchAddressParityTests and WatchAddressEdgeCaseTests.
        await using var pool = CreatePool(MakeOptions("p:1", seeds: ["b:1", "c:1"]));
        pool.SetLeaderForTesting("b:1");
        pool.SetLeaderForTesting("p:1");

        var addrs = pool.GetReadNodeAddresses();
        Assert.Equal("p:1", addrs[0]);
        Assert.Equal("p:1", pool.GetWatchAddress());
    }

    [Fact]
    public async Task DefaultSecondUpdateWins()
    {
        await using var pool = CreatePool(MakeOptions("p:1", seeds: ["b:1", "c:1"]));
        pool.SetLeaderForTesting("b:1");
        pool.SetLeaderForTesting("c:1");

        var addrs = pool.GetReadNodeAddresses();
        Assert.Equal("c:1", addrs[0]);
        Assert.Equal("c:1", pool.GetWatchAddress());
    }

    [Fact]
    public async Task DefaultUnknownLeaderGoesFirstKnownsFollow()
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
    public async Task OptinLeaderPresentButLast()
    {
        // The leader is the last-resort candidate, not excluded. Every follower
        // appears exactly once before it.
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
    public async Task OptinFreshPoolTreatsPrimaryAsLeader()
    {
        // .NET has no "no leader" state (_leaderAddress starts as Options.Address), so a fresh
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
    public async Task OptinRotationCoversAllFollowers()
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
    public async Task OptinWatchNeverLeaderAndRotates()
    {
        // Watch takes the first candidate, and the leader sits last, so it never
        // leads while a follower exists.
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
    public async Task OptinWatchNoFollowersFallsBack()
    {
        await using var pool = CreatePool(MakeOptions("p:1", routeReadsToFollowers: true));
        pool.SetLeaderForTesting("p:1");

        // no followers: the leader-last list is just [leader], and watch takes
        // the first candidate: a usable address, not a throw
        Assert.Equal("p:1", pool.GetWatchAddress());
    }

    [Fact]
    public async Task OptinSingleNodeYieldsLeaderOnly()
    {
        // No special case: the general rule (rotated followers, then leader last)
        // with zero followers yields exactly [leader].
        await using var pool = CreatePool(MakeOptions("p:1", routeReadsToFollowers: true));
        pool.SetLeaderForTesting("p:1");

        var addrs = pool.GetReadNodeAddresses();
        Assert.Equal(["p:1"], addrs);
    }

    [Fact]
    public async Task OptinLeaderResetRestoresFollower()
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
    public async Task OptinOnlyLatestLeaderLast()
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
    public async Task OptinUnknownLeaderLastLikeAnyLeader()
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
    public async Task OptinLeaderIsLastResort()
    {
        // Whatever the rotation does, every candidate list starts with a follower and ends
        // with the leader.
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
    //
    // Rust's clear_leader pin-to-seed arm — the primary itself refusing, so the leader moves to the
    // first seed — was waived here as having no .NET analog. It has one now: a watch dial that the
    // configured primary refuses. Covered in WatchAddressEdgeCaseTests, not by the
    // SetLeaderForTesting adaptations above, which never reach the reset path.
}
