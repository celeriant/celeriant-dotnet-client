using Celeriant.Client.Protocol;

namespace Celeriant.Client.Tests;

public class OptionsBuilderTests
{
    // -----------------------------------------------------------------------
    // Defaults
    // -----------------------------------------------------------------------

    [Fact]
    public void Builder_Defaults_MatchExpectedValues()
    {
        var builder = new CeleriantPoolOptionsBuilder();

        Assert.Equal(string.Empty, builder.Address);
        Assert.Null(builder.SeedAddresses);
        Assert.Null(builder.TlsConfig);
        Assert.Null(builder.IdentityConfig);
        Assert.Equal(10, builder.MaxConnections);
        Assert.Equal(TimeSpan.FromSeconds(5), builder.ConnectionTimeout);
        Assert.Equal(TimeSpan.FromSeconds(30), builder.RequestTimeout);
        Assert.Equal(10_000_000L, builder.MaxRequestSize);
        Assert.Equal(TimeSpan.FromSeconds(25), builder.IdleTimeout);
        Assert.False(builder.RouteReadsToFollowers);
        Assert.Equal(CompressionType.Zstd, builder.CompressionAlgorithm);
        Assert.Equal(1024, builder.AutoCompressionThresholdBytes);
    }

    // -----------------------------------------------------------------------
    // Build validation
    // -----------------------------------------------------------------------

    [Fact]
    public void Build_NoAddress_ThrowsInvalidOperationException()
    {
        var builder = new CeleriantPoolOptionsBuilder();
        Assert.Throws<InvalidOperationException>(() => builder.Build());
    }

    [Fact]
    public void Build_WhitespaceAddress_ThrowsInvalidOperationException()
    {
        var builder = new CeleriantPoolOptionsBuilder { Address = "   " };
        Assert.Throws<InvalidOperationException>(() => builder.Build());
    }

    [Fact]
    public void Build_EmptyAddress_ThrowsInvalidOperationException()
    {
        var builder = new CeleriantPoolOptionsBuilder { Address = "" };
        Assert.Throws<InvalidOperationException>(() => builder.Build());
    }

    // -----------------------------------------------------------------------
    // Build preserves all properties
    // -----------------------------------------------------------------------

    [Fact]
    public void Build_AllPropertiesSet_PreservedInOptions()
    {
        var seeds = new List<string> { "follower1:10000", "follower2:10000" };

        var builder = new CeleriantPoolOptionsBuilder
        {
            Address = "leader:10000",
            SeedAddresses = seeds,
            MaxConnections = 20,
            ConnectionTimeout = TimeSpan.FromSeconds(10),
            RequestTimeout = TimeSpan.FromMinutes(1),
            MaxRequestSize = 50_000_000,
            IdleTimeout = TimeSpan.FromMinutes(10),
            RouteReadsToFollowers = true,
            CompressionAlgorithm = CompressionType.Brotli,
            AutoCompressionThresholdBytes = 4096,
        };

        var options = builder.Build();

        Assert.Equal("leader:10000", options.Address);
        Assert.Same(seeds, options.SeedAddresses);
        Assert.Equal(20, options.MaxConnections);
        Assert.Equal(TimeSpan.FromSeconds(10), options.ConnectionTimeout);
        Assert.Equal(TimeSpan.FromMinutes(1), options.RequestTimeout);
        Assert.Equal(50_000_000L, options.MaxRequestSize);
        Assert.Equal(TimeSpan.FromMinutes(10), options.IdleTimeout);
        Assert.True(options.RouteReadsToFollowers);
        Assert.Equal(CompressionType.Brotli, options.CompressionAlgorithm);
        Assert.Equal(4096, options.AutoCompressionThresholdBytes);
    }

    // -----------------------------------------------------------------------
    // CeleriantPoolOptions direct construction
    // -----------------------------------------------------------------------

    [Fact]
    public void Options_Defaults_MatchBuilderDefaults()
    {
        var options = new CeleriantPoolOptions { Address = "localhost:10000" };

        Assert.Equal(10, options.MaxConnections);
        Assert.Equal(TimeSpan.FromSeconds(5), options.ConnectionTimeout);
        Assert.Equal(TimeSpan.FromSeconds(30), options.RequestTimeout);
        Assert.Equal(10_000_000L, options.MaxRequestSize);
        Assert.Equal(TimeSpan.FromSeconds(25), options.IdleTimeout);
        Assert.False(options.RouteReadsToFollowers);
        Assert.Equal(CompressionType.Zstd, options.CompressionAlgorithm);
        Assert.Equal(1024, options.AutoCompressionThresholdBytes);
    }
}
