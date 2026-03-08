using Microsoft.Extensions.DependencyInjection;

namespace Celeriant.Client.Tests;

public class ServiceCollectionExtensionsTests
{
    [Fact]
    public void AddCeleriantPool_NullServices_ThrowsArgumentNullException()
    {
        IServiceCollection services = null!;
        Assert.Throws<ArgumentNullException>(
            () => services.AddCeleriantPool(o => o.Address = "localhost:10000"));
    }

    [Fact]
    public void AddCeleriantPool_NullConfigure_ThrowsArgumentNullException()
    {
        var services = new ServiceCollection();
        Assert.Throws<ArgumentNullException>(
            () => services.AddCeleriantPool(null!));
    }

    [Fact]
    public void AddCeleriantPool_ReturnsSameServiceCollection()
    {
        var services = new ServiceCollection();
        var result = services.AddCeleriantPool(o => o.Address = "localhost:10000");
        Assert.Same(services, result);
    }

    [Fact]
    public void AddCeleriantPool_RegistersAsSingleton()
    {
        var services = new ServiceCollection();
        services.AddCeleriantPool(o => o.Address = "localhost:10000");

        var descriptor = Assert.Single(services, d => d.ServiceType == typeof(CeleriantPool));
        Assert.Equal(ServiceLifetime.Singleton, descriptor.Lifetime);
    }

    [Fact]
    public void AddCeleriantPool_ConfigureDelegateIsInvoked()
    {
        var services = new ServiceCollection();
        var invoked = false;

        services.AddCeleriantPool(o =>
        {
            o.Address = "test:9999";
            invoked = true;
        });

        // Build the provider to trigger the factory
        var provider = services.BuildServiceProvider();
        var pool = provider.GetRequiredService<CeleriantPool>();

        Assert.True(invoked);
        Assert.NotNull(pool);

        // Clean up
        _ = pool.DisposeAsync();
    }

    [Fact]
    public void AddCeleriantPool_InvalidAddress_ThrowsOnResolve()
    {
        var services = new ServiceCollection();
        services.AddCeleriantPool(o => { /* Address not set */ });

        var provider = services.BuildServiceProvider();
        Assert.Throws<InvalidOperationException>(() => provider.GetRequiredService<CeleriantPool>());
    }
}
