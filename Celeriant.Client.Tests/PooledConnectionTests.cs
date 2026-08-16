using System.Net.Sockets;

namespace Celeriant.Client.Tests;

public class PooledConnectionTests
{
    // PooledConnection's constructor is internal but InternalsVisibleTo is set.
    // CeleriantClient has a private constructor, so we can't create real instances.
    // We test the contract via reflection for now: the behavioral tests are in
    // integration tests where real CeleriantClient instances exist.

    [Fact]
    public void PooledConnection_ImplementsIAsyncDisposable()
    {
        Assert.True(typeof(IAsyncDisposable).IsAssignableFrom(typeof(PooledConnection)));
    }

    [Fact]
    public void PooledConnection_IsSealed()
    {
        Assert.True(typeof(PooledConnection).IsSealed);
    }

    [Fact]
    public void PooledConnection_HasClientProperty()
    {
        var prop = typeof(PooledConnection).GetProperty("Client");
        Assert.NotNull(prop);
        Assert.Equal(typeof(CeleriantClient), prop.PropertyType);
    }

    [Fact]
    public void PooledConnection_HasMarkBrokenMethod()
    {
        var method = typeof(PooledConnection).GetMethod("MarkBroken");
        Assert.NotNull(method);
        Assert.Equal(typeof(void), method.ReturnType);
    }
}
