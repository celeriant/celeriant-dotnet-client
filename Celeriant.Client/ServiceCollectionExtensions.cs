using Celeriant.Client.Protocol;
using Microsoft.Extensions.DependencyInjection;

namespace Celeriant.Client;

/// <summary>
/// Extension methods for registering Celeriant client services with the .NET dependency injection container.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Register an <see cref="ICeleriantPool"/> singleton in the DI container.
    ///
    /// <para>
    /// Usage in <c>Program.cs</c>:
    /// <code>
    /// builder.Services.AddCeleriantPool(options =>
    /// {
    ///     options.Address = "localhost:9200";
    ///     options.MaxConnections = 20;
    /// });
    /// </code>
    /// </para>
    /// </summary>
    /// <param name="services">The service collection to register into.</param>
    /// <param name="configure">Delegate to configure pool options via a mutable builder.</param>
    /// <returns>The <paramref name="services"/> for chaining.</returns>
    public static IServiceCollection AddCeleriantPool(
        this IServiceCollection services,
        Action<CeleriantPoolOptionsBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        services.AddSingleton<ICeleriantPool>(sp =>
        {
            var builder = new CeleriantPoolOptionsBuilder();
            configure(builder);
            return new CeleriantPool(builder.Build());
        });

        return services;
    }
}

/// <summary>
/// Mutable builder for <see cref="CeleriantPoolOptions"/>. Used exclusively with
/// <see cref="ServiceCollectionExtensions.AddCeleriantPool"/>.
/// </summary>
public sealed class CeleriantPoolOptionsBuilder
{
    /// <summary>Primary server address in "host:port" format. Required.</summary>
    public string Address { get; set; } = string.Empty;

    /// <summary>Additional server addresses for failover and read distribution.</summary>
    public List<string>? SeedAddresses { get; set; }

    /// <summary>Optional TLS configuration. Plain TCP is used when null.</summary>
    public ClientTlsConfig? TlsConfig { get; set; }

    /// <summary>Optional identity configuration.</summary>
    public ClientIdentityConfig? IdentityConfig { get; set; }

    /// <summary>Maximum number of pooled connections. Default: 10.</summary>
    public int MaxConnections { get; set; } = 10;

    /// <summary>Timeout for establishing a new TCP connection. Default: 5 seconds.</summary>
    public TimeSpan ConnectionTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>Per-request timeout. Default: 30 seconds.</summary>
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Maximum request payload size in bytes. Default: 10 MB.</summary>
    public long MaxRequestSize { get; set; } = 10_000_000;

    /// <summary>Maximum response payload size in bytes. Default: 64 MB.</summary>
    public long MaxResponseSize { get; set; } = 64 * 1024 * 1024;

    /// <summary>Idle connection timeout. Must be shorter than the server's slow_client_timeout. Default: 25 seconds.</summary>
    public TimeSpan IdleTimeout { get; set; } = TimeSpan.FromSeconds(25);

    /// <summary>When true, route reads only to followers. Default: false.</summary>
    public bool RouteReadsToFollowers { get; set; }

    /// <summary>Compression algorithm for variable-size requests. Default: Zstd.</summary>
    public CompressionType CompressionAlgorithm { get; set; } = CompressionType.Zstd;

    /// <summary>Minimum payload size (bytes) before auto-compression kicks in. Default: 1024.</summary>
    public int AutoCompressionThresholdBytes { get; set; } = 1024;

    /// <summary>Build the immutable <see cref="CeleriantPoolOptions"/>.</summary>
    public CeleriantPoolOptions Build()
    {
        if (string.IsNullOrWhiteSpace(Address))
            throw new InvalidOperationException(
                $"{nameof(CeleriantPoolOptions)}.{nameof(Address)} must be set before building.");

        return new CeleriantPoolOptions
        {
            Address = Address,
            SeedAddresses = SeedAddresses,
            RouteReadsToFollowers = RouteReadsToFollowers,
            TlsConfig = TlsConfig,
            IdentityConfig = IdentityConfig,
            MaxConnections = MaxConnections,
            ConnectionTimeout = ConnectionTimeout,
            RequestTimeout = RequestTimeout,
            MaxRequestSize = MaxRequestSize,
            MaxResponseSize = MaxResponseSize,
            IdleTimeout = IdleTimeout,
            CompressionAlgorithm = CompressionAlgorithm,
            AutoCompressionThresholdBytes = AutoCompressionThresholdBytes,
        };
    }
}
