using Celeriant.Client.Protocol;

namespace Celeriant.Client;

/// <summary>
/// Configuration options for <see cref="CeleriantPool"/>.
/// </summary>
public sealed class CeleriantPoolOptions
{
    /// <summary>Primary server address in "host:port" format. Used as the initial leader candidate.</summary>
    public required string Address { get; init; }

    /// <summary>
    /// Additional server addresses for failover and read distribution.
    /// The pool creates connections to all seed addresses and distributes non-leader
    /// operations across them via round-robin. New nodes discovered through leader
    /// failover are added automatically.
    /// </summary>
    public IReadOnlyList<string>? SeedAddresses { get; init; }

    /// <summary>Optional TLS configuration. Plain TCP is used when null.</summary>
    public ClientTlsConfig? TlsConfig { get; init; }

    /// <summary>Optional identity configuration. No identity handshake is performed when null.</summary>
    public ClientIdentityConfig? IdentityConfig { get; init; }

    /// <summary>Maximum number of pooled connections. Default: 10.</summary>
    public int MaxConnections { get; init; } = 10;

    /// <summary>Timeout for establishing a new TCP connection. Default: 5 seconds.</summary>
    public TimeSpan ConnectionTimeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>Per-request timeout applied to each <c>SendRequestAsync</c> call. Default: 30 seconds.</summary>
    public TimeSpan RequestTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>Maximum allowed request payload size in bytes. Default: 10 MB.</summary>
    public long MaxRequestSize { get; init; } = 10_000_000;

    /// <summary>Maximum allowed response payload size in bytes. Default: 64 MB.</summary>
    public long MaxResponseSize { get; init; } = 64 * 1024 * 1024;

    /// <summary>
    /// How long an idle connection may remain in the pool before being disposed.
    /// Eviction is lazy (checked on checkout, not via a background timer).
    ///
    /// <para>
    /// This must be shorter than the server's <c>slow_client_timeout</c> (default 30 s),
    /// otherwise the server will close idle connections before the client evicts them,
    /// causing a wasted round-trip and <see cref="Errors.ConnectionFailedException"/> on
    /// the next checkout. Default: 25 seconds.
    /// </para>
    /// </summary>
    public TimeSpan IdleTimeout { get; init; } = TimeSpan.FromSeconds(25);

    /// <summary>
    /// When true, read operations (read, aggregate details, list, watch) are routed only to
    /// follower nodes, keeping the leader free for writes. Falls back to the leader if no
    /// followers are available. Default: false (reads go to any node).
    /// </summary>
    public bool RouteReadsToFollowers { get; init; }

    /// <summary>
    /// Compression algorithm used for variable-size requests (writes, schema registration)
    /// when the serialized payload exceeds <see cref="AutoCompressionThresholdBytes"/>.
    /// Default: <see cref="CompressionType.Zstd"/>.
    /// </summary>
    public CompressionType CompressionAlgorithm { get; init; } = CompressionType.Zstd;

    /// <summary>
    /// Minimum serialized payload size (in bytes) before automatic wire compression is applied.
    /// Only affects variable-size messages (writes, schema registration).
    /// Set to 0 to always compress. Set to <see cref="int.MaxValue"/> to disable.
    /// Default: 1024.
    /// </summary>
    public int AutoCompressionThresholdBytes { get; init; } = 1024;
}
