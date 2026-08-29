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
    /// Additional server addresses for failover and follower read routing.
    /// The pool creates connections to all seed addresses; new nodes discovered
    /// through leader failover are added automatically.
    /// </summary>
    public IReadOnlyList<string>? SeedAddresses { get; init; }

    /// <summary>Optional TLS configuration. Plain TCP is used when null.</summary>
    public ClientTlsConfig? TlsConfig { get; init; }

    /// <summary>Optional identity configuration. No identity handshake is performed when null.</summary>
    public ClientIdentityConfig? IdentityConfig { get; init; }

    /// <summary>Maximum number of pooled connections. Default: 10.</summary>
    public int MaxConnections { get; init; } = 10;

    /// <summary>
    /// Timeout for establishing a new connection, applied per round trip rather than to the
    /// establishment as a whole. Default: 5 seconds. A watch connect spends it up to three times
    /// against one node — dial, Identify, subscribe — so size a failover deadline against
    /// <c>3 x ConnectionTimeout x candidates</c>, not against this value alone.
    /// </summary>
    public TimeSpan ConnectionTimeout { get; init; } = TimeSpan.FromSeconds(5);

    /// <summary>Per-request timeout applied to each <c>SendRequestAsync</c> call. Default: 30 seconds.</summary>
    public TimeSpan RequestTimeout { get; init; } = TimeSpan.FromSeconds(30);

    /// <summary>Maximum allowed request payload size in bytes. Default: 10 MB.</summary>
    public long MaxRequestSize { get; init; } = 10_000_000;

    /// <summary>Maximum allowed response payload size in bytes. Default: 64 MB. This bounds a single
    /// wire page, not the whole aggregate; the server chooses the page size, and the client does not
    /// renegotiate it. Keep this at or above the server's configured response page size — set it lower
    /// and a read whose first page exceeds it throws <see cref="Errors.ProtocolException"/> with no
    /// smaller page to fall back to, making large aggregates unreadable through this pool.</summary>
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
    /// When true, read operations (read, aggregate details, list, watch) are routed to
    /// follower nodes, keeping the leader free for writes: sheds leader load but gives up
    /// read-your-writes (followers can lag). The leader remains the last resort: if every
    /// follower fails, reads and watches are served by the leader rather than failing.
    /// Default: false (reads go to the leader).
    /// </summary>
    public bool RouteReadsToFollowers { get; init; }
}
