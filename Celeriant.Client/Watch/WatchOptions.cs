using System.ComponentModel;

namespace Celeriant.Client.Watch;

/// <summary>
/// Options controlling shard routing, TLS, and timeouts for a <see cref="WatchConnection"/>.
/// </summary>
public sealed class WatchOptions
{
    /// <summary>The shard index at which to start for multi-shard watch. Defaults to 0.</summary>
    [EditorBrowsable(EditorBrowsableState.Advanced)]
    public long StartShard { get; init; }

    /// <summary>
    /// If set, skip the single-shard probe and immediately open one connection per shard in
    /// the range [<see cref="StartShard"/>, <see cref="MaxShardHint"/>).
    /// If null, attempt a single connection first and fall back to multi-shard only when the
    /// server returns a shard routing error (9001) that includes <c>num_shards</c>.
    /// </summary>
    [EditorBrowsable(EditorBrowsableState.Advanced)]
    public long? MaxShardHint { get; init; }

    /// <summary>Optional TLS configuration. Plain TCP is used if null.</summary>
    public ClientTlsConfig? TlsConfig { get; init; }

    /// <summary>Optional identity configuration. When set, each watch connection performs
    /// the Identify handshake before sending the watch request.</summary>
    public ClientIdentityConfig? IdentityConfig { get; init; }

    /// <summary>Dial timeout for establishing the watch connection. Null means no timeout:
    /// a black-holed node then stalls for the OS TCP timeout. <see cref="CeleriantPool"/>
    /// fills this from its own connection timeout when unset.</summary>
    public TimeSpan? ConnectionTimeout { get; init; }
}
