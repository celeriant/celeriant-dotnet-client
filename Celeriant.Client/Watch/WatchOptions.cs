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

    /// <summary>
    /// Per-phase timeout for establishing the watch connection. Connecting is three round trips —
    /// the dial (plus TLS), the Identify handshake when <see cref="IdentityConfig"/> is set, and
    /// the watch subscription acknowledgement — and each gets this budget in full, so one node can
    /// take up to three times this value before the connect gives up and routing moves on.
    ///
    /// <para>
    /// Null means no timeout at all, and for the two round trips after the dial that means no
    /// bound whatsoever: the socket is established and the peer is merely silent, so there is no
    /// OS-level TCP timeout waiting to rescue the caller. A node that accepts the connection and
    /// then says nothing parks <c>ConnectAsync</c> indefinitely. <see cref="CeleriantPool"/> fills
    /// this from its own connection timeout when unset, so only a direct
    /// <see cref="WatchConnection.ConnectAsync"/> caller can leave it null.
    /// </para>
    /// </summary>
    public TimeSpan? ConnectionTimeout { get; init; }

    /// <summary>Maximum request payload size in bytes. Null keeps the client default (10 MB).</summary>
    public long? MaxRequestSize { get; init; }

    /// <summary>
    /// Maximum response payload size in bytes; bounds a single watch page. A page past the cap
    /// throws <see cref="Errors.ProtocolException"/>. Null keeps the client default (64 MB).
    /// <see cref="CeleriantPool"/> fills both caps from its own options so a pool-dialled watch
    /// honours the same memory bound as every pooled request.
    /// </summary>
    public long? MaxResponseSize { get; init; }
}
