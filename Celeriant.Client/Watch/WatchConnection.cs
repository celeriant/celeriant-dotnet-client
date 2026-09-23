using System.Runtime.ExceptionServices;
using System.Threading.Channels;
using Celeriant.Client.Errors;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;

namespace Celeriant.Client.Watch;

/// <summary>
/// A long-lived watch connection that streams <see cref="WatchResponse"/> objects from the server.
///
/// <para>
/// Single-shard vs. multi-shard strategy:
/// <list type="number">
///   <item>
///     If <see cref="WatchOptions.MaxShardHint"/> is NOT set, open one connection without a
///     <c>shard_id</c>. If the server returns error code 9001 (filters route to multiple shards)
///     or 9002 (filters do not match the routing rule) with a <c>num_shards</c> value embedded
///     in the error message JSON, automatically fall back to multi-shard mode. An explicit
///     <c>shard_id</c> bypasses the server's routing-rule check, so both cases are recoverable
///     by fanning out one connection per shard.
///   </item>
///   <item>
///     If <see cref="WatchOptions.MaxShardHint"/> IS set, skip the single-connection probe and
///     open one connection per shard in [<see cref="WatchOptions.StartShard"/>, MaxShardHint).
///   </item>
///   <item>
///     Multi-shard mode: one <see cref="CeleriantClient"/> per shard, each draining
///     watch events in a background <see cref="Task"/> and writing into a shared
///     <see cref="Channel{T}"/>. <see cref="NextAsync(CancellationToken)"/> reads from
///     this channel.
///   </item>
/// </list>
/// </para>
///
/// <para>
/// Watch protocol notes: a watch is connection-terminal on the server. The client sends the
/// watch request once; the server acks it immediately (an empty heartbeat-shaped frame) and
/// pushes responses from then on. The server never reads the connection again, so anything
/// the client writes after subscribing is silently ignored and just accumulates in the
/// socket buffer. NextAsync only reads.
/// </para>
/// </summary>
public sealed class WatchConnection : IAsyncDisposable
{
    /// <summary>
    /// Most shards one watch will fan out across (one TCP connection per shard). A client-side
    /// sanity bound, not a protocol limit — raise it if a real cluster exceeds it. On the fallback
    /// path the count comes from the server, and honouring an absurd one exhausts sockets before
    /// the caller gets an exception it can catch.
    /// </summary>
    private const int MaxShards = 1024;

    // Single-shard state.
    private CeleriantClient? _singleClient;
    // First response buffered during the probe handshake.
    private WatchResponse? _bufferedResponse;

    // Multi-shard state.
    private Task[]? _shardTasks;
    private Channel<WatchResponse>? _channel;
    // Stops the background shard readers: on disposal, and on the first shard failure. Once one
    // shard is gone the watch is dead to the caller, so the siblings are reading sockets whose
    // events nobody can ever receive.
    private readonly CancellationTokenSource _readersCts = new();

    // Why the subscription stopped, captured once at the point of failure so re-reading a dead
    // watch replays the same failure instead of lengthening one shared exception's stack trace on
    // every call. Null while the watch is live, and after a disposal (the caller ending it is not
    // a failure to report).
    private ExceptionDispatchInfo? _shardFailure;

    private readonly WatchOptions _options;
    private readonly string _address;
    // 0 while live, 1 once disposal has begun. A single field rather than separate disposed and
    // stopping flags, so two concurrent DisposeAsync calls cannot both pass the guard and race
    // each other through the teardown.
    private int _disposeState;

    /// <summary>True once the caller has begun disposing: the one legitimate way a reader ends.</summary>
    private bool Stopping => Volatile.Read(ref _disposeState) != 0;

    private WatchConnection(string address, WatchOptions options)
    {
        _address = address;
        _options = options;
    }

    /// <summary>
    /// The node this subscription is attached to, in the "host:port" form it was dialled with.
    /// A watch never reconnects, so a caller that reconnects after a failure starts a new
    /// subscription from that node's current tip; comparing this address across reconnects is how
    /// it notices the subscription moved node and events in between may have been missed.
    /// </summary>
    public string Address => _address;

    // -------------------------------------------------------------------------
    // Static factory
    // -------------------------------------------------------------------------

    /// <summary>
    /// Connect to the Celeriant server and start a watch session.
    /// </summary>
    /// <param name="address">Server address in "host:port" format.</param>
    /// <param name="request">The watch filter request.</param>
    /// <param name="options">Connection and shard options.</param>
    /// <param name="ct">Cancellation token.</param>
    public static async Task<WatchConnection> ConnectAsync(
        string address,
        WatchRequest request,
        WatchOptions options,
        CancellationToken ct = default)
    {
        var connection = new WatchConnection(address, options);

        if (options.MaxShardHint.HasValue)
        {
            // Skip probe, go directly to multi-shard.
            await connection.ConnectMultiShardAsync(
                address, request, options.StartShard, options.MaxShardHint.Value,
                shardCountFromServer: false, ct)
                .ConfigureAwait(false);
        }
        else
        {
            // Attempt single-shard first.
            await connection.ConnectSingleShardAsync(address, request, ct).ConfigureAwait(false);
        }

        return connection;
    }

    // -------------------------------------------------------------------------
    // NextAsync overloads
    // -------------------------------------------------------------------------

    /// <summary>
    /// Wait for the next batch of watch events. Blocks indefinitely until a response arrives or
    /// the cancellation token is triggered.
    ///
    /// <para>
    /// If the server stops sending events this method will block forever.
    /// Prefer the <see cref="NextAsync(TimeSpan, CancellationToken)"/> overload or pass a
    /// <see cref="CancellationToken"/> with a timeout to avoid hanging.
    /// </para>
    /// </summary>
    public async Task<WatchResponse> NextAsync(CancellationToken ct = default)
    {
        ThrowIfDisposed();

        if (_channel is not null)
        {
            // Multi-shard: read from the shared channel, which carries events only. The reason a
            // reader stopped is raised from _shardFailure, not through the channel: a channel
            // completed with a cancellation re-throws it raw, and NextAsync(TimeSpan) reads any
            // cancellation as "nothing arrived yet" — reporting a blind subscription as quiet.
            try
            {
                return await _channel.Reader.ReadAsync(ct).ConfigureAwait(false);
            }
            catch (ChannelClosedException)
            {
                // A reader that stopped published why before closing the channel. Nothing published
                // means disposal closed it: DisposeAsync completes the channel itself, ahead of the
                // readers it is cancelling.
                _shardFailure?.Throw();
                throw new ObjectDisposedException(nameof(WatchConnection));
            }
        }

        // Single-shard: return buffered first response (if any), then poll.
        if (_bufferedResponse is not null)
        {
            var buffered = _bufferedResponse;
            _bufferedResponse = null;
            return buffered;
        }

        return await ReadSingleShardResponseAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// True when a read was abandoned part-way through a frame, leaving this subscription's stream
    /// mid-message. <see cref="NextAsync(TimeSpan, CancellationToken)"/> returns null on any
    /// timeout, including this one — a deadline expiring on an empty socket is clean and common —
    /// so a truncated read surfaces on the next read, or immediately through this property.
    /// Always false for a multi-shard subscription: its readers own their sockets, so a cancelled
    /// <c>NextAsync</c> abandons no frame.
    /// </summary>
    public bool IsDesynchronised => _singleClient?.IsMidFrame ?? false;

    /// <summary>
    /// Wait for the next batch of watch events, up to <paramref name="timeout"/>.
    ///
    /// <para>
    /// Returns null on <b>timeout only</b>. A watch never ends on its own, so null never means "the
    /// stream finished" — keep polling. (A loop that does <c>if (resp is null) break;</c> exits a
    /// perfectly healthy watch after one idle window.) A null can also follow a read that was
    /// truncated mid-frame: check <see cref="IsDesynchronised"/> after a null and, if it is true,
    /// dispose and reconnect. The subscription ending for any real reason throws rather than
    /// returning null.
    /// </para>
    /// </summary>
    public async Task<WatchResponse?> NextAsync(TimeSpan timeout, CancellationToken ct = default)
    {
        ThrowIfDisposed();

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(timeout);

        try
        {
            return await NextAsync(timeoutCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            // Timed out (not cancelled by caller).
            return null;
        }
    }

    // -------------------------------------------------------------------------
    // IAsyncDisposable
    // -------------------------------------------------------------------------

    public async ValueTask DisposeAsync()
    {
        // Claim the teardown atomically: a second caller returns instead of walking fields the
        // winner is already tearing down. The write also publishes Stopping ahead of the cancel
        // below, so a reader unblocked by it closes the channel quietly rather than reporting the
        // shutdown the caller asked for as a lost shard.
        if (Interlocked.Exchange(ref _disposeState, 1) != 0)
            return;

        // Cancel background shard reader tasks.
        await CancelReadersAsync().ConfigureAwait(false);

        if (_singleClient is not null)
        {
            await _singleClient.DisposeAsync().ConfigureAwait(false);
            _bufferedResponse = null;
        }

        if (_shardTasks is not null)
        {
            // Signal the channel that no more items will be written.
            _channel?.Writer.TryComplete();

            // Each reader closes its own connection on its way out, so waiting for them all is the
            // teardown: one owner per socket, and no second disposal racing the first.
            try
            {
                await Task.WhenAll(_shardTasks).ConfigureAwait(false);
            }
            catch
            {
                // Suppress exceptions from background tasks on disposal.
            }
        }

        // Last: the readers above hold this token, and a caller still parked in NextAsync may yet
        // reach it. Nothing may observe a disposed source while either is true.
        _readersCts.Dispose();
    }

    // -------------------------------------------------------------------------
    // Single-shard connection
    // -------------------------------------------------------------------------

    private async Task ConnectSingleShardAsync(
        string address,
        WatchRequest originalRequest,
        CancellationToken ct)
    {
        CeleriantClient client = await CreateClientAsync(address, ct).ConfigureAwait(false);

        // Build probe request: no shard_id.
        WatchRequest probeRequest = BuildWatchRequest(originalRequest, shardId: null);

        ClientResponse response;
        try
        {
            response = await SubscribeAsync(client, probeRequest, ct).ConfigureAwait(false);
        }
        catch (CeleriantErrorException ex) when (
            ex.Error.ErrorCode is ErrorResponse.ShardRoutingMultipleShards
                               or ErrorResponse.ShardRoutingIncompatibleFilters)
        {
            // Server told us how many shards there are: fall back to multi-shard.
            // Explicit shard_ids bypass the routing-rule check, so this recovers both
            // multi-shard scopes (9001) and rule-mismatched filters (9002).
            await client.DisposeAsync().ConfigureAwait(false);

            long numShards = ParseNumShards(ex.Error.ErrorMessage);
            if (numShards == 0)
            {
                // Cannot determine shard count: rethrow as server error.
                throw;
            }

            await ConnectMultiShardAsync(
                address, originalRequest, _options.StartShard, numShards, shardCountFromServer: true, ct)
                .ConfigureAwait(false);
            return;
        }
        catch
        {
            await client.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        if (response is not ClientResponse.Watch watchResponse)
        {
            // A well-formed reply of the wrong kind decodes cleanly and is not an acknowledgement
            // of anything. Accepting it hands back a connection subscribed to nothing: the caller
            // holds a watch it believes is live and will wait on it forever.
            await client.DisposeAsync().ConfigureAwait(false);
            throw new Errors.ProtocolException(
                $"Unexpected response type {response.GetType().Name} to the watch subscription.");
        }

        if (watchResponse.Value.Events.Length > 0)
        {
            // Buffer the first response so NextAsync can return it.
            _bufferedResponse = watchResponse.Value;
        }

        // Single-shard connected. The server pushes responses from here on.
        _singleClient = client;
    }

    private async Task<WatchResponse> ReadSingleShardResponseAsync(CancellationToken ct)
    {
        // Read off a local: a caller parked here while another thread disposes must not find the
        // field emptied underneath it and report the disposal as a NullReferenceException.
        CeleriantClient client = _singleClient
            ?? throw new ObjectDisposedException(nameof(WatchConnection));

        // The server pushes responses after the subscription; nothing is sent here.
        // Heartbeats (empty events) are internal and are silently consumed.
        while (true)
        {
            ClientResponse response = await client.ReadResponseAsync(ct).ConfigureAwait(false);

            if (response is ClientResponse.Watch watchResponse)
            {
                if (watchResponse.Value.Events.Length > 0)
                    return watchResponse.Value;

                // Heartbeat: keep reading.
                continue;
            }

            throw new Errors.ProtocolException(
                $"Unexpected response type {response.GetType().Name} during watch.");
        }
    }

    // -------------------------------------------------------------------------
    // Multi-shard connection
    // -------------------------------------------------------------------------

    private async Task ConnectMultiShardAsync(
        string address,
        WatchRequest request,
        long startShard,
        long numShards,
        bool shardCountFromServer,
        CancellationToken ct)
    {
        // Shard ids start at zero. Checked first, so the subtraction below cannot overflow.
        if (startShard < 0)
        {
            throw new ArgumentOutOfRangeException(
                nameof(WatchOptions.StartShard),
                startShard,
                "StartShard names a shard id and cannot be negative.");
        }

        // An empty or inverted range names no shards; opening one connection anyway would hand
        // back a live-looking watch bound to a shard the caller never asked for, blind to the
        // range it believes it is watching and with nothing to ever say so.
        //
        // Which exception depends on where the upper bound came from — here and at every
        // rejection below. A caller's own MaxShardHint is a programmer error no retry or failover
        // can fix, so it is an argument exception, outside the hierarchy a caller's
        // `catch (CeleriantClientException)` around connect would sweep up. A server-reported
        // bound IS a runtime condition — the same options are fine against a larger cluster — so
        // it stays inside that hierarchy. Neither type is retried across candidate nodes:
        // CeleriantPool.WatchAsync fails over only on ConnectionFailed and ConnectionTimeout.
        if (numShards <= startShard)
        {
            string range = $"A watch over shards [{startShard}, {numShards}) covers no shards.";

            throw shardCountFromServer
                ? new Errors.ProtocolException(
                    $"The server reported {numShards} shards. {range} StartShard must be below the "
                    + "shard count for the watch to observe anything.")
                : new ArgumentOutOfRangeException(
                    nameof(WatchOptions.MaxShardHint),
                    numShards,
                    $"{range} MaxShardHint is the exclusive upper bound and must be above "
                    + $"StartShard ({startShard}) for the watch to observe anything.");
        }

        // Checked, because an unchecked cast of a num_shards past int range wraps to a small
        // positive number: the caller would hold a watch over a handful of shards believing it
        // covered every one the server named.
        int shardCount;
        try
        {
            shardCount = checked((int)(numShards - startShard));
        }
        catch (OverflowException overflow)
        {
            string detail =
                $"A watch over shards [{startShard}, {numShards}) spans more shards than can be "
                + "subscribed; the shard count is not usable.";

            throw shardCountFromServer
                ? new Errors.ProtocolException($"The server reported {numShards} shards. " + detail, overflow)
                : new ArgumentOutOfRangeException(nameof(WatchOptions.MaxShardHint), numShards, detail);
        }

        if (shardCount > MaxShards)
        {
            string detail =
                $"A watch over shards [{startShard}, {numShards}) spans {shardCount} shards, past the "
                + $"{MaxShards} this client will open connections for.";

            throw shardCountFromServer
                ? new Errors.ProtocolException($"The server reported {numShards} shards. " + detail)
                : new ArgumentOutOfRangeException(nameof(WatchOptions.MaxShardHint), numShards, detail);
        }

        // Subscribe every shard before returning. A shard that cannot subscribe fails the connect
        // rather than surfacing later through a connection the caller is already holding and
        // already reading events from: a caller that has been handed a WatchConnection is entitled
        // to treat it as watching every shard in the range.
        var connects = new Task<ShardSubscription>[shardCount];
        for (int i = 0; i < shardCount; i++)
            connects[i] = SubscribeShardAsync(address, request, startShard + i, ct);

        ShardSubscription[] shards;
        try
        {
            shards = await Task.WhenAll(connects).ConfigureAwait(false);
        }
        catch
        {
            // WhenAll reports one failure; every shard that did subscribe still owns a socket, and
            // leaving those open leaks a connection per shard on every failed connect. A shard that
            // failed has already closed its own.
            foreach (Task<ShardSubscription> connect in connects)
            {
                if (connect.IsCompletedSuccessfully)
                    await connect.Result.Client.DisposeAsync().ConfigureAwait(false);
            }

            throw;
        }

        // Unbounded channel: background tasks write, NextAsync reads.
        var channel = Channel.CreateUnbounded<WatchResponse>(new UnboundedChannelOptions
        {
            SingleWriter = false,
            SingleReader = true,
        });

        // A subscription ack may already carry events. They were produced before any reader
        // started, so nothing else will ever deliver them; dropping them would be a silent gap
        // spanning exactly the moment the watch was established.
        foreach (ShardSubscription shard in shards)
        {
            if (shard.Backlog is { } backlog)
                channel.Writer.TryWrite(backlog);
        }

        // Deliberately NOT linked to the caller's ct. That token scopes connecting; the
        // subscription it produced is owned by this WatchConnection and ends when the caller
        // disposes it. Linking them retires a live subscription the caller still holds, and a
        // subscription that stops without the caller asking is exactly what a watch must never do.
        CancellationToken shardCt = _readersCts.Token;

        // The readers below observe this object — Stopping and _shardFailure — but neither depends
        // on the fields assigned after this loop, and nothing outside holds the connection until
        // ConnectAsync returns. So starting them here reads no torn state.
        var shardTasks = new Task[shardCount];
        for (int i = 0; i < shardCount; i++)
            shardTasks[i] = RunShardReaderAsync(shards[i].Client, channel.Writer, shardCt);

        _shardTasks = shardTasks;
        _channel = channel;
    }

    /// <summary>A shard connection that has sent its watch request and had it acked.</summary>
    private readonly struct ShardSubscription(CeleriantClient client, WatchResponse? backlog)
    {
        public readonly CeleriantClient Client = client;

        /// <summary>Events that rode in on the ack, before any reader existed to receive them.</summary>
        public readonly WatchResponse? Backlog = backlog;
    }

    /// <summary>
    /// Open one shard's connection and complete its subscription, so that a failure to subscribe is
    /// a failure to connect. Owns the connection until it hands it back: anything that goes wrong
    /// here closes the socket rather than leaving it open behind a thrown exception.
    /// </summary>
    private async Task<ShardSubscription> SubscribeShardAsync(
        string address,
        WatchRequest request,
        long shardId,
        CancellationToken ct)
    {
        CeleriantClient client = await CreateClientAsync(address, ct).ConfigureAwait(false);
        try
        {
            ClientResponse ack = await SubscribeAsync(
                client, BuildWatchRequest(request, shardId), ct).ConfigureAwait(false);

            if (ack is not ClientResponse.Watch watchAck)
            {
                throw new Errors.ProtocolException(
                    $"Unexpected response type {ack.GetType().Name} to the watch subscription for shard {shardId}.");
            }

            return new ShardSubscription(
                client,
                watchAck.Value.Events.Length > 0 ? watchAck.Value : null);
        }
        catch
        {
            await client.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    private async Task RunShardReaderAsync(
        CeleriantClient client,
        ChannelWriter<WatchResponse> writer,
        CancellationToken ct)
    {
        // This reader owns this connection. `await using` rather than a disposal inside the finally
        // below, so the socket has an owner from the first instruction: anything that throws before
        // the try is entered — including building the reason — still closes it.
        await using (client)
        {
            // Non-nullable and pre-set to the fail-closed answer: a reader that stops while the
            // subscription is live always has something to report, so an exit added later that
            // forgets to say why still ends the subscription instead of leaving the caller reading
            // a channel nobody writes to.
            Exception reason = new Errors.ProtocolException(
                "A watch shard reader stopped without reporting a cause; this subscription can no "
                + "longer see that shard's events. Reconnect: events may have been missed.");

            try
            {
                // The subscription was established during connect; the server pushes from here on.
                while (true)
                {
                    ClientResponse response = await client.ReadResponseAsync(ct).ConfigureAwait(false);

                    if (response is not ClientResponse.Watch watchResponse)
                    {
                        reason = new Errors.ProtocolException(
                            $"Unexpected response type {response.GetType().Name} during shard watch.");
                        return;
                    }

                    // Skip heartbeats (empty events): they are internal keep-alive signals.
                    if (watchResponse.Value.Events.Length > 0)
                        await writer.WriteAsync(watchResponse.Value, ct).ConfigureAwait(false);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                reason = ex;
            }
            catch (OperationCanceledException)
            {
                // Keeps the preset reason rather than reporting the cancellation itself. The only
                // token that reaches this reader is the one this class owns, so if it fired without
                // a disposal the subscription is dead, not idle — and a cancellation raised from
                // here would reach NextAsync(TimeSpan) as a null "nothing arrived", telling a blind
                // caller it was fine.
            }
            finally
            {
                // Telling the caller comes first, and nothing between these two statements can
                // throw: a throw ahead of them would abandon the rest of this finally, and on a
                // one-shard fan-out there is no sibling left to close the channel.
                if (!Stopping)
                {
                    Interlocked.CompareExchange(
                        ref _shardFailure, ExceptionDispatchInfo.Capture(reason), null);
                }

                writer.TryComplete();

                // Last, once the caller can already be told. One shard down is the whole watch
                // down: siblings left parked would hold a connection per shard open until a
                // dispose that a reconnect-on-error caller never performs.
                if (!Stopping)
                    await CancelReadersAsync().ConfigureAwait(false);
            }
        }
    }

    /// <summary>
    /// Stop the shard readers. Safe to call from a reader's own teardown and from
    /// <see cref="DisposeAsync"/>, including once the source is already gone.
    /// </summary>
    private async Task CancelReadersAsync()
    {
        try
        {
            await _readersCts.CancelAsync().ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Disposal already stopped them, or a cancellation callback threw on its way out. Both
            // are teardown, and by the time a reader reaches this the caller has already been told
            // why the subscription ended — there is no failure left here worth surfacing.
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /// <summary>
    /// Clone a <see cref="WatchRequest"/> with an overridden <c>shard_id</c>.
    /// </summary>
    private static WatchRequest BuildWatchRequest(WatchRequest source, long? shardId) => new WatchRequest
    {
        CorrelationId = source.CorrelationId,
        RequestedLatency = source.RequestedLatency,
        ShardId = shardId,
        Orgs = source.Orgs,
        AggregateTypes = source.AggregateTypes,
        Aggregates = source.Aggregates,
        OperationTypes = source.OperationTypes,
    };

    /// <summary>
    /// Send the watch request and wait for the server to acknowledge it, bounded by the same
    /// timeout as the dial. A node that accepts the socket and never answers must not park the
    /// connect: <see cref="CeleriantPool.WatchAsync"/> can only try the next candidate node on a
    /// connect that returns, and <see cref="ConnectionTimeoutException"/> is what it treats as
    /// failover-class.
    /// </summary>
    private async Task<ClientResponse> SubscribeAsync(
        CeleriantClient client,
        WatchRequest request,
        CancellationToken ct)
    {
        var subscribe = new ClientRequest.Watch(request);
        if (_options.ConnectionTimeout is not { } timeout)
            return await client.SendRequestAsync(subscribe, ct).ConfigureAwait(false);

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(timeout);
        try
        {
            return await client.SendRequestAsync(subscribe, timeoutCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            throw new ConnectionTimeoutException(
                $"{_address} accepted the connection but did not acknowledge the watch subscription "
                + $"within {timeout}.");
        }
    }

    private async Task<CeleriantClient> CreateClientAsync(string address, CancellationToken ct)
    {
        var client = await CeleriantClient.ConnectAsync(
            address,
            _options.ConnectionTimeout,
            _options.TlsConfig,
            ct).ConfigureAwait(false);

        if (_options.MaxRequestSize is { } maxRequestSize)
            client.WithMaxRequestSize(maxRequestSize);
        if (_options.MaxResponseSize is { } maxResponseSize)
            client.WithMaxResponseSize(maxResponseSize);

        if (_options.IdentityConfig is { } identityConfig)
        {
            try
            {
                await IdentifyWithTimeoutAsync(client, identityConfig, ct).ConfigureAwait(false);
            }
            catch
            {
                await client.DisposeAsync().ConfigureAwait(false);
                throw;
            }
        }

        return client;
    }

    /// <summary>
    /// Run the identity handshake, bounded by the same timeout as the dial. The dial bounds
    /// TCP+TLS and <see cref="SubscribeAsync"/> bounds the subscription ack, but the identify
    /// round trip sits between the two with nothing of its own: watch clients never call
    /// <c>WithTimeout</c>, so without this bound a node that accepts the socket and goes silent
    /// parks connect for ever.
    /// </summary>
    private async Task IdentifyWithTimeoutAsync(
        CeleriantClient client,
        ClientIdentityConfig identityConfig,
        CancellationToken ct)
    {
        if (_options.ConnectionTimeout is not { } timeout)
        {
            await client.IdentifyAsync(identityConfig, ct).ConfigureAwait(false);
            return;
        }

        using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeoutCts.CancelAfter(timeout);
        try
        {
            await client.IdentifyAsync(identityConfig, timeoutCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested && !ct.IsCancellationRequested)
        {
            throw new ConnectionTimeoutException(
                $"{_address} accepted the connection but did not complete the identity handshake "
                + $"within {timeout}.");
        }
    }

    /// <summary>
    /// Find <c>"num_shards":</c> in the error message and parse the following decimal integer.
    /// Returns 0 if not found or parse fails.
    /// </summary>
    private static long ParseNumShards(string errorMessage)
    {
        if (string.IsNullOrEmpty(errorMessage))
            return 0;

        const string marker = "\"num_shards\":";
        int idx = errorMessage.IndexOf(marker, StringComparison.Ordinal);
        if (idx < 0)
            return 0;

        int valueStart = idx + marker.Length;

        // Skip whitespace.
        while (valueStart < errorMessage.Length && errorMessage[valueStart] == ' ')
            valueStart++;

        if (valueStart >= errorMessage.Length)
            return 0;

        // Read consecutive digit characters.
        int valueEnd = valueStart;
        while (valueEnd < errorMessage.Length && char.IsAsciiDigit(errorMessage[valueEnd]))
            valueEnd++;

        if (valueEnd == valueStart)
            return 0;

        return long.TryParse(errorMessage.AsSpan(valueStart, valueEnd - valueStart), out long result)
            ? result
            : 0;
    }

    private void ThrowIfDisposed()
    {
        if (Stopping)
            throw new ObjectDisposedException(nameof(WatchConnection));
    }
}
