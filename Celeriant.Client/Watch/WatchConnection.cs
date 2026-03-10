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
///     <c>shard_id</c>. If the server returns error code 9001 with a <c>num_shards</c> value
///     embedded in the error message JSON, automatically fall back to multi-shard mode.
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
/// Watch protocol notes: the wire protocol is request-response. For each NextAsync call the
/// client sends the watch request again and the server responds with events accumulated since
/// the last poll (or blocks until events are available, acting as a long-poll).
/// </para>
/// </summary>
public sealed class WatchConnection : IAsyncDisposable
{
    private const uint ShardRoutingError = 9001;

    // Single-shard state.
    private CeleriantClient? _singleClient;
    private WatchRequest? _singleRequest;
    // First response buffered during the probe handshake.
    private WatchResponse? _bufferedResponse;

    // Multi-shard state.
    private CeleriantClient[]? _shardClients;
    private Task[]? _shardTasks;
    private Channel<WatchResponse>? _channel;
    // Linked CTS combining the caller's token with _disposeCts; owned by multi-shard mode.
    private CancellationTokenSource? _shardLinkedCts;
    // Internal CTS used to stop background shard reader tasks on disposal.
    private readonly CancellationTokenSource _disposeCts = new();

    private readonly WatchOptions _options;
    private bool _disposed;

    private WatchConnection(WatchOptions options)
    {
        _options = options;
    }

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
        var connection = new WatchConnection(options);

        if (options.MaxShardHint.HasValue)
        {
            // Skip probe, go directly to multi-shard.
            await connection.ConnectMultiShardAsync(
                address, request, options.StartShard, options.MaxShardHint.Value, ct)
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
    /// Wait for the next batch of watch events. Blocks until a response arrives or
    /// the cancellation token is triggered.
    /// </summary>
    public async Task<WatchResponse> NextAsync(CancellationToken ct = default)
    {
        ThrowIfDisposed();

        if (_channel is not null)
        {
            // Multi-shard: read from the shared channel.
            return await _channel.Reader.ReadAsync(ct).ConfigureAwait(false);
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
    /// Wait for the next batch of watch events with a timeout.
    /// Returns null if no response arrives within <paramref name="timeout"/>.
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
        if (_disposed)
            return;

        _disposed = true;

        // Cancel background shard reader tasks.
        _disposeCts.Cancel();
        _disposeCts.Dispose();

        if (_singleClient is not null)
        {
            await _singleClient.DisposeAsync().ConfigureAwait(false);
            _singleClient = null;
            _singleRequest = null;
            _bufferedResponse = null;
        }

        if (_shardClients is not null)
        {
            // Signal the channel that no more items will be written.
            _channel?.Writer.TryComplete();

            // Wait for background reader tasks to finish now that ct is cancelled and
            // the channel is marked complete.
            if (_shardTasks is not null)
            {
                try
                {
                    await Task.WhenAll(_shardTasks).ConfigureAwait(false);
                }
                catch
                {
                    // Suppress exceptions from background tasks on disposal.
                }
            }

            foreach (CeleriantClient c in _shardClients)
                await c.DisposeAsync().ConfigureAwait(false);

            _shardLinkedCts?.Dispose();
            _shardLinkedCts = null;
            _shardClients = null;
            _shardTasks = null;
        }
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
            response = await client.SendRequestAsync(
                new ClientRequest.Watch(probeRequest), _options.Compression, ct)
                .ConfigureAwait(false);
        }
        catch
        {
            await client.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        if (response is ClientResponse.GenericError err && err.Value.ErrorCode == ShardRoutingError)
        {
            // Server told us how many shards there are — fall back to multi-shard.
            await client.DisposeAsync().ConfigureAwait(false);

            long numShards = ParseNumShards(err.Value.ErrorMessage);
            if (numShards == 0)
            {
                // Cannot determine shard count — rethrow as server error.
                throw new CeleriantErrorException(err.Value);
            }

            await ConnectMultiShardAsync(address, originalRequest, _options.StartShard, numShards, ct)
                .ConfigureAwait(false);
            return;
        }

        if (response is ClientResponse.GenericError genericErr)
        {
            await client.DisposeAsync().ConfigureAwait(false);
            throw new CeleriantErrorException(genericErr.Value);
        }

        if (response is ClientResponse.Watch watchResponse && watchResponse.Value.Events.Length > 0)
        {
            // Buffer the first response so NextAsync can return it.
            _bufferedResponse = watchResponse.Value;
        }

        // Single-shard connected. Store client and request for subsequent NextAsync calls.
        _singleClient = client;
        _singleRequest = probeRequest;
    }

    private async Task<WatchResponse> ReadSingleShardResponseAsync(CancellationToken ct)
    {
        // Each NextAsync re-sends the same watch request. The server acts as a long-poll:
        // it responds once events are available (or times out and returns an empty batch).
        // Heartbeats (empty events) are internal and are silently consumed here.
        while (true)
        {
            ClientResponse response = await _singleClient!.SendRequestAsync(
                new ClientRequest.Watch(_singleRequest!), _options.Compression, ct)
                .ConfigureAwait(false);

            if (response is ClientResponse.Watch watchResponse)
            {
                if (watchResponse.Value.Events.Length > 0)
                    return watchResponse.Value;

                // Heartbeat — re-poll.
                continue;
            }

            if (response is ClientResponse.GenericError err)
                throw new CeleriantErrorException(err.Value);

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
        CancellationToken ct)
    {
        int shardCount = (int)(numShards - startShard);
        if (shardCount <= 0)
            shardCount = 1; // Defensive: always open at least one connection.

        var clients = new CeleriantClient[shardCount];
        var connectTasks = new Task[shardCount];

        for (int i = 0; i < shardCount; i++)
        {
            int localI = i;
            connectTasks[i] = Task.Run(async () =>
            {
                clients[localI] = await CreateClientAsync(address, ct).ConfigureAwait(false);
            }, ct);
        }

        try
        {
            await Task.WhenAll(connectTasks).ConfigureAwait(false);
        }
        catch
        {
            // Dispose any clients that connected before the failure.
            foreach (CeleriantClient? c in clients)
            {
                if (c is not null)
                    await c.DisposeAsync().ConfigureAwait(false);
            }
            throw;
        }

        // Unbounded channel: background tasks write, NextAsync reads.
        var channel = Channel.CreateUnbounded<WatchResponse>(new UnboundedChannelOptions
        {
            SingleWriter = false,
            SingleReader = true,
        });

        // Link the caller's ct with the disposal ct so background tasks stop on either.
        // This CTS is stored in _shardLinkedCts and disposed in DisposeAsync.
        var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, _disposeCts.Token);
        CancellationToken shardCt = linkedCts.Token;

        var shardTasks = new Task[shardCount];
        for (int i = 0; i < shardCount; i++)
        {
            long shardId = startShard + i;
            CeleriantClient shardClient = clients[i];
            WatchRequest shardRequest = BuildWatchRequest(request, shardId);
            shardTasks[i] = RunShardReaderAsync(shardClient, shardRequest, channel.Writer, shardCt);
        }

        _shardLinkedCts = linkedCts;
        _shardClients = clients;
        _shardTasks = shardTasks;
        _channel = channel;
    }

    private async Task RunShardReaderAsync(
        CeleriantClient client,
        WatchRequest request,
        ChannelWriter<WatchResponse> writer,
        CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested)
            {
                ClientResponse response = await client.SendRequestAsync(
                    new ClientRequest.Watch(request), _options.Compression, ct)
                    .ConfigureAwait(false);

                if (response is ClientResponse.Watch watchResponse)
                {
                    // Skip heartbeats (empty events) — they are internal keep-alive signals.
                    if (watchResponse.Value.Events.Length > 0)
                        await writer.WriteAsync(watchResponse.Value, ct).ConfigureAwait(false);
                }
                else if (response is ClientResponse.GenericError err)
                {
                    writer.TryComplete(new CeleriantErrorException(err.Value));
                    return;
                }
                else
                {
                    writer.TryComplete(new Errors.ProtocolException(
                        $"Unexpected response type {response.GetType().Name} during shard watch."));
                    return;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown.
        }
        catch (Exception ex)
        {
            writer.TryComplete(ex);
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

    private async Task<CeleriantClient> CreateClientAsync(string address, CancellationToken ct)
    {
        return await CeleriantClient.ConnectAsync(
            address,
            connectionTimeout: null,
            _options.TlsConfig,
            ct).ConfigureAwait(false);
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
        if (_disposed)
            throw new ObjectDisposedException(nameof(WatchConnection));
    }
}
