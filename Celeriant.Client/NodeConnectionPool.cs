using System.Threading.Channels;
using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;

namespace Celeriant.Client;

/// <summary>
/// Manages a pool of connections to a single Celeriant server node.
/// Handles connection creation, idle timeout eviction, and broken connection disposal.
/// Used internally by <see cref="CeleriantPool"/> to manage per-node connection pools.
/// </summary>
internal sealed class NodeConnectionPool : INodeConnectionPool
{
    /// <summary>
    /// How long to fast-fail after a connection attempt to this node fails.
    /// Prevents many tasks from each independently timing out on a dead host.
    /// </summary>
    private static readonly TimeSpan CircuitBreakerCooldown = TimeSpan.FromSeconds(2);

    /// <summary>
    /// Maximum concurrent TCP connection attempts. Limits waste when a node is down
    /// while still allowing parallel connections to healthy nodes.
    /// </summary>
    private const int MaxConcurrentConnects = 32;

    private readonly string _address;
    private readonly CeleriantPoolOptions _options;
    private readonly PoolDictCache _dictCache;
    private readonly Channel<(CeleriantClient client, DateTimeOffset lastUsed)> _idle;
    private readonly SemaphoreSlim _totalSem;
    private readonly SemaphoreSlim _connectSem = new(MaxConcurrentConnects, MaxConcurrentConnects);
    private long _lastConnectFailureTicks;
    private bool _disposed;

    public string Address => _address;

    public NodeConnectionPool(string address, CeleriantPoolOptions options, PoolDictCache dictCache)
    {
        _address = address;
        _options = options;
        _dictCache = dictCache;

        _idle = Channel.CreateBounded<(CeleriantClient, DateTimeOffset)>(
            new BoundedChannelOptions(options.MaxConnections)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleWriter = false,
                SingleReader = false,
            });

        _totalSem = new SemaphoreSlim(options.MaxConnections, options.MaxConnections);
    }

    private bool IsCircuitOpen
    {
        get
        {
            var failedAt = Interlocked.Read(ref _lastConnectFailureTicks);
            return failedAt > 0 && new TimeSpan(DateTimeOffset.UtcNow.Ticks - failedAt) < CircuitBreakerCooldown;
        }
    }

    /// <summary>
    /// Lease a connection to this node. Returns an idle connection if available,
    /// or creates a new one if under the limit.
    /// </summary>
    public async Task<PooledConnection> GetConnectionAsync(CancellationToken ct)
    {
        if (IsCircuitOpen)
            throw new ConnectionFailedException($"Circuit breaker open for {_address}");

        // Fast path: try to reuse an idle connection.
        while (_idle.Reader.TryRead(out var entry))
        {
            if (DateTimeOffset.UtcNow - entry.lastUsed > _options.IdleTimeout)
            {
                await entry.client.DisposeAsync().ConfigureAwait(false);
                _totalSem.Release();
                continue;
            }

            return new PooledConnection(entry.client, ReturnConnectionAsync);
        }

        // Try to create a new connection if under the limit.
        if (_totalSem.Wait(0))
        {
            return await CreateGatedConnectionAsync(ct).ConfigureAwait(false);
        }

        // At the limit — wait for an idle connection.
        while (true)
        {
            ct.ThrowIfCancellationRequested();

            var entry = await _idle.Reader.ReadAsync(ct).ConfigureAwait(false);

            if (DateTimeOffset.UtcNow - entry.lastUsed > _options.IdleTimeout)
            {
                await entry.client.DisposeAsync().ConfigureAwait(false);
                _totalSem.Release();

                if (_totalSem.Wait(0))
                {
                    return await CreateGatedConnectionAsync(ct).ConfigureAwait(false);
                }

                continue;
            }

            return new PooledConnection(entry.client, ReturnConnectionAsync);
        }
    }

    /// <summary>
    /// Gate new TCP connections through a semaphore and check the circuit breaker
    /// before and after waiting. On failure, trip the breaker so queued tasks fail fast.
    /// Caller must already hold a <c>_totalSem</c> permit.
    /// </summary>
    private async Task<PooledConnection> CreateGatedConnectionAsync(CancellationToken ct)
    {
        try
        {
            await _connectSem.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                // Re-check after waiting — breaker may have tripped while queued.
                if (IsCircuitOpen)
                    throw new ConnectionFailedException($"Circuit breaker open for {_address}");

                // Re-check pool — someone ahead of us may have returned a connection.
                if (_idle.Reader.TryRead(out var reuse))
                {
                    if (DateTimeOffset.UtcNow - reuse.lastUsed <= _options.IdleTimeout)
                        return new PooledConnection(reuse.client, ReturnConnectionAsync);
                    await reuse.client.DisposeAsync().ConfigureAwait(false);
                }

                var client = await CreateConnectionAsync(ct).ConfigureAwait(false);
                Interlocked.Exchange(ref _lastConnectFailureTicks, 0);
                return new PooledConnection(client, ReturnConnectionAsync);
            }
            catch
            {
                Interlocked.Exchange(ref _lastConnectFailureTicks, DateTimeOffset.UtcNow.Ticks);
                throw;
            }
            finally
            {
                _connectSem.Release();
            }
        }
        catch
        {
            _totalSem.Release();
            throw;
        }
    }

    /// <inheritdoc />
    public async Task<ClientResponse> ExecuteRequestAsync(
        ClientRequest request,
        CancellationToken ct)
    {
        var conn = await GetConnectionAsync(ct).ConfigureAwait(false);
        await using (conn)
        {
            try
            {
                return await conn.Client.SendRequestAsync(request, ct).ConfigureAwait(false);
            }
            catch (CeleriantClientException)
            {
                conn.MarkBroken();
                throw;
            }
        }
    }

    /// <summary>
    /// Drain and dispose all idle connections, releasing their semaphore slots.
    /// </summary>
    public async Task FlushAsync()
    {
        while (_idle.Reader.TryRead(out var entry))
        {
            try
            {
                await entry.client.DisposeAsync().ConfigureAwait(false);
            }
            catch
            {
                // Suppress disposal errors.
            }
            _totalSem.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
        _idle.Writer.TryComplete();

        while (_idle.Reader.TryRead(out var entry))
        {
            try
            {
                await entry.client.DisposeAsync().ConfigureAwait(false);
            }
            catch
            {
                // Suppress disposal errors.
            }
        }

        _connectSem.Dispose();
        _totalSem.Dispose();
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private async Task<CeleriantClient> CreateConnectionAsync(CancellationToken ct)
    {
        var client = await CeleriantClient.ConnectAsync(
            _address,
            _options.ConnectionTimeout,
            _options.TlsConfig,
            ct).ConfigureAwait(false);

        client.WithMaxRequestSize(_options.MaxRequestSize)
              .WithMaxResponseSize(_options.MaxResponseSize)
              .WithTimeout(_options.RequestTimeout);

        if (_options.IdentityConfig is { } identityConfig)
        {
            // Advertise our known dictionary sha so the server can skip resending its bytes,
            // and resolve a confirmed-but-unsent sha from the shared pool cache.
            await client.IdentifyAsync(identityConfig, _dictCache.LastSha, _dictCache.DictForSha, ct)
                .ConfigureAwait(false);

            // If this connection received (or confirmed) a dictionary, share it pool-wide.
            if (client.CurrentDict is { } dict)
                _dictCache.CacheDict(dict.Sha, dict.Bytes);
        }

        return client;
    }

    private async ValueTask ReturnConnectionAsync(CeleriantClient client, bool broken)
    {
        if (broken || _disposed)
        {
            try
            {
                await client.DisposeAsync().ConfigureAwait(false);
            }
            catch
            {
                // Suppress disposal errors.
            }
            finally
            {
                if (!_disposed)
                    _totalSem.Release();
            }
            return;
        }

        if (!_idle.Writer.TryWrite((client, DateTimeOffset.UtcNow)))
        {
            try
            {
                await client.DisposeAsync().ConfigureAwait(false);
            }
            catch
            {
                // Suppress disposal errors.
            }
            _totalSem.Release();
        }
    }
}
