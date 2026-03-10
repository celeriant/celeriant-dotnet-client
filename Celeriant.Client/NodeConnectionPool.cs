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
    private readonly string _address;
    private readonly CeleriantPoolOptions _options;
    private readonly Channel<(CeleriantClient client, DateTimeOffset lastUsed)> _idle;
    private readonly SemaphoreSlim _totalSem;
    private bool _disposed;

    public string Address => _address;

    public NodeConnectionPool(string address, CeleriantPoolOptions options)
    {
        _address = address;
        _options = options;

        _idle = Channel.CreateBounded<(CeleriantClient, DateTimeOffset)>(
            new BoundedChannelOptions(options.MaxConnections)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleWriter = false,
                SingleReader = false,
            });

        _totalSem = new SemaphoreSlim(options.MaxConnections, options.MaxConnections);
    }

    /// <summary>
    /// Lease a connection to this node. Returns an idle connection if available,
    /// or creates a new one if under the limit.
    /// </summary>
    public async Task<PooledConnection> GetConnectionAsync(CancellationToken ct)
    {
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
            try
            {
                var client = await CreateConnectionAsync(ct).ConfigureAwait(false);
                return new PooledConnection(client, ReturnConnectionAsync);
            }
            catch
            {
                _totalSem.Release();
                throw;
            }
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
                    try
                    {
                        var client = await CreateConnectionAsync(ct).ConfigureAwait(false);
                        return new PooledConnection(client, ReturnConnectionAsync);
                    }
                    catch
                    {
                        _totalSem.Release();
                        throw;
                    }
                }

                continue;
            }

            return new PooledConnection(entry.client, ReturnConnectionAsync);
        }
    }

    /// <inheritdoc />
    public async Task<ClientResponse> ExecuteRequestAsync(
        ClientRequest request,
        CompressionType compression,
        int compressionThreshold,
        CancellationToken ct)
    {
        var conn = await GetConnectionAsync(ct).ConfigureAwait(false);
        await using (conn)
        {
            try
            {
                return await conn.Client.SendRequestAsync(request, compression, compressionThreshold, ct)
                    .ConfigureAwait(false);
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
            await client.IdentifyAsync(identityConfig, ct).ConfigureAwait(false);
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
