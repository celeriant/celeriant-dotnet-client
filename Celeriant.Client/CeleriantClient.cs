using System.Buffers;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;
using Celeriant.Client.Crypto;
using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;

namespace Celeriant.Client;

/// <summary>
/// Low-level single-connection Celeriant client with 1:1 parity with the Rust <c>CeleriantClient</c>.
/// Owns a single TCP connection; no pooling or auto-reconnect. Caller manages the connection lifecycle.
///
/// Use <see cref="ConnectAsync(string, CancellationToken)"/> or one of the other static factory
/// methods to create an instance.
/// </summary>
public sealed class CeleriantClient : ICeleriantClient
{
    private readonly TcpClient _tcpClient;
    private readonly Stream _stream;

    // Protects against concurrent SendRequestAsync calls on the same connection.
    private readonly SemaphoreSlim _sendLock = new(1, 1);

    private long _maxRequestSize = 10_000_000;
    private long _maxResponseSize = 64 * 1024 * 1024; // 64 MB — matches server default
    private TimeSpan? _timeout;

    // Compression dictionary received from the cluster during Identify, if any.
    // When set, variable-size requests above the threshold are compressed with it,
    // and ZstdDict responses are decompressed with it. Null => always uncompressed.
    private CachedDict? _dict;

    /// <summary>
    /// Serialized-payload size (bytes) at or above which a variable-size request is
    /// dictionary-compressed. Mirrors the server's <c>RESPONSE_COMPRESSION_THRESHOLD_BYTES</c>.
    /// </summary>
    private const int CompressionThresholdBytes = 1024;

    /// <summary>The compression dictionary negotiated for this connection, if any.</summary>
    internal CachedDict? CurrentDict => _dict;

    // -------------------------------------------------------------------------
    // Construction
    // -------------------------------------------------------------------------

    private CeleriantClient(TcpClient tcpClient, Stream stream)
    {
        _tcpClient = tcpClient;
        _stream = stream;
    }

    // -------------------------------------------------------------------------
    // Static factory methods
    // -------------------------------------------------------------------------

    /// <summary>
    /// Connect to a Celeriant server over plain TCP.
    /// </summary>
    /// <param name="address">Server address in "host:port" format.</param>
    /// <param name="ct">Cancellation token.</param>
    public static Task<CeleriantClient> ConnectAsync(
        string address,
        CancellationToken ct = default)
        => ConnectAsync(address, connectionTimeout: null, tlsConfig: null, ct);

    /// <summary>
    /// Connect to a Celeriant server over TLS.
    /// </summary>
    /// <param name="address">Server address in "host:port" format.</param>
    /// <param name="tlsConfig">TLS configuration.</param>
    /// <param name="ct">Cancellation token.</param>
    public static Task<CeleriantClient> ConnectTlsAsync(
        string address,
        ClientTlsConfig tlsConfig,
        CancellationToken ct = default)
        => ConnectAsync(address, connectionTimeout: null, tlsConfig, ct);

    /// <summary>
    /// Connect to a Celeriant server with full options.
    /// </summary>
    /// <param name="address">Server address in "host:port" format.</param>
    /// <param name="connectionTimeout">Optional timeout for establishing the connection.</param>
    /// <param name="tlsConfig">Optional TLS configuration; plain TCP if null.</param>
    /// <param name="ct">Cancellation token.</param>
    public static async Task<CeleriantClient> ConnectAsync(
        string address,
        TimeSpan? connectionTimeout = null,
        ClientTlsConfig? tlsConfig = null,
        CancellationToken ct = default)
    {
        (string host, int port) = ParseAddress(address);

        using CancellationTokenSource? timeoutCts =
            connectionTimeout.HasValue ? new CancellationTokenSource(connectionTimeout.Value) : null;

        using CancellationTokenSource? linkedCts = timeoutCts is not null
            ? CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token)
            : null;
        CancellationToken connectCt = linkedCts?.Token ?? ct;

        var tcpClient = new TcpClient();
        try
        {
            // TCP_NODELAY reduces latency for request/response workloads.
            tcpClient.NoDelay = true;

            try
            {
                await tcpClient.ConnectAsync(host, port, connectCt).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (timeoutCts?.IsCancellationRequested == true)
            {
                throw new CeleriantTimeoutException(
                    $"Connection to {address} timed out after {connectionTimeout}.");
            }
            catch (Exception ex) when (ex is SocketException or IOException)
            {
                throw new ConnectionFailedException(
                    $"Failed to connect to {address}: {ex.Message}", ex);
            }

            Stream stream = tcpClient.GetStream();

            if (tlsConfig is not null)
            {
                var sslStream = new SslStream(stream, leaveInnerStreamOpen: false);
                try
                {
                    await sslStream.AuthenticateAsClientAsync(tlsConfig.SslOptions, connectCt)
                        .ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (timeoutCts?.IsCancellationRequested == true)
                {
                    await sslStream.DisposeAsync().ConfigureAwait(false);
                    throw new CeleriantTimeoutException(
                        $"TLS handshake with {address} timed out after {connectionTimeout}.");
                }
                catch (Exception ex) when (ex is IOException or System.Security.Authentication.AuthenticationException)
                {
                    await sslStream.DisposeAsync().ConfigureAwait(false);
                    throw new ConnectionFailedException(
                        $"TLS handshake with {address} failed: {ex.Message}", ex);
                }
                stream = sslStream;
            }

            return new CeleriantClient(tcpClient, stream);
        }
        catch
        {
            tcpClient.Dispose();
            throw;
        }
    }

    // -------------------------------------------------------------------------
    // Configuration (fluent builder; mutates in place since it's a single connection)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Set the maximum allowed request payload size in bytes.
    /// Requests larger than this will throw. Default is 10 MB.
    /// Note: this mutates the current instance and returns it for chaining.
    /// </summary>
    public CeleriantClient WithMaxRequestSize(long maxRequestSize)
    {
        _maxRequestSize = maxRequestSize;
        return this;
    }

    /// <summary>
    /// Set the maximum allowed response payload size in bytes.
    /// Responses larger than this will throw. Default is 64 MB.
    /// Note: this mutates the current instance and returns it for chaining.
    /// </summary>
    public CeleriantClient WithMaxResponseSize(long maxResponseSize)
    {
        _maxResponseSize = maxResponseSize;
        return this;
    }

    /// <summary>
    /// Set a per-request timeout applied to each <see cref="SendRequestAsync(ClientRequest, CancellationToken)"/> call.
    /// Note: this mutates the current instance and returns it for chaining.
    /// </summary>
    public CeleriantClient WithTimeout(TimeSpan timeout)
    {
        _timeout = timeout;
        return this;
    }

    // -------------------------------------------------------------------------
    // Identity verification
    // -------------------------------------------------------------------------

    /// <summary>
    /// Perform the Identify handshake with the server.
    /// Returns the <see cref="Guid"/> client ID assigned by the server, or null if the
    /// server did not include one in the response.
    /// </summary>
    /// <param name="identityConfig">Authentication credentials.</param>
    /// <param name="ct">Cancellation token.</param>
    public Task<Guid?> IdentifyAsync(
        ClientIdentityConfig identityConfig,
        CancellationToken ct = default)
        => IdentifyAsync(identityConfig, knownDictSha: null, dictLookup: null, ct);

    /// <summary>
    /// Perform the Identify handshake, advertising a previously cached compression-dictionary sha
    /// so the server can skip re-sending the bytes when they match.
    /// </summary>
    /// <param name="identityConfig">Authentication credentials.</param>
    /// <param name="knownDictSha">SHA-256 hex of a dictionary this client already holds, or null.</param>
    /// <param name="dictLookup">Resolves dictionary bytes for a sha the server confirms but does not resend.</param>
    /// <param name="ct">Cancellation token.</param>
    internal async Task<Guid?> IdentifyAsync(
        ClientIdentityConfig identityConfig,
        string? knownDictSha,
        Func<string, byte[]?>? dictLookup,
        CancellationToken ct = default)
    {
        IdentifyRequest req;

        var resolvedApiKey = identityConfig.ResolveApiKeyBase64();
        if (!string.IsNullOrEmpty(resolvedApiKey))
        {
            // API key authentication (direct base64 key or Guid-derived)
            req = new IdentifyRequest { ApiKey = resolvedApiKey, KnownDictSha256 = knownDictSha };
        }
        else if (!string.IsNullOrEmpty(identityConfig.PublicKeyBase64)
                 && !string.IsNullOrEmpty(identityConfig.PrivateKeyBase64))
        {
            // RSA signing authentication
            string nonce = CeleriantCrypto.GenerateNonce();
            string signature = CeleriantCrypto.SignNonce(identityConfig.PrivateKeyBase64, nonce);
            req = new IdentifyRequest
            {
                PublicKey = identityConfig.PublicKeyBase64,
                Nonce = nonce,
                Signature = signature,
                KnownDictSha256 = knownDictSha,
            };
        }
        else
        {
            throw new ArgumentException(
                "ClientIdentityConfig must have either ApiKeyBase64, ClientId, or both PublicKeyBase64 and PrivateKeyBase64. " +
                "Use one of the static factory methods: FromApiKey(), FromClientId(), or FromRsaKeyPair().",
                nameof(identityConfig));
        }

        // Identify is always sent uncompressed (no dictionary exists yet on a fresh connection).
        byte[] payload = WireCodec.Serialize(req);
        var header = WireHeader.ForRequest(MessageTypes.Requests.Identify, (uint)payload.Length);

        using CancellationTokenSource? timeoutCts = BuildTimeoutCts(ct);
        CancellationToken effectiveCt = timeoutCts?.Token ?? ct;

        await _sendLock.WaitAsync(effectiveCt).ConfigureAwait(false);
        try
        {
            await SendHeaderAndPayloadAsync(header, payload, effectiveCt).ConfigureAwait(false);

            byte[] headerBuf = new byte[WireHeader.Size];
            await ReadExactIntoAsync(headerBuf, WireHeader.Size, effectiveCt).ConfigureAwait(false);
            WireHeader responseHeader = WireHeader.ParseFrom(headerBuf);

            if (responseHeader.CompressedLength > _maxResponseSize)
                throw new ProtocolException(
                    $"Identify response payload {responseHeader.CompressedLength} bytes exceeds maximum allowed size {_maxResponseSize}.");

            // The Identify response carries the (uncompressed) dictionary bytes, so it can be
            // larger than a fixed-size frame; read exactly CompressedLength bytes.
            int respLen = (int)responseHeader.CompressedLength;
            byte[] responsePayload = new byte[respLen];
            await ReadExactIntoAsync(responsePayload, respLen, effectiveCt).ConfigureAwait(false);

            if (responseHeader.MessageType != MessageTypes.Responses.Identify)
            {
                throw new ProtocolException(
                    $"Expected Identify response (type {MessageTypes.Responses.Identify}), " +
                    $"got type {responseHeader.MessageType}.");
            }

            IdentifyResponse identifyResponse;
            try
            {
                identifyResponse = WireCodec.Deserialize<IdentifyResponse>(responsePayload);
            }
            catch (Exception ex)
            {
                throw new ProtocolException("Failed to deserialize IdentifyResponse.", ex);
            }

            // Resolve the dictionary for this connection:
            //   sha + bytes  → server shipped a new/refreshed dictionary; store it.
            //   sha only     → server confirmed our advertised sha; resolve bytes from the pool cache.
            //   no sha       → cluster is not using ZstdDict; no dictionary.
            _dict = (identifyResponse.CompressionDictSha256, identifyResponse.CompressionDictBytes) switch
            {
                (string sha, byte[] bytes) => new CachedDict(sha, bytes),
                (string sha, null) => dictLookup?.Invoke(sha) is { } cached ? new CachedDict(sha, cached) : null,
                (null, _) => null,
            };

            return identifyResponse.ClientId;
        }
        catch (OperationCanceledException) when (IsTimeoutCancellation(timeoutCts, ct))
        {
            throw new CeleriantTimeoutException("IdentifyAsync timed out.");
        }
        catch (EndOfStreamException ex)
        {
            throw new ConnectionFailedException("Connection closed during IdentifyAsync.", ex);
        }
        catch (IOException ex)
        {
            throw new ConnectionFailedException("IO error during IdentifyAsync.", ex);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    // -------------------------------------------------------------------------
    // Typed convenience methods
    // -------------------------------------------------------------------------

    /// <summary>Send a read request and return the typed response.</summary>
    /// <param name="request">The read request.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <exception cref="AggregateNotFoundException">The aggregate does not exist.</exception>
    /// <exception cref="BatchIndexUnavailableException">The requested batch index has been trimmed. Re-read from <see cref="BatchIndexUnavailableException.MinimumAvailableVersion"/>.</exception>
    public async Task<ReadResponse> ReadAsync(
        ReadRequest request,
        CancellationToken ct = default)
    {
        var response = await SendRequestAsync(new ClientRequest.Read(request), ct: ct).ConfigureAwait(false);
        return response switch
        {
            ClientResponse.Read r => r.Value,
            _ => throw new ProtocolException($"Unexpected response type {response.GetType().Name} for Read."),
        };
    }

    /// <summary>Send a write request and return the typed response.</summary>
    /// <param name="request">The write request.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <exception cref="WriteOccException">Optimistic concurrency violation — the aggregate has been modified. Re-read and retry.</exception>
    /// <exception cref="IdempotencyViolationException">Duplicate write — the event was already accepted. No action needed.</exception>
    /// <exception cref="AggregateNotFoundException">The aggregate does not exist and <c>AllowCreate</c> is false.</exception>
    /// <exception cref="AggregateRecreateNotAllowedException">The aggregate was permanently deleted.</exception>
    /// <exception cref="SchemaValidationException">An event payload does not conform to the registered schema.</exception>
    /// <exception cref="ShardRoutingException">A multi-aggregate write targets aggregates on different shards.</exception>
    /// <exception cref="NotLeaderException">The target node is not the leader (use the pool for automatic failover).</exception>
    public async Task<WriteResponse> WriteAsync(
        WriteRequest request,
        CancellationToken ct = default)
    {
        var response = await SendRequestAsync(new ClientRequest.Write(request), ct: ct).ConfigureAwait(false);
        return response switch
        {
            ClientResponse.Write w => w.Value,
            _ => throw new ProtocolException($"Unexpected response type {response.GetType().Name} for Write."),
        };
    }

    /// <summary>Write events to a single aggregate. Creates the aggregate if it does not exist.</summary>
    /// <param name="key">The aggregate to write to.</param>
    /// <param name="events">One or more events to append.</param>
    /// <param name="clientId">Client ID scoping client-seq idempotency. Use a stable ID per
    /// logical writer — never a fresh random value per call, or idempotency silently stops working.</param>
    /// <param name="allowCreate">Whether to create the aggregate if it does not exist. Defaults to <c>true</c>.</param>
    /// <param name="expectedVersion">If set, the server rejects the write unless the aggregate's
    /// current max event batch index matches this value (optimistic concurrency control).</param>
    /// <param name="enforceClientIdempotency">When <c>true</c>, the server rejects duplicate writes
    /// that share the same <paramref name="clientId"/> and client event index.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <exception cref="WriteOccException">Optimistic concurrency violation — the aggregate has been modified. Re-read and retry.</exception>
    /// <exception cref="IdempotencyViolationException">Duplicate write — the event was already accepted. No action needed.</exception>
    /// <exception cref="AggregateNotFoundException">The aggregate does not exist and <paramref name="allowCreate"/> is false.</exception>
    /// <exception cref="AggregateRecreateNotAllowedException">The aggregate was permanently deleted.</exception>
    /// <exception cref="SchemaValidationException">An event payload does not conform to the registered schema.</exception>
    /// <exception cref="NotLeaderException">The target node is not the leader (use the pool for automatic failover).</exception>
    public Task<WriteResponse> WriteAsync(
        AggregateKey key,
        AggregateEvent[] events,
        Guid clientId,
        bool allowCreate = true,
        long? expectedVersion = null,
        bool enforceClientIdempotency = false,
        CancellationToken ct = default)
        => WriteAsync(new WriteRequest
        {
            ClientId = clientId,
            Writes = new Dictionary<AggregateKey, SingleAggregateWrite>
            {
                [key] = new SingleAggregateWrite
                {
                    AllowCreate = allowCreate,
                    ExpectedVersion = expectedVersion,
                    EnforceClientIdempotency = enforceClientIdempotency,
                    Events = events,
                }
            }
        }, ct);

    /// <summary>Send a delete request and return the typed response.</summary>
    /// <param name="request">The delete request.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <exception cref="DeleteOccException">Optimistic concurrency violation — the aggregate has been modified. Re-read and retry.</exception>
    /// <exception cref="AggregateNotFoundException">The aggregate does not exist.</exception>
    /// <exception cref="NotLeaderException">The target node is not the leader (use the pool for automatic failover).</exception>
    public async Task<SuccessResponse> DeleteAsync(
        DeleteRequest request,
        CancellationToken ct = default)
    {
        var response = await SendRequestAsync(new ClientRequest.Delete(request), ct: ct).ConfigureAwait(false);
        return response switch
        {
            ClientResponse.Delete d => d.Value,
            _ => throw new ProtocolException($"Unexpected response type {response.GetType().Name} for Delete."),
        };
    }

    /// <summary>Send a trim-start request and return the typed response.</summary>
    /// <param name="request">The trim-start request.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <exception cref="AggregateNotFoundException">The aggregate does not exist.</exception>
    /// <exception cref="TrimIndexOutOfRangeException">The trim index is beyond the aggregate's current range.</exception>
    /// <exception cref="NotLeaderException">The target node is not the leader (use the pool for automatic failover).</exception>
    public async Task<SuccessResponse> TrimStartAsync(
        TrimStartRequest request,
        CancellationToken ct = default)
    {
        var response = await SendRequestAsync(new ClientRequest.TrimStart(request), ct: ct).ConfigureAwait(false);
        return response switch
        {
            ClientResponse.TrimStart t => t.Value,
            _ => throw new ProtocolException($"Unexpected response type {response.GetType().Name} for TrimStart."),
        };
    }

    /// <summary>Send an aggregate details request and return the typed response.</summary>
    /// <param name="request">The aggregate details request.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <exception cref="AggregateNotFoundException">The aggregate does not exist.</exception>
    public async Task<AggregateDetailsResponse> AggregateDetailsAsync(
        AggregateDetailsRequest request,
        CancellationToken ct = default)
    {
        var response = await SendRequestAsync(new ClientRequest.AggregateDetails(request), ct: ct).ConfigureAwait(false);
        return response switch
        {
            ClientResponse.AggregateDetails d => d.Value,
            _ => throw new ProtocolException($"Unexpected response type {response.GetType().Name} for AggregateDetails."),
        };
    }

    /// <summary>Send a register-schema request and return the typed response.</summary>
    /// <param name="request">The register-schema request.</param>
    /// <param name="ct">Cancellation token.</param>
    /// <exception cref="SchemaErrorException">The schema is invalid, unsupported, or already registered.</exception>
    /// <exception cref="NotLeaderException">The target node is not the leader (use the pool for automatic failover).</exception>
    public async Task<SuccessResponse> RegisterSchemaAsync(
        RegisterSchemaRequest request,
        CancellationToken ct = default)
    {
        var response = await SendRequestAsync(new ClientRequest.RegisterSchema(request), ct: ct).ConfigureAwait(false);
        return response switch
        {
            ClientResponse.RegisterSchema s => s.Value,
            _ => throw new ProtocolException($"Unexpected response type {response.GetType().Name} for RegisterSchema."),
        };
    }

    // -------------------------------------------------------------------------
    // Core request/response
    // -------------------------------------------------------------------------

    /// <summary>
    /// Send a request and receive a typed response.
    ///
    /// Variable-size requests (writes, schema registration) are dictionary-compressed
    /// automatically when this connection negotiated a dictionary during Identify and the
    /// payload meets the size threshold; everything else is sent uncompressed.
    ///
    /// Throws for transport/protocol errors. Server-side errors always throw a
    /// <see cref="CeleriantErrorException"/> subclass. <see cref="NotLeaderException"/> and
    /// <see cref="IdentityRequiredException"/> are thrown for their respective error codes.
    /// </summary>
    /// <param name="request">The request discriminated union variant to send.</param>
    /// <param name="ct">Cancellation token.</param>
    public async Task<ClientResponse> SendRequestAsync(
        ClientRequest request,
        CancellationToken ct = default)
    {
        // 1. Map request variant to (messageTypeId, serializable payload, isVariableSize)
        (uint messageTypeId, byte[] serialized, bool isVariableSize) = SerializeRequest(request);

        // 2. Choose compression (dictionary-based, auto) and produce the frame body
        (CompressionType effectiveCompression, byte[] payload, uint uncompressedLength) =
            PrepareRequestBody(request, serialized, isVariableSize);
        uint compressedLength = (uint)payload.Length;

        // 3. Validate against MaxRequestSize
        if (compressedLength > _maxRequestSize)
        {
            throw new ArgumentException(
                $"Request payload ({compressedLength} bytes) exceeds MaxRequestSize ({_maxRequestSize} bytes).");
        }

        // 4. Build WireHeader
        WireHeader header = effectiveCompression == CompressionType.None
            ? WireHeader.ForRequest(messageTypeId, compressedLength)
            : WireHeader.ForCompressedRequest(messageTypeId, compressedLength, uncompressedLength, effectiveCompression);

        using CancellationTokenSource? timeoutCts = BuildTimeoutCts(ct);
        CancellationToken effectiveCt = timeoutCts?.Token ?? ct;

        await _sendLock.WaitAsync(effectiveCt).ConfigureAwait(false);
        try
        {
            // 7-8. Write header + payload
            await SendHeaderAndPayloadAsync(header, payload, effectiveCt).ConfigureAwait(false);

            // 9. Read 17-byte response header into pooled buffer
            byte[] headerBuf = ArrayPool<byte>.Shared.Rent(WireHeader.Size);
            await ReadExactIntoAsync(headerBuf, WireHeader.Size, effectiveCt).ConfigureAwait(false);
            WireHeader responseHeader = WireHeader.ParseFrom(headerBuf);
            ArrayPool<byte>.Shared.Return(headerBuf);

            if (responseHeader.CompressedLength > _maxResponseSize)
                throw new InvalidDataException(
                    $"Response payload {responseHeader.CompressedLength} bytes exceeds maximum allowed size {_maxResponseSize}.");

            // 10. Read response payload into pooled buffer
            int respLen = (int)responseHeader.CompressedLength;
            byte[] responsePayload = ArrayPool<byte>.Shared.Rent(respLen);
            try
            {
                await ReadExactIntoAsync(responsePayload, respLen, effectiveCt).ConfigureAwait(false);

                // 5. Decompress dictionary-compressed responses.
                if ((CompressionType)responseHeader.CompressionType != CompressionType.None)
                {
                    byte[] compressed = responsePayload.AsSpan(0, respLen).ToArray();
                    byte[] decompressed = DecompressResponse(responseHeader, compressed);
                    return DeserializeResponse(responseHeader.MessageType, decompressed);
                }

                // 6. Deserialize and map to ClientResponse
                return DeserializeResponse(responseHeader.MessageType,
                    new ReadOnlyMemory<byte>(responsePayload, 0, respLen));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(responsePayload);
            }
        }
        catch (OperationCanceledException) when (IsTimeoutCancellation(timeoutCts, ct))
        {
            throw new CeleriantTimeoutException("SendRequestAsync timed out.");
        }
        catch (EndOfStreamException ex)
        {
            throw new ConnectionFailedException("Connection closed during request.", ex);
        }
        catch (IOException ex)
        {
            throw new ConnectionFailedException("IO error during request.", ex);
        }
        finally
        {
            _sendLock.Release();
        }
    }

    // -------------------------------------------------------------------------
    // IAsyncDisposable
    // -------------------------------------------------------------------------

    public async ValueTask DisposeAsync()
    {
        await _stream.DisposeAsync().ConfigureAwait(false);
        _tcpClient.Dispose();
        _sendLock.Dispose();
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /// <summary>
    /// Parse "host:port" into (host, port).
    /// </summary>
    private static (string host, int port) ParseAddress(string address)
    {
        int lastColon = address.LastIndexOf(':');
        if (lastColon < 0 || lastColon == address.Length - 1)
            throw new ArgumentException($"Invalid address format '{address}'. Expected 'host:port'.", nameof(address));

        string host = address[..lastColon];
        if (!int.TryParse(address[(lastColon + 1)..], out int port) || port is < 1 or > 65535)
            throw new ArgumentException($"Invalid port in address '{address}'.", nameof(address));

        return (host, port);
    }

    /// <summary>
    /// Write header + payload to the stream in a single call.
    /// TCP_NODELAY ensures the data is sent immediately without Nagle buffering.
    /// </summary>
    private async Task SendHeaderAndPayloadAsync(WireHeader header, byte[] payload, CancellationToken ct)
    {
        int totalLen = WireHeader.Size + payload.Length;
        byte[] combined = ArrayPool<byte>.Shared.Rent(totalLen);
        try
        {
            header.WriteTo(combined);
            Buffer.BlockCopy(payload, 0, combined, WireHeader.Size, payload.Length);
            await _stream.WriteAsync(combined.AsMemory(0, totalLen), ct).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(combined);
        }
    }

    /// <summary>
    /// Read exactly <paramref name="count"/> bytes from the stream into <paramref name="buf"/>.
    /// </summary>
    private async Task ReadExactIntoAsync(byte[] buf, int count, CancellationToken ct)
    {
        int totalRead = 0;
        while (totalRead < count)
        {
            int read = await _stream.ReadAsync(buf.AsMemory(totalRead, count - totalRead), ct).ConfigureAwait(false);
            if (read == 0)
                throw new EndOfStreamException($"Connection closed while reading payload (expected {count} bytes, got {totalRead}).");
            totalRead += read;
        }
    }

    /// <summary>
    /// Synchronous send-request/receive-response for maximum throughput.
    /// No lock, no CTS, no async overhead. Caller must ensure single-threaded access.
    /// </summary>
    public ClientResponse SendRequest(ClientRequest request)
    {
        (uint messageTypeId, byte[] serialized, bool isVariableSize) = SerializeRequest(request);

        (CompressionType effectiveCompression, byte[] payload, uint uncompressedLength) =
            PrepareRequestBody(request, serialized, isVariableSize);
        uint compressedLength = (uint)payload.Length;

        WireHeader header = effectiveCompression == CompressionType.None
            ? WireHeader.ForRequest(messageTypeId, compressedLength)
            : WireHeader.ForCompressedRequest(messageTypeId, compressedLength, uncompressedLength, effectiveCompression);

        try
        {
            // Write header + payload in one call
            int totalLen = WireHeader.Size + payload.Length;
            byte[] combined = ArrayPool<byte>.Shared.Rent(totalLen);
            try
            {
                header.WriteTo(combined);
                Buffer.BlockCopy(payload, 0, combined, WireHeader.Size, payload.Length);
                _stream.Write(combined, 0, totalLen);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(combined);
            }

            // Read 17-byte response header — stackalloc avoids heap allocation
            Span<byte> headerBuf = stackalloc byte[WireHeader.Size];
            ReadExactSync(headerBuf);
            var responseHeader = WireHeader.ParseFrom(headerBuf);

            // Read response payload into pooled buffer
            int respLen = (int)responseHeader.CompressedLength;
            byte[] responsePayload = ArrayPool<byte>.Shared.Rent(respLen);
            try
            {
                ReadExactSync(responsePayload.AsSpan(0, respLen));

                if ((CompressionType)responseHeader.CompressionType != CompressionType.None)
                {
                    // Decompress needs its own byte[] — only happens for compressed responses
                    byte[] compressed = responsePayload.AsSpan(0, respLen).ToArray();
                    byte[] decompressed = DecompressResponse(responseHeader, compressed);
                    return DeserializeResponse(responseHeader.MessageType, decompressed);
                }

                return DeserializeResponse(responseHeader.MessageType,
                    new ReadOnlyMemory<byte>(responsePayload, 0, respLen));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(responsePayload);
            }
        }
        catch (EndOfStreamException ex)
        {
            throw new ConnectionFailedException("Connection closed during request.", ex);
        }
        catch (IOException ex)
        {
            throw new ConnectionFailedException("IO error during request.", ex);
        }
    }

    // -------------------------------------------------------------------------
    // Compression helpers
    // -------------------------------------------------------------------------

    /// <summary>
    /// Choose compression for an outbound request and produce the frame body.
    /// Only variable-size requests are eligible, only when a dictionary has been negotiated
    /// and the request's logical payload meets <see cref="CompressionThresholdBytes"/>.
    /// </summary>
    private (CompressionType compression, byte[] payload, uint uncompressedLength) PrepareRequestBody(
        ClientRequest request, byte[] serialized, bool isVariableSize)
    {
        if (_dict is { } dict && isVariableSize && PayloadBytes(request) >= CompressionThresholdBytes)
        {
            byte[] compressed = WireCodec.CompressWithDict(serialized, dict.Bytes);
            return (CompressionType.ZstdDict, compressed, (uint)serialized.Length);
        }

        return (CompressionType.None, serialized, (uint)serialized.Length);
    }

    /// <summary>
    /// Logical payload size used for the compression threshold decision — the event values
    /// for a write, or the schema text for a schema registration. Mirrors the Rust client.
    /// </summary>
    private static long PayloadBytes(ClientRequest request) => request switch
    {
        ClientRequest.Write w => w.Value.Writes.Values
            .SelectMany(static sw => sw.Events)
            .Sum(static e => (long)(e.EventValue?.Length ?? 0)),
        ClientRequest.RegisterSchema s => Encoding.UTF8.GetByteCount(s.Value.Schema),
        _ => 0,
    };

    /// <summary>
    /// Decompress a response frame according to its compression type.
    /// </summary>
    private byte[] DecompressResponse(WireHeader responseHeader, byte[] compressed)
    {
        var compression = (CompressionType)responseHeader.CompressionType;
        return compression switch
        {
            CompressionType.None => compressed,
            CompressionType.ZstdDict when _dict is { } dict
                => WireCodec.DecompressWithDict(compressed, responseHeader.UncompressedLength, dict.Bytes),
            CompressionType.ZstdDict
                => throw new ProtocolException("Received a ZstdDict-compressed response but no dictionary is cached for this connection."),
            _ => throw new ProtocolException($"Unknown compression type {(byte)compression} in response."),
        };
    }

    private void ReadExactSync(Span<byte> buf)
    {
        int count = buf.Length;
        int totalRead = 0;
        while (totalRead < count)
        {
            int read = _stream.Read(buf[totalRead..]);
            if (read == 0)
                throw new EndOfStreamException($"Connection closed while reading (expected {count} bytes, got {totalRead}).");
            totalRead += read;
        }
    }

    /// <summary>
    /// Map a <see cref="ClientRequest"/> variant to its wire message type ID, serialized bytes,
    /// and whether the message is considered variable-size (eligible for compression).
    /// </summary>
    private static (uint messageTypeId, byte[] serialized, bool isVariableSize) SerializeRequest(ClientRequest request)
    {
        return request switch
        {
            ClientRequest.AggregateDetails r =>
                (MessageTypes.Requests.AggregateDetails, WireCodec.Serialize(r.Value), false),

            ClientRequest.Read r =>
                (MessageTypes.Requests.Read, WireCodec.Serialize(r.Value), false),

            // Write is variable-size
            ClientRequest.Write r =>
                (MessageTypes.Requests.Write, WireCodec.Serialize(r.Value), true),

            ClientRequest.TrimStart r =>
                (MessageTypes.Requests.TrimStart, WireCodec.Serialize(r.Value), false),

            ClientRequest.Delete r =>
                (MessageTypes.Requests.Delete, WireCodec.Serialize(r.Value), false),

            ClientRequest.Watch r =>
                (MessageTypes.Requests.Watch, WireCodec.Serialize(r.Value), false),

            ClientRequest.ListOrgs r =>
                (MessageTypes.Requests.ListOrgs, WireCodec.Serialize(r.Value), false),

            ClientRequest.ListAggregateTypes r =>
                (MessageTypes.Requests.ListAggregateTypes, WireCodec.Serialize(r.Value), false),

            ClientRequest.ListAggregates r =>
                (MessageTypes.Requests.ListAggregates, WireCodec.Serialize(r.Value), false),

            // RegisterSchema is variable-size
            ClientRequest.RegisterSchema r =>
                (MessageTypes.Requests.RegisterSchema, WireCodec.Serialize(r.Value), true),

            ClientRequest.Identify r =>
                (MessageTypes.Requests.Identify, WireCodec.Serialize(r.Value), false),

            _ => throw new ArgumentOutOfRangeException(nameof(request), request, "Unknown ClientRequest variant.")
        };
    }

    /// <summary>
    /// Deserialize a response payload based on the message type ID and return the appropriate
    /// <see cref="ClientResponse"/> variant. Throws <see cref="NotLeaderException"/> or
    /// <see cref="IdentityRequiredException"/> for special error codes.
    /// </summary>
    private static ClientResponse DeserializeResponse(uint messageType, ReadOnlyMemory<byte> payload)
    {
        try
        {
            return messageType switch
            {
                MessageTypes.Responses.AggregateDetails =>
                    new ClientResponse.AggregateDetails(WireCodec.Deserialize<AggregateDetailsResponse>(payload)),

                // Read response is variable-size
                MessageTypes.Responses.Read =>
                    new ClientResponse.Read(WireCodec.Deserialize<ReadResponse>(payload)),

                MessageTypes.Responses.Write =>
                    new ClientResponse.Write(WireCodec.Deserialize<WriteResponse>(payload)),

                MessageTypes.Responses.TrimStart =>
                    new ClientResponse.TrimStart(WireCodec.Deserialize<SuccessResponse>(payload)),

                MessageTypes.Responses.Delete =>
                    new ClientResponse.Delete(WireCodec.Deserialize<SuccessResponse>(payload)),

                MessageTypes.Responses.ProtocolError =>
                    new ClientResponse.ProtocolError(WireCodec.Deserialize<ProtocolErrorResponse>(payload)),

                MessageTypes.Responses.GenericError =>
                    MapErrorResponse(WireCodec.Deserialize<ErrorResponse>(payload)),

                // Watch response is variable-size
                MessageTypes.Responses.Watch =>
                    new ClientResponse.Watch(WireCodec.Deserialize<WatchResponse>(payload)),

                // List responses are variable-size
                MessageTypes.Responses.ListOrgs =>
                    new ClientResponse.ListOrgs(WireCodec.Deserialize<ListOrgsResponse>(payload)),

                MessageTypes.Responses.ListAggregateTypes =>
                    new ClientResponse.ListAggregateTypes(WireCodec.Deserialize<ListAggregateTypesResponse>(payload)),

                MessageTypes.Responses.ListAggregates =>
                    new ClientResponse.ListAggregates(WireCodec.Deserialize<ListAggregatesResponse>(payload)),

                MessageTypes.Responses.RegisterSchema =>
                    new ClientResponse.RegisterSchema(WireCodec.Deserialize<SuccessResponse>(payload)),

                MessageTypes.Responses.Identify =>
                    new ClientResponse.Identify(WireCodec.Deserialize<IdentifyResponse>(payload)),

                _ => throw new ProtocolException($"Unknown response message type: {messageType}.")
            };
        }
        catch (ProtocolException)
        {
            throw;
        }
        catch (CeleriantClientException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new ProtocolException($"Failed to deserialize response with message type {messageType}.", ex);
        }
    }

    /// <summary>
    /// Map a deserialized <see cref="ErrorResponse"/> to a typed exception.
    /// Always throws — never returns.
    /// </summary>
    private static ClientResponse MapErrorResponse(ErrorResponse error)
    {
        if (error.IsNotLeader)
            throw new NotLeaderException(error, error.ParseLeaderAddress());

        if (error.IsIdentityRequired)
            throw new IdentityRequiredException(error);

        if (error.IsServerBusy)
            throw new ServerBusyException(error);

        throw ErrorExceptionFactory.Create(error);
    }

    /// <summary>
    /// Create a timeout <see cref="CancellationTokenSource"/> linked to <paramref name="ct"/>
    /// if a timeout is configured; returns null otherwise.
    /// </summary>
    private CancellationTokenSource? BuildTimeoutCts(CancellationToken ct)
    {
        if (_timeout is null)
            return null;

        var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(_timeout.Value);
        return cts;
    }

    /// <summary>
    /// Returns true if the cancellation was triggered by our timeout CTS (not the caller's token).
    /// </summary>
    private static bool IsTimeoutCancellation(CancellationTokenSource? timeoutCts, CancellationToken callerCt)
        => timeoutCts?.IsCancellationRequested == true && !callerCt.IsCancellationRequested;
}

