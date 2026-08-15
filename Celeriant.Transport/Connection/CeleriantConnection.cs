using System.Buffers;
using System.Net.Security;
using System.Net.Sockets;

namespace Celeriant.Transport;

/// <summary>
/// Low-level single-connection transport shared by every Celeriant .NET client. Owns one TCP
/// (optionally TLS) connection; no pooling or auto-reconnect — a pool layer manages lifecycle and
/// leader failover. Handles the 17-byte wire framing, zstd-dictionary compression, and the
/// celeriant_msg Identify handshake. The body codec and message-type ids come from the injected
/// <see cref="IConnectionCodec"/>; transport failures are surfaced through the injected
/// <see cref="ITransportExceptionFactory"/> so each product keeps its own exception types.
///
/// <para>
/// Not thread-safe for concurrent requests on the same instance (a send lock serializes the
/// frame/read cycle, but a connection is meant to serve one logical unit of work at a time). On any
/// transport or protocol error the connection is poisoned (stream framing is indeterminate) and
/// must be discarded.
/// </para>
/// </summary>
public sealed class CeleriantConnection : IAsyncDisposable
{
    private readonly TcpClient _tcpClient;
    private readonly Stream _stream;
    private readonly IConnectionCodec _codec;
    private readonly ITransportExceptionFactory _ex;

    // Serializes the send/read cycle against concurrent callers on the same connection.
    private readonly SemaphoreSlim _sendLock = new(1, 1);

    private long _maxRequestSize = 16 * 1024 * 1024;
    private long _maxResponseSize = 64 * 1024 * 1024;
    private int _compressionThreshold = 1024;
    private TimeSpan? _timeout;
    private bool _poisoned;

    // Compression dictionary negotiated during Identify; null until then, or when the cluster isn't
    // using ZstdDict. Drives request compression and ZstdDict response decompression.
    private CachedDict? _dict;

    /// <summary>The compression dictionary negotiated for this connection, if any.</summary>
    public CachedDict? CurrentDict => _dict;

    /// <summary>True once a transport/protocol error has indeterminate-framed this connection.</summary>
    public bool IsPoisoned => _poisoned;

    private CeleriantConnection(
        TcpClient tcpClient, Stream stream, IConnectionCodec codec, ITransportExceptionFactory ex)
    {
        _tcpClient = tcpClient;
        _stream = stream;
        _codec = codec;
        _ex = ex;
    }

    // -------------------------------------------------------------------------
    // Connect
    // -------------------------------------------------------------------------

    /// <summary>Connect over plain TCP or TLS (when <paramref name="sslOptions"/> is non-null).</summary>
    public static async Task<CeleriantConnection> ConnectAsync(
        string address,
        TimeSpan? connectionTimeout,
        SslClientAuthenticationOptions? sslOptions,
        IConnectionCodec codec,
        ITransportExceptionFactory ex,
        CancellationToken ct = default)
    {
        (string host, int port) = ParseAddress(address);

        using CancellationTokenSource? timeoutCts =
            connectionTimeout.HasValue ? new CancellationTokenSource(connectionTimeout.Value) : null;
        using CancellationTokenSource? linkedCts = timeoutCts is not null
            ? CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token)
            : null;
        CancellationToken connectCt = linkedCts?.Token ?? ct;

        var tcpClient = new TcpClient { NoDelay = true };
        try
        {
            try
            {
                await tcpClient.ConnectAsync(host, port, connectCt).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (timeoutCts?.IsCancellationRequested == true)
            {
                throw ex.Timeout($"Connection to {address} timed out after {connectionTimeout}.");
            }
            catch (Exception inner) when (inner is SocketException or IOException)
            {
                throw ex.ConnectionFailed($"Failed to connect to {address}: {inner.Message}", inner);
            }

            Stream stream = tcpClient.GetStream();
            if (sslOptions is not null)
            {
                var sslStream = new SslStream(stream, leaveInnerStreamOpen: false);
                try
                {
                    await sslStream.AuthenticateAsClientAsync(sslOptions, connectCt).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (timeoutCts?.IsCancellationRequested == true)
                {
                    await sslStream.DisposeAsync().ConfigureAwait(false);
                    throw ex.Timeout($"TLS handshake with {address} timed out after {connectionTimeout}.");
                }
                catch (Exception inner) when (inner is IOException or System.Security.Authentication.AuthenticationException)
                {
                    await sslStream.DisposeAsync().ConfigureAwait(false);
                    throw ex.ConnectionFailed($"TLS handshake with {address} failed: {inner.Message}", inner);
                }
                stream = sslStream;
            }

            return new CeleriantConnection(tcpClient, stream, codec, ex);
        }
        catch
        {
            tcpClient.Dispose();
            throw;
        }
    }

    // -------------------------------------------------------------------------
    // Fluent config
    // -------------------------------------------------------------------------

    public CeleriantConnection WithMaxRequestSize(long bytes) { _maxRequestSize = bytes; return this; }
    public CeleriantConnection WithMaxResponseSize(long bytes) { _maxResponseSize = bytes; return this; }
    public CeleriantConnection WithTimeout(TimeSpan timeout) { _timeout = timeout; return this; }
    public CeleriantConnection WithCompressionThreshold(int bytes) { _compressionThreshold = bytes; return this; }

    // -------------------------------------------------------------------------
    // Identify (celeriant_msg type 14)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Perform the Identify handshake. Negotiates the compression dictionary (advertising
    /// <paramref name="knownDictSha"/> so the server can skip re-sending bytes it already gave us,
    /// resolving a confirmed-but-unsent sha via <paramref name="dictLookup"/>) and returns the
    /// server-assigned client id, if any. Identify is always sent uncompressed.
    /// </summary>
    public async Task<Guid?> IdentifyAsync(
        IdentifyParams identity,
        string? knownDictSha = null,
        Func<string, byte[]?>? dictLookup = null,
        CancellationToken ct = default)
    {
        byte[] payload = _codec.EncodeIdentify(identity with { KnownDictSha256 = knownDictSha });
        var header = WireHeader.ForRequest(_codec.ProtocolVersion, _codec.IdentifyRequestType, (uint)payload.Length);

        using CancellationTokenSource? timeoutCts = BuildTimeoutCts(ct);
        CancellationToken effectiveCt = timeoutCts?.Token ?? ct;

        await _sendLock.WaitAsync(effectiveCt).ConfigureAwait(false);
        try
        {
            await SendHeaderAndPayloadAsync(header, payload, effectiveCt).ConfigureAwait(false);
            (uint respType, byte[] body) = await ReadFrameCoreAsync(effectiveCt).ConfigureAwait(false);

            if (respType != _codec.IdentifyResponseType)
            {
                // A failed Identify arrives as a product error frame, not the success type.
                throw _codec.TryMapErrorFrame(respType, body)
                    ?? Poison(_ex.Protocol(
                        $"Expected Identify response (type {_codec.IdentifyResponseType}), got {respType}."));
            }

            IdentifyResult result;
            try
            {
                result = _codec.DecodeIdentify(body);
            }
            catch (Exception inner)
            {
                throw Poison(_ex.Protocol("Failed to deserialize IdentifyResponse.", inner));
            }

            // sha + bytes  → server shipped a new/refreshed dictionary; store it.
            // sha only     → server confirmed our advertised sha; resolve bytes from the pool cache.
            // no sha       → cluster is not using ZstdDict; no dictionary.
            _dict = (result.DictSha, result.DictBytes) switch
            {
                (string sha, byte[] bytes) => new CachedDict(sha, bytes),
                (string sha, null) => dictLookup?.Invoke(sha) is { } cached ? new CachedDict(sha, cached) : null,
                (null, _) => null,
            };

            return result.ClientId;
        }
        catch (OperationCanceledException) when (IsTimeoutCancellation(timeoutCts, ct))
        {
            throw Poison(_ex.Timeout("IdentifyAsync timed out."));
        }
        catch (EndOfStreamException inner)
        {
            throw Poison(_ex.ConnectionFailed("Connection closed during IdentifyAsync.", inner));
        }
        catch (IOException inner)
        {
            throw Poison(_ex.ConnectionFailed("IO error during IdentifyAsync.", inner));
        }
        finally
        {
            _sendLock.Release();
        }
    }

    // -------------------------------------------------------------------------
    // Request / response
    // -------------------------------------------------------------------------

    /// <summary>
    /// Send a request frame and return the raw response frame (decompressed). The caller supplies
    /// the serialized body; <paramref name="compressible"/> + <paramref name="logicalPayloadBytes"/>
    /// drive dictionary compression (only when a dictionary was negotiated and the logical payload
    /// meets the threshold). Mapping the response type/body to a typed result is the caller's job.
    /// </summary>
    public async Task<RawFrame> SendAsync(
        uint requestType,
        byte[] serializedBody,
        bool compressible,
        long logicalPayloadBytes,
        CancellationToken ct = default)
    {
        if (_poisoned)
            throw _ex.ConnectionFailed("Connection is poisoned; discard and reconnect.");

        WireHeader header = BuildRequestHeader(requestType, serializedBody, compressible, logicalPayloadBytes, out byte[] body);

        using CancellationTokenSource? timeoutCts = BuildTimeoutCts(ct);
        CancellationToken effectiveCt = timeoutCts?.Token ?? ct;

        await _sendLock.WaitAsync(effectiveCt).ConfigureAwait(false);
        try
        {
            await SendHeaderAndPayloadAsync(header, body, effectiveCt).ConfigureAwait(false);
            (uint respType, byte[] respBody) = await ReadFrameCoreAsync(effectiveCt).ConfigureAwait(false);
            return new RawFrame(respType, respBody);
        }
        catch (OperationCanceledException) when (IsTimeoutCancellation(timeoutCts, ct))
        {
            throw Poison(_ex.Timeout("Request timed out."));
        }
        catch (EndOfStreamException inner)
        {
            throw Poison(_ex.ConnectionFailed("Connection closed during request.", inner));
        }
        catch (IOException inner)
        {
            throw Poison(_ex.ConnectionFailed("IO error during request.", inner));
        }
        finally
        {
            _sendLock.Release();
        }
    }

    /// <summary>
    /// Synchronous send/receive for maximum throughput: no lock, no CTS, no async overhead. The
    /// caller must guarantee single-threaded access to this connection.
    /// </summary>
    public RawFrame SendRequest(
        uint requestType,
        byte[] serializedBody,
        bool compressible,
        long logicalPayloadBytes)
    {
        if (_poisoned)
            throw _ex.ConnectionFailed("Connection is poisoned; discard and reconnect.");

        WireHeader header = BuildRequestHeader(requestType, serializedBody, compressible, logicalPayloadBytes, out byte[] body);

        try
        {
            int totalLen = WireHeader.Size + body.Length;
            byte[] combined = ArrayPool<byte>.Shared.Rent(totalLen);
            try
            {
                header.WriteTo(combined);
                Buffer.BlockCopy(body, 0, combined, WireHeader.Size, body.Length);
                _stream.Write(combined, 0, totalLen);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(combined);
            }

            Span<byte> headerBuf = stackalloc byte[WireHeader.Size];
            ReadExactSync(headerBuf);
            var responseHeader = WireHeader.ParseFrom(headerBuf);
            return ReadBodySync(responseHeader);
        }
        catch (EndOfStreamException inner)
        {
            throw Poison(_ex.ConnectionFailed("Connection closed during request.", inner));
        }
        catch (IOException inner)
        {
            throw Poison(_ex.ConnectionFailed("IO error during request.", inner));
        }
    }

    /// <summary>
    /// Read a single server-pushed frame without sending a request — used by watch connections,
    /// where the server streams frames after the initial subscription.
    /// </summary>
    public async Task<RawFrame> ReadFrameAsync(CancellationToken ct = default)
    {
        try
        {
            (uint respType, byte[] body) = await ReadFrameCoreAsync(ct).ConfigureAwait(false);
            return new RawFrame(respType, body);
        }
        catch (EndOfStreamException inner)
        {
            throw Poison(_ex.ConnectionFailed("Connection closed while reading frame.", inner));
        }
        catch (IOException inner)
        {
            throw Poison(_ex.ConnectionFailed("IO error while reading frame.", inner));
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
    // Framing helpers
    // -------------------------------------------------------------------------

    /// <summary>
    /// Choose compression for an outbound request, produce the frame body, and build its header.
    /// Compression applies only when a dictionary is negotiated, the request is
    /// <paramref name="compressible"/>, and <paramref name="logicalPayloadBytes"/> meets the threshold.
    /// </summary>
    private WireHeader BuildRequestHeader(
        uint requestType, byte[] serialized, bool compressible, long logicalPayloadBytes, out byte[] body)
    {
        CompressionType compression = CompressionType.None;
        body = serialized;
        uint uncompressedLength = (uint)serialized.Length;

        if (_dict is { } dict && compressible && logicalPayloadBytes >= _compressionThreshold)
        {
            body = DictCompression.CompressWithDict(serialized, dict.Bytes);
            compression = CompressionType.ZstdDict;
        }

        uint compressedLength = (uint)body.Length;
        if (compressedLength > _maxRequestSize)
            throw new ArgumentException(
                $"Request payload ({compressedLength} bytes) exceeds MaxRequestSize ({_maxRequestSize} bytes).");

        return compression == CompressionType.None
            ? WireHeader.ForRequest(_codec.ProtocolVersion, requestType, compressedLength)
            : WireHeader.ForCompressedRequest(_codec.ProtocolVersion, requestType, compressedLength, uncompressedLength, compression);
    }

    private async Task<(uint respType, byte[] body)> ReadFrameCoreAsync(CancellationToken ct)
    {
        byte[] headerBuf = ArrayPool<byte>.Shared.Rent(WireHeader.Size);
        WireHeader responseHeader;
        try
        {
            await ReadExactIntoAsync(headerBuf, WireHeader.Size, ct).ConfigureAwait(false);
            responseHeader = WireHeader.ParseFrom(headerBuf);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(headerBuf);
        }

        if (responseHeader.CompressedLength > _maxResponseSize)
            throw Poison(_ex.Protocol(
                $"Response payload {responseHeader.CompressedLength} bytes exceeds maximum allowed size {_maxResponseSize}."));

        int respLen = (int)responseHeader.CompressedLength;
        byte[] responsePayload = new byte[respLen];
        await ReadExactIntoAsync(responsePayload, respLen, ct).ConfigureAwait(false);

        return (responseHeader.MessageType, Decompress(responseHeader, responsePayload));
    }

    private RawFrame ReadBodySync(WireHeader responseHeader)
    {
        if (responseHeader.CompressedLength > _maxResponseSize)
            throw Poison(_ex.Protocol(
                $"Response payload {responseHeader.CompressedLength} bytes exceeds maximum allowed size {_maxResponseSize}."));

        int respLen = (int)responseHeader.CompressedLength;
        byte[] responsePayload = ArrayPool<byte>.Shared.Rent(respLen);
        try
        {
            ReadExactSync(responsePayload.AsSpan(0, respLen));
            // Decompress needs an exact-length buffer; copy out of the pooled rental.
            byte[] exact = responsePayload.AsSpan(0, respLen).ToArray();
            return new RawFrame(responseHeader.MessageType, Decompress(responseHeader, exact));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(responsePayload);
        }
    }

    private byte[] Decompress(WireHeader header, byte[] payload)
        => (CompressionType)header.CompressionType switch
        {
            CompressionType.None => payload,
            CompressionType.ZstdDict when _dict is { } d =>
                DictCompression.DecompressWithDict(payload, header.UncompressedLength, d.Bytes),
            CompressionType.ZstdDict =>
                throw Poison(_ex.Protocol("Received a ZstdDict-compressed response but no dictionary is cached for this connection.")),
            _ => throw Poison(_ex.Protocol($"Unknown compression type {header.CompressionType} in response.")),
        };

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

    private async Task ReadExactIntoAsync(byte[] buf, int count, CancellationToken ct)
    {
        int totalRead = 0;
        while (totalRead < count)
        {
            int read = await _stream.ReadAsync(buf.AsMemory(totalRead, count - totalRead), ct).ConfigureAwait(false);
            if (read == 0)
                throw new EndOfStreamException($"Connection closed while reading (expected {count} bytes, got {totalRead}).");
            totalRead += read;
        }
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

    // -------------------------------------------------------------------------
    // Misc helpers
    // -------------------------------------------------------------------------

    private TException Poison<TException>(TException ex) where TException : Exception
    {
        _poisoned = true;
        return ex;
    }

    private CancellationTokenSource? BuildTimeoutCts(CancellationToken ct)
    {
        if (_timeout is null)
            return null;
        var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(_timeout.Value);
        return cts;
    }

    private static bool IsTimeoutCancellation(CancellationTokenSource? timeoutCts, CancellationToken callerCt)
        => timeoutCts?.IsCancellationRequested == true && !callerCt.IsCancellationRequested;

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
}
