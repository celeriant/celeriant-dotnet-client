using System.Text;
using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Transport;

namespace Celeriant.Client;

/// <summary>
/// Low-level single-connection Celeriant client. Owns a single TCP connection; no pooling or
/// auto-reconnect. Caller manages the connection lifecycle.
///
/// <para>
/// The wire framing, zstd-dictionary compression, and Identify handshake live in the shared
/// <see cref="CeleriantConnection"/>; this type adds the V3 (MessagePack) body codec, the typed
/// request/response mapping, and the storage exception taxonomy.
/// </para>
/// </summary>
public sealed class CeleriantClient : ICeleriantClient
{
    private const long DefaultMaxRequestSize = 10_000_000;
    private const long DefaultMaxResponseSize = 64 * 1024 * 1024;

    private readonly CeleriantConnection _conn;

    private CeleriantClient(CeleriantConnection conn) => _conn = conn;

    /// <summary>The compression dictionary negotiated for this connection, if any.</summary>
    internal CachedDict? CurrentDict => _conn.CurrentDict;

    /// <summary>True once a transport/protocol error has indeterminate-framed this connection.</summary>
    public bool IsPoisoned => _conn.IsPoisoned;

    // -------------------------------------------------------------------------
    // Static factory methods
    // -------------------------------------------------------------------------

    /// <summary>Connect to a Celeriant server over plain TCP.</summary>
    public static Task<CeleriantClient> ConnectAsync(string address, CancellationToken ct = default)
        => ConnectAsync(address, connectionTimeout: null, tlsConfig: null, ct);

    /// <summary>Connect to a Celeriant server over TLS.</summary>
    public static Task<CeleriantClient> ConnectTlsAsync(
        string address, ClientTlsConfig tlsConfig, CancellationToken ct = default)
        => ConnectAsync(address, connectionTimeout: null, tlsConfig, ct);

    /// <summary>Connect to a Celeriant server with full options.</summary>
    public static async Task<CeleriantClient> ConnectAsync(
        string address,
        TimeSpan? connectionTimeout = null,
        ClientTlsConfig? tlsConfig = null,
        CancellationToken ct = default)
    {
        var conn = await CeleriantConnection.ConnectAsync(
            address,
            connectionTimeout,
            tlsConfig?.SslOptions,
            StorageConnectionCodec.Instance,
            StorageTransportExceptionFactory.Instance,
            ct).ConfigureAwait(false);

        conn.WithMaxRequestSize(DefaultMaxRequestSize)
            .WithMaxResponseSize(DefaultMaxResponseSize);

        return new CeleriantClient(conn);
    }

    // -------------------------------------------------------------------------
    // Configuration (fluent; mutates the underlying single connection)
    // -------------------------------------------------------------------------

    /// <summary>Set the maximum allowed request payload size in bytes. Default is 10 MB.</summary>
    public CeleriantClient WithMaxRequestSize(long maxRequestSize)
    {
        _conn.WithMaxRequestSize(maxRequestSize);
        return this;
    }

    /// <summary>Set the maximum allowed response payload size in bytes. Default is 64 MB.</summary>
    public CeleriantClient WithMaxResponseSize(long maxResponseSize)
    {
        _conn.WithMaxResponseSize(maxResponseSize);
        return this;
    }

    /// <summary>Set a per-request timeout applied to each request.</summary>
    public CeleriantClient WithTimeout(TimeSpan timeout)
    {
        _conn.WithTimeout(timeout);
        return this;
    }

    // -------------------------------------------------------------------------
    // Identity verification
    // -------------------------------------------------------------------------

    /// <summary>
    /// Perform the Identify handshake with the server. Returns the <see cref="Guid"/> client ID
    /// assigned by the server, or null if the server did not include one.
    /// </summary>
    public Task<Guid?> IdentifyAsync(ClientIdentityConfig identityConfig, CancellationToken ct = default)
        => IdentifyAsync(identityConfig, knownDictSha: null, dictLookup: null, ct);

    /// <summary>
    /// Perform the Identify handshake, advertising a previously cached compression-dictionary sha
    /// so the server can skip re-sending the bytes when they match.
    /// </summary>
    internal Task<Guid?> IdentifyAsync(
        ClientIdentityConfig identityConfig,
        string? knownDictSha,
        Func<string, byte[]?>? dictLookup,
        CancellationToken ct = default)
    {
        var identity = IdentifyParams.ForCredentials(
            identityConfig.ResolveApiKeyBase64(),
            identityConfig.PublicKeyBase64,
            identityConfig.PrivateKeyBase64,
            knownDictSha,
            allowAnonymous: false);

        return _conn.IdentifyAsync(identity, knownDictSha, dictLookup, ct);
    }

    // -------------------------------------------------------------------------
    // Typed convenience methods
    // -------------------------------------------------------------------------

    /// <summary>Send a read request and return the typed response.</summary>
    /// <exception cref="AggregateNotFoundException">The aggregate does not exist.</exception>
    /// <exception cref="BatchIndexUnavailableException">The requested batch index has been trimmed. Re-read from <see cref="BatchIndexUnavailableException.MinimumAvailableVersion"/>.</exception>
    public async Task<ReadResponse> ReadAsync(ReadRequest request, CancellationToken ct = default)
    {
        var response = await SendRequestAsync(new ClientRequest.Read(request), ct).ConfigureAwait(false);
        return response switch
        {
            ClientResponse.Read r => r.Value,
            _ => throw new ProtocolException($"Unexpected response type {response.GetType().Name} for Read."),
        };
    }

    /// <summary>Send a write request and return the typed response.</summary>
    /// <exception cref="WriteOccException">Optimistic concurrency violation: re-read and retry.</exception>
    /// <exception cref="IdempotencyViolationException">Duplicate write: already accepted. No action needed.</exception>
    /// <exception cref="AggregateNotFoundException">The aggregate does not exist and <c>AllowCreate</c> is false.</exception>
    /// <exception cref="AggregateRecreateNotAllowedException">The aggregate was permanently deleted.</exception>
    /// <exception cref="SchemaValidationException">An event payload does not conform to the registered schema.</exception>
    /// <exception cref="ShardRoutingException">A multi-aggregate write targets aggregates on different shards.</exception>
    /// <exception cref="NotLeaderException">The target node is not the leader (use the pool for automatic failover).</exception>
    public async Task<WriteResponse> WriteAsync(WriteRequest request, CancellationToken ct = default)
    {
        var response = await SendRequestAsync(new ClientRequest.Write(request), ct).ConfigureAwait(false);
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
    /// logical writer: never a fresh random value per call, or idempotency silently stops working.</param>
    /// <param name="allowCreate">Whether to create the aggregate if it does not exist. Defaults to <c>true</c>.</param>
    /// <param name="expectedVersion">If set, the server rejects the write unless the aggregate's
    /// current max event batch index matches this value (optimistic concurrency control).</param>
    /// <param name="enforceClientIdempotency">When <c>true</c>, the server rejects duplicate writes
    /// that share the same <paramref name="clientId"/> and client event index.</param>
    /// <param name="ct">Cancellation token.</param>
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
    /// <exception cref="DeleteOccException">Optimistic concurrency violation: re-read and retry.</exception>
    /// <exception cref="AggregateNotFoundException">The aggregate does not exist.</exception>
    /// <exception cref="NotLeaderException">The target node is not the leader (use the pool for automatic failover).</exception>
    public async Task<SuccessResponse> DeleteAsync(DeleteRequest request, CancellationToken ct = default)
    {
        var response = await SendRequestAsync(new ClientRequest.Delete(request), ct).ConfigureAwait(false);
        return response switch
        {
            ClientResponse.Delete d => d.Value,
            _ => throw new ProtocolException($"Unexpected response type {response.GetType().Name} for Delete."),
        };
    }

    /// <summary>Send a trim-start request and return the typed response.</summary>
    /// <exception cref="AggregateNotFoundException">The aggregate does not exist.</exception>
    /// <exception cref="TrimIndexOutOfRangeException">The trim index is beyond the aggregate's current range.</exception>
    /// <exception cref="NotLeaderException">The target node is not the leader (use the pool for automatic failover).</exception>
    public async Task<SuccessResponse> TrimStartAsync(TrimStartRequest request, CancellationToken ct = default)
    {
        var response = await SendRequestAsync(new ClientRequest.TrimStart(request), ct).ConfigureAwait(false);
        return response switch
        {
            ClientResponse.TrimStart t => t.Value,
            _ => throw new ProtocolException($"Unexpected response type {response.GetType().Name} for TrimStart."),
        };
    }

    /// <summary>Send an aggregate details request and return the typed response.</summary>
    /// <exception cref="AggregateNotFoundException">The aggregate does not exist.</exception>
    public async Task<AggregateDetailsResponse> AggregateDetailsAsync(AggregateDetailsRequest request, CancellationToken ct = default)
    {
        var response = await SendRequestAsync(new ClientRequest.AggregateDetails(request), ct).ConfigureAwait(false);
        return response switch
        {
            ClientResponse.AggregateDetails d => d.Value,
            _ => throw new ProtocolException($"Unexpected response type {response.GetType().Name} for AggregateDetails."),
        };
    }

    /// <summary>Send a register-schema request and return the typed response.</summary>
    /// <exception cref="SchemaErrorException">The schema is invalid, unsupported, or already registered.</exception>
    /// <exception cref="NotLeaderException">The target node is not the leader (use the pool for automatic failover).</exception>
    public async Task<SuccessResponse> RegisterSchemaAsync(RegisterSchemaRequest request, CancellationToken ct = default)
    {
        var response = await SendRequestAsync(new ClientRequest.RegisterSchema(request), ct).ConfigureAwait(false);
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
    /// Send a request and receive a typed response. Variable-size requests (writes, schema
    /// registration) are dictionary-compressed automatically when this connection negotiated a
    /// dictionary during Identify and the payload meets the threshold.
    /// </summary>
    public async Task<ClientResponse> SendRequestAsync(ClientRequest request, CancellationToken ct = default)
    {
        (uint messageTypeId, byte[] serialized, bool isVariableSize) = SerializeRequest(request);
        RawFrame frame = await _conn.SendAsync(messageTypeId, serialized, isVariableSize, PayloadBytes(request), ct)
            .ConfigureAwait(false);
        return DeserializeResponse(frame.MessageType, frame.Body);
    }

    /// <summary>
    /// Synchronous send-request/receive-response for maximum throughput. Caller must ensure
    /// single-threaded access to this connection.
    /// </summary>
    public ClientResponse SendRequest(ClientRequest request)
    {
        (uint messageTypeId, byte[] serialized, bool isVariableSize) = SerializeRequest(request);
        RawFrame frame = _conn.SendRequest(messageTypeId, serialized, isVariableSize, PayloadBytes(request));
        return DeserializeResponse(frame.MessageType, frame.Body);
    }

    /// <summary>
    /// Read a single server-pushed response without sending a request. Used by watch connections,
    /// where the server streams responses after the initial subscription.
    /// </summary>
    internal async Task<ClientResponse> ReadResponseAsync(CancellationToken ct = default)
    {
        RawFrame frame = await _conn.ReadFrameAsync(ct).ConfigureAwait(false);
        return DeserializeResponse(frame.MessageType, frame.Body);
    }

    public ValueTask DisposeAsync() => _conn.DisposeAsync();

    // -------------------------------------------------------------------------
    // Request/response (de)serialization
    // -------------------------------------------------------------------------

    /// <summary>
    /// Map a <see cref="ClientRequest"/> variant to its wire message type ID, serialized bytes,
    /// and whether the message is variable-size (eligible for compression).
    /// </summary>
    private static (uint messageTypeId, byte[] serialized, bool isVariableSize) SerializeRequest(ClientRequest request)
        => request switch
        {
            ClientRequest.AggregateDetails r => (MessageTypes.Requests.AggregateDetails, WireCodec.Serialize(r.Value), false),
            ClientRequest.Read r => (MessageTypes.Requests.Read, WireCodec.Serialize(r.Value), false),
            ClientRequest.Write r => (MessageTypes.Requests.Write, WireCodec.Serialize(r.Value), true),
            ClientRequest.TrimStart r => (MessageTypes.Requests.TrimStart, WireCodec.Serialize(r.Value), false),
            ClientRequest.Delete r => (MessageTypes.Requests.Delete, WireCodec.Serialize(r.Value), false),
            ClientRequest.Watch r => (MessageTypes.Requests.Watch, WireCodec.Serialize(r.Value), false),
            ClientRequest.ListOrgs r => (MessageTypes.Requests.ListOrgs, WireCodec.Serialize(r.Value), false),
            ClientRequest.ListAggregateTypes r => (MessageTypes.Requests.ListAggregateTypes, WireCodec.Serialize(r.Value), false),
            ClientRequest.ListAggregates r => (MessageTypes.Requests.ListAggregates, WireCodec.Serialize(r.Value), false),
            ClientRequest.RegisterSchema r => (MessageTypes.Requests.RegisterSchema, WireCodec.Serialize(r.Value), true),
            ClientRequest.Identify r => (MessageTypes.Requests.Identify, WireCodec.Serialize(r.Value), false),
            _ => throw new ArgumentOutOfRangeException(nameof(request), request, "Unknown ClientRequest variant."),
        };

    /// <summary>
    /// Logical payload size used for the compression threshold decision: the event values for a
    /// write, or the schema text for a schema registration.
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
    /// Deserialize a response payload based on the message type ID and return the appropriate
    /// <see cref="ClientResponse"/> variant. Throws typed exceptions for error frames.
    /// </summary>
    private static ClientResponse DeserializeResponse(uint messageType, ReadOnlyMemory<byte> payload)
    {
        try
        {
            return messageType switch
            {
                MessageTypes.Responses.AggregateDetails => new ClientResponse.AggregateDetails(WireCodec.Deserialize<AggregateDetailsResponse>(payload)),
                MessageTypes.Responses.Read => new ClientResponse.Read(WireCodec.Deserialize<ReadResponse>(payload)),
                MessageTypes.Responses.Write => new ClientResponse.Write(WireCodec.Deserialize<WriteResponse>(payload)),
                MessageTypes.Responses.TrimStart => new ClientResponse.TrimStart(WireCodec.Deserialize<SuccessResponse>(payload)),
                MessageTypes.Responses.Delete => new ClientResponse.Delete(WireCodec.Deserialize<SuccessResponse>(payload)),
                MessageTypes.Responses.ProtocolError => new ClientResponse.ProtocolError(WireCodec.Deserialize<ProtocolErrorResponse>(payload)),
                MessageTypes.Responses.GenericError => throw CreateException(WireCodec.Deserialize<ErrorResponse>(payload)),
                MessageTypes.Responses.Watch => new ClientResponse.Watch(WireCodec.Deserialize<WatchResponse>(payload)),
                MessageTypes.Responses.ListOrgs => new ClientResponse.ListOrgs(WireCodec.Deserialize<ListOrgsResponse>(payload)),
                MessageTypes.Responses.ListAggregateTypes => new ClientResponse.ListAggregateTypes(WireCodec.Deserialize<ListAggregateTypesResponse>(payload)),
                MessageTypes.Responses.ListAggregates => new ClientResponse.ListAggregates(WireCodec.Deserialize<ListAggregatesResponse>(payload)),
                MessageTypes.Responses.RegisterSchema => new ClientResponse.RegisterSchema(WireCodec.Deserialize<SuccessResponse>(payload)),
                MessageTypes.Responses.Identify => new ClientResponse.Identify(WireCodec.Deserialize<IdentifyResponse>(payload)),
                _ => throw new ProtocolException($"Unknown response message type: {messageType}."),
            };
        }
        catch (ProtocolException) { throw; }
        catch (CeleriantClientException) { throw; }
        catch (Exception ex)
        {
            throw new ProtocolException($"Failed to deserialize response with message type {messageType}.", ex);
        }
    }

    /// <summary>
    /// Map a deserialized <see cref="ErrorResponse"/> to the typed exception it should raise.
    /// Special-cases NotLeader (carries the leader address for pool failover), IdentityRequired,
    /// and ServerBusy; everything else goes through <see cref="ErrorExceptionFactory"/>.
    /// </summary>
    internal static Exception CreateException(ErrorResponse error)
    {
        if (error.IsNotLeader)
            return new NotLeaderException(error, error.ParseLeaderAddress());
        if (error.IsIdentityRequired)
            return new IdentityRequiredException(error);
        if (error.IsServerBusy)
            return new ServerBusyException(error);
        return ErrorExceptionFactory.Create(error);
    }
}
