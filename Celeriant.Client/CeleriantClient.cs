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
/// <remarks>
/// <see cref="DisposeAsync"/> closes the connection. Calling any request method afterwards throws
/// <see cref="ObjectDisposedException"/>; a second <see cref="DisposeAsync"/> is a no-op.
/// </remarks>
public sealed class CeleriantClient : ICeleriantClient
{
    private const long DefaultMaxRequestSize = 10_000_000;
    private const long DefaultMaxResponseSize = 64 * 1024 * 1024;

    private readonly CeleriantConnection _conn;
    private bool _disposed;

    private CeleriantClient(CeleriantConnection conn) => _conn = conn;

    /// <summary>The compression dictionary negotiated for this connection, if any.</summary>
    internal CachedDict? CurrentDict => _conn.CurrentDict;

    /// <summary>True once an error has left this connection's framing indeterminate; discard it.</summary>
    public bool IsPoisoned => _conn.IsPoisoned;

    /// <summary>
    /// True when a read was abandoned part-way through a frame, leaving the stream mid-message.
    /// </summary>
    public bool IsMidFrame => _conn.IsMidFrame;

    /// <summary>
    /// True when this idle connection must not be handed out again: the peer closed it, or left
    /// bytes on it. A non-blocking peek the pool runs before reusing an idle connection.
    /// See <see cref="Celeriant.Transport.CeleriantConnection.IsUnfitForReuse"/>.
    /// </summary>
    public bool IsUnfitForReuse => _conn.IsUnfitForReuse;

    // -------------------------------------------------------------------------
    // Static factory methods
    // -------------------------------------------------------------------------

    /// <summary>Connect to a Celeriant server over TLS.</summary>
    /// <exception cref="ArgumentException"><paramref name="address"/> is not "host:port", or the port is out of range.</exception>
    /// <exception cref="ConnectionFailedException">The server could not be reached, or the TLS handshake failed.</exception>
    /// <exception cref="ConnectionTimeoutException">The connection or handshake did not complete within the timeout.</exception>
    public static Task<CeleriantClient> ConnectTlsAsync(
        string address, ClientTlsConfig tlsConfig, CancellationToken ct = default)
        => ConnectAsync(address, connectionTimeout: null, tlsConfig, ct);

    /// <summary>Connect to a Celeriant server. Plain TCP when <paramref name="tlsConfig"/> is null,
    /// TLS otherwise. This is the single connect overload; the optional parameters cover every case.</summary>
    /// <exception cref="ArgumentException"><paramref name="address"/> is not "host:port", or the port is out of range.</exception>
    /// <exception cref="ConnectionFailedException">The server could not be reached, or the TLS handshake failed.</exception>
    /// <exception cref="ConnectionTimeoutException">The connection did not complete within <paramref name="connectionTimeout"/>.</exception>
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

    /// <summary>Set the maximum allowed request payload size in bytes. Default is 10 MB.
    /// Mutates this client and returns it for chaining; it is not a copy.</summary>
    public CeleriantClient WithMaxRequestSize(long maxRequestSize)
    {
        _conn.WithMaxRequestSize(maxRequestSize);
        return this;
    }

    /// <summary>Set the maximum allowed response payload size in bytes. Default is 64 MB. Bounds a
    /// single wire page; a response page exceeding it throws <see cref="ProtocolException"/>, so keep
    /// it at or above the server's response page size. Mutates this client and returns it for
    /// chaining; it is not a copy.</summary>
    public CeleriantClient WithMaxResponseSize(long maxResponseSize)
    {
        _conn.WithMaxResponseSize(maxResponseSize);
        return this;
    }

    /// <summary>Set a per-request timeout applied to each request.
    /// Mutates this client and returns it for chaining; it is not a copy.</summary>
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
        ObjectDisposedException.ThrowIf(_disposed, this);

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
    /// <returns>A <see cref="WriteResponse"/> whose <see cref="WriteResponse.MaxAggregateVersion"/> is
    /// the aggregate's new version — for a single-aggregate write; <c>null</c> for a multi-aggregate one.</returns>
    /// <exception cref="WriteOccException">Optimistic concurrency violation: re-read and retry.</exception>
    /// <exception cref="IdempotencyViolationException">The client seq was already accepted. Safe to ignore only when retrying the identical write; otherwise the new event was rejected and not stored — read the seq back and compare EventId. See the exception's own docs.</exception>
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
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Without an id on the wire there is nothing to check the response against.
        Guid? sent = CorrelationIdOf(request);
        if (sent is null)
        {
            sent = Guid.NewGuid();
            request = WithCorrelationId(request, sent.Value);
        }

        (uint messageTypeId, byte[] serialized, bool isVariableSize) = SerializeRequest(request);
        RawFrame frame = await _conn.SendAsync(messageTypeId, serialized, isVariableSize, PayloadBytes(request), ct)
            .ConfigureAwait(false);

        ClientResponse response = DecodeFrame(frame.MessageType, frame.Body);
        VerifyCorrelation(response, sent);
        VerifyResponseType(messageTypeId, frame.MessageType);
        return ThrowIfError(response);
    }

    /// <summary>
    /// Bind the response to the request before it is interpreted. Detecting the mismatch and
    /// then leaving the connection usable would be worse than not detecting it, so this poisons
    /// the connection: the frame just read belonged to someone else, meaning this stream is a
    /// reply behind and everything after it would be too.
    /// </summary>
    private void VerifyCorrelation(ClientResponse response, Guid? sent)
    {
        if (!CarriesCorrelationId(response))
            return;

        // A null echo is a mismatch, not a "cannot verify". The case that would justify passing
        // it through — the server erroring before it decoded the request body, with no id to echo
        // back — cannot reach here: the protocol answers that with ProtocolError, which
        // CarriesCorrelationId already exempts. A GenericError is only produced once the request
        // decoded, so the id was in hand. The Rust client compares the same way.
        Guid? received = CorrelationIdOf(response);
        if (received == sent)
            return;

        _conn.PoisonForCorrelationMismatch();
        throw new ProtocolException(
            $"Correlation id mismatch: sent {sent?.ToString() ?? "null"}, received " +
            $"{received?.ToString() ?? "null"} — the connection returned another request's response.");
    }

    /// <summary>
    /// Synchronous send-request/receive-response for maximum throughput. Caller must ensure
    /// single-threaded access to this connection.
    /// </summary>
    public ClientResponse SendRequest(ClientRequest request)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Identical binding to the async path. The synchronous send cannot itself be
        // cancelled, but it shares the connection with one that can, so it inherits a
        // dirty stream and must not read someone else's reply off it.
        Guid? sent = CorrelationIdOf(request);
        if (sent is null)
        {
            sent = Guid.NewGuid();
            request = WithCorrelationId(request, sent.Value);
        }

        (uint messageTypeId, byte[] serialized, bool isVariableSize) = SerializeRequest(request);
        RawFrame frame = _conn.SendRequest(messageTypeId, serialized, isVariableSize, PayloadBytes(request));

        ClientResponse response = DecodeFrame(frame.MessageType, frame.Body);
        VerifyCorrelation(response, sent);
        VerifyResponseType(messageTypeId, frame.MessageType);
        return ThrowIfError(response);
    }

    /// <summary>
    /// Read a single server-pushed response without sending a request. Used by watch connections,
    /// where the server streams responses after the initial subscription.
    /// </summary>
    internal async Task<ClientResponse> ReadResponseAsync(CancellationToken ct = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        RawFrame frame = await _conn.ReadFrameAsync(ct).ConfigureAwait(false);
        return DeserializeResponse(frame.MessageType, frame.Body);
    }

    public ValueTask DisposeAsync()
    {
        _disposed = true;
        return _conn.DisposeAsync();
    }

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
            ClientRequest.Write r => (MessageTypes.Requests.Write, SerializeWrite(r.Value), true),
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
    /// Serialize a write, rejecting a null <see cref="AggregateEvent.EventValue"/> first. A null
    /// serializes to a msgpack nil the server cannot decode as a byte array, and it does not fail the
    /// request cleanly — it corrupts the frame and drops the connection, surfacing as an opaque
    /// <c>ConnectionFailedException</c> a retry loop cannot tell from a real network fault. Catching
    /// it here turns one bad field into an <see cref="ArgumentException"/> before anything is sent.
    /// </summary>
    private static byte[] SerializeWrite(WriteRequest request)
    {
        foreach ((AggregateKey key, SingleAggregateWrite write) in request.Writes)
        {
            AggregateEvent[] events = write.Events;

            // An events-less write is a no-op the server rejects with an opaque error; catch it here
            // with the same client-side clarity as the checks below.
            if (events.Length == 0)
                throw new ArgumentException(
                    $"The write for aggregate {key} has no events; include at least one.", nameof(request));

            // A version cannot be negative; a negative ExpectedVersion would cast to a huge u64 and come
            // back as a WriteOccException naming an absurd version. Reject it with an honest message.
            if (write.ExpectedVersion is < 0)
                throw new ArgumentException(
                    $"ExpectedVersion for aggregate {key} is {write.ExpectedVersion}; it cannot be negative "
                    + "(use 0 to require the aggregate does not yet exist).",
                    nameof(request));

            // Distinct client seqs are only load-bearing when idempotency is enforced, and the server does
            // NOT dedupe duplicates within one write — both would silently store, defeating the per-seq
            // tracking. Reject an in-batch collision here so the guarantee the caller opted into holds.
            HashSet<long>? seenSeqs = write.EnforceClientIdempotency ? new HashSet<long>(events.Length) : null;

            for (int i = 0; i < events.Length; i++)
            {
                AggregateEvent e = events[i];

                if (e.EventValue is null)
                    throw new ArgumentException(
                        $"Event at index {i} for aggregate {key} has a null EventValue. EventValue must be "
                        + "non-null; use an empty array for an empty payload.",
                        nameof(request));

                // EventTypeMajor identifies the event's schema and is required; the server rejects 0.
                if (e.EventTypeMajor == 0)
                    throw new ArgumentException(
                        $"Event at index {i} for aggregate {key} has EventTypeMajor 0; it is required and "
                        + "must be non-zero.",
                        nameof(request));

                // The wire encodes EventTimestamp as unsigned epoch milliseconds and cannot represent a
                // time before 1970. A pre-epoch value — most often a default/unset DateTimeOffset — would
                // wrap to a far-future timestamp and silently corrupt timestamp-bounded reads, so it is
                // rejected here rather than stored wrong.
                if (e.EventTimestamp.ToUnixTimeMilliseconds() < 0)
                    throw new ArgumentException(
                        $"Event at index {i} for aggregate {key} has an EventTimestamp before the Unix epoch "
                        + $"({e.EventTimestamp:o}); it cannot be represented on the wire. Set it to the event's "
                        + "actual time (a default/unset DateTimeOffset is invalid).",
                        nameof(request));

                if (seenSeqs is not null && !seenSeqs.Add(e.ClientSeq))
                    throw new ArgumentException(
                        $"Event at index {i} for aggregate {key} reuses ClientSeq {e.ClientSeq} within one "
                        + "write while EnforceClientIdempotency is set; give each event a distinct ClientSeq "
                        + "(the server does not dedupe duplicates within a single write).",
                        nameof(request));
            }
        }

        return WireCodec.Serialize(request);
    }

    /// <summary>
    /// Logical payload size used for the compression threshold decision: the event values for a
    /// write, or the schema text for a schema registration.
    /// </summary>
    internal static long PayloadBytes(ClientRequest request) => request switch
    {
        ClientRequest.Write w => w.Value.Writes.Values
            .SelectMany(static sw => sw.Events)
            .Sum(static e => (long)(e.EventValue?.Length ?? 0)),
        ClientRequest.RegisterSchema s => Encoding.UTF8.GetByteCount(s.Value.Schema),
        _ => 0,
    };


    // ─── Correlation binding ────────────────────────────────────────────────
    //
    // Nothing else binds a response to the request that produced it: the transport
    // writes a frame and reads the next one back, so a connection that is one reply
    // behind hands out a plausible answer to a question nobody asked. Filling the id
    // and checking the echo turns that from silent corruption into a thrown error.

    /// <summary>
    /// The correlation id the caller set, or null. Written out per variant because the
    /// union's payloads share no base type.
    /// </summary>
    private static Guid? CorrelationIdOf(ClientRequest request) => request switch
    {
        ClientRequest.AggregateDetails r => r.Value.CorrelationId,
        ClientRequest.Read r => r.Value.CorrelationId,
        ClientRequest.Write r => r.Value.CorrelationId,
        ClientRequest.TrimStart r => r.Value.CorrelationId,
        ClientRequest.Delete r => r.Value.CorrelationId,
        ClientRequest.Watch r => r.Value.CorrelationId,
        ClientRequest.ListOrgs r => r.Value.CorrelationId,
        ClientRequest.ListAggregateTypes r => r.Value.CorrelationId,
        ClientRequest.ListAggregates r => r.Value.CorrelationId,
        ClientRequest.RegisterSchema r => r.Value.CorrelationId,
        ClientRequest.Identify r => r.Value.CorrelationId,
        _ => null,
    };

    /// <summary>
    /// Fill the correlation id only when the caller left it unset: it is a user-facing
    /// application tag that callers set and read back, so their value must survive.
    ///
    /// The binding guarantee is therefore conditional — it holds while caller-supplied
    /// ids are unique per request. A caller reusing one id across several requests (a
    /// gateway threading its per-HTTP-request id through the several store calls that
    /// serve one page, say) makes the echo check vacuous for those requests, because a
    /// stale reply carries a matching id.
    /// </summary>
    private static ClientRequest WithCorrelationId(ClientRequest request, Guid id) => request switch
    {
        ClientRequest.AggregateDetails r => new ClientRequest.AggregateDetails(r.Value with { CorrelationId = id }),
        ClientRequest.Read r => new ClientRequest.Read(r.Value with { CorrelationId = id }),
        ClientRequest.Write r => new ClientRequest.Write(r.Value with { CorrelationId = id }),
        ClientRequest.TrimStart r => new ClientRequest.TrimStart(r.Value with { CorrelationId = id }),
        ClientRequest.Delete r => new ClientRequest.Delete(r.Value with { CorrelationId = id }),
        ClientRequest.Watch r => new ClientRequest.Watch(r.Value with { CorrelationId = id }),
        ClientRequest.ListOrgs r => new ClientRequest.ListOrgs(r.Value with { CorrelationId = id }),
        ClientRequest.ListAggregateTypes r => new ClientRequest.ListAggregateTypes(r.Value with { CorrelationId = id }),
        ClientRequest.ListAggregates r => new ClientRequest.ListAggregates(r.Value with { CorrelationId = id }),
        ClientRequest.RegisterSchema r => new ClientRequest.RegisterSchema(r.Value with { CorrelationId = id }),
        ClientRequest.Identify r => new ClientRequest.Identify(r.Value with { CorrelationId = id }),
        _ => request,
    };

    /// <summary>
    /// The correlation id the server echoed, or null for the two response types that
    /// structurally cannot carry one: Watch (a server-push stream, with no request to
    /// bind to) and ProtocolError (sent when the request could not be parsed far enough
    /// to recover an id — the Rust server sends it with no fields at all).
    /// </summary>
    private static Guid? CorrelationIdOf(ClientResponse response) => response switch
    {
        ClientResponse.AggregateDetails r => r.Value.CorrelationId,
        ClientResponse.Read r => r.Value.CorrelationId,
        ClientResponse.Write r => r.Value.CorrelationId,
        ClientResponse.TrimStart r => r.Value.CorrelationId,
        ClientResponse.Delete r => r.Value.CorrelationId,
        ClientResponse.GenericError r => r.Value.CorrelationId,
        ClientResponse.ListOrgs r => r.Value.CorrelationId,
        ClientResponse.ListAggregateTypes r => r.Value.CorrelationId,
        ClientResponse.ListAggregates r => r.Value.CorrelationId,
        ClientResponse.RegisterSchema r => r.Value.CorrelationId,
        ClientResponse.Identify r => r.Value.CorrelationId,
        _ => null,
    };

    private static bool CarriesCorrelationId(ClientResponse response)
        => response is not (ClientResponse.Watch or ClientResponse.ProtocolError);

    /// <summary>
    /// Bind the response to the KIND of request as well as to its id. The correlation check cannot
    /// carry this on its own: <see cref="CarriesCorrelationId"/> exempts <c>Watch</c>, whose frame
    /// has no correlation field on the wire at all, so a Watch frame arriving in answer to a read
    /// passes every other check and is handed back to the caller as a successful read of a
    /// different shape. The connection is a reply out of step, so it is poisoned like a
    /// correlation mismatch. Mirrors the Rust client's <c>expected_response_type</c> check.
    ///
    /// <para>
    /// Error frames are exempt because they are what the server answers ANY request with: a
    /// <c>GenericError</c> is a legitimate reply to every request type.
    /// </para>
    /// </summary>
    private void VerifyResponseType(uint requestType, uint responseType)
    {
        if (responseType is MessageTypes.Responses.GenericError or MessageTypes.Responses.ProtocolError)
            return;

        if (ExpectedResponseType(requestType) is not { } expected || responseType == expected)
            return;

        _conn.PoisonForCorrelationMismatch();
        throw new ProtocolException(
            $"Response type mismatch: request type {requestType} is answered with response type "
            + $"{expected}, and type {responseType} arrived — the connection is a reply out of step.");
    }

    /// <summary>
    /// The response type each request is answered with, or null for a request this client does not
    /// send through here (Identify has its own round trip on the transport).
    /// </summary>
    private static uint? ExpectedResponseType(uint requestType) => requestType switch
    {
        MessageTypes.Requests.AggregateDetails => MessageTypes.Responses.AggregateDetails,
        MessageTypes.Requests.Read => MessageTypes.Responses.Read,
        MessageTypes.Requests.Write => MessageTypes.Responses.Write,
        MessageTypes.Requests.TrimStart => MessageTypes.Responses.TrimStart,
        MessageTypes.Requests.Delete => MessageTypes.Responses.Delete,
        MessageTypes.Requests.Watch => MessageTypes.Responses.Watch,
        MessageTypes.Requests.ListOrgs => MessageTypes.Responses.ListOrgs,
        MessageTypes.Requests.ListAggregateTypes => MessageTypes.Responses.ListAggregateTypes,
        MessageTypes.Requests.ListAggregates => MessageTypes.Responses.ListAggregates,
        MessageTypes.Requests.RegisterSchema => MessageTypes.Responses.RegisterSchema,
        _ => null,
    };

    /// <summary>
    /// Deserialize a response payload based on the message type ID and return the appropriate
    /// <see cref="ClientResponse"/> variant. Throws typed exceptions for error frames.
    /// </summary>
    private static ClientResponse DeserializeResponse(uint messageType, ReadOnlyMemory<byte> payload)
        => ThrowIfError(DecodeFrame(messageType, payload));

    /// <summary>
    /// Turn an error response into the exception it represents. Split from
    /// <see cref="DecodeFrame"/> so a caller can inspect the decoded frame first: a
    /// <c>GenericError</c> is a legal reply to every request type, so a stale error frame
    /// checked after this point is indistinguishable from this request's own failure.
    /// </summary>
    private static ClientResponse ThrowIfError(ClientResponse response) => response switch
    {
        ClientResponse.GenericError e => throw CreateException(e.Value),
        _ => response,
    };

    private static ClientResponse DecodeFrame(uint messageType, ReadOnlyMemory<byte> payload)
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
                MessageTypes.Responses.GenericError => new ClientResponse.GenericError(WireCodec.Deserialize<ErrorResponse>(payload)),
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
