using System.Text.Json;
using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Responses;

[MessagePackObject]
public sealed class ErrorResponse
{
    // --- Read errors: 1xxx ---
    public const uint ReadUnavailableVersion = 1000;
    public const uint ReadAggregateNotExists = 1001;
    public const uint ReadCacheLoadLockTimeout = 1002;
    public const uint ReadCacheLoadFileScan = 1003;
    public const uint ReadFetchDatablocks = 1004;
    public const uint ReadFetchMetablocks = 1005;

    // --- Write errors: 2xxx ---
    public const uint WriteEmptyEventsList = 2000;
    public const uint WriteZeroEventType = 2001;
    public const uint WriteClientIdempotencyViolation = 2002;
    public const uint WriteOptimisticConcurrencyViolation = 2003;
    public const uint WriteFailedToSerialiseDatablocks = 2004;
    public const uint WriteAggregateNotExists = 2005;
    public const uint WriteAggregateRecreateNotAllowed = 2006;
    public const uint WriteReplicationError = 2007;
    public const uint WriteFsyncError = 2008;
    public const uint WriteCacheAggregateClientError = 2009;
    public const uint WriteAggregateExistsCacheError = 2010;
    public const uint WriteNotLeader = 2011;
    /// <summary>Replication queue is saturated — request could not be accepted. Client should retry (treated as server-busy).</summary>
    public const uint WriteReplicationBackpressure = 2012;
    /// <summary>Write is fsynced but replication is not yet confirmed — duplicate of an in-flight write. Hold the client seq and retry.</summary>
    public const uint WriteInflightDuplicate = 2013;

    // --- Schema errors: 2020-2029 ---
    public const uint RegisterSchemaAlreadyExists = 2020;
    public const uint RegisterSchemaInvalid = 2021;
    public const uint WriteSchemaValidationFailed = 2022;
    public const uint WriteSchemaCompilationFailed = 2023;
    public const uint RegisterSchemaUnsupportedType = 2024;
    public const uint RegisterSchemaCacheLoadError = 2025;
    public const uint RegisterSchemaFsyncError = 2026;
    public const uint RegisterSchemaCannotAcceptWrites = 2027;
    public const uint RegisterSchemaReplicationError = 2028;
    public const uint RegisterSchemaCoordinationFailed = 2029;

    // --- Trim errors: 3xxx ---
    public const uint TrimAggregateNotExists = 3000;
    public const uint TrimCacheError = 3001;
    public const uint TrimReplicationError = 3002;
    public const uint TrimFsyncError = 3003;
    public const uint TrimIndexOutOfRange = 3004;
    public const uint TrimNotLeader = 3005;
    /// <summary>Replication queue is saturated — request could not be accepted. Client should retry (treated as server-busy).</summary>
    public const uint TrimReplicationBackpressure = 3006;

    // --- Delete errors: 4xxx ---
    public const uint DeleteAggregateNotExists = 4000;
    public const uint DeleteEmptyDeleteList = 4001;
    public const uint DeleteOptimisticConcurrencyViolation = 4002;
    public const uint DeleteCacheError = 4003;
    public const uint DeleteReplicationError = 4004;
    public const uint DeleteFsyncError = 4005;
    public const uint DeleteNotLeader = 4006;
    /// <summary>Replication queue is saturated — request could not be accepted. Client should retry (treated as server-busy).</summary>
    public const uint DeleteReplicationBackpressure = 4007;

    // --- Listing errors: 5xxx ---
    public const uint ListOrgsDiskRead = 5000;
    public const uint ListAggregateTypesDiskRead = 5001;
    public const uint ListAggregatesDiskRead = 5002;

    // --- Replication batch errors: 6xxx ---
    public const uint ReplicationBatchFsync = 6000;
    public const uint ReplicationBatchSerialiseDatablocks = 6001;
    public const uint ReplicationBatchWalSeqGap = 6002;

    // --- Exists / aggregate-details errors: 7xxx ---
    public const uint ExistsCacheError = 7000;
    public const uint ExistsAggregateNotExists = 7001;
    public const uint ExistsMetablockReadError = 7002;

    // --- Watch errors: 8xxx ---
    public const uint WatchRequestInvalid = 8000;
    public const uint WatchLatencyTooHigh = 8001;
    public const uint WatchReadIo = 8002;
    public const uint WatchReadSerialization = 8003;
    public const uint WatchReadOther = 8004;
    public const uint WatchTooManySubscribers = 8005;

    // --- Shard routing errors: 9xxx ---
    public const uint ShardRoutingNoKey = 9000;
    public const uint ShardRoutingMultipleShards = 9001;
    public const uint ShardRoutingIncompatibleFilters = 9002;

    // --- Server health errors: 11xxx ---
    /// <summary>Shard's inter-shard channel is full — request could not be routed. Client should retry.</summary>
    public const uint ServerBusy = 11000;

    // --- Identity & authentication errors: 10xxx ---
    public const uint IdentifyInvalidNonce = 10001;
    public const uint IdentifyInvalidSignature = 10002;
    public const uint IdentifyMismatch = 10003;
    public const uint IdentifyRequired = 10004;
    public const uint AuthRequired = 10005;
    public const uint AuthInvalidKey = 10006;
    public const uint AuthInsufficientPermissions = 10007;

    [Key(0)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? CorrelationId { get; init; }

    [Key(1)]
    public uint ErrorCode { get; init; }

    [Key(2)]
    public string ErrorMessage { get; init; } = "";

    // Lazily parsed JSON fields from ErrorMessage. Null until first access.
    private IReadOnlyDictionary<string, JsonElement>? _parsedFields;

    [IgnoreMember]
    public bool IsNotLeader => ErrorCode is WriteNotLeader or TrimNotLeader or DeleteNotLeader;

    [IgnoreMember]
    public bool IsIdentityRequired => ErrorCode == IdentifyRequired;

    [IgnoreMember]
    public bool IsServerBusy => ErrorCode is ServerBusy
        or WriteReplicationBackpressure
        or TrimReplicationBackpressure
        or DeleteReplicationBackpressure;

    /// <summary>
    /// The error message parsed as a flat JSON object. Each key maps to its raw
    /// <see cref="JsonElement"/> value. Returns an empty dictionary if the message
    /// is not valid JSON or is empty.
    ///
    /// <para>Parsed once on first access and cached for the lifetime of this instance.</para>
    /// </summary>
    [IgnoreMember]
    public IReadOnlyDictionary<string, JsonElement> Fields => _parsedFields ??= ParseFields();

    /// <summary>
    /// Attempt to parse a leader address from the error message JSON.
    /// The error message may be a JSON object like <c>{"leader_address":"host:port"}</c>.
    /// Returns null if parsing fails or the field is absent.
    /// </summary>
    public string? ParseLeaderAddress() => GetString("leader_address");

    /// <summary>
    /// Get a string field from the parsed JSON, or null if absent.
    /// </summary>
    internal string? GetString(string field)
        => Fields.TryGetValue(field, out var el) && el.ValueKind == JsonValueKind.String
            ? el.GetString()
            : null;

    /// <summary>
    /// Get a long field from the parsed JSON, or null if absent.
    /// </summary>
    internal long? GetLong(string field)
        => Fields.TryGetValue(field, out var el) && el.TryGetInt64(out long val)
            ? val
            : null;

    private IReadOnlyDictionary<string, JsonElement> ParseFields()
    {
        if (string.IsNullOrEmpty(ErrorMessage))
            return EmptyFields;
        try
        {
            var dict = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(ErrorMessage);
            return dict ?? EmptyFields;
        }
        catch (JsonException)
        {
            return EmptyFields;
        }
    }

    private static readonly IReadOnlyDictionary<string, JsonElement> EmptyFields =
        new Dictionary<string, JsonElement>();
}
