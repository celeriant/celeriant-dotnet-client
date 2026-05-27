using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Maps server error codes to typed exception instances with parsed JSON fields.
/// </summary>
internal static class ErrorExceptionFactory
{
    /// <summary>
    /// Create the most specific exception for the given error response.
    /// Returns a subclass of <see cref="CeleriantErrorException"/> when the error code
    /// maps to a known category; otherwise returns a plain <see cref="CeleriantErrorException"/>.
    /// </summary>
    public static CeleriantErrorException Create(ErrorResponse error)
    {
        return error.ErrorCode switch
        {
            // --- Read errors (1xxx) ---
            ErrorResponse.ReadUnavailableVersion
                => new BatchIndexUnavailableException(error),
            ErrorResponse.ReadAggregateNotExists
                => new AggregateNotFoundException(error),
            ErrorResponse.ReadCacheLoadLockTimeout
                or ErrorResponse.ReadCacheLoadFileScan
                or ErrorResponse.ReadFetchDatablocks
                or ErrorResponse.ReadFetchMetablocks
                => new ServerInternalErrorException(error),

            // --- Write errors (2xxx) ---
            ErrorResponse.WriteOptimisticConcurrencyViolation
                => new WriteOccException(error),
            ErrorResponse.WriteClientIdempotencyViolation
                => new IdempotencyViolationException(error),
            ErrorResponse.WriteAggregateNotExists
                => new AggregateNotFoundException(error),
            ErrorResponse.WriteAggregateRecreateNotAllowed
                => new AggregateRecreateNotAllowedException(error),
            // Prior write fsynced but not yet durable — retriable (hold client seq, retry).
            ErrorResponse.WriteInflightDuplicate
                => new InflightDuplicateWriteException(error),
            >= ErrorResponse.WriteEmptyEventsList and <= ErrorResponse.WriteAggregateRecreateNotAllowed
                => new WriteErrorException(error),
            ErrorResponse.WriteReplicationError
                or ErrorResponse.WriteFsyncError
                or ErrorResponse.WriteCacheAggregateClientError
                or ErrorResponse.WriteAggregateExistsCacheError
                => new ServerInternalErrorException(error),
            // Write not-leader — defensive: MapErrorResponse handles IsNotLeader before calling this factory
            ErrorResponse.WriteNotLeader
                => new CeleriantErrorException(error),

            // --- Schema errors (2020-2029) ---
            ErrorResponse.WriteSchemaValidationFailed
                => new SchemaValidationException(error),
            ErrorResponse.RegisterSchemaAlreadyExists
                or ErrorResponse.RegisterSchemaInvalid
                or ErrorResponse.WriteSchemaCompilationFailed
                or ErrorResponse.RegisterSchemaUnsupportedType
                => new SchemaErrorException(error),
            ErrorResponse.RegisterSchemaCacheLoadError
                or ErrorResponse.RegisterSchemaFsyncError
                or ErrorResponse.RegisterSchemaCannotAcceptWrites
                or ErrorResponse.RegisterSchemaReplicationError
                or ErrorResponse.RegisterSchemaCoordinationFailed
                => new ServerInternalErrorException(error),

            // --- Trim errors (3xxx) ---
            ErrorResponse.TrimAggregateNotExists
                => new AggregateNotFoundException(error),
            ErrorResponse.TrimIndexOutOfRange
                => new TrimIndexOutOfRangeException(error),
            ErrorResponse.TrimCacheError
                or ErrorResponse.TrimReplicationError
                or ErrorResponse.TrimFsyncError
                => new ServerInternalErrorException(error),
            // Trim not-leader — defensive: MapErrorResponse handles IsNotLeader before calling this factory
            ErrorResponse.TrimNotLeader
                => new CeleriantErrorException(error),

            // --- Delete errors (4xxx) ---
            ErrorResponse.DeleteOptimisticConcurrencyViolation
                => new DeleteOccException(error),
            ErrorResponse.DeleteAggregateNotExists
                => new AggregateNotFoundException(error),
            ErrorResponse.DeleteEmptyDeleteList
                => new DeleteErrorException(error),
            ErrorResponse.DeleteCacheError
                or ErrorResponse.DeleteReplicationError
                or ErrorResponse.DeleteFsyncError
                => new ServerInternalErrorException(error),
            // Delete not-leader — defensive: MapErrorResponse handles IsNotLeader before calling this factory
            ErrorResponse.DeleteNotLeader
                => new CeleriantErrorException(error),

            // --- Listing errors (5xxx) ---
            ErrorResponse.ListOrgsDiskRead
                or ErrorResponse.ListAggregateTypesDiskRead
                or ErrorResponse.ListAggregatesDiskRead
                => new ServerInternalErrorException(error),

            // --- Replication batch errors (6xxx) ---
            ErrorResponse.ReplicationBatchFsync
                or ErrorResponse.ReplicationBatchSerialiseDatablocks
                or ErrorResponse.ReplicationBatchWalSeqGap
                => new ServerInternalErrorException(error),

            // --- Exists / aggregate-details errors (7xxx) ---
            ErrorResponse.ExistsAggregateNotExists
                => new AggregateNotFoundException(error),
            ErrorResponse.ExistsCacheError
                or ErrorResponse.ExistsMetablockReadError
                => new ServerInternalErrorException(error),

            // --- Watch errors (8xxx) ---
            ErrorResponse.WatchRequestInvalid
                or ErrorResponse.WatchLatencyTooHigh
                => new WatchErrorException(error),
            ErrorResponse.WatchReadIo
                or ErrorResponse.WatchReadSerialization
                or ErrorResponse.WatchReadOther
                => new ServerInternalErrorException(error),

            // --- Shard routing errors (9xxx) — typically handled by list/watch retry logic ---
            ErrorResponse.ShardRoutingMultipleShards
                => new ShardRoutingException(error),
            ErrorResponse.ShardRoutingNoKey
                or ErrorResponse.ShardRoutingIncompatibleFilters
                => new CeleriantErrorException(error),

            // --- Identity errors (10xxx) ---
            ErrorResponse.IdentifyInvalidNonce
                or ErrorResponse.IdentifyInvalidSignature
                or ErrorResponse.IdentifyMismatch
                => new AuthErrorException(error),
            // IdentifyRequired — defensive: MapErrorResponse handles IsIdentityRequired before calling this factory
            ErrorResponse.IdentifyRequired
                => new CeleriantErrorException(error),
            ErrorResponse.AuthRequired
                or ErrorResponse.AuthInvalidKey
                or ErrorResponse.AuthInsufficientPermissions
                => new AuthErrorException(error),

            // Unknown error codes
            _ => new CeleriantErrorException(error),
        };
    }
}
