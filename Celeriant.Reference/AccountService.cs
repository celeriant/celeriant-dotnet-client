using Celeriant.Client;
using Celeriant.Client.Errors;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Serialization;
using Npgsql;

namespace Celeriant.Reference;

/// <summary>
/// Projection state returned by catch-up. Contains everything needed for validation and writes.
/// </summary>
public sealed record AccountProjection(
    Guid AccountId,
    string AccountName,
    long BalanceCents,
    long LastBatchIndex,
    long MaxClientSeq);

public sealed record WriteResult(long BalanceCents, long BatchIndex);

public sealed class AccountService(
    ICeleriantPool pool,
    NpgsqlDataSource db,
    ILogger<AccountService> logger,
    IdempotencyCache idempotency)
{
    private const int MaxRetries = 3;
    private static readonly IEventSerializer Serializer = JsonEventSerializer.Default;

    // ───────────────────────── Catch-Up ─────────────────────────

    /// <summary>
    /// Lazy catch-up: read projection from Postgres, read new events from Celeriant,
    /// replay, upsert. Returns fresh projection state.
    ///
    /// last_client_event_index is persisted in Postgres alongside the balance, so when
    /// the projection is already current we avoid an extra Celeriant read. During replay
    /// of new batches we scan for our ClientId and update the running max.
    /// </summary>
    public async Task<AccountProjection> CatchUpAsync(
        Guid accountId,
        long? minBatchIndex = null,
        CancellationToken ct = default)
    {
        var key = Constants.AccountKey(accountId);

        // Step 1: Read current projection from Postgres (includes last_client_event_index)
        long balanceCents = 0;
        long lastBatchIndex = 0;
        long maxClientSeq = 0;
        string accountName = "";

        await using (var cmd = db.CreateCommand(
            "SELECT account_name, balance_cents, last_batch_index, last_client_event_index FROM account_balances WHERE account_id = @id"))
        {
            cmd.Parameters.AddWithValue("id", accountId);
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            if (await reader.ReadAsync(ct))
            {
                accountName = reader.GetString(0);
                balanceCents = reader.GetInt64(1);
                lastBatchIndex = reader.GetInt64(2);
                maxClientSeq = reader.GetInt64(3);
            }
        }

        // Step 2: Read new events from Celeriant (from lastBatchIndex + 1)
        var fromIndex = lastBatchIndex + 1;

        // If caller needs a minimum freshness and projection is already fresh enough, return early
        if (minBatchIndex.HasValue && lastBatchIndex >= minBatchIndex.Value)
            return new AccountProjection(accountId, accountName, balanceCents, lastBatchIndex, maxClientSeq);

        ReadResponse response;
        try
        {
            response = await pool.ReadAsync(new ReadRequest
            {
                AggregateKey = key,
                Filters = ReadFilters.From(fromIndex),
            }, ct);
        }
        catch (AggregateNotFoundException)
        {
            return new AccountProjection(accountId, accountName, balanceCents, lastBatchIndex, maxClientSeq);
        }

        if (response.EventBatches.Length == 0)
        {
            return new AccountProjection(accountId, accountName, balanceCents, lastBatchIndex, maxClientSeq);
        }

        // Step 3: Replay new events — update balance, track maxClientSeq, warm the caches.
        // Warm-window aging is batch-vs-tip in server time; mixing in the local clock
        // would let skew silently disable warming.
        var newBalance = balanceCents;
        long newBatchIndex = lastBatchIndex;
        var tipTs = response.EventBatches[^1].ServerTimestamp;
        var warmWindow = IdempotencyCache.WarmWindow;

        foreach (var batch in response.EventBatches)
        {
            newBatchIndex = batch.AggregateVersion;
            var trackClientSeq = batch.ClientId == Constants.ServiceClientId;
            var warmCache = tipTs - batch.ServerTimestamp < warmWindow;

            foreach (var evt in batch.Events)
            {
                if (trackClientSeq && evt.ClientSeq > maxClientSeq)
                    maxClientSeq = evt.ClientSeq;

                newBalance = ReplayEvent(newBalance, evt);

                if (warmCache && evt.EventId is { } eid)
                {
                    idempotency.Set(eid, accountId, new IdempotencyEntry(newBalance, batch.AggregateVersion));
                    // Record the seq's owner so an IdempotencyViolation can be verified.
                    if (trackClientSeq)
                        idempotency.SetSeqOwner(accountId, evt.ClientSeq, eid);
                }
            }
        }

        // Step 4: UPSERT into Postgres (conditional — won't go backwards)
        if (newBatchIndex > lastBatchIndex)
        {
            await using var cmd = db.CreateCommand(@"
                INSERT INTO account_balances (account_id, account_name, balance_cents, last_batch_index, last_client_event_index, updated_at)
                VALUES (@id, @name, @balance, @batchIndex, @clientSeq, now())
                ON CONFLICT (account_id) DO UPDATE
                SET balance_cents = @balance,
                    account_name = COALESCE(NULLIF(@name, ''), account_balances.account_name),
                    last_batch_index = @batchIndex, last_client_event_index = @clientSeq, updated_at = now()
                WHERE account_balances.last_batch_index < @batchIndex");
            cmd.Parameters.AddWithValue("id", accountId);
            cmd.Parameters.AddWithValue("name", accountName);
            cmd.Parameters.AddWithValue("balance", newBalance);
            cmd.Parameters.AddWithValue("batchIndex", newBatchIndex);
            cmd.Parameters.AddWithValue("clientSeq", maxClientSeq);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        return new AccountProjection(accountId, accountName, newBalance, newBatchIndex, maxClientSeq);
    }

    // ───────────────────────── Write: Deposit ─────────────────────────

    public async Task<WriteResult> DepositAsync(
        Guid accountId, int amountCents, Guid? eventId = null, CancellationToken ct = default)
    {
        var projection = await CatchUpAsync(accountId, ct: ct);

        // event_id is supplied via the HTTP Idempotency-Key. If a prior attempt already landed
        // (and catch-up warmed the cache), return its outcome without writing again.
        if (eventId is { } eid && idempotency.TryGet(eid, accountId, out var hit))
            return new WriteResult(hit.BalanceCents, hit.AggregateVersion);

        var clientSeq = projection.MaxClientSeq + 1;
        var reDeriveCei = false;

        for (var attempt = 1; attempt <= MaxRetries; attempt++)
        {
            if (attempt > 1)
            {
                await Backoff(attempt, ct);
                projection = await CatchUpAsync(accountId, ct: ct);
                if (eventId is { } reid && idempotency.TryGet(reid, accountId, out var rehit))
                    return new WriteResult(rehit.BalanceCents, rehit.AggregateVersion);
                if (reDeriveCei)
                {
                    clientSeq = projection.MaxClientSeq + 1;
                    reDeriveCei = false;
                }
            }

            if (amountCents <= 0)
                throw new ValidationException("Amount must be positive.");

            var newBalance = projection.BalanceCents + amountCents;

            var evt = AggregateEventExtensions.Create(1L, new Deposited(amountCents), Serializer,
                clientSeq: clientSeq, eventId: eventId);

            try
            {
                await pool.WriteAsync(
                    Constants.AccountKey(accountId),
                    [evt],
                    clientId: Constants.ServiceClientId,
                    allowCreate: true,
                    expectedVersion: projection.LastBatchIndex,
                    enforceClientIdempotency: true,
                    ct: ct);

                var newBatchIndex = projection.LastBatchIndex + 1;
                // Caches before the projection bump: the bump kills the replay path
                // for same-key siblings, so the cache must already answer by then.
                if (eventId is { } seid)
                {
                    idempotency.Set(seid, accountId, new IdempotencyEntry(newBalance, newBatchIndex));
                    idempotency.SetSeqOwner(accountId, clientSeq, seid);
                }

                await UpdateProjectionOptimistically(accountId, projection.AccountName,
                    newBalance, newBatchIndex, projection.LastBatchIndex, clientSeq, ct);

                return new WriteResult(newBalance, newBatchIndex);
            }
            catch (WriteOccException) when (attempt < MaxRetries)
            {
                logger.LogDebug("OCC conflict on deposit for {AccountId}, attempt {Attempt}", accountId, attempt);
                reDeriveCei = true;
                continue;
            }
            catch (CeleriantTimeoutException) when (attempt < MaxRetries)
            {
                // Timeout is ambiguous — our write may have landed.
                // Hold clientSeq constant so IdempotencyViolation catches the landed write.
                logger.LogWarning("Timeout on deposit for {AccountId}, attempt {Attempt}", accountId, attempt);
                continue;
            }
            catch (InflightDuplicateWriteException) when (attempt < MaxRetries)
            {
                // Prior attempt fsynced but not yet durable; treating it as success would be a
                // false ack if it later rolls back. Hold clientSeq and retry.
                logger.LogDebug("Inflight duplicate on deposit for {AccountId}, attempt {Attempt}", accountId, attempt);
                continue;
            }
            catch (IdempotencyViolationException)
            {
                // Someone landed this clientSeq: our timed-out prior attempt, or a sibling
                // request that derived the same number. Verify before claiming success;
                // a false "done" silently drops the deposit.
                var p = await CatchUpAsync(accountId, ct: ct);
                if (eventId is { } veid)
                {
                    if (idempotency.TryGet(veid, accountId, out var vhit))
                        return new WriteResult(vhit.BalanceCents, vhit.AggregateVersion);

                    var owner = idempotency.SeqOwner(accountId, clientSeq);
                    if (owner == veid)
                    {
                        logger.LogInformation("Idempotency hit on deposit for {AccountId} — prior attempt landed", accountId);
                        return new WriteResult(p.BalanceCents, p.LastBatchIndex);
                    }
                    if (owner is not null)
                    {
                        // A sibling took the seq; our event never landed.
                        logger.LogInformation("ClientSeq {ClientSeq} on {AccountId} taken by a sibling — re-deriving", clientSeq, accountId);
                        reDeriveCei = true;
                        continue;
                    }
                }
                // Unknown ownership: refuse to guess.
                throw new OccExhaustedException("Deposit state unverifiable after idempotency violation — retry the request.");
            }
        }

        throw new OccExhaustedException("Deposit failed after retries — account was modified concurrently.");
    }

    // ───────────────────────── Write: Withdraw ─────────────────────────

    public async Task<WriteResult> WithdrawAsync(
        Guid accountId, int amountCents, Guid? eventId = null, CancellationToken ct = default)
    {
        var projection = await CatchUpAsync(accountId, ct: ct);

        if (eventId is { } eid && idempotency.TryGet(eid, accountId, out var hit))
            return new WriteResult(hit.BalanceCents, hit.AggregateVersion);

        var clientSeq = projection.MaxClientSeq + 1;
        var reDeriveCei = false;

        for (var attempt = 1; attempt <= MaxRetries; attempt++)
        {
            if (attempt > 1)
            {
                await Backoff(attempt, ct);
                projection = await CatchUpAsync(accountId, ct: ct);
                if (eventId is { } reid && idempotency.TryGet(reid, accountId, out var rehit))
                    return new WriteResult(rehit.BalanceCents, rehit.AggregateVersion);
                if (reDeriveCei)
                {
                    clientSeq = projection.MaxClientSeq + 1;
                    reDeriveCei = false;
                }
            }

            if (amountCents <= 0)
                throw new ValidationException("Amount must be positive.");

            if (projection.BalanceCents < amountCents)
                throw new InsufficientFundsException(projection.BalanceCents, amountCents);

            var newBalance = projection.BalanceCents - amountCents;

            var evt = AggregateEventExtensions.Create(2L, new Withdrawn(amountCents), Serializer,
                clientSeq: clientSeq, eventId: eventId);

            try
            {
                await pool.WriteAsync(
                    Constants.AccountKey(accountId),
                    [evt],
                    clientId: Constants.ServiceClientId,
                    allowCreate: true,
                    expectedVersion: projection.LastBatchIndex,
                    enforceClientIdempotency: true,
                    ct: ct);

                var newBatchIndex = projection.LastBatchIndex + 1;
                // Caches before the projection bump, as in deposit.
                if (eventId is { } seid)
                {
                    idempotency.Set(seid, accountId, new IdempotencyEntry(newBalance, newBatchIndex));
                    idempotency.SetSeqOwner(accountId, clientSeq, seid);
                }

                await UpdateProjectionOptimistically(accountId, projection.AccountName,
                    newBalance, newBatchIndex, projection.LastBatchIndex, clientSeq, ct);

                return new WriteResult(newBalance, newBatchIndex);
            }
            catch (WriteOccException) when (attempt < MaxRetries)
            {
                logger.LogDebug("OCC conflict on withdraw for {AccountId}, attempt {Attempt}", accountId, attempt);
                reDeriveCei = true;
                continue;
            }
            catch (CeleriantTimeoutException) when (attempt < MaxRetries)
            {
                logger.LogWarning("Timeout on withdraw for {AccountId}, attempt {Attempt}", accountId, attempt);
                continue;
            }
            catch (InflightDuplicateWriteException) when (attempt < MaxRetries)
            {
                logger.LogDebug("Inflight duplicate on withdraw for {AccountId}, attempt {Attempt}", accountId, attempt);
                continue;
            }
            catch (IdempotencyViolationException)
            {
                // Same verification as deposit.
                var p = await CatchUpAsync(accountId, ct: ct);
                if (eventId is { } veid)
                {
                    if (idempotency.TryGet(veid, accountId, out var vhit))
                        return new WriteResult(vhit.BalanceCents, vhit.AggregateVersion);

                    var owner = idempotency.SeqOwner(accountId, clientSeq);
                    if (owner == veid)
                    {
                        logger.LogInformation("Idempotency hit on withdraw for {AccountId} — prior attempt landed", accountId);
                        return new WriteResult(p.BalanceCents, p.LastBatchIndex);
                    }
                    if (owner is not null)
                    {
                        logger.LogInformation("ClientSeq {ClientSeq} on {AccountId} taken by a sibling — re-deriving", clientSeq, accountId);
                        reDeriveCei = true;
                        continue;
                    }
                }
                throw new OccExhaustedException("Withdrawal state unverifiable after idempotency violation — retry the request.");
            }
        }

        throw new OccExhaustedException("Withdrawal failed after retries — account was modified concurrently.");
    }

    // ───────────────────────── Write: Transfer ─────────────────────────

    public sealed record TransferResult(WriteResult From, WriteResult To);

    public async Task<TransferResult> TransferAsync(
        Guid fromAccountId, Guid toAccountId, int amountCents, Guid? eventId = null, CancellationToken ct = default)
    {
        if (fromAccountId == toAccountId)
            throw new ValidationException("Cannot transfer to the same account.");

        var fromProjection = await CatchUpAsync(fromAccountId, ct: ct);
        var toProjection = await CatchUpAsync(toAccountId, ct: ct);

        // After catching up both aggregates, a prior landed attempt warms both cache entries.
        // Reconstruct the result only when both sides hit.
        if (CachedTransfer(eventId, fromAccountId, toAccountId) is { } cached)
            return cached;

        // Derive ClientSeq for each aggregate independently — per (AggregateKey, ClientId)
        var fromClientSeq = fromProjection.MaxClientSeq + 1;
        var toClientSeq = toProjection.MaxClientSeq + 1;
        var reDeriveCei = false;

        for (var attempt = 1; attempt <= MaxRetries; attempt++)
        {
            if (attempt > 1)
            {
                await Backoff(attempt, ct);
                fromProjection = await CatchUpAsync(fromAccountId, ct: ct);
                toProjection = await CatchUpAsync(toAccountId, ct: ct);
                if (CachedTransfer(eventId, fromAccountId, toAccountId) is { } rehit)
                    return rehit;
                if (reDeriveCei)
                {
                    fromClientSeq = fromProjection.MaxClientSeq + 1;
                    toClientSeq = toProjection.MaxClientSeq + 1;
                    reDeriveCei = false;
                }
            }

            if (amountCents <= 0)
                throw new ValidationException("Amount must be positive.");

            if (fromProjection.BalanceCents < amountCents)
                throw new InsufficientFundsException(fromProjection.BalanceCents, amountCents);

            var fromKey = Constants.AccountKey(fromAccountId);
            var toKey = Constants.AccountKey(toAccountId);

            var transferOutEvt = AggregateEventExtensions.Create(3L,
                new TransferredOut(amountCents, toAccountId), Serializer,
                clientSeq: fromClientSeq, eventId: eventId);

            var transferInEvt = AggregateEventExtensions.Create(4L,
                new TransferredIn(amountCents, fromAccountId), Serializer,
                clientSeq: toClientSeq, eventId: eventId);

            var writeRequest = new WriteRequest
            {
                ClientId = Constants.ServiceClientId,
                Writes = new Dictionary<AggregateKey, SingleAggregateWrite>
                {
                    [fromKey] = new SingleAggregateWrite
                    {
                        Events = [transferOutEvt],
                        AllowCreate = true,
                        ExpectedVersion = fromProjection.LastBatchIndex,
                        EnforceClientIdempotency = true,
                    },
                    [toKey] = new SingleAggregateWrite
                    {
                        Events = [transferInEvt],
                        AllowCreate = true,
                        ExpectedVersion = toProjection.LastBatchIndex,
                        EnforceClientIdempotency = true,
                    },
                },
            };

            try
            {
                await pool.WriteAsync(writeRequest, ct);

                var newFromBalance = fromProjection.BalanceCents - amountCents;
                var newToBalance = toProjection.BalanceCents + amountCents;
                var newFromBatch = fromProjection.LastBatchIndex + 1;
                var newToBatch = toProjection.LastBatchIndex + 1;

                // Caches before the projection bumps, as in deposit.
                if (eventId is { } seid)
                {
                    idempotency.Set(seid, fromAccountId, new IdempotencyEntry(newFromBalance, newFromBatch));
                    idempotency.Set(seid, toAccountId, new IdempotencyEntry(newToBalance, newToBatch));
                    idempotency.SetSeqOwner(fromAccountId, fromClientSeq, seid);
                    idempotency.SetSeqOwner(toAccountId, toClientSeq, seid);
                }

                await UpdateProjectionOptimistically(fromAccountId, fromProjection.AccountName,
                    newFromBalance, newFromBatch, fromProjection.LastBatchIndex, fromClientSeq, ct);
                await UpdateProjectionOptimistically(toAccountId, toProjection.AccountName,
                    newToBalance, newToBatch, toProjection.LastBatchIndex, toClientSeq, ct);

                return new TransferResult(
                    new WriteResult(newFromBalance, newFromBatch),
                    new WriteResult(newToBalance, newToBatch));
            }
            catch (WriteOccException) when (attempt < MaxRetries)
            {
                logger.LogDebug("OCC conflict on transfer {From}->{To}, attempt {Attempt}",
                    fromAccountId, toAccountId, attempt);
                reDeriveCei = true;
                continue;
            }
            catch (CeleriantTimeoutException) when (attempt < MaxRetries)
            {
                logger.LogWarning("Timeout on transfer {From}->{To}, attempt {Attempt}",
                    fromAccountId, toAccountId, attempt);
                continue;
            }
            catch (InflightDuplicateWriteException) when (attempt < MaxRetries)
            {
                logger.LogDebug("Inflight duplicate on transfer {From}->{To}, attempt {Attempt}",
                    fromAccountId, toAccountId, attempt);
                continue;
            }
            catch (IdempotencyViolationException)
            {
                // At least one leg's clientSeq was consumed: our prior transfer, or a
                // sibling's write on either account. Verify.
                fromProjection = await CatchUpAsync(fromAccountId, ct: ct);
                toProjection = await CatchUpAsync(toAccountId, ct: ct);
                if (CachedTransfer(eventId, fromAccountId, toAccountId) is { } vhit)
                    return vhit;

                if (eventId is { } veid)
                {
                    var fromOwner = idempotency.SeqOwner(fromAccountId, fromClientSeq);
                    var toOwner = idempotency.SeqOwner(toAccountId, toClientSeq);

                    if ((fromOwner is not null && fromOwner != veid) || (toOwner is not null && toOwner != veid))
                    {
                        logger.LogInformation("Transfer clientSeq taken by a sibling — re-deriving");
                        reDeriveCei = true;
                        continue;
                    }
                    // The write is all-or-nothing: owning either leg proves the whole
                    // transfer landed.
                    if (fromOwner == veid || toOwner == veid)
                    {
                        logger.LogInformation("Idempotency hit on transfer — prior attempt landed");
                        return new TransferResult(
                            new WriteResult(fromProjection.BalanceCents, fromProjection.LastBatchIndex),
                            new WriteResult(toProjection.BalanceCents, toProjection.LastBatchIndex));
                    }
                }
                throw new OccExhaustedException("Transfer state unverifiable after idempotency violation — retry the request.");
            }
        }

        throw new OccExhaustedException("Transfer failed after retries — accounts were modified concurrently.");
    }

    /// <summary>
    /// Reconstruct a transfer result from the idempotency cache. Returns null unless
    /// <b>both</b> aggregates have a cache entry for <paramref name="eventId"/>.
    /// </summary>
    private TransferResult? CachedTransfer(Guid? eventId, Guid fromAccountId, Guid toAccountId)
    {
        if (eventId is not { } eid)
            return null;
        if (!idempotency.TryGet(eid, fromAccountId, out var from))
            return null;
        if (!idempotency.TryGet(eid, toAccountId, out var to))
            return null;

        return new TransferResult(
            new WriteResult(from.BalanceCents, from.AggregateVersion),
            new WriteResult(to.BalanceCents, to.AggregateVersion));
    }

    // ───────────────────────── Event History ─────────────────────────

    public async Task<(object[] Events, long CurrentBatchIndex, long BalanceCents)> GetHistoryAsync(
        Guid accountId, long? fromBatchIndex = null, CancellationToken ct = default)
    {
        // Catch up first so projection is current
        var projection = await CatchUpAsync(accountId, ct: ct);

        var key = Constants.AccountKey(accountId);
        try
        {
            var response = await pool.ReadAsync(new ReadRequest
            {
                AggregateKey = key,
                Filters = ReadFilters.From(fromBatchIndex ?? 1),
            }, ct);

            var events = response.EventBatches.SelectMany(b =>
                b.Events.Select(e => FormatEvent(b, e))).ToArray();

            return (events, projection.LastBatchIndex, projection.BalanceCents);
        }
        catch (AggregateNotFoundException)
        {
            return ([], projection.LastBatchIndex, projection.BalanceCents);
        }
    }

    // ───────────────────────── Helpers ─────────────────────────

    private static long ReplayEvent(long balanceCents, AggregateEvent evt)
    {
        return evt.EventTypeMajor switch
        {
            1 => balanceCents + Serializer.Deserialize<Deposited>(evt.EventValue).AmountCents,
            2 => balanceCents - Serializer.Deserialize<Withdrawn>(evt.EventValue).AmountCents,
            3 => balanceCents - Serializer.Deserialize<TransferredOut>(evt.EventValue).AmountCents,
            4 => balanceCents + Serializer.Deserialize<TransferredIn>(evt.EventValue).AmountCents,
            _ => balanceCents,
        };
    }

    private static object FormatEvent(AggregateEventBatch batch, AggregateEvent evt)
    {
        var (typeName, amountCents) = evt.EventTypeMajor switch
        {
            1 => ("Deposited", Serializer.Deserialize<Deposited>(evt.EventValue).AmountCents),
            2 => ("Withdrawn", Serializer.Deserialize<Withdrawn>(evt.EventValue).AmountCents),
            3 => ("TransferredOut", Serializer.Deserialize<TransferredOut>(evt.EventValue).AmountCents),
            4 => ("TransferredIn", Serializer.Deserialize<TransferredIn>(evt.EventValue).AmountCents),
            _ => ("Unknown", 0),
        };

        return new
        {
            batchIndex = batch.AggregateVersion,
            type = typeName,
            amountCents,
            timestamp = batch.ServerTimestamp,
        };
    }

    private async Task UpdateProjectionOptimistically(
        Guid accountId, string accountName, long newBalance, long newBatchIndex,
        long expectedBatchIndex, long clientSeq, CancellationToken ct)
    {
        try
        {
            await using var cmd = db.CreateCommand(@"
                UPDATE account_balances
                SET balance_cents = @balance, last_batch_index = @batchIndex,
                    last_client_event_index = @clientSeq, updated_at = now()
                WHERE account_id = @id AND last_batch_index = @expectedBatchIndex");
            cmd.Parameters.AddWithValue("id", accountId);
            cmd.Parameters.AddWithValue("balance", newBalance);
            cmd.Parameters.AddWithValue("batchIndex", newBatchIndex);
            cmd.Parameters.AddWithValue("expectedBatchIndex", expectedBatchIndex);
            cmd.Parameters.AddWithValue("clientSeq", clientSeq);
            await cmd.ExecuteNonQueryAsync(ct);
            // 0 rows affected is fine — next read will catch up (M-PASS 0 rows)
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // Postgres failure after successful Celeriant write — log and continue.
            // The write DID succeed (Celeriant is source of truth). Projection self-heals on next catch-up.
            // See FAILURE-ANALYSIS.md M-FAIL.
            logger.LogWarning(ex, "Failed to update projection for {AccountId} — will self-heal on next catch-up", accountId);
        }
    }

    private static async Task Backoff(int attempt, CancellationToken ct)
    {
        var delayMs = (int)(100 * Math.Pow(2, attempt - 1)) + Random.Shared.Next(0, 50);
        await Task.Delay(delayMs, ct);
    }
}

// ───────────────────────── Domain Exceptions ─────────────────────────

public sealed class ValidationException(string message) : Exception(message);

public sealed class InsufficientFundsException(long balanceCents, int requestedCents)
    : Exception($"Cannot process ${requestedCents / 100m:F2} — balance is ${balanceCents / 100m:F2}")
{
    public long BalanceCents => balanceCents;
}

public sealed class OccExhaustedException(string message) : Exception(message);
