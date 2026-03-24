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
    long MaxClientEventIndex);

public sealed record WriteResult(long BalanceCents, long BatchIndex);

public sealed class AccountService(
    ICeleriantPool pool,
    NpgsqlDataSource db,
    ILogger<AccountService> logger)
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
        long maxClientEventIndex = 0;
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
                maxClientEventIndex = reader.GetInt64(3);
            }
        }

        // Step 2: Read new events from Celeriant (from lastBatchIndex + 1)
        var fromIndex = lastBatchIndex + 1;

        // If caller needs a minimum freshness and projection is already fresh enough, return early
        if (minBatchIndex.HasValue && lastBatchIndex >= minBatchIndex.Value)
            return new AccountProjection(accountId, accountName, balanceCents, lastBatchIndex, maxClientEventIndex);

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
            return new AccountProjection(accountId, accountName, balanceCents, lastBatchIndex, maxClientEventIndex);
        }

        if (response.EventBatches.Length == 0)
        {
            return new AccountProjection(accountId, accountName, balanceCents, lastBatchIndex, maxClientEventIndex);
        }

        // Step 3: Replay new events — update balance and track maxClientEventIndex from new batches
        var newBalance = balanceCents;
        long newBatchIndex = lastBatchIndex;

        foreach (var batch in response.EventBatches)
        {
            newBatchIndex = batch.EventBatchIndex;

            // Track max ClientEventIndex for our service ClientId across new batches
            if (batch.ClientId == Constants.ServiceClientId)
            {
                foreach (var evt in batch.Events)
                {
                    if (evt.ClientEventIndex > maxClientEventIndex)
                        maxClientEventIndex = evt.ClientEventIndex;
                }
            }

            foreach (var evt in batch.Events)
            {
                newBalance = ReplayEvent(newBalance, evt);
            }
        }

        // Step 4: UPSERT into Postgres (conditional — won't go backwards)
        if (newBatchIndex > lastBatchIndex)
        {
            await using var cmd = db.CreateCommand(@"
                INSERT INTO account_balances (account_id, account_name, balance_cents, last_batch_index, last_client_event_index, updated_at)
                VALUES (@id, @name, @balance, @batchIndex, @clientEventIndex, now())
                ON CONFLICT (account_id) DO UPDATE
                SET balance_cents = @balance, account_name = @name,
                    last_batch_index = @batchIndex, last_client_event_index = @clientEventIndex, updated_at = now()
                WHERE account_balances.last_batch_index < @batchIndex");
            cmd.Parameters.AddWithValue("id", accountId);
            cmd.Parameters.AddWithValue("name", accountName);
            cmd.Parameters.AddWithValue("balance", newBalance);
            cmd.Parameters.AddWithValue("batchIndex", newBatchIndex);
            cmd.Parameters.AddWithValue("clientEventIndex", maxClientEventIndex);
            await cmd.ExecuteNonQueryAsync(ct);
        }

        return new AccountProjection(accountId, accountName, newBalance, newBatchIndex, maxClientEventIndex);
    }

    // ───────────────────────── Write: Deposit ─────────────────────────

    public async Task<WriteResult> DepositAsync(Guid accountId, int amountCents, CancellationToken ct = default)
    {
        var projection = await CatchUpAsync(accountId, ct: ct);
        var clientEventIndex = projection.MaxClientEventIndex + 1;
        var reDeriveCei = false;

        for (var attempt = 1; attempt <= MaxRetries; attempt++)
        {
            if (attempt > 1)
            {
                await Backoff(attempt, ct);
                projection = await CatchUpAsync(accountId, ct: ct);
                if (reDeriveCei)
                {
                    clientEventIndex = projection.MaxClientEventIndex + 1;
                    reDeriveCei = false;
                }
            }

            if (amountCents <= 0)
                throw new ValidationException("Amount must be positive.");

            var newBalance = projection.BalanceCents + amountCents;

            var evt = AggregateEventExtensions.Create(1L, new Deposited(amountCents), Serializer,
                clientEventIndex: clientEventIndex);

            try
            {
                await pool.WriteAsync(
                    Constants.AccountKey(accountId),
                    [evt],
                    clientId: Constants.ServiceClientId,
                    allowCreate: true,
                    expectedEventBatchIndex: projection.LastBatchIndex,
                    enforceClientIdempotency: true,
                    ct: ct);

                var newBatchIndex = projection.LastBatchIndex + 1;
                await UpdateProjectionOptimistically(accountId, projection.AccountName,
                    newBalance, newBatchIndex, projection.LastBatchIndex, clientEventIndex, ct);

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
                // Hold clientEventIndex constant so IdempotencyViolation catches the landed write.
                logger.LogWarning("Timeout on deposit for {AccountId}, attempt {Attempt}", accountId, attempt);
                continue;
            }
            catch (IdempotencyViolationException)
            {
                // Our prior attempt within this request already landed (K-FAIL recovery).
                // This can only follow a timeout retry (clientEventIndex held constant).
                logger.LogInformation("Idempotency hit on deposit for {AccountId} — prior attempt landed", accountId);
                projection = await CatchUpAsync(accountId, ct: ct);
                return new WriteResult(projection.BalanceCents, projection.LastBatchIndex);
            }
        }

        throw new OccExhaustedException("Deposit failed after retries — account was modified concurrently.");
    }

    // ───────────────────────── Write: Withdraw ─────────────────────────

    public async Task<WriteResult> WithdrawAsync(Guid accountId, int amountCents, CancellationToken ct = default)
    {
        var projection = await CatchUpAsync(accountId, ct: ct);
        var clientEventIndex = projection.MaxClientEventIndex + 1;
        var reDeriveCei = false;

        for (var attempt = 1; attempt <= MaxRetries; attempt++)
        {
            if (attempt > 1)
            {
                await Backoff(attempt, ct);
                projection = await CatchUpAsync(accountId, ct: ct);
                if (reDeriveCei)
                {
                    clientEventIndex = projection.MaxClientEventIndex + 1;
                    reDeriveCei = false;
                }
            }

            if (amountCents <= 0)
                throw new ValidationException("Amount must be positive.");

            if (projection.BalanceCents < amountCents)
                throw new InsufficientFundsException(projection.BalanceCents, amountCents);

            var newBalance = projection.BalanceCents - amountCents;

            var evt = AggregateEventExtensions.Create(2L, new Withdrawn(amountCents), Serializer,
                clientEventIndex: clientEventIndex);

            try
            {
                await pool.WriteAsync(
                    Constants.AccountKey(accountId),
                    [evt],
                    clientId: Constants.ServiceClientId,
                    allowCreate: true,
                    expectedEventBatchIndex: projection.LastBatchIndex,
                    enforceClientIdempotency: true,
                    ct: ct);

                var newBatchIndex = projection.LastBatchIndex + 1;
                await UpdateProjectionOptimistically(accountId, projection.AccountName,
                    newBalance, newBatchIndex, projection.LastBatchIndex, clientEventIndex, ct);

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
            catch (IdempotencyViolationException)
            {
                logger.LogInformation("Idempotency hit on withdraw for {AccountId} — prior attempt landed", accountId);
                projection = await CatchUpAsync(accountId, ct: ct);
                return new WriteResult(projection.BalanceCents, projection.LastBatchIndex);
            }
        }

        throw new OccExhaustedException("Withdrawal failed after retries — account was modified concurrently.");
    }

    // ───────────────────────── Write: Transfer ─────────────────────────

    public sealed record TransferResult(WriteResult From, WriteResult To);

    public async Task<TransferResult> TransferAsync(
        Guid fromAccountId, Guid toAccountId, int amountCents, CancellationToken ct = default)
    {
        if (fromAccountId == toAccountId)
            throw new ValidationException("Cannot transfer to the same account.");

        var fromProjection = await CatchUpAsync(fromAccountId, ct: ct);
        var toProjection = await CatchUpAsync(toAccountId, ct: ct);

        // Derive ClientEventIndex for each aggregate independently — per (AggregateKey, ClientId)
        var fromClientEventIndex = fromProjection.MaxClientEventIndex + 1;
        var toClientEventIndex = toProjection.MaxClientEventIndex + 1;
        var reDeriveCei = false;

        for (var attempt = 1; attempt <= MaxRetries; attempt++)
        {
            if (attempt > 1)
            {
                await Backoff(attempt, ct);
                fromProjection = await CatchUpAsync(fromAccountId, ct: ct);
                toProjection = await CatchUpAsync(toAccountId, ct: ct);
                if (reDeriveCei)
                {
                    fromClientEventIndex = fromProjection.MaxClientEventIndex + 1;
                    toClientEventIndex = toProjection.MaxClientEventIndex + 1;
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
                clientEventIndex: fromClientEventIndex);

            var transferInEvt = AggregateEventExtensions.Create(4L,
                new TransferredIn(amountCents, fromAccountId), Serializer,
                clientEventIndex: toClientEventIndex);

            var writeRequest = new WriteRequest
            {
                ClientId = Constants.ServiceClientId,
                Writes = new Dictionary<AggregateKey, SingleAggregateWrite>
                {
                    [fromKey] = new SingleAggregateWrite
                    {
                        Events = [transferOutEvt],
                        AllowCreate = true,
                        ExpectedEventBatchIndex = fromProjection.LastBatchIndex,
                        EnforceClientIdempotency = true,
                    },
                    [toKey] = new SingleAggregateWrite
                    {
                        Events = [transferInEvt],
                        AllowCreate = true,
                        ExpectedEventBatchIndex = toProjection.LastBatchIndex,
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

                await UpdateProjectionOptimistically(fromAccountId, fromProjection.AccountName,
                    newFromBalance, newFromBatch, fromProjection.LastBatchIndex, fromClientEventIndex, ct);
                await UpdateProjectionOptimistically(toAccountId, toProjection.AccountName,
                    newToBalance, newToBatch, toProjection.LastBatchIndex, toClientEventIndex, ct);

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
            catch (IdempotencyViolationException)
            {
                logger.LogInformation("Idempotency hit on transfer — prior attempt landed");
                fromProjection = await CatchUpAsync(fromAccountId, ct: ct);
                toProjection = await CatchUpAsync(toAccountId, ct: ct);
                return new TransferResult(
                    new WriteResult(fromProjection.BalanceCents, fromProjection.LastBatchIndex),
                    new WriteResult(toProjection.BalanceCents, toProjection.LastBatchIndex));
            }
        }

        throw new OccExhaustedException("Transfer failed after retries — accounts were modified concurrently.");
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
            batchIndex = batch.EventBatchIndex,
            type = typeName,
            amountCents,
            timestamp = batch.ServerTimestamp,
        };
    }

    private async Task UpdateProjectionOptimistically(
        Guid accountId, string accountName, long newBalance, long newBatchIndex,
        long expectedBatchIndex, long clientEventIndex, CancellationToken ct)
    {
        try
        {
            await using var cmd = db.CreateCommand(@"
                UPDATE account_balances
                SET balance_cents = @balance, last_batch_index = @batchIndex,
                    last_client_event_index = @clientEventIndex, updated_at = now()
                WHERE account_id = @id AND last_batch_index = @expectedBatchIndex");
            cmd.Parameters.AddWithValue("id", accountId);
            cmd.Parameters.AddWithValue("balance", newBalance);
            cmd.Parameters.AddWithValue("batchIndex", newBatchIndex);
            cmd.Parameters.AddWithValue("expectedBatchIndex", expectedBatchIndex);
            cmd.Parameters.AddWithValue("clientEventIndex", clientEventIndex);
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
