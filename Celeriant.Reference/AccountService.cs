using Celeriant.Client;
using Celeriant.Client.Errors;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Serialization;
using Npgsql;
using NpgsqlTypes;

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

/// <summary>Catch-up output: fresh projection state, plus the original response if the request already landed.</summary>
public sealed record CatchUpResult(AccountProjection Projection, WriteResult? Hit);

/// <summary>
/// Account service with a Postgres-backed projection, safe to run as many
/// replicas (e.g. k8s HPA) sharing one ServiceClientId.
///
/// The projection cursor lives in Postgres, so the request-response cache must
/// live there too: once any replica bumps the shared last_batch_index, no other
/// replica's catch-up will ever replay those events, and an in-memory cache
/// could never be warmed. Cursor and cache move together, atomically, in the
/// same statement. The happy path stays at one Postgres round trip (projection
/// row + response row in one query) plus the Celeriant catch-up read and the
/// write itself.
///
/// Default READ COMMITTED is enough. Celeriant's expectedVersion guard is the
/// serialization point; Postgres holds no invariant that spans statements.
/// That stays true only while each persist remains a single statement.
/// </summary>
public sealed class AccountService(
    ICeleriantPool pool,
    NpgsqlDataSource db,
    ILogger<AccountService> logger)
{
    private const int MaxRetries = 3;
    private static readonly IEventSerializer Serializer = JsonEventSerializer.Default;

    // ───────────────────────── Catch-Up ─────────────────────────

    /// <summary>
    /// Lazy catch-up: read the projection and response rows from Postgres in one
    /// query, read new events from Celeriant, fold, persist. Returns fresh
    /// projection state, plus the original response if <paramref name="eventId"/>
    /// already landed.
    /// </summary>
    public async Task<CatchUpResult> CatchUpAsync(
        Guid accountId,
        long? minBatchIndex = null,
        Guid? eventId = null,
        CancellationToken ct = default)
    {
        var key = Constants.AccountKey(accountId);

        // Step 1: projection row and response row, one round trip. The response
        // row answers "did this request already land?" for retries arriving on
        // a different replica than the one that served the original.
        long balanceCents = 0;
        long lastBatchIndex = 0;
        long maxClientSeq = 0;
        var accountName = "";
        WriteResult? hit = null;

        await using (var cmd = db.CreateCommand(@"
            SELECT b.account_name, b.balance_cents, b.last_batch_index, b.last_client_event_index,
                   r.balance_cents, r.batch_index
            FROM account_balances b
            LEFT JOIN request_responses r
              ON r.event_id = @eid AND r.aggregate_id = b.account_id AND r.expires_at > now()
            WHERE b.account_id = @id"))
        {
            cmd.Parameters.AddWithValue("id", accountId);
            cmd.Parameters.Add(new NpgsqlParameter("eid", NpgsqlDbType.Uuid) { Value = (object?)eventId ?? DBNull.Value });
            await using var reader = await cmd.ExecuteReaderAsync(ct);
            if (await reader.ReadAsync(ct))
            {
                accountName = reader.GetString(0);
                balanceCents = reader.GetInt64(1);
                lastBatchIndex = reader.GetInt64(2);
                maxClientSeq = reader.GetInt64(3);
                if (!reader.IsDBNull(4) && !reader.IsDBNull(5))
                    hit = new WriteResult(reader.GetInt64(4), reader.GetInt64(5));
            }
        }

        if (hit is not null)
            return new CatchUpResult(
                new AccountProjection(accountId, accountName, balanceCents, lastBatchIndex, maxClientSeq), hit);

        // If caller needs a minimum freshness and projection is already fresh enough, return early
        if (minBatchIndex.HasValue && lastBatchIndex >= minBatchIndex.Value)
            return new CatchUpResult(
                new AccountProjection(accountId, accountName, balanceCents, lastBatchIndex, maxClientSeq), null);

        // Step 2: read new events from Celeriant, following pagination. Buffering
        // the whole backlog is fine for the demo; a production fold over long
        // histories would stream batches instead, or start from a snapshot.
        var fromIndex = lastBatchIndex + 1;
        var batches = new List<AggregateEventBatch>();
        try
        {
            await foreach (var batch in pool.ReadAllAsync(key, ReadFilters.From(fromIndex), ct))
                batches.Add(batch);
        }
        catch (AggregateNotFoundException)
        {
            return new CatchUpResult(
                new AccountProjection(accountId, accountName, balanceCents, lastBatchIndex, maxClientSeq), null);
        }

        if (batches.Count == 0)
            return new CatchUpResult(
                new AccountProjection(accountId, accountName, balanceCents, lastBatchIndex, maxClientSeq), null);

        // Step 3: replay new events, collecting response rows for the recent
        // window. A replayed event gets its remaining lifetime, the window minus
        // its server-time age (batch vs tip; the local clock would let skew
        // misjudge it), so a row can never outlive the stated window. Keyed by
        // event id (sorted) so a reused key cannot produce two rows for one
        // upsert, and concurrent replicas upsert in the same order.
        var tipTs = batches[^1].ServerTimestamp;

        var warmRows = new SortedDictionary<Guid, (long Balance, long BatchIndex, long RemainingMs)>();
        WriteResult? found = null;

        var newBalance = balanceCents;
        var newBatchIndex = lastBatchIndex;

        foreach (var batch in batches)
        {
            newBatchIndex = batch.AggregateVersion;
            var trackClientSeq = batch.ClientId == Constants.ServiceClientId;
            var age = tipTs - batch.ServerTimestamp;
            if (age < TimeSpan.Zero)
                age = TimeSpan.Zero;

            foreach (var evt in batch.Events)
            {
                if (trackClientSeq && evt.ClientSeq > maxClientSeq)
                    maxClientSeq = evt.ClientSeq;

                newBalance = ReplayEvent(newBalance, evt);

                if (age < Verify.DedupWindow && evt.EventId is { } eid)
                {
                    var remainingMs = (long)(Verify.DedupWindow - age).TotalMilliseconds;
                    warmRows[eid] = (newBalance, batch.AggregateVersion, remainingMs);
                    if (eventId == eid)
                        found = new WriteResult(newBalance, batch.AggregateVersion);
                }
            }
        }

        // Step 4: persist the cursor and the response rows in one atomic
        // statement. The bump kills the replay path for every replica, so the
        // rows must be visible no later than the bump; atomicity guarantees it.
        // The upsert refreshes any existing row rather than skipping it, so a
        // re-warmed event id never loses its entry.
        if (newBatchIndex > lastBatchIndex)
        {
            var eids = new Guid[warmRows.Count];
            var bals = new long[warmRows.Count];
            var vers = new long[warmRows.Count];
            var rems = new long[warmRows.Count];
            var i = 0;
            foreach (var (eid, row) in warmRows)
            {
                eids[i] = eid;
                bals[i] = row.Balance;
                vers[i] = row.BatchIndex;
                rems[i] = row.RemainingMs;
                i++;
            }

            await using (var cmd = db.CreateCommand(@"
                WITH proj AS (
                    INSERT INTO account_balances (account_id, account_name, balance_cents, last_batch_index, last_client_event_index, updated_at)
                    VALUES (@id, @name, @balance, @batchIndex, @clientSeq, now())
                    ON CONFLICT (account_id) DO UPDATE
                    SET balance_cents = @balance,
                        account_name = COALESCE(NULLIF(@name, ''), account_balances.account_name),
                        last_batch_index = @batchIndex, last_client_event_index = @clientSeq, updated_at = now()
                    WHERE account_balances.last_batch_index < @batchIndex
                )
                INSERT INTO request_responses (event_id, aggregate_id, balance_cents, batch_index, expires_at)
                SELECT t.eid, @id, t.bal, t.ver, now() + t.rem_ms * interval '1 millisecond'
                FROM unnest(@eids, @bals, @vers, @rems) AS t(eid, bal, ver, rem_ms)
                ON CONFLICT (event_id, aggregate_id) DO UPDATE
                SET balance_cents = EXCLUDED.balance_cents,
                    batch_index = EXCLUDED.batch_index,
                    expires_at = GREATEST(request_responses.expires_at, EXCLUDED.expires_at)"))
            {
                cmd.Parameters.AddWithValue("id", accountId);
                cmd.Parameters.AddWithValue("name", accountName);
                cmd.Parameters.AddWithValue("balance", newBalance);
                cmd.Parameters.AddWithValue("batchIndex", newBatchIndex);
                cmd.Parameters.AddWithValue("clientSeq", maxClientSeq);
                cmd.Parameters.AddWithValue("eids", eids);
                cmd.Parameters.AddWithValue("bals", bals);
                cmd.Parameters.AddWithValue("vers", vers);
                cmd.Parameters.AddWithValue("rems", rems);
                await cmd.ExecuteNonQueryAsync(ct);
            }

            // Housekeeping, deliberately outside the atomic statement: a delete
            // and an upsert touching the same row in one statement is undefined
            // in Postgres. This path only runs when the cursor was behind;
            // production would run it on a timer instead.
            await using (var cleanup = db.CreateCommand(
                "DELETE FROM request_responses WHERE expires_at < now()"))
            {
                await cleanup.ExecuteNonQueryAsync(ct);
            }
        }

        return new CatchUpResult(
            new AccountProjection(accountId, accountName, newBalance, newBatchIndex, maxClientSeq), found);
    }

    // ───────────────────────── Write: Deposit ─────────────────────────

    public async Task<WriteResult> DepositAsync(
        Guid accountId, int amountCents, Guid eventId, CancellationToken ct = default)
    {
        var (projection, hit) = await CatchUpAsync(accountId, eventId: eventId, ct: ct);
        if (hit is not null)
            return hit;

        var clientSeq = projection.MaxClientSeq + 1;
        var reDeriveCei = false;

        for (var attempt = 1; attempt <= MaxRetries; attempt++)
        {
            if (attempt > 1)
            {
                await Backoff(attempt, ct);
                (projection, hit) = await CatchUpAsync(accountId, eventId: eventId, ct: ct);
                if (hit is not null)
                    return hit;
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
                await RecordWriteAsync(accountId, eventId, newBalance, newBatchIndex,
                    projection.LastBatchIndex, clientSeq, ct);
                return new WriteResult(newBalance, newBatchIndex);
            }
            catch (WriteOccException)
            {
                logger.LogDebug("OCC conflict on deposit for {AccountId}, attempt {Attempt}", accountId, attempt);
                reDeriveCei = true;
                continue;
            }
            catch (CeleriantTimeoutException)
            {
                // Timeout is ambiguous; hold clientSeq constant so an
                // IdempotencyViolation catches the landed write.
                logger.LogWarning("Timeout on deposit for {AccountId}, attempt {Attempt}", accountId, attempt);
                continue;
            }
            catch (InflightDuplicateWriteException)
            {
                // Prior attempt accepted but not yet confirmed durable; treating it
                // as success would be a false ack if it later rolls back.
                logger.LogDebug("Inflight duplicate on deposit for {AccountId}, attempt {Attempt}", accountId, attempt);
                continue;
            }
            catch (IdempotencyViolationException)
            {
                // Someone landed this clientSeq: our timed-out prior attempt, or a
                // sibling request that derived the same number. The stream knows which.
                switch (await Verify.WhoOwnsSeqAsync(pool, accountId, clientSeq, eventId, ct))
                {
                    case SeqOwnership.Ours:
                    {
                        logger.LogInformation("Idempotency hit on deposit for {AccountId}: prior attempt landed", accountId);
                        var (p, h) = await CatchUpAsync(accountId, eventId: eventId, ct: ct);
                        return h ?? new WriteResult(p.BalanceCents, p.LastBatchIndex);
                    }
                    case SeqOwnership.Sibling:
                        logger.LogInformation("ClientSeq {ClientSeq} on {AccountId} taken by a sibling; re-deriving", clientSeq, accountId);
                        reDeriveCei = true;
                        continue;
                    default:
                        throw new OccExhaustedException("Deposit state unverifiable after idempotency violation; retry the request.");
                }
            }
        }

        throw new OccExhaustedException("Deposit did not complete after retries: concurrent updates or timeouts. Retry the request.");
    }

    // ───────────────────────── Write: Withdraw ─────────────────────────

    public async Task<WriteResult> WithdrawAsync(
        Guid accountId, int amountCents, Guid eventId, CancellationToken ct = default)
    {
        var (projection, hit) = await CatchUpAsync(accountId, eventId: eventId, ct: ct);
        if (hit is not null)
            return hit;

        var clientSeq = projection.MaxClientSeq + 1;
        var reDeriveCei = false;

        for (var attempt = 1; attempt <= MaxRetries; attempt++)
        {
            if (attempt > 1)
            {
                await Backoff(attempt, ct);
                (projection, hit) = await CatchUpAsync(accountId, eventId: eventId, ct: ct);
                if (hit is not null)
                    return hit;
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
                await RecordWriteAsync(accountId, eventId, newBalance, newBatchIndex,
                    projection.LastBatchIndex, clientSeq, ct);
                return new WriteResult(newBalance, newBatchIndex);
            }
            catch (WriteOccException)
            {
                logger.LogDebug("OCC conflict on withdraw for {AccountId}, attempt {Attempt}", accountId, attempt);
                reDeriveCei = true;
                continue;
            }
            catch (CeleriantTimeoutException)
            {
                logger.LogWarning("Timeout on withdraw for {AccountId}, attempt {Attempt}", accountId, attempt);
                continue;
            }
            catch (InflightDuplicateWriteException)
            {
                logger.LogDebug("Inflight duplicate on withdraw for {AccountId}, attempt {Attempt}", accountId, attempt);
                continue;
            }
            catch (IdempotencyViolationException)
            {
                // Same verification as deposit.
                switch (await Verify.WhoOwnsSeqAsync(pool, accountId, clientSeq, eventId, ct))
                {
                    case SeqOwnership.Ours:
                    {
                        logger.LogInformation("Idempotency hit on withdraw for {AccountId}: prior attempt landed", accountId);
                        var (p, h) = await CatchUpAsync(accountId, eventId: eventId, ct: ct);
                        return h ?? new WriteResult(p.BalanceCents, p.LastBatchIndex);
                    }
                    case SeqOwnership.Sibling:
                        logger.LogInformation("ClientSeq {ClientSeq} on {AccountId} taken by a sibling; re-deriving", clientSeq, accountId);
                        reDeriveCei = true;
                        continue;
                    default:
                        throw new OccExhaustedException("Withdrawal state unverifiable after idempotency violation; retry the request.");
                }
            }
        }

        throw new OccExhaustedException("Withdrawal did not complete after retries: concurrent updates or timeouts. Retry the request.");
    }

    // ───────────────────────── Write: Transfer ─────────────────────────

    public sealed record TransferResult(WriteResult From, WriteResult To);

    public async Task<TransferResult> TransferAsync(
        Guid fromAccountId, Guid toAccountId, int amountCents, Guid eventId, CancellationToken ct = default)
    {
        if (fromAccountId == toAccountId)
            throw new ValidationException("Cannot transfer to the same account.");

        var (fromProjection, fromHit) = await CatchUpAsync(fromAccountId, eventId: eventId, ct: ct);
        var (toProjection, toHit) = await CatchUpAsync(toAccountId, eventId: eventId, ct: ct);
        if (await ResolveTransferHitsAsync(eventId, fromAccountId, toAccountId, fromHit, toHit, ct) is { } done)
            return done;

        // Derive ClientSeq for each aggregate independently; it is per (AggregateKey, ClientId)
        var fromClientSeq = fromProjection.MaxClientSeq + 1;
        var toClientSeq = toProjection.MaxClientSeq + 1;
        var reDeriveCei = false;

        for (var attempt = 1; attempt <= MaxRetries; attempt++)
        {
            if (attempt > 1)
            {
                await Backoff(attempt, ct);
                (fromProjection, fromHit) = await CatchUpAsync(fromAccountId, eventId: eventId, ct: ct);
                (toProjection, toHit) = await CatchUpAsync(toAccountId, eventId: eventId, ct: ct);
                if (await ResolveTransferHitsAsync(eventId, fromAccountId, toAccountId, fromHit, toHit, ct) is { } redone)
                    return redone;
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

                await RecordWriteAsync(fromAccountId, eventId, newFromBalance, newFromBatch,
                    fromProjection.LastBatchIndex, fromClientSeq, ct);
                await RecordWriteAsync(toAccountId, eventId, newToBalance, newToBatch,
                    toProjection.LastBatchIndex, toClientSeq, ct);

                return new TransferResult(
                    new WriteResult(newFromBalance, newFromBatch),
                    new WriteResult(newToBalance, newToBatch));
            }
            catch (WriteOccException)
            {
                logger.LogDebug("OCC conflict on transfer {From}->{To}, attempt {Attempt}",
                    fromAccountId, toAccountId, attempt);
                reDeriveCei = true;
                continue;
            }
            catch (CeleriantTimeoutException)
            {
                logger.LogWarning("Timeout on transfer {From}->{To}, attempt {Attempt}",
                    fromAccountId, toAccountId, attempt);
                continue;
            }
            catch (InflightDuplicateWriteException)
            {
                logger.LogDebug("Inflight duplicate on transfer {From}->{To}, attempt {Attempt}",
                    fromAccountId, toAccountId, attempt);
                continue;
            }
            catch (IdempotencyViolationException)
            {
                // At least one leg's clientSeq was consumed; the error does not say
                // which. The write is all-or-nothing, so owning either leg proves the
                // whole transfer landed; a sibling owning a leg proves it did not.
                var fromOwner = await Verify.WhoOwnsSeqAsync(pool, fromAccountId, fromClientSeq, eventId, ct);
                var verdict = fromOwner == SeqOwnership.Unwritten
                    ? await Verify.WhoOwnsSeqAsync(pool, toAccountId, toClientSeq, eventId, ct)
                    : fromOwner;

                switch (verdict)
                {
                    case SeqOwnership.Ours:
                    {
                        logger.LogInformation("Idempotency hit on transfer: prior attempt landed");
                        var (fp, fh) = await CatchUpAsync(fromAccountId, eventId: eventId, ct: ct);
                        var (tp, th) = await CatchUpAsync(toAccountId, eventId: eventId, ct: ct);
                        if (await ResolveTransferHitsAsync(eventId, fromAccountId, toAccountId, fh, th, ct) is { } vdone)
                            return vdone;
                        return new TransferResult(
                            new WriteResult(fp.BalanceCents, fp.LastBatchIndex),
                            new WriteResult(tp.BalanceCents, tp.LastBatchIndex));
                    }
                    case SeqOwnership.Sibling:
                        logger.LogInformation("Transfer clientSeq taken by a sibling; re-deriving");
                        reDeriveCei = true;
                        continue;
                    default:
                        throw new OccExhaustedException("Transfer state unverifiable after idempotency violation; retry the request.");
                }
            }
        }

        throw new OccExhaustedException("Transfer did not complete after retries: concurrent updates or timeouts. Retry the request.");
    }

    /// <summary>
    /// The transfer write is all-or-nothing, so a response-cache hit on EITHER
    /// leg proves the whole transfer landed. Reconstruct a missing leg (row
    /// expired) from current state.
    /// </summary>
    private async Task<TransferResult?> ResolveTransferHitsAsync(
        Guid eventId, Guid fromAccountId, Guid toAccountId,
        WriteResult? fromHit, WriteResult? toHit, CancellationToken ct)
    {
        switch (fromHit, toHit)
        {
            case ({ } f, { } t):
                return new TransferResult(f, t);
            case ({ } f, null):
            {
                var (tp, th) = await CatchUpAsync(toAccountId, eventId: eventId, ct: ct);
                return new TransferResult(f, th ?? new WriteResult(tp.BalanceCents, tp.LastBatchIndex));
            }
            case (null, { } t):
            {
                var (fp, fh) = await CatchUpAsync(fromAccountId, eventId: eventId, ct: ct);
                return new TransferResult(fh ?? new WriteResult(fp.BalanceCents, fp.LastBatchIndex), t);
            }
            default:
                return null;
        }
    }

    // ───────────────────────── Event History ─────────────────────────

    public async Task<(object[] Events, long CurrentBatchIndex, long BalanceCents)> GetHistoryAsync(
        Guid accountId, long? fromBatchIndex = null, CancellationToken ct = default)
    {
        // Catch up first so projection is current
        var (projection, _) = await CatchUpAsync(accountId, ct: ct);

        var key = Constants.AccountKey(accountId);
        try
        {
            var events = new List<object>();
            await foreach (var batch in pool.ReadAllAsync(key, ReadFilters.From(fromBatchIndex ?? 1), ct))
            {
                foreach (var evt in batch.Events)
                    events.Add(FormatEvent(batch, evt));
            }

            return (events.ToArray(), projection.LastBatchIndex, projection.BalanceCents);
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

    /// <summary>
    /// Persist a successful write: response row and projection bump in one
    /// atomic statement, so no replica can ever observe the bump without the
    /// row. A Postgres failure here is logged and swallowed: the Celeriant
    /// write succeeded (Celeriant is the source of truth), and with neither
    /// the row nor the bump applied, the next catch-up replays the event and
    /// self-heals.
    /// </summary>
    private async Task RecordWriteAsync(
        Guid accountId, Guid eventId, long newBalance, long newBatchIndex,
        long expectedBatchIndex, long clientSeq, CancellationToken ct)
    {
        try
        {
            await using var cmd = db.CreateCommand(@"
                WITH proj AS (
                    UPDATE account_balances
                    SET balance_cents = @balance, last_batch_index = @batchIndex,
                        last_client_event_index = @clientSeq, updated_at = now()
                    WHERE account_id = @id AND last_batch_index = @expectedBatchIndex
                )
                INSERT INTO request_responses (event_id, aggregate_id, balance_cents, batch_index, expires_at)
                VALUES (@eid, @id, @balance, @batchIndex, now() + @windowMs * interval '1 millisecond')
                ON CONFLICT (event_id, aggregate_id) DO UPDATE
                SET balance_cents = EXCLUDED.balance_cents,
                    batch_index = EXCLUDED.batch_index,
                    expires_at = GREATEST(request_responses.expires_at, EXCLUDED.expires_at)");
            cmd.Parameters.AddWithValue("id", accountId);
            cmd.Parameters.AddWithValue("eid", eventId);
            cmd.Parameters.AddWithValue("balance", newBalance);
            cmd.Parameters.AddWithValue("batchIndex", newBatchIndex);
            cmd.Parameters.AddWithValue("expectedBatchIndex", expectedBatchIndex);
            cmd.Parameters.AddWithValue("clientSeq", clientSeq);
            cmd.Parameters.AddWithValue("windowMs", (long)Verify.DedupWindow.TotalMilliseconds);
            await cmd.ExecuteNonQueryAsync(ct);
            // 0 projection rows affected is fine; the next read will catch up.
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogWarning(ex, "Failed to persist write for {AccountId}, will self-heal on next catch-up", accountId);
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
    : Exception($"Cannot process ${requestedCents / 100m:F2}, balance is ${balanceCents / 100m:F2}")
{
    public long BalanceCents => balanceCents;
}

public sealed class OccExhaustedException(string message) : Exception(message);
