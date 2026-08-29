using Celeriant.Client;
using Celeriant.Client.Errors;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Serialization;

namespace Celeriant.Reference;

/// <summary>
/// Account service with an in-memory projection, safe to run as many replicas
/// (e.g. k8s HPA) sharing one ServiceClientId.
///
/// Each replica folds the stream itself, so each replica's fold rebuilds its own
/// request-dedup index as a side effect of replay: the cursor and the index live
/// together in process memory and move together under one lock. A retry landing
/// on any replica is caught because that replica either already folded the
/// original event (index hit) or folds it during this request's own catch-up. No
/// Postgres, no shared cache, no extra network hops: the happy path is catch-up
/// read, fold, validate, write.
///
/// NOTE: the projection and response cache are per process, NOT shared across
/// replicas — unlike the Postgres backend, where cursor and cache are shared. A
/// retry that lands on a different replica than the original is still caught,
/// but by that replica re-folding the stream rather than by reading a shared
/// row. Correctness rests on Celeriant's ClientSeq/expectedVersion guards, which
/// are shared; the in-memory cache is only a fast path.
///
/// Bounds: dedup-index entries carry an expiry deadline and are evicted during
/// the fold. The account map is bounded by the demo's fixed account set; a
/// production service would cap it (LRU) and rebuild evicted accounts by
/// re-folding the stream, which is the same code path as a cold start.
///
/// Registered as a singleton so the projection survives across requests.
/// </summary>
public sealed class MemAccountService : IAccountService
{
    private const int MaxRetries = 3;
    private static readonly IEventSerializer Serializer = JsonEventSerializer.Default;

    private readonly ICeleriantPool _pool;
    private readonly ILogger<MemAccountService> _logger;

    // The lock guarding _accounts. Never held across an await: lock, mutate, unlock.
    private readonly object _lock = new();
    private readonly Dictionary<Guid, MemAccount> _accounts = new();

    public MemAccountService(ICeleriantPool pool, ILogger<MemAccountService> logger)
    {
        _pool = pool;
        _logger = logger;

        // Pre-seed names; everything else is folded from the stream.
        foreach (var (name, id, _) in Constants.Accounts)
            _accounts[id] = new MemAccount { AccountName = name };
    }

    private sealed class MemAccount
    {
        public string AccountName = "";
        public long BalanceCents;
        public long LastBatchIndex;
        public long MaxClientSeq;

        /// <summary>event id -> the response that write produced. Maintained by the fold, evicted by expiry.</summary>
        public readonly Dictionary<Guid, RecentWrite> Recent = new();
    }

    private sealed class RecentWrite
    {
        public long BalanceCents;
        public long BatchIndex;

        /// <summary>
        /// Monotonic-clock deadline (<see cref="NowMs"/> units) after which this
        /// entry stops being trusted. An entry created by our own write gets the
        /// full window from now; an entry created by replaying a batch gets the
        /// window minus the batch's age, where age is measured in SERVER time
        /// (batch vs tip-of-read) so clock skew cannot misjudge it. Only the
        /// remaining lifetime runs on the local monotonic clock.
        /// </summary>
        public long ExpiresAtMs;
    }

    // Monotonic milliseconds; the .NET equivalent of Rust's Instant.
    private static long NowMs() => Environment.TickCount64;

    private static AccountProjection ProjectionOf(Guid accountId, MemAccount acc) =>
        new(accountId, acc.AccountName, acc.BalanceCents, acc.LastBatchIndex, acc.MaxClientSeq);

    private static WriteResult? HitOf(MemAccount acc, Guid? eventId)
    {
        if (eventId is not { } eid || !acc.Recent.TryGetValue(eid, out var r))
            return null;
        if (r.ExpiresAtMs <= NowMs())
            return null;
        return new WriteResult(r.BalanceCents, r.BatchIndex);
    }

    // ───────────────────────── Catch-Up ─────────────────────────

    /// <summary>
    /// Lazy catch-up: read new events from Celeriant from this replica's own
    /// cursor, fold them into the projection and the dedup index under one lock.
    /// Returns fresh projection state, plus the original response if
    /// <paramref name="eventId"/> already landed.
    /// </summary>
    public async Task<CatchUpResult> CatchUpAsync(
        Guid accountId,
        long? minBatchIndex = null,
        Guid? eventId = null,
        CancellationToken ct = default)
    {
        // Step 1: index and freshness checks against current state.
        long fromIndex;
        lock (_lock)
        {
            if (_accounts.TryGetValue(accountId, out var acc))
            {
                if (HitOf(acc, eventId) is { } hit)
                    return new CatchUpResult(ProjectionOf(accountId, acc), hit);
                if (minBatchIndex.HasValue && acc.LastBatchIndex >= minBatchIndex.Value)
                    return new CatchUpResult(ProjectionOf(accountId, acc), null);
                fromIndex = acc.LastBatchIndex + 1;
            }
            else
            {
                fromIndex = 1;
            }
        }

        // Step 2: read new events from Celeriant, following pagination. Buffering
        // the whole backlog is fine for the demo; a production fold over long
        // histories would stream batches instead, or start from a snapshot.
        var batches = new List<AggregateEventBatch>();
        try
        {
            await foreach (var batch in _pool.ReadAllAsync(
                Constants.AccountKey(accountId), ReadFilters.From(fromIndex), ct))
                batches.Add(batch);
        }
        catch (AggregateNotFoundException)
        {
            lock (_lock)
            {
                return _accounts.TryGetValue(accountId, out var acc)
                    ? new CatchUpResult(ProjectionOf(accountId, acc), null)
                    : new CatchUpResult(new AccountProjection(accountId, "", 0, 0, 0), null);
            }
        }

        // Step 3: fold under the lock. A sibling request may have folded some of
        // these batches while we were reading; the version guard makes the
        // overlap a no-op. The dedup index is maintained mid-fold: an event
        // inside the window is indexed in the same pass that applies it, with its
        // remaining lifetime (the window minus the batch's server-time age). Age
        // is batch-vs-tip in server time; using the local clock for age would let
        // skew silently disable indexing.
        var tipTs = batches.Count > 0 ? batches[^1].ServerTimestamp : default;
        var window = Verify.DedupWindow;
        var now = NowMs();

        lock (_lock)
        {
            if (!_accounts.TryGetValue(accountId, out var acc))
            {
                acc = new MemAccount();
                _accounts[accountId] = acc;
            }

            foreach (var batch in batches)
            {
                if (batch.AggregateVersion <= acc.LastBatchIndex)
                    continue; // a sibling already folded this batch
                acc.LastBatchIndex = batch.AggregateVersion;
                var trackClientSeq = batch.ClientId == Constants.ServiceClientId;
                var age = tipTs - batch.ServerTimestamp;
                if (age < TimeSpan.Zero)
                    age = TimeSpan.Zero;

                foreach (var evt in batch.Events)
                {
                    if (trackClientSeq && evt.ClientSeq > acc.MaxClientSeq)
                        acc.MaxClientSeq = evt.ClientSeq;

                    acc.BalanceCents = AccountEvents.ReplayEvent(acc.BalanceCents, evt);

                    if (age < window && evt.EventId is { } eid)
                    {
                        var remainingMs = (long)(window - age).TotalMilliseconds;
                        acc.Recent[eid] = new RecentWrite
                        {
                            BalanceCents = acc.BalanceCents,
                            BatchIndex = batch.AggregateVersion,
                            ExpiresAtMs = now + remainingMs,
                        };
                    }
                }
            }

            // Evict expired entries.
            var expired = acc.Recent.Where(kv => kv.Value.ExpiresAtMs <= now)
                .Select(kv => kv.Key).ToList();
            foreach (var k in expired)
                acc.Recent.Remove(k);

            return new CatchUpResult(ProjectionOf(accountId, acc), HitOf(acc, eventId));
        }
    }

    /// <summary>
    /// Record a successful write: index entry first, then the projection bump,
    /// under one lock acquisition. The bump is conditional so it never goes
    /// backwards if a sibling's fold got there first.
    /// </summary>
    private void RecordWrite(
        Guid accountId, Guid eventId, long newBalance, long newBatchIndex,
        long expectedBatchIndex, long clientSeq)
    {
        lock (_lock)
        {
            if (!_accounts.TryGetValue(accountId, out var acc))
            {
                acc = new MemAccount();
                _accounts[accountId] = acc;
            }

            acc.Recent[eventId] = new RecentWrite
            {
                BalanceCents = newBalance,
                BatchIndex = newBatchIndex,
                // The write just happened, so it gets the full window. If the bump
                // below wins, this replica never re-folds its own batch, so this
                // entry is the only record it will ever have. Its lifetime must not
                // depend on anything else; a fold-tip-relative stamp would let an
                // idle account's entry be born almost expired.
                ExpiresAtMs = NowMs() + (long)Verify.DedupWindow.TotalMilliseconds,
            };

            if (acc.LastBatchIndex == expectedBatchIndex)
            {
                acc.BalanceCents = newBalance;
                acc.LastBatchIndex = newBatchIndex;
                acc.MaxClientSeq = Math.Max(acc.MaxClientSeq, clientSeq);
            }
        }
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
                await Verify.Backoff(attempt, ct);
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
                await _pool.WriteAsync(
                    Constants.AccountKey(accountId),
                    [evt],
                    clientId: Constants.ServiceClientId,
                    allowCreate: true,
                    expectedVersion: projection.LastBatchIndex,
                    enforceClientIdempotency: true,
                    ct: ct);

                var newBatchIndex = projection.LastBatchIndex + 1;
                RecordWrite(accountId, eventId, newBalance, newBatchIndex,
                    projection.LastBatchIndex, clientSeq);
                return new WriteResult(newBalance, newBatchIndex);
            }
            catch (WriteOccException)
            {
                _logger.LogDebug("OCC conflict on deposit for {AccountId}, attempt {Attempt}", accountId, attempt);
                reDeriveCei = true;
                continue;
            }
            catch (CeleriantTimeoutException)
            {
                // Timeout is ambiguous; hold clientSeq constant so an
                // IdempotencyViolation catches the landed write.
                _logger.LogWarning("Timeout on deposit for {AccountId}, attempt {Attempt}", accountId, attempt);
                continue;
            }
            catch (InflightDuplicateWriteException)
            {
                // Prior attempt accepted but not yet confirmed durable; treating it
                // as success would be a false ack if it later rolls back.
                _logger.LogDebug("Inflight duplicate on deposit for {AccountId}, attempt {Attempt}", accountId, attempt);
                continue;
            }
            catch (IdempotencyViolationException)
            {
                // Someone landed this clientSeq: our timed-out prior attempt, or a
                // sibling request that derived the same number. The stream knows which.
                switch (await Verify.WhoOwnsSeqAsync(_pool, accountId, clientSeq, eventId, ct))
                {
                    case SeqOwnership.Ours:
                    {
                        _logger.LogInformation("Idempotency hit on deposit for {AccountId}: prior attempt landed", accountId);
                        var (p, h) = await CatchUpAsync(accountId, eventId: eventId, ct: ct);
                        return h ?? new WriteResult(p.BalanceCents, p.LastBatchIndex);
                    }
                    case SeqOwnership.Sibling:
                        _logger.LogInformation("ClientSeq {ClientSeq} on {AccountId} taken by a sibling; re-deriving", clientSeq, accountId);
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
                await Verify.Backoff(attempt, ct);
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
                await _pool.WriteAsync(
                    Constants.AccountKey(accountId),
                    [evt],
                    clientId: Constants.ServiceClientId,
                    allowCreate: true,
                    expectedVersion: projection.LastBatchIndex,
                    enforceClientIdempotency: true,
                    ct: ct);

                var newBatchIndex = projection.LastBatchIndex + 1;
                RecordWrite(accountId, eventId, newBalance, newBatchIndex,
                    projection.LastBatchIndex, clientSeq);
                return new WriteResult(newBalance, newBatchIndex);
            }
            catch (WriteOccException)
            {
                _logger.LogDebug("OCC conflict on withdraw for {AccountId}, attempt {Attempt}", accountId, attempt);
                reDeriveCei = true;
                continue;
            }
            catch (CeleriantTimeoutException)
            {
                _logger.LogWarning("Timeout on withdraw for {AccountId}, attempt {Attempt}", accountId, attempt);
                continue;
            }
            catch (InflightDuplicateWriteException)
            {
                _logger.LogDebug("Inflight duplicate on withdraw for {AccountId}, attempt {Attempt}", accountId, attempt);
                continue;
            }
            catch (IdempotencyViolationException)
            {
                // Same verification as deposit.
                switch (await Verify.WhoOwnsSeqAsync(_pool, accountId, clientSeq, eventId, ct))
                {
                    case SeqOwnership.Ours:
                    {
                        _logger.LogInformation("Idempotency hit on withdraw for {AccountId}: prior attempt landed", accountId);
                        var (p, h) = await CatchUpAsync(accountId, eventId: eventId, ct: ct);
                        return h ?? new WriteResult(p.BalanceCents, p.LastBatchIndex);
                    }
                    case SeqOwnership.Sibling:
                        _logger.LogInformation("ClientSeq {ClientSeq} on {AccountId} taken by a sibling; re-deriving", clientSeq, accountId);
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

    public async Task<TransferResult> TransferAsync(
        Guid fromAccountId, Guid toAccountId, int amountCents, Guid eventId, CancellationToken ct = default)
    {
        if (fromAccountId == toAccountId)
            throw new ValidationException("Cannot transfer to the same account.");

        var (fromProjection, fromHit) = await CatchUpAsync(fromAccountId, eventId: eventId, ct: ct);
        var (toProjection, toHit) = await CatchUpAsync(toAccountId, eventId: eventId, ct: ct);
        if (await ResolveTransferHitsAsync(eventId, fromAccountId, toAccountId, fromHit, toHit, ct) is { } done)
            return done;

        var fromClientSeq = fromProjection.MaxClientSeq + 1;
        var toClientSeq = toProjection.MaxClientSeq + 1;
        var reDeriveCei = false;

        for (var attempt = 1; attempt <= MaxRetries; attempt++)
        {
            if (attempt > 1)
            {
                await Verify.Backoff(attempt, ct);
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
                await _pool.WriteAsync(writeRequest, ct);

                var newFromBalance = fromProjection.BalanceCents - amountCents;
                var newToBalance = toProjection.BalanceCents + amountCents;
                var newFromBatch = fromProjection.LastBatchIndex + 1;
                var newToBatch = toProjection.LastBatchIndex + 1;

                RecordWrite(fromAccountId, eventId, newFromBalance, newFromBatch,
                    fromProjection.LastBatchIndex, fromClientSeq);
                RecordWrite(toAccountId, eventId, newToBalance, newToBatch,
                    toProjection.LastBatchIndex, toClientSeq);

                return new TransferResult(
                    new WriteResult(newFromBalance, newFromBatch),
                    new WriteResult(newToBalance, newToBatch));
            }
            catch (WriteOccException)
            {
                _logger.LogDebug("OCC conflict on transfer {From}->{To}, attempt {Attempt}",
                    fromAccountId, toAccountId, attempt);
                reDeriveCei = true;
                continue;
            }
            catch (CeleriantTimeoutException)
            {
                _logger.LogWarning("Timeout on transfer {From}->{To}, attempt {Attempt}",
                    fromAccountId, toAccountId, attempt);
                continue;
            }
            catch (InflightDuplicateWriteException)
            {
                _logger.LogDebug("Inflight duplicate on transfer {From}->{To}, attempt {Attempt}",
                    fromAccountId, toAccountId, attempt);
                continue;
            }
            catch (IdempotencyViolationException)
            {
                // At least one leg's clientSeq was consumed; the error does not say
                // which. The write is all-or-nothing, so owning either leg proves the
                // whole transfer landed; a sibling owning a leg proves it did not.
                var fromOwner = await Verify.WhoOwnsSeqAsync(_pool, fromAccountId, fromClientSeq, eventId, ct);
                var verdict = fromOwner == SeqOwnership.Unwritten
                    ? await Verify.WhoOwnsSeqAsync(_pool, toAccountId, toClientSeq, eventId, ct)
                    : fromOwner;

                switch (verdict)
                {
                    case SeqOwnership.Ours:
                    {
                        _logger.LogInformation("Idempotency hit on transfer: prior attempt landed");
                        var (fp, fh) = await CatchUpAsync(fromAccountId, eventId: eventId, ct: ct);
                        var (tp, th) = await CatchUpAsync(toAccountId, eventId: eventId, ct: ct);
                        if (await ResolveTransferHitsAsync(eventId, fromAccountId, toAccountId, fh, th, ct) is { } vdone)
                            return vdone;
                        return new TransferResult(
                            new WriteResult(fp.BalanceCents, fp.LastBatchIndex),
                            new WriteResult(tp.BalanceCents, tp.LastBatchIndex));
                    }
                    case SeqOwnership.Sibling:
                        _logger.LogInformation("Transfer clientSeq taken by a sibling; re-deriving");
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
    /// leg proves the whole transfer landed. Reconstruct a missing leg (entry
    /// expired, or never folded on this replica) from current state.
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
        // Catch up first so projection is current.
        var (projection, _) = await CatchUpAsync(accountId, ct: ct);

        var key = Constants.AccountKey(accountId);
        try
        {
            var events = new List<object>();
            await foreach (var batch in _pool.ReadAllAsync(key, ReadFilters.From(fromBatchIndex ?? 1), ct))
            {
                foreach (var evt in batch.Events)
                    events.Add(AccountEvents.FormatEvent(batch, evt));
            }

            return (events.ToArray(), projection.LastBatchIndex, projection.BalanceCents);
        }
        catch (AggregateNotFoundException)
        {
            return ([], projection.LastBatchIndex, projection.BalanceCents);
        }
    }
}
