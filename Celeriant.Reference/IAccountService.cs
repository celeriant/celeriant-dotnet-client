namespace Celeriant.Reference;

/// <summary>Result of a two-legged transfer write.</summary>
public sealed record TransferResult(WriteResult From, WriteResult To);

/// <summary>
/// The account projection service. Two backends implement it — a Postgres-backed
/// projection (<see cref="AccountService"/>) and an in-process one
/// (<see cref="MemAccountService"/>) — chosen at startup by the
/// <c>Projection</c> configuration key. They share the same write loop and dedup
/// guarantees; they differ only in where the projection cursor (and therefore
/// the request-dedup index) lives. Mirrors Rust's <c>Backend</c> enum in
/// <c>main.rs</c>.
/// </summary>
public interface IAccountService
{
    Task<CatchUpResult> CatchUpAsync(
        Guid accountId,
        long? minBatchIndex = null,
        Guid? eventId = null,
        CancellationToken ct = default);

    Task<WriteResult> DepositAsync(
        Guid accountId, int amountCents, Guid eventId, CancellationToken ct = default);

    Task<WriteResult> WithdrawAsync(
        Guid accountId, int amountCents, Guid eventId, CancellationToken ct = default);

    Task<TransferResult> TransferAsync(
        Guid fromAccountId, Guid toAccountId, int amountCents, Guid eventId, CancellationToken ct = default);

    Task<(object[] Events, long CurrentBatchIndex, long BalanceCents)> GetHistoryAsync(
        Guid accountId, long? fromBatchIndex = null, CancellationToken ct = default);
}
