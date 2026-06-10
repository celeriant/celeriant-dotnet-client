using Celeriant.Client;
using Celeriant.Client.Errors;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;

namespace Celeriant.Reference;

public enum SeqOwnership
{
    /// <summary>The contested seq carries our event id: the prior attempt landed.</summary>
    Ours,

    /// <summary>A sibling request consumed the seq; our event never landed.</summary>
    Sibling,

    /// <summary>
    /// No event holds this seq for our client id. After a single-aggregate
    /// IdempotencyViolation this is inconsistent; in a transfer it means the
    /// violation was on the other leg.
    /// </summary>
    Unwritten,
}

public static class Verify
{
    /// <summary>
    /// How long a request's event id stays resolvable to its original response.
    /// Past this window a retried request writes a fresh event. The window is a
    /// stated property of the API, not a safety boundary; the server's ClientSeq
    /// check is what prevents double-writes.
    /// </summary>
    public static readonly TimeSpan DedupWindow = TimeSpan.FromSeconds(90);

    /// <summary>
    /// Who owns a contested ClientSeq? Answered from the stream itself, so it
    /// works on any replica with no shared state. The seq filters match on batch
    /// metadata, so every batch except the one holding the seq is skipped without
    /// reading its events. This is the error path: an IdempotencyViolation already
    /// told us the seq is consumed and durable; the only question is by whom.
    /// </summary>
    public static async Task<SeqOwnership> WhoOwnsSeqAsync(
        ICeleriantPool pool, Guid accountId, long clientSeq, Guid eventId, CancellationToken ct = default)
    {
        ReadResponse resp;
        try
        {
            resp = await pool.ReadAsync(new ReadRequest
            {
                AggregateKey = Constants.AccountKey(accountId),
                Filters = ReadFilters.From(1) with
                {
                    MinClientSeq = clientSeq,
                    MaxClientSeq = clientSeq,
                    IncludeClientId = Constants.ServiceClientId,
                },
            }, ct);
        }
        catch (AggregateNotFoundException)
        {
            // Only reachable when a lagging read replica hides the aggregate. Lag
            // can only hide events, never misattribute them, so the safe verdict
            // is "not visible yet"; the caller surfaces a retryable error rather
            // than guessing.
            return SeqOwnership.Unwritten;
        }

        var evt = resp.EventBatches.SelectMany(b => b.Events)
            .FirstOrDefault(e => e.ClientSeq == clientSeq);
        if (evt is null)
            return SeqOwnership.Unwritten;
        return evt.EventId == eventId ? SeqOwnership.Ours : SeqOwnership.Sibling;
    }
}
