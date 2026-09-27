using System.Runtime.CompilerServices;
using Celeriant.Client.Errors;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;

namespace Celeriant.Client.Streaming;

/// <summary>
/// Extension methods on <see cref="CeleriantClient"/> that expose paginated read operations
/// as <see cref="IAsyncEnumerable{T}"/> streams.
///
/// <para>
/// The server returns event batches in pages. When <see cref="ReadResponse.NextAggregateVersion"/>
/// is non-null, additional pages are available. These extensions handle the pagination loop
/// automatically, yielding each <see cref="AggregateEventBatch"/> as it arrives.
/// </para>
/// </summary>
public static class ReadExtensions
{
    /// <summary>
    /// Stream all event batches for an aggregate, automatically following pagination cursors.
    /// </summary>
    /// <param name="client">The client connection.</param>
    /// <param name="key">The aggregate to read from.</param>
    /// <param name="filters">Read filters (starting batch index, event type filters, etc.).</param>
    /// <param name="ct">Cancellation token.</param>
    public static IAsyncEnumerable<AggregateEventBatch> ReadAllAsync(
        this CeleriantClient client,
        AggregateKey key,
        ReadFilters? filters = null,
        CancellationToken ct = default)
        => PaginateAsync(client.ReadAsync, key, filters ?? ReadFilters.From(1), ct);

    /// <summary>
    /// Follow pagination cursors, fetching each page with <paramref name="readPage"/>. A page's
    /// batches are yielded only after its whole response has arrived, so re-fetching a lost page
    /// from the same cursor neither repeats nor skips a batch.
    /// </summary>
    internal static async IAsyncEnumerable<AggregateEventBatch> PaginateAsync(
        Func<ReadRequest, CancellationToken, Task<ReadResponse>> readPage,
        AggregateKey key,
        ReadFilters filters,
        [EnumeratorCancellation] CancellationToken ct)
    {
        long? nextIndex = Math.Max(1, filters.FromAggregateVersion);

        while (nextIndex is not null)
        {
            ct.ThrowIfCancellationRequested();

            var currentFilters = nextIndex == filters.FromAggregateVersion
                ? filters
                : filters with { FromAggregateVersion = nextIndex.Value };

            var response = await readPage(new ReadRequest
            {
                AggregateKey = key,
                Filters = currentFilters,
            }, ct).ConfigureAwait(false);

            foreach (var batch in response.EventBatches)
                yield return batch;

            nextIndex = response.NextAggregateVersion;
        }
    }
}
