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
        => ReadAllAsyncCore(client, key, filters ?? ReadFilters.From(1), ct);

    private static async IAsyncEnumerable<AggregateEventBatch> ReadAllAsyncCore(
        CeleriantClient client,
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

            var response = await client.ReadAsync(new ReadRequest
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
