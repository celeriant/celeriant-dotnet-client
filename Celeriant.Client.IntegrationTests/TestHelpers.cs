using Celeriant.Client.Requests;
using Celeriant.Client.Responses;

namespace Celeriant.Client.IntegrationTests;

/// <summary>
/// Shared helpers used across integration test classes.
/// </summary>
internal static class TestHelpers
{
    /// <summary>
    /// Build a fresh <see cref="AggregateKey"/> with random Guids to avoid cross-test collisions.
    /// </summary>
    public static AggregateKey NewKey() => new AggregateKey(
        orgId: Guid.NewGuid(),
        aggregateTypeId: Guid.NewGuid(),
        aggregateId: Guid.NewGuid());

    /// <summary>
    /// Build a minimal <see cref="WriteRequest"/> that writes one event to the given key.
    /// </summary>
    public static WriteRequest SingleEventWrite(
        AggregateKey key,
        byte[] eventValue,
        long clientEventIndex = 1,
        bool allowCreate = true)
    {
        return new WriteRequest
        {
            ClientId = Guid.NewGuid(),
            Writes = new Dictionary<AggregateKey, SingleAggregateWrite>
            {
                [key] = new SingleAggregateWrite
                {
                    AllowCreate = allowCreate,
                    Events = new[]
                    {
                        new AggregateEvent
                        {
                            ClientEventIndex = clientEventIndex,
                            EventIndex = 0,
                            EventTimestamp = DateTimeOffset.UtcNow,
                            EventTypeMajor = 1,
                            EventTypeMinor = 0,
                            EventValue = eventValue,
                        }
                    }
                }
            }
        };
    }

    /// <summary>
    /// Build a <see cref="WriteRequest"/> with multiple events.
    /// </summary>
    public static WriteRequest MultiEventWrite(
        AggregateKey key,
        IReadOnlyList<byte[]> eventValues,
        bool allowCreate = true)
    {
        var events = eventValues
            .Select((ev, i) => new AggregateEvent
            {
                ClientEventIndex = i + 1,
                EventIndex = 0,
                EventTimestamp = DateTimeOffset.UtcNow,
                EventTypeMajor = 1,
                EventTypeMinor = 0,
                EventValue = ev,
            })
            .ToArray();

        return new WriteRequest
        {
            ClientId = Guid.NewGuid(),
            Writes = new Dictionary<AggregateKey, SingleAggregateWrite>
            {
                [key] = new SingleAggregateWrite
                {
                    AllowCreate = allowCreate,
                    Events = events
                }
            }
        };
    }

    /// <summary>
    /// Build a <see cref="ReadRequest"/> reading from event batch index 1 (all events).
    /// </summary>
    public static ReadRequest ReadAllRequest(AggregateKey key) => new ReadRequest
    {
        AggregateKey = key,
        Filters = ReadFilters.From(1)
    };

    /// <summary>
    /// Build an <see cref="AggregateDetailsRequest"/> for the given key.
    /// </summary>
    public static AggregateDetailsRequest DetailsRequest(AggregateKey key) =>
        new AggregateDetailsRequest { AggregateKey = key };
}
