using Celeriant.Client.Responses;

namespace Celeriant.Client.Serialization;

/// <summary>
/// Extension methods for creating and reading <see cref="AggregateEvent"/> payloads
/// using an <see cref="IEventSerializer"/>.
/// </summary>
public static class AggregateEventExtensions
{
    /// <summary>
    /// Create an <see cref="AggregateEvent"/> by serializing <paramref name="payload"/>
    /// with the given <paramref name="serializer"/>.
    /// </summary>
    /// <param name="eventTypeMajor">Major event type identifier (maps to a registered schema).</param>
    /// <param name="payload">The event payload object to serialize.</param>
    /// <param name="serializer">Serializer to encode the payload to bytes.</param>
    /// <param name="clientSeq">Client-assigned index within the batch. <b>Defaults to 1</b>, so if you
    /// build several events for one write with repeated <c>Create</c> calls you must pass a distinct,
    /// increasing value to each (1, 2, 3, …): left at the default they all share seq 1. The server does
    /// not dedupe duplicate seqs within one write, so a collision silently corrupts per-seq idempotency
    /// tracking; the client rejects it with <c>ArgumentException</c> when <c>EnforceClientIdempotency</c>
    /// is set. A single-event write can keep the default.</param>
    /// <param name="eventTypeMinor">Minor event type identifier. Defaults to 0.</param>
    /// <param name="eventId">Optional client-assigned event ID for deduplication.</param>
    /// <param name="timestamp">Event timestamp. Defaults to <see cref="DateTimeOffset.UtcNow"/>.</param>
    public static AggregateEvent Create<T>(
        long eventTypeMajor,
        T payload,
        IEventSerializer serializer,
        long clientSeq = 1,
        long eventTypeMinor = 0,
        Guid? eventId = null,
        DateTimeOffset? timestamp = null)
        => new()
        {
            ClientSeq = clientSeq,
            EventSeq = 0,
            EventTimestamp = timestamp ?? DateTimeOffset.UtcNow,
            EventTypeMajor = eventTypeMajor,
            EventTypeMinor = eventTypeMinor,
            EventId = eventId,
            EventValue = serializer.Serialize(payload),
        };

    /// <summary>
    /// Deserialize the <see cref="AggregateEvent.EventValue"/> payload to <typeparamref name="T"/>.
    /// </summary>
    public static T GetValue<T>(this AggregateEvent evt, IEventSerializer serializer)
        => serializer.Deserialize<T>(evt.EventValue);
}
