using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when a write is rejected because an event payload does not conform to the
/// registered schema for its event type (error 2022).
/// </summary>
public class SchemaValidationException : SchemaErrorException
{
    /// <summary>
    /// The major event type version that failed validation.
    /// </summary>
    public long FailedEventTypeMajor { get; }

    /// <summary>
    /// The minor event type version that failed validation.
    /// </summary>
    public long FailedEventTypeMinor { get; }

    /// <summary>
    /// Zero-based position of the failing event within the request's events array — not the event's
    /// <see cref="AggregateEvent.ClientSeq"/>. Index the array you sent by this value.
    /// </summary>
    public long FailedEventIndex { get; }

    /// <summary>
    /// The validation error message describing why the payload does not conform to the schema.
    /// </summary>
    public string? FailedValidationError { get; }

    public SchemaValidationException(ErrorResponse error) : base(error)
    {
        FailedEventTypeMajor = error.GetLong("event_type_major") ?? 0;
        FailedEventTypeMinor = error.GetLong("event_type_minor") ?? 0;
        FailedEventIndex = error.GetLong("client_event_index") ?? 0;
        FailedValidationError = error.GetString("validation_error");
    }
}
