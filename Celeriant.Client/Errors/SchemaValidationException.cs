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
    /// The client event index of the event that failed validation within the batch.
    /// </summary>
    public long FailedClientSeq { get; }

    /// <summary>
    /// The validation error message describing why the payload does not conform to the schema.
    /// </summary>
    public string? FailedValidationError { get; }

    public SchemaValidationException(ErrorResponse error) : base(error)
    {
        FailedEventTypeMajor = error.GetLong("event_type_major") ?? 0;
        FailedEventTypeMinor = error.GetLong("event_type_minor") ?? 0;
        FailedClientSeq = error.GetLong("client_event_index") ?? 0;
        FailedValidationError = error.GetString("validation_error");
    }
}
