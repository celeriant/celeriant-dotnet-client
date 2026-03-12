using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when the server returns an error during schema registration or schema validation.
/// </summary>
public class SchemaErrorException : CeleriantErrorException
{
    /// <summary>
    /// The event type major version involved in the error.
    /// </summary>
    public long? EventTypeMajor { get; }

    /// <summary>
    /// The event type minor version involved in the error.
    /// </summary>
    public long? EventTypeMinor { get; }

    /// <summary>
    /// The schema type that was invalid or unsupported (error 2021, 2024).
    /// </summary>
    public long? SchemaType { get; }

    /// <summary>
    /// The schema parse or compilation error detail (error 2021, 2023).
    /// </summary>
    public string? ParseError { get; }

    /// <summary>
    /// The validation error when an event fails schema validation (error 2022).
    /// </summary>
    public string? ValidationError { get; }

    /// <summary>
    /// The client event index of the event that failed schema validation (error 2022).
    /// </summary>
    public long? ClientEventIndex { get; }

    public SchemaErrorException(ErrorResponse error) : base(error)
    {
        EventTypeMajor = error.GetLong("event_type_major");
        EventTypeMinor = error.GetLong("event_type_minor");
        SchemaType = error.GetLong("schema_type");
        ParseError = error.GetString("parse_error") ?? error.GetString("compilation_error");
        ValidationError = error.GetString("validation_error");
        ClientEventIndex = error.GetLong("client_event_index");
    }
}
