namespace Celeriant.Client.Requests;

/// <summary>
/// Schema format for event validation.
/// Matches Rust <c>SchemaType</c> enum values.
/// </summary>
public enum SchemaType : byte
{
    Json = 0,
    Avro = 1,
    Protobuf = 2,
}
