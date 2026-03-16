using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when the server returns an error during schema registration or schema validation.
/// </summary>
public class SchemaErrorException(ErrorResponse error) : CeleriantErrorException(error);
