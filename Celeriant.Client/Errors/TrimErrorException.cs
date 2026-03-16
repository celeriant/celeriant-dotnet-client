using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when the server returns an error during a trim-start operation.
/// </summary>
public class TrimErrorException(ErrorResponse error) : CeleriantErrorException(error);
