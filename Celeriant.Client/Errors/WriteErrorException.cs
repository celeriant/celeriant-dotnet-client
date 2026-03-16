using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when the server returns an error during a write operation.
/// </summary>
public class WriteErrorException(ErrorResponse error) : CeleriantErrorException(error);
