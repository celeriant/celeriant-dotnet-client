using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when the server returns an error during a delete operation.
/// </summary>
public class DeleteErrorException(ErrorResponse error) : CeleriantErrorException(error);
