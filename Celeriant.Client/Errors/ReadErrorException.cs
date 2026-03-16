using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when the server returns an error during a read or aggregate-details operation.
/// </summary>
public class ReadErrorException(ErrorResponse error) : CeleriantErrorException(error);
