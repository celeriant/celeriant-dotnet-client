using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when a write targets a deleted aggregate that was deleted with <c>AllowRecreate = false</c> (error 2006).
/// This aggregate is permanently deleted and cannot accept new events.
/// </summary>
public class AggregateRecreateNotAllowedException(ErrorResponse error) : WriteErrorException(error);
