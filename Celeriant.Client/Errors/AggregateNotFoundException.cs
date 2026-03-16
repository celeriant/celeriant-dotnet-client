using Celeriant.Client.Responses;

namespace Celeriant.Client.Errors;

/// <summary>
/// Thrown when an operation targets an aggregate that does not exist.
/// Applies to reads (1001), writes (2005), deletes (4000), trims (3000), and details (7001).
/// </summary>
public class AggregateNotFoundException : CeleriantErrorException
{
    public AggregateNotFoundException(ErrorResponse error) : base(error) { }
}
