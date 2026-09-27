using Celeriant.Client.Requests;
namespace Celeriant.Client.Responses;

/// <summary>A failed version pin, in unsigned numeric aggregate-key order.</summary>
public sealed record AggregateConflict(AggregateKey Key, long Expected, long Current);
