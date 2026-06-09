using Celeriant.Client.Responses;

namespace Celeriant.Client;

/// <summary>
/// Discriminated union of all response types received from a Celeriant server.
///
/// Transport-level and protocol errors throw exceptions; server-side errors
/// (including auth errors that are not special-cased) are returned as
/// <see cref="GenericError"/> or <see cref="ProtocolError"/>.
/// </summary>
public abstract record ClientResponse
{
    private ClientResponse() { }

    public sealed record AggregateDetails(AggregateDetailsResponse Value) : ClientResponse;
    public sealed record Read(ReadResponse Value) : ClientResponse;
    public sealed record Write(WriteResponse Value) : ClientResponse;
    public sealed record TrimStart(SuccessResponse Value) : ClientResponse;
    public sealed record Delete(SuccessResponse Value) : ClientResponse;
    public sealed record GenericError(ErrorResponse Value) : ClientResponse;
    public sealed record ProtocolError(ProtocolErrorResponse Value) : ClientResponse;
    public sealed record Watch(WatchResponse Value) : ClientResponse;
    public sealed record ListOrgs(ListOrgsResponse Value) : ClientResponse;
    public sealed record ListAggregateTypes(ListAggregateTypesResponse Value) : ClientResponse;
    public sealed record ListAggregates(ListAggregatesResponse Value) : ClientResponse;
    public sealed record RegisterSchema(SuccessResponse Value) : ClientResponse;
    public sealed record Identify(IdentifyResponse Value) : ClientResponse;
}
