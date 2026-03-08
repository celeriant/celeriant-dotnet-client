using Celeriant.Client.Requests;

namespace Celeriant.Client;

/// <summary>
/// Discriminated union of all request types that can be sent to a Celeriant server.
///
/// Each variant wraps its typed payload. Use pattern matching to dispatch:
/// <code>
/// var request = new ClientRequest.Write(new WriteRequest { ... });
/// uint messageType = request switch
/// {
///     ClientRequest.Write => MessageTypes.Requests.Write,
///     ...
/// };
/// </code>
/// </summary>
public abstract record ClientRequest
{
    private ClientRequest() { }

    public sealed record AggregateDetails(AggregateDetailsRequest Value) : ClientRequest;
    public sealed record Read(ReadRequest Value) : ClientRequest;
    public sealed record Write(WriteRequest Value) : ClientRequest;
    public sealed record TrimStart(TrimStartRequest Value) : ClientRequest;
    public sealed record Delete(DeleteRequest Value) : ClientRequest;
    public sealed record Watch(WatchRequest Value) : ClientRequest;
    public sealed record ListOrgs(ListOrgsRequest Value) : ClientRequest;
    public sealed record ListAggregateTypes(ListAggregateTypesRequest Value) : ClientRequest;
    public sealed record ListAggregates(ListAggregatesRequest Value) : ClientRequest;
    public sealed record RegisterSchema(RegisterSchemaRequest Value) : ClientRequest;
    public sealed record Identify(IdentifyRequest Value) : ClientRequest;
}
