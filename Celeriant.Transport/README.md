# Celeriant.Transport

Shared wire transport for Celeriant .NET clients: length-prefixed framing, zstd
dictionary compression, RSA identity crypto, and a generic per-node connection pool
with circuit breaking and idle eviction.

You normally don't reference this package directly: `Celeriant.Client` depends on it
and exposes the storage-facing API. Reference it yourself only when building a new
Celeriant product client on the shared wire protocol; implement
`ITransportExceptionFactory` to surface transport failures as your product's own
exception types.

- [Celeriant](https://celeriant.io)
- [GitHub](https://github.com/celeriant/celeriant-dotnet-client)

## License

Apache 2.0
