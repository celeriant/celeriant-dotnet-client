namespace Celeriant.Transport;

/// <summary>
/// Product-neutral outcome of decoding an Identify response. The connection uses
/// <see cref="DictSha"/>/<see cref="DictBytes"/> to negotiate the per-connection compression
/// dictionary and returns <see cref="ClientId"/> to the caller.
/// </summary>
public readonly record struct IdentifyResult(
    Guid? ClientId,
    byte? AccessLevel,
    string? DictSha,
    byte[]? DictBytes);
