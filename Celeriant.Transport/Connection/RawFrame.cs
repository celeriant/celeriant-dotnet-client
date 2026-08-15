namespace Celeriant.Transport;

/// <summary>
/// A decoded wire frame: the response message-type id and its decompressed body bytes. The
/// connection deals only in raw frames; mapping the type to a product response (and decoding the
/// body) is the caller's job, via its own codec.
/// </summary>
public readonly record struct RawFrame(uint MessageType, byte[] Body);
