namespace Celeriant.Client.Responses;

/// <summary>
/// Access level granted after identity verification.
/// Matches Rust <c>AccessLevel</c> enum discriminants.
/// </summary>
public enum AccessLevel : byte
{
    ReadWrite = 1,
    ReadOnly = 2,
}
