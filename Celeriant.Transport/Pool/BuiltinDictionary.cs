using System.Security.Cryptography;

namespace Celeriant.Transport;

/// <summary>
/// The json-web-events-v1 compression dictionary every Celeriant server and client links. Bundled
/// byte-identical to the Rust server's <c>celeriant_wal/dicts/json_web_events_v1.zstd_dict</c>, so a
/// fresh connection can advertise its sha and a built-in server never ships the bytes.
/// </summary>
public static class BuiltinDictionary
{
    public const string Name = "json-web-events-v1";

    private const string ResourceName = "Celeriant.Transport.Dicts.json_web_events_v1.zstd_dict";

    /// <summary>The built-in dictionary and its lowercase hex SHA-256. One instance per process.</summary>
    public static CachedDict Dict { get; } = Load();

    private static CachedDict Load()
    {
        using Stream stream = typeof(BuiltinDictionary).Assembly.GetManifestResourceStream(ResourceName)
            ?? throw new InvalidOperationException(
                $"Embedded resource '{ResourceName}' is missing from Celeriant.Transport; the package is broken.");
        var bytes = new byte[stream.Length];
        stream.ReadExactly(bytes);
        return new CachedDict(Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant(), bytes);
    }
}
