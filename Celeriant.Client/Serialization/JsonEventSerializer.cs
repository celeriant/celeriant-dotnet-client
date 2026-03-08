using System.Text.Json;

namespace Celeriant.Client.Serialization;

/// <summary>
/// JSON event serializer using <see cref="System.Text.Json"/>.
/// Zero additional dependencies — uses the BCL serializer.
/// </summary>
public sealed class JsonEventSerializer : IEventSerializer
{
    /// <summary>Shared instance with default <see cref="JsonSerializerOptions"/>.</summary>
    public static readonly JsonEventSerializer Default = new();

    private readonly JsonSerializerOptions _options;

    public JsonEventSerializer(JsonSerializerOptions? options = null)
        => _options = options ?? JsonSerializerOptions.Default;

    public byte[] Serialize<T>(T value)
        => JsonSerializer.SerializeToUtf8Bytes(value, _options);

    public T Deserialize<T>(ReadOnlySpan<byte> data)
        => JsonSerializer.Deserialize<T>(data, _options)!;
}
