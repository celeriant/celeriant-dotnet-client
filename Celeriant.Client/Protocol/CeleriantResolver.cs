using MessagePack;
using MessagePack.Formatters;
using MessagePack.Resolvers;

namespace Celeriant.Client.Protocol;

/// <summary>
/// Custom MessagePack resolver that registers Celeriant-specific formatters,
/// then falls back to the standard resolver chain.
///
/// All types decorated with <c>[MessagePackObject]</c> use
/// integer-keyed fields (array mode) matching Rust serde positional field order
/// </summary>
internal sealed class CeleriantResolver : IFormatterResolver
{
    public static readonly CeleriantResolver Instance = new();

    /// <summary>
    /// Pre-built serializer options for all Celeriant message types.
    /// </summary>
    public static readonly MessagePackSerializerOptions Options =
        MessagePackSerializerOptions.Standard
            .WithResolver(Instance)
            .WithCompression(MessagePackCompression.None);

    private CeleriantResolver() { }

    public IMessagePackFormatter<T>? GetFormatter<T>()
    {
        return FormatterCache<T>.Formatter;
    }

    private static class FormatterCache<T>
    {
        public static readonly IMessagePackFormatter<T>? Formatter = GetFormatterCore();

        private static IMessagePackFormatter<T>? GetFormatterCore()
        {
            // Guid formatters (binary 16-byte in-memory representation)
            if (typeof(T) == typeof(Guid))
                return (IMessagePackFormatter<T>)(object)CeleriantGuidFormatter.Instance;
            if (typeof(T) == typeof(Guid?))
                return (IMessagePackFormatter<T>)(object)CeleriantNullableGuidFormatter.Instance;

            // Guid collection formatters (each element as 16-byte binary)
            // Note: HashSet<Guid>? is a reference type so both nullable and non-nullable
            // use the same runtime type (HashSet<Guid>). The NullableGuidHashSetFormatter
            // handles null by writing nil, so it is safe to use for both.
            if (typeof(T) == typeof(HashSet<Guid>))
                return (IMessagePackFormatter<T>)(object)NullableGuidHashSetFormatter.Instance;

            // Fall back to the standard resolver chain
            // (handles [MessagePackObject] classes, primitives, arrays, etc.)
            return StandardResolver.Instance.GetFormatter<T>();
        }
    }
}
