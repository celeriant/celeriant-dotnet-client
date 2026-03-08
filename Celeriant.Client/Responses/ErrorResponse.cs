using System.Text.Json;
using MessagePack;
using MessagePack.Formatters;
using Celeriant.Client.Protocol;

namespace Celeriant.Client.Responses;

[MessagePackObject]
public sealed class ErrorResponse
{
    // Well-known error codes
    public const uint WriteNotLeader = 2011;
    public const uint TrimNotLeader = 3005;
    public const uint DeleteNotLeader = 4006;
    public const uint IdentifyRequired = 10004;
    public const uint AuthRequired = 1001;
    public const uint AuthInvalidKey = 1002;
    public const uint AuthInsufficientPermissions = 1003;

    [Key(0)]
    [MessagePackFormatter(typeof(CeleriantNullableGuidFormatter))]
    public Guid? CorrelationId { get; init; }

    [Key(1)]
    public uint ErrorCode { get; init; }

    [Key(2)]
    public string ErrorMessage { get; init; } = "";

    [IgnoreMember]
    public bool IsNotLeader => ErrorCode is WriteNotLeader or TrimNotLeader or DeleteNotLeader;

    [IgnoreMember]
    public bool IsIdentityRequired => ErrorCode == IdentifyRequired;

    /// <summary>
    /// Attempt to parse a leader address from the error message JSON.
    /// The error message may be a JSON object like <c>{"leader_address":"host:port"}</c>.
    /// Returns null if parsing fails or the field is absent.
    /// </summary>
    public string? ParseLeaderAddress()
    {
        if (string.IsNullOrEmpty(ErrorMessage))
            return null;
        try
        {
            using JsonDocument doc = JsonDocument.Parse(ErrorMessage);
            if (doc.RootElement.TryGetProperty("leader_address", out JsonElement el))
                return el.GetString();
        }
        catch (JsonException)
        {
            // Not JSON - fall through
        }
        return null;
    }
}
