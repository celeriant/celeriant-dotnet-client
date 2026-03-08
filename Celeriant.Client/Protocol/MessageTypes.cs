namespace Celeriant.Client.Protocol;

/// <summary>
/// Wire protocol message type IDs for V3 (MessagePack) protocol.
/// These correspond to the message_type field in the 17-byte WireHeader.
/// </summary>
public static class MessageTypes
{
    // Request type IDs
    public static class Requests
    {
        public const uint AggregateDetails = 1;
        public const uint Read = 2;
        public const uint Write = 3;
        public const uint TrimStart = 4;
        public const uint Delete = 5;
        public const uint Watch = 6;
        public const uint ListOrgs = 7;
        public const uint ListAggregateTypes = 8;
        public const uint ListAggregates = 9;
        public const uint RegisterSchema = 10;
        public const uint Identify = 14;
    }

    // Response type IDs
    public static class Responses
    {
        public const uint AggregateDetails = 1;
        public const uint Read = 2;
        public const uint Write = 3;
        public const uint TrimStart = 4;
        public const uint Delete = 5;
        public const uint ProtocolError = 6;
        public const uint GenericError = 7;
        public const uint Watch = 8;
        public const uint ListOrgs = 9;
        public const uint ListAggregateTypes = 10;
        public const uint ListAggregates = 11;
        public const uint RegisterSchema = 12;
        public const uint Identify = 16;
    }
}
