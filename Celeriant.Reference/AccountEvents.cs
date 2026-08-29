using System.Security.Cryptography;
using System.Text;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Client.Serialization;

namespace Celeriant.Reference;

// Event types: same domain as the simple demo
public sealed record Deposited(int AmountCents);
public sealed record Withdrawn(int AmountCents);
public sealed record TransferredOut(int AmountCents, Guid ToAccountId);
public sealed record TransferredIn(int AmountCents, Guid FromAccountId);

/// <summary>
/// Shared event fold and formatting, used by every projection backend (Postgres
/// and in-memory) so they replay and render identically. Mirrors Rust's
/// <c>events.rs</c>.
/// </summary>
public static class AccountEvents
{
    private static readonly IEventSerializer Serializer = JsonEventSerializer.Default;

    /// <summary>
    /// Fold one event into the running balance. Unknown event types fold as a
    /// no-op: the demo favours staying up over halting on schema drift. A
    /// production fold should fail loudly instead; a silently skipped event is
    /// a silently wrong balance.
    /// </summary>
    public static long ReplayEvent(long balanceCents, AggregateEvent evt)
    {
        return evt.EventTypeMajor switch
        {
            1 => balanceCents + Serializer.Deserialize<Deposited>(evt.EventValue).AmountCents,
            2 => balanceCents - Serializer.Deserialize<Withdrawn>(evt.EventValue).AmountCents,
            3 => balanceCents - Serializer.Deserialize<TransferredOut>(evt.EventValue).AmountCents,
            4 => balanceCents + Serializer.Deserialize<TransferredIn>(evt.EventValue).AmountCents,
            _ => balanceCents,
        };
    }

    /// <summary>Render one event for the history endpoint.</summary>
    public static object FormatEvent(AggregateEventBatch batch, AggregateEvent evt)
    {
        switch (evt.EventTypeMajor)
        {
            case 1:
                return new
                {
                    batchIndex = batch.AggregateVersion,
                    type = "Deposited",
                    amountCents = Serializer.Deserialize<Deposited>(evt.EventValue).AmountCents,
                    timestamp = batch.ServerTimestamp,
                };
            case 2:
                return new
                {
                    batchIndex = batch.AggregateVersion,
                    type = "Withdrawn",
                    amountCents = Serializer.Deserialize<Withdrawn>(evt.EventValue).AmountCents,
                    timestamp = batch.ServerTimestamp,
                };
            case 3:
            {
                var t = Serializer.Deserialize<TransferredOut>(evt.EventValue);
                return new
                {
                    batchIndex = batch.AggregateVersion,
                    type = "TransferredOut",
                    amountCents = t.AmountCents,
                    toAccountId = t.ToAccountId,
                    timestamp = batch.ServerTimestamp,
                };
            }
            case 4:
            {
                var t = Serializer.Deserialize<TransferredIn>(evt.EventValue);
                return new
                {
                    batchIndex = batch.AggregateVersion,
                    type = "TransferredIn",
                    amountCents = t.AmountCents,
                    fromAccountId = t.FromAccountId,
                    timestamp = batch.ServerTimestamp,
                };
            }
            default:
                return new
                {
                    batchIndex = batch.AggregateVersion,
                    type = "Unknown",
                    amountCents = 0,
                    timestamp = batch.ServerTimestamp,
                };
        }
    }
}

public static class Constants
{
    private static readonly Guid Namespace = Guid.Parse("a1b2c3d4-e5f6-7890-abcd-ef1234567890");

    public static readonly Guid OrgId = DeterministicGuid("DemoOrg");
    public static readonly Guid AccountTypeId = DeterministicGuid("Account");

    /// <summary>
    /// Single service-owned ClientId. All API instances share this identity.
    /// ClientSeq is per (AggregateKey, ClientId): OCC serialises concurrent writes.
    /// </summary>
    public static readonly Guid ServiceClientId = DeterministicGuid("ReferenceApiService");

    public static readonly (string Name, Guid Id, int SeedCents)[] Accounts =
    [
        ("Alice", DeterministicGuid("Alice"), 50_000),
        ("Bob", DeterministicGuid("Bob"), 25_000),
        ("Charlie", DeterministicGuid("Charlie"), 10_000),
    ];

    public static AggregateKey AccountKey(Guid accountId) =>
        new(OrgId, AccountTypeId, accountId);

    public static Guid DeterministicGuid(string name)
    {
        var namespaceBytes = Namespace.ToByteArray();
        SwapGuidByteOrder(namespaceBytes);

        var nameBytes = Encoding.UTF8.GetBytes(name);
        var input = new byte[namespaceBytes.Length + nameBytes.Length];
        Buffer.BlockCopy(namespaceBytes, 0, input, 0, namespaceBytes.Length);
        Buffer.BlockCopy(nameBytes, 0, input, namespaceBytes.Length, nameBytes.Length);

        var hash = SHA1.HashData(input);
        var guidBytes = new byte[16];
        Array.Copy(hash, 0, guidBytes, 0, 16);

        guidBytes[6] = (byte)((guidBytes[6] & 0x0F) | 0x50); // version 5
        guidBytes[8] = (byte)((guidBytes[8] & 0x3F) | 0x80); // variant 10xx

        SwapGuidByteOrder(guidBytes);
        return new Guid(guidBytes);
    }

    private static void SwapGuidByteOrder(byte[] guid)
    {
        (guid[0], guid[3]) = (guid[3], guid[0]);
        (guid[1], guid[2]) = (guid[2], guid[1]);
        (guid[4], guid[5]) = (guid[5], guid[4]);
        (guid[6], guid[7]) = (guid[7], guid[6]);
    }
}
