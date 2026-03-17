using System.Security.Cryptography;
using System.Text;
using Celeriant.Client.Requests;

namespace Celeriant.Demo.Domain;

public static class DemoConstants
{
    private static readonly Guid DemoNamespace = Guid.Parse("a1b2c3d4-e5f6-7890-abcd-ef1234567890");

    public static readonly Guid OrgId = DeterministicGuid("DemoOrg");
    public static readonly Guid AccountTypeId = DeterministicGuid("Account");

    public static readonly (string Name, Guid Id, int SeedCents)[] Accounts =
    [
        ("Alice", DeterministicGuid("Alice"), 50_000),
        ("Bob", DeterministicGuid("Bob"), 25_000),
        ("Charlie", DeterministicGuid("Charlie"), 10_000),
    ];

    public static readonly (string Name, Guid Id)[] Clients =
    [
        ("Machine A", DeterministicGuid("MachineA")),
        ("Machine B", DeterministicGuid("MachineB")),
    ];

    public static AggregateKey AccountKey(Guid accountId) =>
        new(OrgId, AccountTypeId, accountId);

    /// <summary>
    /// UUID v5-style deterministic GUID from a name string.
    /// </summary>
    public static Guid DeterministicGuid(string name)
    {
        var namespaceBytes = DemoNamespace.ToByteArray();
        SwapGuidByteOrder(namespaceBytes);

        var nameBytes = Encoding.UTF8.GetBytes(name);
        var input = new byte[namespaceBytes.Length + nameBytes.Length];
        Buffer.BlockCopy(namespaceBytes, 0, input, 0, namespaceBytes.Length);
        Buffer.BlockCopy(nameBytes, 0, input, namespaceBytes.Length, nameBytes.Length);

        var hash = SHA1.HashData(input);

        var guidBytes = new byte[16];
        Array.Copy(hash, 0, guidBytes, 0, 16);

        // Set version 5
        guidBytes[6] = (byte)((guidBytes[6] & 0x0F) | 0x50);
        // Set variant 10xx
        guidBytes[8] = (byte)((guidBytes[8] & 0x3F) | 0x80);

        SwapGuidByteOrder(guidBytes);
        return new Guid(guidBytes);
    }

    private static void SwapGuidByteOrder(byte[] guid)
    {
        // .NET Guid stores first 3 groups in little-endian; UUID is big-endian
        (guid[0], guid[3]) = (guid[3], guid[0]);
        (guid[1], guid[2]) = (guid[2], guid[1]);
        (guid[4], guid[5]) = (guid[5], guid[4]);
        (guid[6], guid[7]) = (guid[7], guid[6]);
    }
}
