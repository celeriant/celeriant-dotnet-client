namespace Celeriant.Demo.Domain;

public sealed record Deposited(int AmountCents);
public sealed record Withdrawn(int AmountCents);
public sealed record TransferredOut(int AmountCents, Guid ToAccountId);
public sealed record TransferredIn(int AmountCents, Guid FromAccountId);
