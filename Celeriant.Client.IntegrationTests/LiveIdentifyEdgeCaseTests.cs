using System.Security.Cryptography;
using Celeriant.Client.Errors;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;

namespace Celeriant.Client.IntegrationTests;

/// <summary>
/// Identity handshake edge cases against the real Rust server.
/// </summary>
public sealed class LiveIdentifyEdgeCaseTests
{
    private static readonly TimeSpan Deadline = TimeSpan.FromSeconds(10);

    private static AggregateEvent[] OneEvent() =>
    [
        new() { ClientSeq = 1, EventTimestamp = DateTimeOffset.UtcNow, EventTypeMajor = 1, EventValue = "{\"m\":1}"u8.ToArray() },
    ];

    private static AggregateKey AnyKey() => new(Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid());

    /// <summary>
    /// A server with no API keys file rejects the pool's API key on every Identify, so the caller sees
    /// <see cref="AuthInvalidKeyException"/> on every write. A rejected Identify must not open the
    /// node's dial breaker, or the second write would see <see cref="PoolUnavailableException"/>.
    /// </summary>
    [SkippableFact]
    public async Task APoolWithARejectedApiKeyReportsTheTypedErrorOnTheSecondWriteToo()
    {
        Skip.If(RustServerProcess.SkipReason is not null, RustServerProcess.SkipReason);
        await using var server = await RustServerProcess.StartAsync(new RustServerOptions());
        await using var pool = new CeleriantPool(new CeleriantPoolOptions
        {
            Address = server.Address,
            IdentityConfig = ClientIdentityConfig.FromApiKey(Convert.ToBase64String(RandomNumberGenerator.GetBytes(32))),
            ConnectionTimeout = Deadline,
            RequestTimeout = Deadline,
        });

        Exception? first = await Record.ExceptionAsync(() => pool.WriteAsync(AnyKey(), OneEvent(), Guid.NewGuid()));
        Exception? second = await Record.ExceptionAsync(() => pool.WriteAsync(AnyKey(), OneEvent(), Guid.NewGuid()));

        Assert.IsType<AuthInvalidKeyException>(first);
        Assert.True(second is AuthInvalidKeyException,
            $"second write got {second?.GetType().Name}: {second?.Message}");
    }

    /// <summary>
    /// A standalone client that has already identified implicitly (its first request) and is then
    /// asked to <c>IdentifyAsync</c> sends a second Identify on the open connection. The server only
    /// accepts Identify as the first frame. The caller should get a clear local error, or the call
    /// should be a no-op; it must not leave a client that fails its next request.
    /// </summary>
    [SkippableFact]
    public async Task IdentifyAsyncAfterTheImplicitIdentifyDoesNotBreakTheClient()
    {
        Skip.If(RustServerProcess.SkipReason is not null, RustServerProcess.SkipReason);
        await using var server = await RustServerProcess.StartAsync(new RustServerOptions());
        await using var client = await CeleriantClient.ConnectAsync(server.Address, connectionTimeout: Deadline);
        client.WithTimeout(Deadline);

        await client.WriteAsync(AnyKey(), OneEvent(), Guid.NewGuid());
        Exception? identify = await Record.ExceptionAsync(() => client.IdentifyAsync(ClientIdentityConfig.FromClientId(Guid.NewGuid())));
        Exception? next = await Record.ExceptionAsync(() => client.WriteAsync(AnyKey(), OneEvent(), Guid.NewGuid()));

        Assert.True(next is null,
            $"IdentifyAsync after the implicit Identify -> {identify?.GetType().Name}: {identify?.Message}; "
            + $"then the next write -> {next?.GetType().Name}: {next?.Message}; poisoned={client.IsPoisoned}"
            + $"{Environment.NewLine}server log:{Environment.NewLine}{server.LogTail()}");
    }
}
