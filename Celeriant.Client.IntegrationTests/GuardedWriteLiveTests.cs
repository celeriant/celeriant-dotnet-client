using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;

namespace Celeriant.Client.IntegrationTests;

/// <summary>
/// Guarded writes and complete OCC conflicts against a real server: a rejected batch changes no
/// aggregate, and the conflict list names every stale pin in key order.
/// </summary>
[Collection("Server")]
public sealed class GuardedWriteLiveTests
{
    private readonly ServerFixture _fixture;
    private readonly Guid _writer = Guid.NewGuid();

    public GuardedWriteLiveTests(ServerFixture fixture) => _fixture = fixture;

    [SkippableTheory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Rejected_batch_reports_every_conflict_in_key_order_and_changes_nothing(bool delete)
    {
        await using var pool = CreatePool();
        var org = Guid.NewGuid();
        var type = Guid.NewGuid();
        var keys = new[] { 255, 256, 257 }.Select(id => new AggregateKey(org, type,
            Guid.Parse($"00000000-0000-0000-0000-{id:x12}"))).ToArray();
        var created = new List<AggregateKey>();
        try
        {
            for (var i = 0; i < keys.Length; i++)
                for (var n = 0; n <= i; n++)
                {
                    await pool.WriteAsync(Write(new() { [keys[i]] = Append(n, create: n == 0) }));
                    if (n == 0) created.Add(keys[i]);
                }
            var before = new List<byte[]>();
            foreach (var key in keys) before.Add(await History(pool, key));
            Exception? error;
            if (delete)
                error = await Record.ExceptionAsync(() => pool.DeleteAsync(new DeleteRequest
                {
                    ClientId = _writer,
                    Deletes = keys.Reverse().ToDictionary(k => k, k => new SingleAggregateDelete
                    {
                        ExpectedVersion = 10 + Array.IndexOf(keys, k),
                    }),
                }));
            else
                error = await Record.ExceptionAsync(() => pool.WriteAsync(Write(
                    keys.Reverse().ToDictionary(k => k, k => Append(10 + Array.IndexOf(keys, k))))));
            AssertConflicts(error, delete, keys, 10, 1);
            for (var i = 0; i < keys.Length; i++) Assert.Equal(before[i], await History(pool, keys[i]));
        }
        finally { if (created.Count != 0) await Cleanup(pool, created.ToArray()); }
    }

    [SkippableFact]
    public async Task Guards_do_not_append_and_a_rejected_guarded_write_changes_nothing()
    {
        await using var pool = CreatePool();
        var org = Guid.NewGuid();
        var type = Guid.NewGuid();
        var fence = new AggregateKey(org, type, Guid.NewGuid());
        var target = new AggregateKey(org, type, Guid.NewGuid());
        var created = false;
        try
        {
            await pool.WriteAsync(Write(new() { [fence] = Append(0, true), [target] = Append(0, true) }));
            created = true;
            var originalFence = await History(pool, fence);
            var guardOnly = await pool.WriteAsync(Write(new() { [fence] = SingleAggregateWrite.Guard(1) }));
            Assert.Null(guardOnly.MaxAggregateVersion);
            var appended = await pool.WriteAsync(Write(new()
            {
                [fence] = SingleAggregateWrite.Guard(1),
                [target] = Append(1),
            }));
            Assert.Equal(2, appended.MaxAggregateVersion);
            Assert.Equal(originalFence, await History(pool, fence));
            var originalTarget = await History(pool, target);
            var error = await Record.ExceptionAsync(() => pool.WriteAsync(Write(new()
            {
                [fence] = SingleAggregateWrite.Guard(0),
                [target] = Append(2),
            })));
            AssertConflicts(error, false, [fence], 0, 1);
            Assert.Equal(originalFence, await History(pool, fence));
            Assert.Equal(originalTarget, await History(pool, target));
        }
        finally { if (created) await Cleanup(pool, [fence, target]); }
    }

    private CeleriantPool CreatePool()
    {
        Skip.If(!_fixture.IsAvailable, "Server not running");
        return new(new CeleriantPoolOptions { Address = _fixture.Address });
    }

    private static SingleAggregateWrite Append(long expected, bool create = false) => new()
    {
        ExpectedVersion = expected, AllowCreate = create,
        Events = [new() { ClientSeq = 1, EventTimestamp = DateTimeOffset.UtcNow,
            EventTypeMajor = 1, EventValue = [42] }],
    };

    private WriteRequest Write(Dictionary<AggregateKey, SingleAggregateWrite> writes) =>
        new() { ClientId = _writer, Writes = writes };

    private static void AssertConflicts(Exception? error, bool delete, AggregateKey[] keys,
        long expected, long current)
    {
        var conflicts = delete
            ? Assert.IsType<DeleteOccException>(error).Conflicts
            : Assert.IsType<WriteOccException>(error).Conflicts;
        Assert.Equal(keys.Select((k, i) => new AggregateConflict(k, expected + i, current + i)), conflicts);
    }

    private static async Task<byte[]> History(CeleriantPool pool, AggregateKey key) =>
        WireCodec.Serialize((await pool.ReadAsync(new ReadRequest
        {
            AggregateKey = key, Filters = ReadFilters.From(1),
        })).EventBatches);

    private Task Cleanup(CeleriantPool pool, AggregateKey[] keys) => pool.DeleteAsync(new DeleteRequest
    {
        ClientId = _writer,
        Deletes = keys.ToDictionary(k => k, _ => new SingleAggregateDelete()),
    });
}
