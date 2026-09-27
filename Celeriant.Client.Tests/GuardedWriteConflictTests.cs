using Celeriant.Client.Errors;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Transport;
using static Celeriant.Client.Tests.OccTestData;

namespace Celeriant.Client.Tests;

public sealed class GuardedWriteConflictTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Complete_binary_conflicts_preserve_keys_scalars_and_connection(bool delete)
    {
        await using var server = new RecordingFrameServer(frame => Task.FromResult(
            frame.Type is 3 or 5
                ? new FrameReply(delete ? 14u : 13u, ConflictBody(Correlation, Keys))
                : Success(frame)));
        await using var pool = Pool(server.Address);
        for (var attempt = 0; attempt < 2; attempt++)
        {
            var error = await Record.ExceptionAsync(() => Mutate(pool, delete));
            AssertConflicts(error, delete, Keys, 10, 20);
        }
        Assert.Equal(2, server.Frames.Count(f => f.Type is 3 or 5));
        Assert.Single(server.Frames.Select(f => f.Connection).Distinct());
        AssertHandshakes(server);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Direct_client_checks_version_before_mutation(bool legacyServer)
    {
        await using var server = new RecordingFrameServer(frame => Task.FromResult(
            frame.Type == 14
                ? Identify(frame, legacyServer ? 3u : 5u)
                : Success(frame, legacyServer ? 3u : 5u)));
        var failure = await Record.ExceptionAsync(async () =>
        {
            await using var client = await CeleriantClient.ConnectAsync(server.Address,
                connectionTimeout: TimeSpan.FromSeconds(2));
            await client.WriteAsync(Write());
        });
        if (legacyServer)
        {
            Assert.NotNull(failure);
            Assert.DoesNotContain(server.Frames, f => f.Type == 3);
        }
        else
        {
            Assert.Null(failure);
            Assert.Contains(server.Frames, f => f.Type == 3);
        }
        AssertHandshakes(server);
    }

    [Fact]
    public async Task Pool_reconnect_checks_version_before_any_further_mutation()
    {
        await using var server = new RecordingFrameServer(frame => Task.FromResult(
            frame.Type == 14 ? Identify(frame, frame.Connection == 1 ? 5u : 3u)
                : frame.Type == 3 ? new FrameReply(13, [0x92, 0xc0, 0x90]) : Success(frame)));
        await using var pool = Pool(server.Address);
        Assert.NotNull(await Record.ExceptionAsync(() => pool.WriteAsync(Write())));
        Assert.NotNull(await Record.ExceptionAsync(() => pool.WriteAsync(Write())));
        Assert.Equal(1, server.Frames.Count(f => f.Type == 3));
        Assert.True(server.Frames.Select(f => f.Connection).Distinct().Count() >= 2);
        AssertHandshakes(server);
    }

    [Fact]
    public async Task Mid_connection_version_change_is_not_accepted_or_replayed()
    {
        var mutations = 0;
        await using var server = new RecordingFrameServer(frame => Task.FromResult(
            frame.Type == 3 ? Success(frame, Interlocked.Increment(ref mutations) == 2 ? 3u : 5u)
                : Success(frame)));
        await using var pool = Pool(server.Address);
        await pool.WriteAsync(Write());
        AssertProtocolFailure(await Record.ExceptionAsync(() => pool.WriteAsync(Write())));
        Assert.Equal(2, mutations);
        await pool.WriteAsync(Write());
        Assert.Equal(3, mutations);
        Assert.Equal(2, server.Frames.Where(f => f.Type == 3).Select(f => f.Connection).Distinct().Count());
        AssertHandshakes(server);
    }

    /// <summary>
    /// The fixture rows replay the frozen V5 conflict frames, whose versions are all u64::MAX.
    /// </summary>
    [Theory]
    [InlineData("correlation", false)]
    [InlineData("operation", false)]
    [InlineData("truncated", false)]
    [InlineData("expected-overflow", false)]
    [InlineData("current-overflow", false)]
    [InlineData("legacy-json", false)]
    [InlineData("fixture-single", false)]
    [InlineData("fixture-many", false)]
    [InlineData("correlation", true)]
    [InlineData("operation", true)]
    [InlineData("truncated", true)]
    [InlineData("expected-overflow", true)]
    [InlineData("current-overflow", true)]
    [InlineData("legacy-json", true)]
    [InlineData("fixture-single", true)]
    [InlineData("fixture-many", true)]
    public async Task Invalid_conflicts_are_protocol_failures_retire_connection_and_never_replay(string defect, bool delete)
    {
        var fixture = defect.StartsWith("fixture-")
            ? await FrozenFixtureBody($"v5-{(delete ? "delete" : "write")}-{defect["fixture-".Length..]}")
            : null;
        var mutations = 0;
        await using var server = new RecordingFrameServer(frame =>
        {
            if (frame.Type is not (3 or 5)) return Task.FromResult(Success(frame));
            if (Interlocked.Increment(ref mutations) != 1) return Task.FromResult(Success(frame));
            var body = fixture ?? ConflictBody(defect == "correlation" ? Guid.Empty : Correlation, Keys,
                defect == "expected-overflow" ? ulong.MaxValue : 10,
                defect == "current-overflow" ? ulong.MaxValue : 20);
            if (defect == "truncated") body = body[..^1];
            uint type = delete ? 14u : 13u;
            if (defect == "operation") type = delete ? 13u : 14u;
            if (defect == "legacy-json")
            {
                type = 7;
                body = WireCodec.Serialize(new ErrorResponse
                {
                    CorrelationId = Correlation, ErrorCode = delete ? 4002u : 2003u,
                    ErrorMessage = "{\"expected_version\":10,\"current_aggregate_version\":20}",
                });
            }
            return Task.FromResult(new FrameReply(type, body));
        });
        await using var pool = Pool(server.Address);
        var error = await Record.ExceptionAsync(() => Mutate(pool, delete));
        AssertProtocolFailure(error);
        Assert.Equal(1, mutations);
        await Mutate(pool, delete);
        Assert.Equal(2, mutations);
        Assert.Equal(2, server.Frames.Where(f => f.Type is 3 or 5).Select(f => f.Connection).Distinct().Count());
    }

    [Theory]
    [InlineData(false, false, 0L, true)]
    [InlineData(false, false, 7L, true)]
    [InlineData(false, false, null, false)]
    [InlineData(true, false, 0L, false)]
    [InlineData(false, true, 0L, false)]
    [InlineData(true, true, 0L, false)]
    public async Task Guard_validation_accepts_only_pinned_empty_entries_without_forbidden_flags(
        bool create, bool idempotency, long? expected, bool valid)
    {
        await using var server = new RecordingFrameServer(frame => Task.FromResult(Success(frame)));
        await using var pool = Pool(server.Address);
        var request = Write(new Dictionary<AggregateKey, SingleAggregateWrite>
        {
            [Keys[0]] = new() { Events = [], ExpectedVersion = expected,
                AllowCreate = create, EnforceClientIdempotency = idempotency },
        });
        var error = await Record.ExceptionAsync(() => pool.WriteAsync(request));
        if (valid) Assert.Null(error);
        else Assert.IsAssignableFrom<ArgumentException>(error);
        Assert.Equal(valid ? 1 : 0, server.Frames.Count(f => f.Type == 3));
    }

    /// <summary>
    /// A conflict body is 25 bytes plus 74 per pinned key; that must fit the response ceiling
    /// before the request is sent, because the server may answer with every pin.
    /// </summary>
    [Theory]
    [InlineData(false, true, 98, false)]
    [InlineData(false, true, 99, true)]
    [InlineData(true, true, 98, false)]
    [InlineData(true, true, 99, true)]
    [InlineData(false, false, 24, false)]
    [InlineData(false, false, 25, true)]
    [InlineData(true, false, 24, false)]
    [InlineData(true, false, 25, true)]
    public async Task Receive_capacity_is_reserved_before_sending_even_when_pin_would_succeed(
        bool delete, bool pinned, long ceiling, bool fits)
    {
        await using var server = new RecordingFrameServer(frame => Task.FromResult(Success(frame)));
        await using var pool = Pool(server.Address, ceiling);
        var error = await Record.ExceptionAsync(() => Mutate(pool, delete, [Keys[0]], pinned ? 10 : null));
        if (fits) Assert.Null(error);
        else Assert.IsType<ArgumentException>(error);
        Assert.Equal(fits ? 1 : 0, server.Frames.Count(f => f.Type is 3 or 5));
    }

    private static void AssertConflicts(Exception? error, bool delete, AggregateKey[] keys,
        long expected, long current)
    {
        IReadOnlyList<AggregateConflict> conflicts;
        if (delete)
        {
            var typed = Assert.IsType<DeleteOccException>(error);
            Assert.Equal(4002u, typed.Error.ErrorCode);
            Assert.Equal(expected, typed.ExpectedVersion);
            Assert.Equal(current, typed.CurrentAggregateVersion);
            conflicts = typed.Conflicts;
        }
        else
        {
            var typed = Assert.IsType<WriteOccException>(error);
            Assert.Equal(2003u, typed.Error.ErrorCode);
            Assert.Equal(expected, typed.ExpectedVersion);
            Assert.Equal(current, typed.CurrentAggregateVersion);
            conflicts = typed.Conflicts;
        }
        Assert.Equal(keys.Select((k, i) => new AggregateConflict(k, expected + i, current + i)), conflicts);
    }

    private static void AssertProtocolFailure(Exception? failure)
    {
        Assert.NotNull(failure);
        var chain = new List<Exception>();
        for (var e = failure; e is not null; e = e.InnerException) chain.Add(e);
        Assert.Contains(chain, e => e is ProtocolException);
        Assert.DoesNotContain(chain, e => e is WriteOccException or DeleteOccException);
    }

    private static void AssertHandshakes(RecordingFrameServer server)
    {
        Assert.NotEmpty(server.Frames);
        foreach (var connection in server.Frames.GroupBy(f => f.Connection))
        {
            Assert.Equal(14u, connection.First().Type);
            Assert.All(connection, frame => Assert.Equal(5u, frame.Version));
        }
    }

    private static async Task<byte[]> FrozenFixtureBody(string name)
    {
        var root = Environment.GetEnvironmentVariable("CELERIANT_PROTOCOL_FIXTURES")
            ?? Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "../../../../fixtures/client-protocol"));
        var frame = await File.ReadAllBytesAsync(Path.Combine(root, $"{name}.frame.bin"));
        Assert.Equal(5u, WireHeader.ParseFrom(frame).Version);
        return frame[WireHeader.Size..];
    }

    private static async Task Mutate(CeleriantPool pool, bool delete, AggregateKey[]? keys = null, long? expected = 10)
    {
        if (delete) await pool.DeleteAsync(Delete(keys ?? Keys, expected));
        else await pool.WriteAsync(Write((keys ?? Keys).Reverse().ToDictionary(k => k, _ => Append(expected))));
    }
}
