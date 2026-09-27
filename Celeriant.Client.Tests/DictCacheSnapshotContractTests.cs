using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Transport;
using MessagePack;

namespace Celeriant.Client.Tests;

/// <summary>
/// The pool dictionary cache seen from the wire. A pool
/// connection snapshots the pool's cache before it writes Identify, advertises that sha, and resolves a
/// sha-only confirm against the snapshot, so another connection replacing the pool's learned dictionary
/// mid-handshake cannot make it fail or decode with the wrong dictionary. The Rust client's
/// <c>celeriant_client_tokio/src/dict_cache.rs</c> follows the same rule.
///
/// Every server is a scripted fake at one address whose connections may hold different dictionaries, as
/// a pool sees when its address list spans nodes that disagree.
///
/// Run: dotnet test Celeriant.Client.Tests --filter FullyQualifiedName~DictCacheSnapshotContractTests
/// </summary>
public class DictCacheSnapshotContractTests
{
    private static readonly TimeSpan Prompt = TimeSpan.FromSeconds(5);

    /// <summary>An API key the fake accepts, so these pools send credentials in their Identify.</summary>
    private static readonly ClientIdentityConfig ApiKey = ClientIdentityConfig.FromApiKey(Convert.ToBase64String(new byte[32]));

    private static byte[] Noise(ulong seed)
    {
        var bytes = new byte[4096];
        ulong x = seed;
        for (int i = 0; i < bytes.Length; i++)
        {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            bytes[i] = (byte)x;
        }
        return bytes;
    }

    private static string Sha(byte[] dict) => Convert.ToHexString(SHA256.HashData(dict)).ToLowerInvariant();

    /// <summary>Org ids cut from <paramref name="dict"/>, so only that dictionary decodes the compressed reply.</summary>
    private static Guid[] OrgIds(byte[] dict)
        => Enumerable.Range(0, dict.Length / 16).Select(i => new Guid(dict.AsSpan(i * 16, 16), bigEndian: true)).ToArray();

    private sealed record Harness(CeleriantPool Pool, Func<DictCache> Cache);

    /// <summary>A pool against <paramref name="server"/> whose node pool's <see cref="DictCache"/> the test can reach.</summary>
    private static Harness PoolFor(ScriptedDictServer server)
    {
        DictCache? captured = null;
        var options = new CeleriantPoolOptions
        {
            Address = server.Address,
            IdentityConfig = ApiKey,
            MaxConnections = 4,
            ConnectionTimeout = Prompt,
            RequestTimeout = Prompt,
        };
        var pool = new CeleriantPool(options, (addr, opts, cache) =>
        {
            captured = cache;
            return new NodeConnectionPool(addr, opts, cache);
        });
        return new Harness(pool, () => captured ?? throw new InvalidOperationException("the pool built no node pool"));
    }

    /// <summary>Send ListOrgs on this lease and assert the dict-compressed reply decodes to <paramref name="dict"/>'s ids.</summary>
    private static async Task AssertDecodesWith(PooledConnection conn, byte[] dict, string who)
    {
        var response = await conn.Client
            .SendRequestAsync(new ClientRequest.ListOrgs(new ListOrgsRequest { ShardId = 0 }))
            .WaitAsync(Prompt);

        var orgs = Assert.IsType<ClientResponse.ListOrgs>(response).Value.Orgs.Select(o => o.OrgId).ToArray();
        Guid[] want = OrgIds(dict);
        int wrong = orgs.Zip(want).Count(p => p.First != p.Second);
        Assert.True(orgs.Length == want.Length && wrong == 0,
            $"{who}: decoded with the wrong dictionary: {orgs.Length} ids, {wrong} of {want.Length} differ");
    }

    // ---------------------------------------------------------------------------------------------
    // A fresh pool advertises the built-in
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task AFreshPoolAdvertisesTheBuiltinAndABuiltinServerShipsNothing()
    {
        byte[] builtin = BuiltinDictionary.Dict.Bytes;
        await using var server = new ScriptedDictServer(new Step(builtin));
        var h = PoolFor(server);
        await using var pool = h.Pool;

        await using var conn = await pool.GetConnectionAsync().WaitAsync(Prompt);

        Assert.Equal(new string?[] { BuiltinDictionary.Dict.Sha }, server.Advertised);
        Assert.Equal(Sha(builtin), BuiltinDictionary.Dict.Sha);
        Assert.Equal(0, server.Ships);
        await AssertDecodesWith(conn, builtin, "the built-in connection");
        server.AssertNoFaults();
    }

    // ---------------------------------------------------------------------------------------------
    // Learning a shipped dictionary once per pool
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task AfterOneConnectionLearnsACustomDictTheNextAdvertisesItAndIsNotShippedIt()
    {
        byte[] x = Noise(1);
        await using var server = new ScriptedDictServer(new Step(x), new Step(x));
        var h = PoolFor(server);
        await using var pool = h.Pool;

        await using var first = await pool.GetConnectionAsync().WaitAsync(Prompt);
        await using var second = await pool.GetConnectionAsync().WaitAsync(Prompt);

        Assert.Equal(new string?[] { BuiltinDictionary.Dict.Sha, Sha(x) }, server.Advertised);
        Assert.Equal(1, server.Ships);
        Assert.Equal(Sha(x), h.Cache().Snapshot().Sha);
        await AssertDecodesWith(second, x, "the connection resolved from the pool cache");
        await AssertDecodesWith(first, x, "the connection that was shipped the dictionary");
        server.AssertNoFaults();
    }

    // ---------------------------------------------------------------------------------------------
    // Snapshot at advertise
    // ---------------------------------------------------------------------------------------------

    /// <summary>
    /// A connection keeps decoding with the dictionary its own server confirmed after another connection
    /// replaced the pool's learned dictionary. Guards against a response decoded with the pool's "current"
    /// dictionary instead of this connection's.
    /// </summary>
    [Fact]
    public async Task AConnectionDecodesWithItsOwnDictAfterThePoolLearnsAnother()
    {
        byte[] x = Noise(1), y = Noise(2);
        await using var server = new ScriptedDictServer(new Step(x), new Step(y));
        var h = PoolFor(server);
        await using var pool = h.Pool;

        await using var a = await pool.GetConnectionAsync().WaitAsync(Prompt);
        await using var b = await pool.GetConnectionAsync().WaitAsync(Prompt);
        Assert.Null(h.Cache().Lookup(Sha(x))); // setup: learning y evicted x from the pool cache

        await AssertDecodesWith(a, x, "connection a");
        await AssertDecodesWith(b, y, "connection b");
        Assert.Equal(new string?[] { BuiltinDictionary.Dict.Sha, Sha(x) }, server.Advertised);
        server.AssertNoFaults();
    }

    public enum Interleaver
    {
        /// <summary>A third pool connection, to a node holding y, completes its handshake.</summary>
        AnotherConnectionLearnsY,
        /// <summary>Any other actor learns y straight into the pool's cache.</summary>
        DirectLearnOfY,
    }

    /// <summary>
    /// Connection 1 advertises x and its server confirms x without bytes. Between the
    /// advertise and the confirm, y is learned into the same cache and the one-slot cache evicts x. The
    /// confirm must still resolve against what connection 1 advertised: no DictUnavailable, no
    /// ProtocolException, and replies decode with x.
    /// </summary>
    [Theory]
    [InlineData(Interleaver.AnotherConnectionLearnsY)]
    [InlineData(Interleaver.DirectLearnOfY)]
    public async Task AConfirmedShaSurvivesAConcurrentLearn(Interleaver interleaver)
    {
        byte[] x = Noise(1), y = Noise(2);
        var got = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var server = new ScriptedDictServer(
            new Step(x),
            new Step(x, got, gate.Task),
            new Step(y));
        var h = PoolFor(server);
        await using var pool = h.Pool;

        await using var learnsX = await pool.GetConnectionAsync().WaitAsync(Prompt);
        Task<PooledConnection> confirmed = pool.GetConnectionAsync();
        await got.Task.WaitAsync(Prompt);

        PooledConnection? learnsY = null;
        try
        {
            if (interleaver == Interleaver.AnotherConnectionLearnsY)
                learnsY = await pool.GetConnectionAsync().WaitAsync(Prompt);
            else
                h.Cache().Learn(new CachedDict(Sha(y), y));
            Assert.Null(h.Cache().Lookup(Sha(x))); // setup: x is gone from the shared slot
            gate.SetResult();

            Exception? failure = await Record.ExceptionAsync(async () =>
            {
                await using var conn = await confirmed.WaitAsync(Prompt);
                await AssertDecodesWith(conn, x, "the connection that advertised x");
            });

            Assert.Equal(Sha(x), server.Advertised[1]); // setup: connection 1 advertised x
            Assert.True(failure is null,
                $"the server confirmed the sha this connection advertised, yet it failed: {failure}");
            // x was shipped to the first connection only; the interleaving connection is shipped y.
            Assert.Equal(interleaver == Interleaver.AnotherConnectionLearnsY ? 2 : 1, server.Ships);
        }
        finally
        {
            gate.TrySetResult();
            if (learnsY is not null)
                await learnsY.DisposeAsync();
        }
        server.AssertNoFaults();
    }

    // ---------------------------------------------------------------------------------------------
    // The scripted fake
    // ---------------------------------------------------------------------------------------------

    /// <summary>
    /// What the fake does with one accepted connection, by accept order: answer Identify with
    /// <paramref name="Dict"/>'s sha, shipping the bytes unless the client advertised that sha, then
    /// answer every ListOrgs with a reply dict-compressed with <paramref name="Dict"/>. With
    /// <paramref name="Got"/> and <paramref name="Gate"/>, signal Got on receiving Identify and hold
    /// the reply until Gate completes.
    /// </summary>
    private sealed record Step(byte[] Dict, TaskCompletionSource? Got = null, Task? Gate = null);

    private sealed class ScriptedDictServer : IAsyncDisposable
    {
        private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource _stop = new(TimeSpan.FromSeconds(30));
        private readonly Step[] _steps;
        private readonly string?[] _advertised;
        private readonly bool[] _received;
        private readonly List<Task> _sessions = [];
        private readonly System.Collections.Concurrent.ConcurrentQueue<Exception> _faults = [];
        private readonly Task _accepting;
        private int _ships;

        public ScriptedDictServer(params Step[] steps)
        {
            _steps = steps;
            _advertised = new string?[steps.Length];
            _received = new bool[steps.Length];
            _listener.Start();
            _accepting = AcceptAsync();
        }

        public string Address => $"127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}";

        /// <summary>known_dict_sha256 from each connection's Identify, by accept order, up to the last one received.</summary>
        public string?[] Advertised
        {
            get
            {
                lock (_advertised)
                {
                    int last = Array.FindLastIndex(_received, r => r);
                    return _advertised.Take(last + 1).ToArray();
                }
            }
        }

        /// <summary>How many Identify replies carried the dictionary bytes.</summary>
        public int Ships => Volatile.Read(ref _ships);

        public void AssertNoFaults() => Assert.Empty(_faults);

        private async Task AcceptAsync()
        {
            try
            {
                for (int i = 0; i < _steps.Length; i++)
                {
                    var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                    int index = i;
                    lock (_sessions)
                        _sessions.Add(Task.Run(() => ServeAsync(client, index)));
                }
                await Task.Delay(Timeout.Infinite, _stop.Token);
            }
            catch (OperationCanceledException) { }
        }

        private async Task ServeAsync(TcpClient client, int index)
        {
            Step step = _steps[index];
            using (client)
            {
                try
                {
                    client.NoDelay = true;
                    var stream = client.GetStream();

                    var (header, body) = await ReadFrameAsync(stream, null);
                    if (header.MessageType != MessageTypes.Requests.Identify)
                        throw new InvalidOperationException($"connection {index}: first frame was type {header.MessageType}, not Identify");
                    var identify = WireCodec.Deserialize<IdentifyRequest>(body);
                    lock (_advertised)
                    {
                        _advertised[index] = identify.KnownDictSha256;
                        _received[index] = true;
                    }

                    if (step.Got is not null)
                    {
                        step.Got.TrySetResult();
                        await step.Gate!.WaitAsync(_stop.Token);
                    }

                    string sha = Sha(step.Dict);
                    bool ship = identify.KnownDictSha256 != sha;
                    if (ship)
                        Interlocked.Increment(ref _ships);
                    await WriteFrameAsync(stream, MessageTypes.Responses.Identify,
                        IdentifyBody(identify.CorrelationId, sha, ship ? step.Dict : null), null);

                    while (!_stop.IsCancellationRequested)
                    {
                        (header, body) = await ReadFrameAsync(stream, step.Dict);
                        if (header.MessageType != MessageTypes.Requests.ListOrgs)
                            continue;
                        var request = WireCodec.Deserialize<ListOrgsRequest>(body);
                        byte[] reply = WireCodec.Serialize(new ListOrgsResponse
                        {
                            CorrelationId = request.CorrelationId,
                            Orgs = OrgIds(step.Dict).Select(id => new OrgListItem { OrgId = id }).ToArray(),
                        });
                        await WriteFrameAsync(stream, MessageTypes.Responses.ListOrgs, reply, step.Dict);
                    }
                }
                catch (EndOfStreamException) { }
                catch (IOException) { }
                catch (OperationCanceledException) { }
                catch (Exception error) { _faults.Enqueue(error); }
            }
        }

        private async Task<(WireHeader Header, byte[] Body)> ReadFrameAsync(Stream stream, byte[]? dict)
        {
            var headerBytes = new byte[WireHeader.Size];
            await stream.ReadExactlyAsync(headerBytes, _stop.Token);
            var header = WireHeader.ParseFrom(headerBytes);
            var body = new byte[header.CompressedLength];
            await stream.ReadExactlyAsync(body, _stop.Token);
            if (header.CompressionType == (byte)CompressionType.ZstdDict)
            {
                if (dict is null)
                    throw new InvalidOperationException("the client compressed a frame before the handshake");
                body = DictCompression.DecompressWithDict(body, header.UncompressedLength, dict);
            }
            return (header, body);
        }

        private async Task WriteFrameAsync(Stream stream, uint messageType, byte[] body, byte[]? dict)
        {
            WireHeader header;
            byte[] payload = body;
            if (dict is null)
            {
                header = WireHeader.ForRequest(WireHeader.ProtocolVersionV5, messageType, (uint)body.Length);
            }
            else
            {
                payload = DictCompression.CompressWithDict(body, dict);
                if (payload.Length >= body.Length / 3)
                    throw new InvalidOperationException(
                        $"fixture: a {body.Length}-byte reply compressed to {payload.Length}; it must lean on the dictionary");
                header = WireHeader.ForCompressedRequest(WireHeader.ProtocolVersionV5, messageType,
                    (uint)payload.Length, (uint)body.Length, CompressionType.ZstdDict);
            }
            var frame = new byte[WireHeader.Size + payload.Length];
            header.WriteTo(frame);
            payload.CopyTo(frame, WireHeader.Size);
            await stream.WriteAsync(frame, _stop.Token);
            await stream.FlushAsync(_stop.Token);
        }

        /// <summary>The server's five-field IdentifyResponse, dictionary bytes as a msgpack integer array.</summary>
        private static byte[] IdentifyBody(Guid? correlation, string sha, byte[]? dict)
        {
            var buffer = new ArrayBufferWriter<byte>();
            var writer = new MessagePackWriter(buffer);
            writer.WriteArrayHeader(5);
            CeleriantNullableGuidFormatter.Instance.Serialize(ref writer, correlation, WireCodec.Options);
            writer.WriteNil();   // client_id
            writer.WriteNil();   // access_level
            writer.Write(sha);   // compression_dict_sha256
            if (dict is null)
            {
                writer.WriteNil();
            }
            else
            {
                writer.WriteArrayHeader(dict.Length);
                foreach (byte b in dict)
                    writer.Write(b);
            }
            writer.Flush();
            return buffer.WrittenSpan.ToArray();
        }

        public async ValueTask DisposeAsync()
        {
            await _stop.CancelAsync();
            _listener.Stop();
            await _accepting;
            Task[] sessions;
            lock (_sessions)
                sessions = [.. _sessions];
            await Task.WhenAll(sessions);
            _stop.Dispose();
        }
    }
}
