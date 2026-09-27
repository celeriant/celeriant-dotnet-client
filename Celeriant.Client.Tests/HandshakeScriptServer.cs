using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using Celeriant.Client.Protocol;
using Celeriant.Client.Requests;
using Celeriant.Client.Responses;
using Celeriant.Transport;

namespace Celeriant.Client.Tests;

/// <summary>One request frame as the server read it. <see cref="Index"/> is 0 for the first frame on a connection.</summary>
internal sealed record HandshakeFrame(int Connection, int Index, uint Type, byte[] Body);

/// <summary>What the server does with one request frame: write <see cref="Frames"/> in order, then hang up when <see cref="Close"/>.</summary>
internal sealed record HandshakeReply(IReadOnlyList<(uint Type, byte[] Body)> Frames, bool Close = false)
{
    public static readonly HandshakeReply Silence = new([]);
    public static readonly HandshakeReply HangUp = new([], Close: true);
}

/// <summary>
/// A real TCP peer that sees every frame, Identify included, and answers each from a script. Unlike
/// <see cref="FakeCeleriantServer"/> it does not answer an anonymous Identify on its own, so a test
/// can assert on the handshake itself and script the server's reply to it, including a rejection
/// followed by a close, the way the real server rejects an Identify.
///
/// <para>
/// Frames the client compresses with a dictionary are decompressed with <c>dict</c>, the one
/// dictionary this server hands out.
/// </para>
/// </summary>
internal sealed class HandshakeScriptServer : IAsyncDisposable
{
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _stop = new(TimeSpan.FromSeconds(60));
    private readonly Func<HandshakeFrame, HandshakeReply> _script;
    private readonly byte[]? _dict;
    private readonly Task _accepting;
    private readonly ConcurrentBag<Task> _sessions = [];
    private int _accepted;

    public readonly ConcurrentQueue<HandshakeFrame> Frames = [];
    public readonly ConcurrentQueue<Exception> Faults = [];

    public HandshakeScriptServer(Func<HandshakeFrame, HandshakeReply> script, byte[]? dict = null)
    {
        _script = script;
        _dict = dict;
        _listener.Start();
        _accepting = AcceptAsync();
    }

    public string Address => $"127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}";

    /// <summary>TCP connections accepted so far: the client's dial count against this node.</summary>
    public int Accepted => Volatile.Read(ref _accepted);

    /// <summary>The frames each connection sent, in order, keyed by connection ordinal.</summary>
    public IReadOnlyDictionary<int, HandshakeFrame[]> ByConnection()
        => Frames.GroupBy(f => f.Connection).ToDictionary(g => g.Key, g => g.OrderBy(f => f.Index).ToArray());

    private async Task AcceptAsync()
    {
        try
        {
            while (!_stop.IsCancellationRequested)
            {
                var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                int id = Interlocked.Increment(ref _accepted);
                _sessions.Add(Task.Run(() => ServeAsync(client, id)));
            }
        }
        catch (OperationCanceledException) { }
        catch (ObjectDisposedException) { }
        catch (SocketException) { }
    }

    private async Task ServeAsync(TcpClient client, int id)
    {
        using (client)
        {
            try
            {
                client.NoDelay = true;
                var stream = client.GetStream();
                for (int index = 0; !_stop.IsCancellationRequested; index++)
                {
                    var headerBytes = new byte[WireHeader.Size];
                    await stream.ReadExactlyAsync(headerBytes, _stop.Token);
                    var header = WireHeader.ParseFrom(headerBytes);
                    var body = new byte[header.CompressedLength];
                    await stream.ReadExactlyAsync(body, _stop.Token);
                    if (header.CompressionType == (byte)CompressionType.ZstdDict)
                    {
                        if (_dict is null)
                            throw new InvalidOperationException($"connection {id}: a dict-compressed frame, but this server ships no dictionary");
                        body = DictCompression.DecompressWithDict(body, header.UncompressedLength, _dict);
                    }

                    var frame = new HandshakeFrame(id, index, header.MessageType, body);
                    Frames.Enqueue(frame);
                    HandshakeReply reply = _script(frame);
                    foreach (var (type, replyBody) in reply.Frames)
                        await stream.WriteAsync(FakeServerSession.BuildFrame(type, replyBody), _stop.Token);
                    await stream.FlushAsync(_stop.Token);
                    if (reply.Close)
                        return;
                }
            }
            catch (EndOfStreamException) { }
            catch (IOException) { }
            catch (OperationCanceledException) { }
            catch (ObjectDisposedException) { }
            catch (Exception error) { Faults.Enqueue(error); }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync();
        _listener.Stop();
        await _accepting;
        await Task.WhenAll(_sessions);
        _stop.Dispose();
    }
}

/// <summary>The replies a <see cref="HandshakeScriptServer"/> script builds from, answering each request as the real server would.</summary>
internal static class HandshakeReplies
{
    public static string Sha(byte[] bytes) => Convert.ToHexString(SHA256.HashData(bytes)).ToLowerInvariant();

    public static IdentifyRequest Identify(HandshakeFrame frame) => WireCodec.Deserialize<IdentifyRequest>(frame.Body);

    /// <summary>
    /// Accept the Identify. With <paramref name="dict"/>, report its sha and ship the bytes only when the
    /// client advertised a different sha, as the server does. <paramref name="ships"/> counts the ships.
    /// </summary>
    public static HandshakeReply Identified(HandshakeFrame frame, byte[]? dict = null, ShipCounter? ships = null)
    {
        var identify = Identify(frame);
        string? sha = dict is null ? null : Sha(dict);
        bool ship = dict is not null && identify.KnownDictSha256 != sha;
        if (ship && ships is not null)
            Interlocked.Increment(ref ships.Value);
        return new([(MessageTypes.Responses.Identify, WireCodec.Serialize(new IdentifyResponse
        {
            CorrelationId = identify.CorrelationId,
            CompressionDictSha256 = sha,
            CompressionDictBytes = ship ? dict : null,
        }))]);
    }

    /// <summary>Reject the Identify with a GenericError carrying <paramref name="code"/>, then close, as the server does.</summary>
    public static HandshakeReply Rejected(HandshakeFrame frame, uint code) => new(
        [(MessageTypes.Responses.GenericError,
            FakeServerProtocol.ErrorFrame(code, $"identify rejected with {code}", Identify(frame).CorrelationId))],
        Close: true);

    /// <summary>Answer the frame with a ProtocolError (type 6), echoing the correlation id when <paramref name="echo"/>.</summary>
    public static HandshakeReply ProtocolError(HandshakeFrame frame, bool echo, bool close) => new(
        [(MessageTypes.Responses.ProtocolError,
            FakeServerProtocol.ProtocolErrorFrame(1, "protocol error", echo ? Identify(frame).CorrelationId : null))],
        Close: close);

    /// <summary>Answer a Write or a Watch successfully; anything else is a fixture error.</summary>
    public static HandshakeReply Served(HandshakeFrame frame) => frame.Type switch
    {
        MessageTypes.Requests.Write => new([(MessageTypes.Responses.Write, WireCodec.Serialize(new WriteResponse
        {
            CorrelationId = WireCodec.Deserialize<WriteRequest>(frame.Body).CorrelationId,
            MaxAggregateVersion = 1,
        }))]),
        MessageTypes.Requests.Watch => new([(MessageTypes.Responses.Watch, WireCodec.Serialize(new WatchResponse()))]),
        _ => throw new InvalidOperationException($"connection {frame.Connection}: unexpected request type {frame.Type}"),
    };

    /// <summary>A server that accepts every Identify (shipping <paramref name="dict"/> when set) and serves every request.</summary>
    public static Func<HandshakeFrame, HandshakeReply> Healthy(byte[]? dict = null, ShipCounter? ships = null)
        => frame => frame.Type == MessageTypes.Requests.Identify ? Identified(frame, dict, ships) : Served(frame);
}

/// <summary>How many Identify replies carried the dictionary bytes.</summary>
internal sealed class ShipCounter
{
    public int Value;
}
