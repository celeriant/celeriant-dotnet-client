using System.Net;
using System.Net.Sockets;
using Celeriant.Transport;

namespace Celeriant.Client.Tests;

/// <summary>
/// A controllable Celeriant server for black-box client tests: real TCP, real 17-byte framing,
/// scripted replies.
///
/// <para>
/// The handler decides when (and whether) each request is answered, so a test can park the client
/// on the response read and cancel it there without racing a wall clock against a real reply.
/// </para>
/// </summary>
internal sealed class FakeCeleriantServer : IAsyncDisposable
{
    /// <summary>Called once per received request frame, on the session's read loop.</summary>
    public delegate Task RequestHandler(FakeServerSession session, uint messageType, byte[] body);

    private readonly TcpListener _listener;
    private readonly RequestHandler _handler;
    private readonly Action<int>? _onSessionEnded;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _acceptLoop;
    private int _connectionCount;

    private FakeCeleriantServer(TcpListener listener, RequestHandler handler, Action<int>? onSessionEnded)
    {
        _listener = listener;
        _handler = handler;
        _onSessionEnded = onSessionEnded;
        _acceptLoop = Task.Run(AcceptLoopAsync);
    }

    /// <param name="onSessionEnded">
    /// Called with the <see cref="FakeServerSession.ConnectionId"/> once a session's read loop ends,
    /// which for a watch is when the client closes the socket. It is how a test observes, from the
    /// server side, that a connection the client opened was actually released.
    /// </param>
    public static FakeCeleriantServer Start(RequestHandler handler, Action<int>? onSessionEnded = null)
        => Start(handler, 0, onSessionEnded);

    /// <param name="port">
    /// The loopback port to bind, or 0 for any free one. A fixed port is how a test brings a node
    /// back up at an address the client has already cached, after that same address spent a while
    /// refusing connections.
    /// </param>
    public static FakeCeleriantServer Start(RequestHandler handler, int port, Action<int>? onSessionEnded = null)
    {
        var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Start();
        return new FakeCeleriantServer(listener, handler, onSessionEnded);
    }

    /// <summary>"host:port" of the listening socket, in the form the client expects.</summary>
    public string Address => $"127.0.0.1:{((IPEndPoint)_listener.LocalEndpoint).Port}";

    /// <summary>Total TCP connections accepted so far.</summary>
    public int ConnectionsAccepted => Volatile.Read(ref _connectionCount);

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        _listener.Stop();
        try { await _acceptLoop; } catch { /* shutdown */ }
        _cts.Dispose();
    }

    private async Task AcceptLoopAsync()
    {
        while (!_cts.IsCancellationRequested)
        {
            TcpClient client;
            try
            {
                client = await _listener.AcceptTcpClientAsync(_cts.Token);
            }
            catch
            {
                return;
            }

            int id = Interlocked.Increment(ref _connectionCount);
            _ = Task.Run(() => SessionLoopAsync(client, id));
        }
    }

    private async Task SessionLoopAsync(TcpClient client, int connectionId)
    {
        using (client)
        {
            client.NoDelay = true;
            var stream = client.GetStream();
            var session = new FakeServerSession(connectionId, stream);
            var header = new byte[WireHeader.Size];

            try
            {
                while (true)
                {
                    await ReadExactAsync(stream, header, WireHeader.Size, _cts.Token);
                    var parsed = WireHeader.ParseFrom(header);
                    var body = new byte[parsed.CompressedLength];
                    await ReadExactAsync(stream, body, body.Length, _cts.Token);
                    await _handler(session, parsed.MessageType, body);
                }
            }
            catch
            {
                // Client hung up, the server is shutting down, or a reply was written to a socket
                // the client had already abandoned. All are normal ends to a session here.
            }
            finally
            {
                _onSessionEnded?.Invoke(connectionId);
            }
        }
    }

    private static async Task ReadExactAsync(Stream stream, byte[] buffer, int count, CancellationToken ct)
    {
        int read = 0;
        while (read < count)
        {
            int n = await stream.ReadAsync(buffer.AsMemory(read, count - read), ct);
            if (n == 0)
                throw new EndOfStreamException();
            read += n;
        }
    }
}

/// <summary>One accepted connection, from the fake server's side.</summary>
internal sealed class FakeServerSession(int connectionId, Stream stream)
{
    /// <summary>1-based ordinal of the TCP connection this request arrived on.</summary>
    public int ConnectionId { get; } = connectionId;

    public Task SendFrameAsync(uint messageType, byte[] body)
        => SendRawAsync(BuildFrame(messageType, body));

    /// <summary>Write arbitrary bytes: used to deliver a deliberately truncated frame.</summary>
    public async Task SendRawAsync(ReadOnlyMemory<byte> bytes)
    {
        await stream.WriteAsync(bytes);
        await stream.FlushAsync();
    }

    /// <summary>
    /// Drop this connection the way a dying node does: anything already written is flushed, then the
    /// client's next read sees end-of-stream. The session loop ends on its own once the socket is gone.
    /// </summary>
    public void Close() => stream.Close();

    public static byte[] BuildFrame(uint messageType, byte[] body)
    {
        var frame = new byte[WireHeader.Size + body.Length];
        WireHeader
            .ForRequest(WireHeader.ProtocolVersionV3, messageType, (uint)body.Length)
            .WriteTo(frame);
        body.CopyTo(frame, WireHeader.Size);
        return frame;
    }
}
