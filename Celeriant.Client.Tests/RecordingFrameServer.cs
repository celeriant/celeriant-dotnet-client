using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Celeriant.Transport;

namespace Celeriant.Client.Tests;

internal sealed record RecordedFrame(int Connection, uint Version, uint Type, byte[] Body);
internal sealed record FrameReply(uint Type, byte[] Body, uint Version = 5);

/// <summary>
/// A real TCP peer that records every request frame and answers each one from a callback, so a
/// test can assert on exactly what reached the wire and on which connection.
/// </summary>
internal sealed class RecordingFrameServer : IAsyncDisposable
{
    private readonly TcpListener listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource stop = new(TimeSpan.FromSeconds(30));
    private readonly Func<RecordedFrame, Task<FrameReply>> reply;
    private readonly Task accepting;
    private readonly ConcurrentBag<Task> sessions = [];
    internal readonly ConcurrentQueue<RecordedFrame> Frames = [];
    internal readonly ConcurrentQueue<Exception> Faults = [];
    internal string Address => $"127.0.0.1:{((IPEndPoint)listener.LocalEndpoint).Port}";

    internal RecordingFrameServer(Func<RecordedFrame, Task<FrameReply>> reply)
    {
        this.reply = reply;
        listener.Start();
        accepting = Accept();
    }

    private async Task Accept()
    {
        var id = 0;
        try
        {
            while (!stop.IsCancellationRequested)
            {
                var client = await listener.AcceptTcpClientAsync(stop.Token);
                sessions.Add(Serve(client, ++id));
            }
        }
        catch (OperationCanceledException) { }
    }

    private async Task Serve(TcpClient client, int id)
    {
        using (client)
        {
            try
            {
                var stream = client.GetStream();
                while (!stop.IsCancellationRequested)
                {
                    var header = new byte[17];
                    await stream.ReadExactlyAsync(header, stop.Token);
                    var parsed = WireHeader.ParseFrom(header);
                    Assert.Equal(0, parsed.CompressionType);
                    var body = new byte[parsed.CompressedLength];
                    await stream.ReadExactlyAsync(body, stop.Token);
                    var request = new RecordedFrame(id, parsed.Version, parsed.MessageType, body);
                    Frames.Enqueue(request);
                    var response = await reply(request);
                    var bytes = new byte[17 + response.Body.Length];
                    WireHeader.ForRequest(response.Version, response.Type, (uint)response.Body.Length).WriteTo(bytes);
                    response.Body.CopyTo(bytes, 17);
                    await stream.WriteAsync(bytes, stop.Token);
                }
            }
            catch (EndOfStreamException) { }
            catch (IOException) { }
            catch (OperationCanceledException) { }
            catch (Exception error) { Faults.Enqueue(error); }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await stop.CancelAsync();
        await accepting;
        await Task.WhenAll(sessions);
        listener.Stop();
        stop.Dispose();
        Assert.Empty(Faults);
    }
}
