using System.Net;
using System.Net.Security;
using System.Net.Sockets;

namespace Celeriant.Transport.Tests;

/// <summary>
/// Adversarial-review pin: the transport's connect path (dial + TLS handshake) must classify
/// a timeout as ConnectTimeout (failover-class), not Timeout (request-class). Nothing else in
/// either suite exercises this classification against the real connect code.
/// </summary>
public class AdvRevRoutingTests
{
    private sealed class ConnectTimeoutMarker(string message) : Exception(message);
    private sealed class RequestTimeoutMarker(string message) : Exception(message);

    private sealed class RecordingExceptionFactory : ITransportExceptionFactory
    {
        public Exception Timeout(string message) => new RequestTimeoutMarker(message);
        public Exception ConnectTimeout(string message) => new ConnectTimeoutMarker(message);
        public Exception ConnectionFailed(string message, Exception? inner = null) => new IOException(message, inner);
        public Exception Protocol(string message, Exception? inner = null) => new InvalidDataException(message);
    }

    private sealed class StubCodec : IConnectionCodec
    {
        public uint ProtocolVersion => 3;
        public uint IdentifyRequestType => 14;
        public uint IdentifyResponseType => 16;
        public byte[] EncodeIdentify(in IdentifyParams identity) => [];
        public IdentifyResult DecodeIdentify(ReadOnlySpan<byte> body) => default!;
        public Exception? TryMapErrorFrame(uint messageType, ReadOnlySpan<byte> body) => null;
    }

    [Fact]
    public async Task ConnectTimeout_OnStalledTlsHandshake_IsConnectTimeoutClass()
    {
        // Listener accepts TCP but never speaks TLS: the handshake stalls until the
        // connection timeout fires. This is the "black-holed leader" the routing
        // change exists for; it must surface as ConnectTimeout, not request Timeout.
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        try
        {
            var ssl = new SslClientAuthenticationOptions { TargetHost = "localhost" };
            var thrown = await Record.ExceptionAsync(() => CeleriantConnection.ConnectAsync(
                $"127.0.0.1:{port}",
                TimeSpan.FromMilliseconds(250),
                ssl,
                new StubCodec(),
                new RecordingExceptionFactory()));

            Assert.IsType<ConnectTimeoutMarker>(thrown);
        }
        finally
        {
            listener.Stop();
        }
    }

    [Fact]
    public async Task ConnectTimeout_OnStalledTcpDial_IsConnectTimeoutClass()
    {
        // Saturate a backlog-1 listener that never accepts: further SYNs are dropped
        // by the kernel and the dial stalls until the connection timeout fires.
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start(backlog: 1);
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        var fillers = new List<TcpClient>();
        try
        {
            // Fill the accept queue until a connect no longer completes promptly.
            for (int i = 0; i < 16; i++)
            {
                var filler = new TcpClient();
                fillers.Add(filler);
                var attempt = filler.ConnectAsync(IPAddress.Loopback, port);
                if (await Task.WhenAny(attempt, Task.Delay(200)) != attempt)
                    break; // backlog full: this SYN is stalling
            }

            var thrown = await Record.ExceptionAsync(() => CeleriantConnection.ConnectAsync(
                $"127.0.0.1:{port}",
                TimeSpan.FromMilliseconds(500),
                sslOptions: null,
                new StubCodec(),
                new RecordingExceptionFactory()));

            Assert.IsType<ConnectTimeoutMarker>(thrown);
        }
        finally
        {
            foreach (var f in fillers) f.Dispose();
            listener.Stop();
        }
    }
}
