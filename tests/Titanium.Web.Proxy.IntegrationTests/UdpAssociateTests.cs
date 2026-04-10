using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Titanium.Web.Proxy;
using Titanium.Web.Proxy.IntegrationTests.Setup;

namespace Titanium.Web.Proxy.IntegrationTests;

[TestClass]
public class UdpAssociateTests
{
    private static readonly TimeSpan ReceiveTimeout = TimeSpan.FromSeconds(5);

    [TestMethod]
    public async Task Can_Relay_Udp_Associate_Datagram_To_Ipv4_Target()
    {
        using var proxy = new TestSocksProxyServer();
        using var echoServer = new UdpEchoServer();
        using var socksClient = await SocksUdpAssociateClient.ConnectAsync(proxy.ListeningPort);

        var associateReply = await socksClient.AssociateAsync();

        Assert.AreNotEqual(IPAddress.Any, associateReply.Address);
        Assert.AreEqual(IPAddress.Loopback, associateReply.Address.MapToIPv4());

        var payload = Encoding.ASCII.GetBytes("udp-associate-ipv4");
        var result = await socksClient.SendAsync(echoServer.EndPoint, payload);

        CollectionAssert.AreEqual(payload, result.Payload);
        Assert.AreEqual(echoServer.EndPoint.Address, result.SourceEndPoint.Address);
        Assert.AreEqual(echoServer.EndPoint.Port, result.SourceEndPoint.Port);
    }

    [TestMethod]
    public async Task Can_Relay_Udp_Associate_Datagram_To_Domain_Target()
    {
        using var proxy = new TestSocksProxyServer();
        using var echoServer = new UdpEchoServer();
        using var socksClient = await SocksUdpAssociateClient.ConnectAsync(proxy.ListeningPort);

        await socksClient.AssociateAsync();

        var payload = Encoding.ASCII.GetBytes("udp-associate-domain");
        var result = await socksClient.SendToDomainAsync("localhost", echoServer.EndPoint.Port, payload);

        CollectionAssert.AreEqual(payload, result.Payload);
        Assert.AreEqual(IPAddress.Loopback, result.SourceEndPoint.Address.MapToIPv4());
        Assert.AreEqual(echoServer.EndPoint.Port, result.SourceEndPoint.Port);
    }

    private sealed class SocksUdpAssociateClient : IDisposable
    {
        private readonly TcpClient controlClient;
        private readonly NetworkStream controlStream;
        private readonly UdpClient udpClient;
        private IPEndPoint? relayEndPoint;

        private SocksUdpAssociateClient(TcpClient controlClient, UdpClient udpClient)
        {
            this.controlClient = controlClient;
            controlStream = controlClient.GetStream();
            this.udpClient = udpClient;
        }

        public static async Task<SocksUdpAssociateClient> ConnectAsync(int socksPort)
        {
            var tcpClient = new TcpClient(AddressFamily.InterNetwork);
            await tcpClient.ConnectAsync(IPAddress.Loopback, socksPort);

            var udpClient = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0));
            return new SocksUdpAssociateClient(tcpClient, udpClient);
        }

        public async Task<IPEndPoint> AssociateAsync()
        {
            await controlStream.WriteAsync(new byte[] { 5, 1, 0 });

            var authReply = new byte[2];
            await ReadExactlyAsync(controlStream, authReply);
            CollectionAssert.AreEqual(new byte[] { 5, 0 }, authReply);

            var request = new byte[] { 5, 3, 0, 1, 0, 0, 0, 0, 0, 0 };
            await controlStream.WriteAsync(request);

            var replyHead = new byte[4];
            await ReadExactlyAsync(controlStream, replyHead);

            Assert.AreEqual(5, replyHead[0]);
            Assert.AreEqual(0, replyHead[1]);
            Assert.AreEqual(0, replyHead[2]);

            var addressBytes = replyHead[3] switch
            {
                1 => new byte[4],
                4 => new byte[16],
                _ => throw new InvalidDataException($"Unexpected SOCKS5 address type: {replyHead[3]}."),
            };

            await ReadExactlyAsync(controlStream, addressBytes);

            var portBytes = new byte[2];
            await ReadExactlyAsync(controlStream, portBytes);

            relayEndPoint = new IPEndPoint(
                new IPAddress(addressBytes),
                (portBytes[0] << 8) | portBytes[1]);

            udpClient.Connect(relayEndPoint);
            return relayEndPoint;
        }

        public async Task<UdpRelayResult> SendAsync(IPEndPoint targetEndPoint, byte[] payload)
        {
            var packet = BuildIpPacket(targetEndPoint, payload);
            return await SendAndReceiveAsync(packet);
        }

        public async Task<UdpRelayResult> SendToDomainAsync(string host, int port, byte[] payload)
        {
            var packet = BuildDomainPacket(host, port, payload);
            return await SendAndReceiveAsync(packet);
        }

        private async Task<UdpRelayResult> SendAndReceiveAsync(byte[] packet)
        {
            Assert.IsNotNull(relayEndPoint);
            await udpClient.SendAsync(packet, packet.Length);

            using var cts = new CancellationTokenSource(ReceiveTimeout);
            var result = await udpClient.ReceiveAsync(cts.Token);

            Assert.IsTrue(UdpSocks5Header.TryParse(
                result.Buffer, result.Buffer.Length, out int headerLength, out IPEndPoint sourceEndPoint, out string domainName));
            Assert.IsNull(domainName);

            var payload = new byte[result.Buffer.Length - headerLength];
            Buffer.BlockCopy(result.Buffer, headerLength, payload, 0, payload.Length);

            return new UdpRelayResult(sourceEndPoint, payload);
        }

        private byte[] BuildIpPacket(IPEndPoint targetEndPoint, byte[] payload)
        {
            var headerLength = targetEndPoint.AddressFamily == AddressFamily.InterNetwork ? 10 : 22;
            var packet = new byte[headerLength + payload.Length];
            var actualHeaderLength = UdpSocks5Header.Write(packet, headerLength, targetEndPoint);
            Buffer.BlockCopy(payload, 0, packet, actualHeaderLength, payload.Length);
            return packet;
        }

        private static byte[] BuildDomainPacket(string host, int port, byte[] payload)
        {
            var hostBytes = Encoding.ASCII.GetBytes(host);
            var headerLength = 4 + 1 + hostBytes.Length + 2;
            var packet = new byte[headerLength + payload.Length];

            packet[0] = 0;
            packet[1] = 0;
            packet[2] = 0;
            packet[3] = 3;
            packet[4] = (byte)hostBytes.Length;
            Buffer.BlockCopy(hostBytes, 0, packet, 5, hostBytes.Length);
            packet[5 + hostBytes.Length] = (byte)(port >> 8);
            packet[5 + hostBytes.Length + 1] = (byte)(port & 0xff);
            Buffer.BlockCopy(payload, 0, packet, headerLength, payload.Length);

            return packet;
        }

        public void Dispose()
        {
            udpClient.Dispose();
            controlStream.Dispose();
            controlClient.Dispose();
        }
    }

    private sealed class UdpEchoServer : IDisposable
    {
        private readonly CancellationTokenSource cancellationTokenSource = new();
        private readonly Task loopTask;
        private readonly UdpClient udpServer;

        public UdpEchoServer()
        {
            udpServer = new UdpClient(new IPEndPoint(IPAddress.Loopback, 0));
            EndPoint = (IPEndPoint)udpServer.Client.LocalEndPoint!;
            loopTask = RunAsync(cancellationTokenSource.Token);
        }

        public IPEndPoint EndPoint { get; }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var result = await udpServer.ReceiveAsync(cancellationToken);
                    await udpServer.SendAsync(result.Buffer, result.Buffer.Length, result.RemoteEndPoint);
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (ObjectDisposedException)
            {
            }
        }

        public void Dispose()
        {
            cancellationTokenSource.Cancel();
            udpServer.Dispose();
            try
            {
                loopTask.GetAwaiter().GetResult();
            }
            catch (OperationCanceledException)
            {
            }

            cancellationTokenSource.Dispose();
        }
    }

    private readonly record struct UdpRelayResult(IPEndPoint SourceEndPoint, byte[] Payload);

    private static async Task ReadExactlyAsync(Stream stream, byte[] buffer)
    {
        var offset = 0;
        while (offset < buffer.Length)
        {
            var read = await stream.ReadAsync(buffer, offset, buffer.Length - offset);
            if (read == 0)
                throw new EndOfStreamException("Unexpected end of SOCKS5 control stream.");

            offset += read;
        }
    }
}
