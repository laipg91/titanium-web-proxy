using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Titanium.Web.Proxy.Extensions;
using Titanium.Web.Proxy.Models;
using Titanium.Web.Proxy.Network.Tcp;

namespace Titanium.Web.Proxy
{
    /// <summary>
    ///     Handles SOCKS5 UDP ASSOCIATE (CMD=0x03) per RFC 1928.
    /// </summary>
    public partial class ProxyServer
    {
        private const int DefaultUdpAssociateTimeoutSeconds = 120;

        // Atomic counter for active UDP relay sessions.
        private int udpAssociateSessionCount;

        /// <summary>
        /// Handles the SOCKS5 UDP ASSOCIATE command.
        /// Entry point called from SocksClientHandler when CMD=0x03 is received.
        /// SOCKS5 auth has already been completed when this is called.
        /// </summary>
        /// <param name="endPoint">The proxy endpoint.</param>
        /// <param name="clientConnection">The client connection.</param>
        /// <param name="tcpStream">The client TCP stream.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        private async Task HandleUdpAssociate(
            SocksProxyEndPoint endPoint,
            TcpClientConnection clientConnection,
            System.IO.Stream tcpStream,
            CancellationToken cancellationToken)
        {
            if (!endPoint.EnableUdpAssociate)
            {
                await SendSocks5UdpReplyError(tcpStream, 0x07, cancellationToken);
                return;
            }

            if (endPoint.MaxUdpAssociateSessions > 0)
            {
                var count = Interlocked.Increment(ref udpAssociateSessionCount);
                if (count > endPoint.MaxUdpAssociateSessions)
                {
                    Interlocked.Decrement(ref udpAssociateSessionCount);
                    await SendSocks5UdpReplyError(tcpStream, 0x02, cancellationToken);
                    return;
                }
            }

            var bindAddress = ResolveUdpBindAddress(endPoint, clientConnection);
            var idleTimeoutSeconds = endPoint.UdpAssociateTimeoutSeconds > 0
                ? endPoint.UdpAssociateTimeoutSeconds
                : DefaultUdpAssociateTimeoutSeconds;
            var idleTimeout = TimeSpan.FromSeconds(idleTimeoutSeconds);

            Socket? relaySocket = null;
            Socket? remoteSocket = null;
            byte[]? clientToRemoteBuf = null;
            byte[]? remoteToClientBuf = null;
            UdpSocketAwaitable? relayAwaitable = null;
            UdpSocketAwaitable? remoteAwaitable = null;
            var relaySession = new UdpRelaySession();

            try
            {
                var addressFamily = bindAddress.AddressFamily;

                relaySocket = new Socket(addressFamily, SocketType.Dgram, ProtocolType.Udp);
                relaySocket.Bind(new IPEndPoint(bindAddress, 0));
                var relayLocalEp = (IPEndPoint)relaySocket.LocalEndPoint!;

                remoteSocket = new Socket(addressFamily, SocketType.Dgram, ProtocolType.Udp);
                remoteSocket.Bind(new IPEndPoint(bindAddress, 0));

                await SendSocks5UdpAssociateReply(
                    tcpStream,
                    relayLocalEp.Address,
                    relayLocalEp.Port,
                    cancellationToken);

                clientToRemoteBuf = BufferPool.GetBuffer(65535);
                remoteToClientBuf = BufferPool.GetBuffer(65535 + UdpSocks5Header.MaxHeaderSize);

                var tcpPeerAddress = ((IPEndPoint)clientConnection.RemoteEndPoint).Address;

                relayAwaitable = new UdpSocketAwaitable(addressFamily);
                remoteAwaitable = new UdpSocketAwaitable(addressFamily);

                using (var relayCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
                {
                    await Task.WhenAny(
                        RunRelayLoops(
                            relaySocket,
                            remoteSocket,
                            tcpPeerAddress,
                            relaySession,
                            clientToRemoteBuf,
                            remoteToClientBuf,
                            endPoint.EnableUdpSsrfFilter,
                            relayAwaitable,
                            remoteAwaitable,
                            relayCts.Token),
                        MonitorUdpTcpLifetime(tcpStream, relayCts),
                        WatchIdleTimeout(idleTimeout, relaySession, relayCts));

                    relayCts.Cancel();
                }
            }
            finally
            {
                if (clientToRemoteBuf != null) BufferPool.ReturnBuffer(clientToRemoteBuf);
                if (remoteToClientBuf != null) BufferPool.ReturnBuffer(remoteToClientBuf);
                relayAwaitable?.Dispose();
                remoteAwaitable?.Dispose();
                relaySocket?.Dispose();
                remoteSocket?.Dispose();
                relaySession.Dispose();

                if (endPoint.MaxUdpAssociateSessions > 0)
                    Interlocked.Decrement(ref udpAssociateSessionCount);
            }
        }

        /// <summary>
        /// Resolves the IP address to bind both the relay and remote sockets to.
        /// </summary>
        private static IPAddress ResolveUdpBindAddress(
            SocksProxyEndPoint endPoint,
            TcpClientConnection clientConnection)
        {
            var bindAddress = endPoint.IpAddress;
            if (!bindAddress.Equals(IPAddress.Any) && !bindAddress.Equals(IPAddress.IPv6Any))
                return bindAddress;

            var tcpLocalAddress = ((IPEndPoint)clientConnection.LocalEndPoint).Address;
            if (!tcpLocalAddress.Equals(IPAddress.Any) && !tcpLocalAddress.Equals(IPAddress.IPv6Any))
                return tcpLocalAddress;

            return bindAddress.AddressFamily == AddressFamily.InterNetworkV6
                ? IPAddress.IPv6Loopback
                : IPAddress.Loopback;
        }

        /// <summary>
        /// Runs both relay loops (Client-to-Remote and Remote-to-Client).
        /// </summary>
        private static async Task RunRelayLoops(
            Socket relaySocket,
            Socket remoteSocket,
            IPAddress expectedClientAddress,
            UdpRelaySession relaySession,
            byte[] clientToRemoteBuf,
            byte[] remoteToClientBuf,
            bool enableSsrfFilter,
            UdpSocketAwaitable relayAwaitable,
            UdpSocketAwaitable remoteAwaitable,
            CancellationToken ct)
        {
            var loopA = LoopClientToRemote(
                relaySocket,
                remoteSocket,
                expectedClientAddress,
                relaySession,
                clientToRemoteBuf,
                enableSsrfFilter,
                relayAwaitable,
                remoteAwaitable,
                ct);

            var loopB = LoopRemoteToClient(
                remoteSocket,
                relaySocket,
                relaySession,
                remoteToClientBuf,
                remoteAwaitable,
                relayAwaitable,
                ct);

            await Task.WhenAll(loopA, loopB);
        }

        /// <summary>
        /// Loop A: client -> relay -> remote.
        /// SOCKS5 UDP header is stripped via offset.
        /// </summary>
        private static async Task LoopClientToRemote(
            Socket relaySocket,
            Socket remoteSocket,
            IPAddress expectedClientAddress,
            UdpRelaySession relaySession,
            byte[] buf,
            bool enableSsrfFilter,
            UdpSocketAwaitable recvAwaitable,
            UdpSocketAwaitable sendAwaitable,
            CancellationToken ct)
        {
            var dnsCache = new ConcurrentDictionary<string, DnsCacheEntry>();
            var dnsCacheTtlTicks = TimeSpan.FromMinutes(5).Ticks;

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var recvResult = await recvAwaitable.ReceiveFromAsync(relaySocket, buf, 0, buf.Length);
                    var receivedBytes = recvResult.ReceivedBytes;
                    var sourceEp = recvResult.RemoteEp;

                    if (receivedBytes < UdpSocks5Header.MinSize) continue;
                    if (!sourceEp.Address.Equals(expectedClientAddress)) continue;

                    int headerLen;
                    IPEndPoint destEp;
                    string? domainName;
                    if (!UdpSocks5Header.TryParse(buf, receivedBytes, out headerLen, out destEp, out domainName))
                        continue;

                    if (domainName != null)
                    {
                        IPAddress? resolved = null;
                        var now = DateTime.UtcNow;
                        var dnsCacheKey = GetDnsCacheKey(domainName, remoteSocket.AddressFamily);

                        if (dnsCache.TryGetValue(dnsCacheKey, out var cached) &&
                            now.Ticks < cached.ExpiryTicks)
                        {
                            resolved = cached.Address;
                        }
                        else
                        {
                            try
                            {
                                var addrs = await System.Net.Dns.GetHostAddressesAsync(domainName);
                                if (addrs == null || addrs.Length == 0) continue;

                                resolved = SelectBestResolvedAddress(addrs, remoteSocket.AddressFamily);
                                if (resolved == null) continue;

                                dnsCache[dnsCacheKey] = new DnsCacheEntry(
                                    resolved,
                                    DateTime.UtcNow.Ticks + dnsCacheTtlTicks);
                            }
                            catch
                            {
                                continue;
                            }
                        }

                        destEp = new IPEndPoint(resolved, destEp.Port);
                    }

                    if (enableSsrfFilter && destEp.Address.IsInternal())
                    {
                        System.Diagnostics.Debug.WriteLine(
                            $"[UDP Associate] SSRF Filter blocked internal IP: {destEp.Address}");
                        continue;
                    }

                    relaySession.SetClientUdpEndPoint(sourceEp);
                    await sendAwaitable.SendToAsync(
                        remoteSocket,
                        buf,
                        headerLen,
                        receivedBytes - headerLen,
                        destEp);
                    relaySession.Touch();
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch
                {
                    // Keep the relay alive across transient UDP/socket errors.
                }
            }
        }

        private static string GetDnsCacheKey(string domainName, AddressFamily addressFamily)
        {
            return domainName + "|" + (int)addressFamily;
        }

        private static IPAddress? SelectBestResolvedAddress(IPAddress[] addresses, AddressFamily addressFamily)
        {
            foreach (var address in addresses)
            {
                if (address.AddressFamily == addressFamily)
                    return address;
            }

            return addresses.Length > 0 ? addresses[0] : null;
        }

        /// <summary>
        /// Loop B: remote -> remote socket -> client.
        /// Payload is received after a reserved header prefix, then the SOCKS5 header is written in-place.
        /// </summary>
        private static async Task LoopRemoteToClient(
            Socket remoteSocket,
            Socket relaySocket,
            UdpRelaySession relaySession,
            byte[] buf,
            UdpSocketAwaitable recvAwaitable,
            UdpSocketAwaitable sendAwaitable,
            CancellationToken ct)
        {
            var offset = UdpSocks5Header.MaxHeaderSize;

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var clientEp = relaySession.ClientUdpEndPoint;
                    if (clientEp == null)
                    {
                        await Task.Delay(5, ct);
                        continue;
                    }

                    var recvResult = await recvAwaitable.ReceiveFromAsync(
                        remoteSocket,
                        buf,
                        offset,
                        buf.Length - offset);
                    var receivedBytes = recvResult.ReceivedBytes;
                    var sourceEp = recvResult.RemoteEp;

                    var headerLen = UdpSocks5Header.Write(buf, offset, sourceEp);
                    if (headerLen < offset)
                        Buffer.BlockCopy(buf, offset, buf, headerLen, receivedBytes);

                    await sendAwaitable.SendToAsync(
                        relaySocket,
                        buf,
                        0,
                        headerLen + receivedBytes,
                        clientEp);
                    relaySession.Touch();
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch
                {
                    // Keep the relay alive across transient UDP/socket errors.
                }
            }
        }

        /// <summary>
        /// Monitors the SOCKS TCP connection. If it closes, the UDP relay session is terminated.
        /// </summary>
        private static async Task MonitorUdpTcpLifetime(
            System.IO.Stream tcpStream,
            CancellationTokenSource relayCts)
        {
            try
            {
                var buf = new byte[4];
                while (!relayCts.IsCancellationRequested)
                {
                    var read = await tcpStream.ReadAsync(buf, 0, buf.Length, relayCts.Token);
                    if (read == 0) break;
                }
            }
            catch
            {
                // Normal shutdown or socket abort.
            }
            finally
            {
                relayCts.Cancel();
            }
        }

        /// <summary>
        /// Watches for inactivity on the UDP relay. If no activity occurs within the timeout, the session is terminated.
        /// </summary>
        private static async Task WatchIdleTimeout(
            TimeSpan timeout,
            UdpRelaySession relaySession,
            CancellationTokenSource relayCts)
        {
            var checkInterval = TimeSpan.FromSeconds(30);

            try
            {
                while (!relayCts.IsCancellationRequested)
                {
                    await Task.Delay(checkInterval, relayCts.Token);

                    var idleFor = DateTime.UtcNow - relaySession.LastActivityUtc;
                    if (idleFor < timeout) continue;

                    System.Diagnostics.Debug.WriteLine(
                        $"[UDP Associate] Idle timeout ({timeout.TotalMinutes:F0} min) - closing relay.");
                    relayCts.Cancel();
                    return;
                }
            }
            catch (OperationCanceledException)
            {
                // Normal shutdown.
            }
        }

        /// <summary>
        /// Sends the SOCKS5 reply to the client after a successful UDP ASSOCIATE bind.
        /// </summary>
        private static async Task SendSocks5UdpAssociateReply(
            System.IO.Stream stream, IPAddress bindAddress, int port, CancellationToken ct)
        {
            var addrBytes = bindAddress.GetAddressBytes();
            var replyLen = 4 + addrBytes.Length + 2;
            var reply = new byte[replyLen];
            reply[0] = 5; // VER
            reply[1] = 0; // REP = success
            reply[2] = 0; // RSV
            reply[3] = bindAddress.AddressFamily == AddressFamily.InterNetworkV6 ? (byte)4 : (byte)1;
            addrBytes.CopyTo(reply, 4);
            reply[4 + addrBytes.Length] = (byte)(port >> 8);
            reply[4 + addrBytes.Length + 1] = (byte)(port & 0xFF);
            await stream.WriteAsync(reply, 0, reply.Length, ct);
        }

        private static async Task SendSocks5UdpReplyError(
            System.IO.Stream stream, byte repCode, CancellationToken ct)
        {
            var reply = new byte[] { 5, repCode, 0, 1, 0, 0, 0, 0, 0, 0 };
            await stream.WriteAsync(reply, 0, reply.Length, ct);
        }
    }
}
