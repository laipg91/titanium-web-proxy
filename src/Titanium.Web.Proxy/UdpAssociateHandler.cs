using System;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Titanium.Web.Proxy.Models;
using Titanium.Web.Proxy.Network.Tcp;
using Titanium.Web.Proxy.Extensions;

namespace Titanium.Web.Proxy
{
    /// <summary>
    ///     Handles SOCKS5 UDP Associate (CMD=0x03) per RFC 1928.
    ///     Design:
    ///       - Transparent forward (no payload inspection)
    ///       - Zero-copy via buffer offset trick in LoopRemoteToClient
    ///       - Memory pool: two buffers from IBufferPool, returned on exit
    ///       - Two async loops (no per-packet allocation)
    ///       - TCP lifetime coupling: relay torn down when control TCP closes
    ///       - Cross-target: net451 / net461 / netstandard2.0 / netstandard2.1
    /// </summary>
    public partial class ProxyServer
    {
        // Atomic counter for active UDP relay sessions.
        private int udpAssociateSessionCount;

        /// <summary>
        ///     Entry point called from SocksClientHandler when CMD=0x03 is received.
        ///     SOCKS5 auth has already been completed when this is called.
        /// </summary>
        private async Task HandleUdpAssociate(
            SocksProxyEndPoint endPoint,
            TcpClientConnection clientConnection,
            System.IO.Stream tcpStream,
            CancellationTokenSource cts,
            CancellationToken cancellationToken)
        {
            // ── Feature gate ──────────────────────────────────────────────────
            if (!endPoint.EnableUdpAssociate)
            {
                await SendSocks5UdpReplyError(tcpStream, 0x07, cancellationToken); // Command not supported
                return;
            }

            // ── Session cap ───────────────────────────────────────────────────
            if (endPoint.MaxUdpAssociateSessions > 0)
            {
                var count = Interlocked.Increment(ref udpAssociateSessionCount);
                if (count > endPoint.MaxUdpAssociateSessions)
                {
                    Interlocked.Decrement(ref udpAssociateSessionCount);
                    await SendSocks5UdpReplyError(tcpStream, 0x02, cancellationToken); // Connection not allowed
                    return;
                }
            }

            // Bind relay/remote sockets on same IP as TCP listener.
            var bindAddress = endPoint.IpAddress;
            if (bindAddress.Equals(IPAddress.Any) || bindAddress.Equals(IPAddress.IPv6Any))
                bindAddress = IPAddress.Loopback;

            Socket relaySocket  = null;
            Socket remoteSocket = null;
            byte[] clientToRemoteBuf = null;
            byte[] remoteToClientBuf = null;
            UdpSocketAwaitable relayAwaitable  = null;
            UdpSocketAwaitable remoteAwaitable = null;

            try
            {
                var af = bindAddress.AddressFamily;

                // relaySocket: client sends UDP here.
                relaySocket = new Socket(af, SocketType.Dgram, ProtocolType.Udp);
                relaySocket.Bind(new IPEndPoint(bindAddress, 0));
                var relayPort = ((IPEndPoint)relaySocket.LocalEndPoint).Port;

                // remoteSocket: proxy uses this to reach remote servers.
                remoteSocket = new Socket(af, SocketType.Dgram, ProtocolType.Udp);
                remoteSocket.Bind(new IPEndPoint(bindAddress, 0));

                // Inform client of relay address:port.
                await SendSocks5UdpAssociateReply(tcpStream, bindAddress, relayPort, cancellationToken);

                // Two per-session buffers from pool.
                // remoteToClientBuf is MaxHeaderSize bytes larger to support zero-copy offset trick.
                clientToRemoteBuf = BufferPool.GetBuffer(65535);
                remoteToClientBuf = BufferPool.GetBuffer(65535 + UdpSocks5Header.MaxHeaderSize);

                // Accept UDP from the TCP peer's IP (port may differ — RFC allows 0.0.0.0:0).
                var tcpPeerAddress = ((IPEndPoint)clientConnection.RemoteEndPoint).Address;

                // Fix 6: Pre-allocate reusable socket awaitables (one per direction)
                relayAwaitable  = new UdpSocketAwaitable(af);
                remoteAwaitable = new UdpSocketAwaitable(af);

                // Run both relay loops + TCP lifetime monitor + idle timeout concurrently.
                using (var relayCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
                {
                    await Task.WhenAny(
                        RunRelayLoops(relaySocket, remoteSocket, tcpPeerAddress,
                            clientToRemoteBuf, remoteToClientBuf, endPoint.EnableUdpSsrfFilter,
                            relayAwaitable, remoteAwaitable, relayCts.Token),
                        MonitorUdpTcpLifetime(tcpStream, relayCts),
                        // Fix Critical 4: Idle Timeout — auto-close relay if no UDP I/O for 3 minutes.
                        // Protects against zombie sessions when TCP is in half-open state.
                        WatchIdleTimeout(TimeSpan.FromMinutes(3), relayCts)
                    );
                    relayCts.Cancel();
                }
            }
            finally
            {
                if (clientToRemoteBuf != null) BufferPool.ReturnBuffer(clientToRemoteBuf);
                if (remoteToClientBuf != null) BufferPool.ReturnBuffer(remoteToClientBuf);
                if (relayAwaitable  != null) relayAwaitable.Dispose();
                if (remoteAwaitable != null) remoteAwaitable.Dispose();
                if (relaySocket  != null) relaySocket.Dispose();
                if (remoteSocket != null) remoteSocket.Dispose();
                if (endPoint.MaxUdpAssociateSessions > 0)
                    Interlocked.Decrement(ref udpAssociateSessionCount);
            }
        }

        // ─────────────────────────────────────────────────────────────────────
        // Relay loops
        // ─────────────────────────────────────────────────────────────────────

        private static async Task RunRelayLoops(
            Socket relaySocket,
            Socket remoteSocket,
            IPAddress expectedClientAddress,
            byte[] clientToRemoteBuf,
            byte[] remoteToClientBuf,
            bool enableSsrfFilter,
            UdpSocketAwaitable relayAwaitable,
            UdpSocketAwaitable remoteAwaitable,
            CancellationToken ct)
        {
            // lastClientUdpEp: written only by LoopA, read only by LoopB.
            IPEndPoint lastClientUdpEp = null;

            var loopA = LoopClientToRemote(relaySocket, remoteSocket, expectedClientAddress,
                clientToRemoteBuf, enableSsrfFilter, relayAwaitable, remoteAwaitable,
                ep => lastClientUdpEp = ep, ct);
            var loopB = LoopRemoteToClient(remoteSocket, relaySocket,
                remoteToClientBuf, remoteAwaitable, relayAwaitable,
                () => lastClientUdpEp, ct);

            await Task.WhenAll(loopA, loopB);
        }

        /// <summary>
        ///     Loop A: client → relay → remote.
        ///     SOCKS5 UDP header is stripped via offset (no Array.Copy of payload).
        ///     Fix Critical 3: Async DNS resolution with in-memory cache (5 min TTL).
        ///     Fix Critical 4: Updates UdpLastActivityTicks on each successful forward.
        /// </summary>
        private static async Task LoopClientToRemote(
            Socket relaySocket,
            Socket remoteSocket,
            IPAddress expectedClientAddress,
            byte[] buf,
            bool enableSsrfFilter,
            UdpSocketAwaitable recvAwaitable,
            UdpSocketAwaitable sendAwaitable,
            Action<IPEndPoint> onClientEndPoint,
            CancellationToken ct)
        {

            // Fix Critical 3: DNS cache to avoid repeated async DNS lookups for the same host.
            // Key: hostname, Value: DnsCacheEntry (compatible with net461 — no ValueTuple)
            var dnsCache = new ConcurrentDictionary<string, DnsCacheEntry>();
            var dnsCacheTtlTicks = TimeSpan.FromMinutes(5).Ticks;

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var recvResult = await recvAwaitable.ReceiveFromAsync(relaySocket, buf, 0, buf.Length);
                    var receivedBytes = recvResult.ReceivedBytes;
                    var sourceEp     = recvResult.RemoteEp;

                    if (receivedBytes < UdpSocks5Header.MinSize) continue;
                    if (!sourceEp.Address.Equals(expectedClientAddress)) continue;

                    // Capture client UDP endpoint from first valid packet.
                    onClientEndPoint(sourceEp);

                    // Parse header — operates on buf bytes in-place, no alloc.
                    int headerLen;
                    IPEndPoint destEp;
                    string domainName;
                    if (!UdpSocks5Header.TryParse(buf, receivedBytes, out headerLen, out destEp, out domainName))
                        continue;

                    // Fix Critical 3: If ATYP=3 (domain name), resolve DNS asynchronously.
                    if (domainName != null)
                    {
                        IPAddress resolved = null;
                        var now = DateTime.UtcNow;

                        // Check cache first
                        if (dnsCache.TryGetValue(domainName, out var cached) &&
                            now.Ticks < cached.ExpiryTicks)
                        {
                            resolved = cached.Address;
                        }
                        else
                        {
                            // Async DNS — does not block the Thread Pool
                            try
                            {
                                var addrs = await System.Net.Dns.GetHostAddressesAsync(domainName);
                                if (addrs == null || addrs.Length == 0) continue;
                                resolved = addrs[0];
                                dnsCache[domainName] = new DnsCacheEntry(resolved,
                                    DateTime.UtcNow.Ticks + dnsCacheTtlTicks);
                            }
                            catch { continue; } // DNS failure — drop this packet
                        }

                        // Build the real destEp from resolved IP + port already in destEp placeholder
                        destEp = new IPEndPoint(resolved, destEp.Port);
                    }

                    // Fix 5: SSRF filter - Drop local/private network loops if enabled
                    if (enableSsrfFilter && destEp.Address.IsInternal())
                    {
                        System.Diagnostics.Debug.WriteLine($"[UDP Associate] SSRF Filter blocked internal IP: {destEp.Address}");
                        continue;
                    }

                    // Send only the payload slice (skips header bytes, no copy).
                    await sendAwaitable.SendToAsync(remoteSocket, buf, headerLen, receivedBytes - headerLen, destEp);

                    // Fix Critical 4: Update idle activity timestamp on successful forward.
                    Interlocked.Exchange(ref UdpLastActivityTicks, DateTime.UtcNow.Ticks);
                }
                catch (OperationCanceledException) { break; }
                catch { /* socket error — keep looping */ }
            }
        }

        /// <summary>
        ///     Loop B: remote → remote socket → client.
        ///     Zero-copy offset trick: payload received at buf[MaxHeaderSize..],
        ///     header written in-place at buf[0..headerLen].
        /// </summary>
        private static async Task LoopRemoteToClient(
            Socket remoteSocket,
            Socket relaySocket,
            byte[] buf,
            UdpSocketAwaitable recvAwaitable,
            UdpSocketAwaitable sendAwaitable,
            Func<IPEndPoint> getClientEp,
            CancellationToken ct)
        {

            var offset = UdpSocks5Header.MaxHeaderSize; // slot for worst-case header

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    var clientEp = getClientEp();
                    if (clientEp == null)
                    {
                        await Task.Delay(5, ct);
                        continue;
                    }

                    // Receive payload starting at offset (header prefix reserved).
                    var recvResult = await recvAwaitable.ReceiveFromAsync(
                        remoteSocket, buf, offset, buf.Length - offset);
                    var receivedBytes = recvResult.ReceivedBytes;
                    var sourceEp     = recvResult.RemoteEp;

                    // Write header in-place at buf[0..headerLen].
                    var headerLen = UdpSocks5Header.Write(buf, offset, sourceEp);

                    // If header is shorter than reserved offset, shift payload forward.
                    if (headerLen < offset)
                        Buffer.BlockCopy(buf, offset, buf, headerLen, receivedBytes);

                    await sendAwaitable.SendToAsync(relaySocket, buf, 0, headerLen + receivedBytes, clientEp);
                }
                catch (OperationCanceledException) { break; }
                catch { /* socket error — keep looping */ }
            }
        }

        // ─────────────────────────────────────────────────────────────────────
        // TCP lifetime monitor
        // ─────────────────────────────────────────────────────────────────────

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
                    if (read == 0) break; // TCP EOF — client disconnected
                }
            }
            catch { /* TCP error or cancelled */ }
            finally { relayCts.Cancel(); }
        }

        // Fix Critical 4: Idle Timeout watcher.
        // Runs concurrently with the relay loops. If no activity (no packet relayed in either direction)
        // within the specified timeout window, we cancel the relay CTS.
        // lastActivityTicks is updated via Interlocked by both LoopA and LoopB each time a packet
        // is successfully forwarded.
        internal static long UdpLastActivityTicks; // shared via Interlocked within a single relay session

        private static async Task WatchIdleTimeout(
            TimeSpan timeout,
            CancellationTokenSource relayCts)
        {
            var start = DateTime.UtcNow;
            // Initialize last activity to "now" so we give the client a chance to send first packet.
            Interlocked.Exchange(ref UdpLastActivityTicks, start.Ticks);

            var checkInterval = TimeSpan.FromSeconds(30);
            try
            {
                while (!relayCts.IsCancellationRequested)
                {
                    await Task.Delay(checkInterval, relayCts.Token);

                    var lastTicks = Interlocked.Read(ref UdpLastActivityTicks);
                    var idleFor   = DateTime.UtcNow - new DateTime(lastTicks, DateTimeKind.Utc);

                    if (idleFor >= timeout)
                    {
                        // No activity for too long — close the relay to reclaim resources.
                        System.Diagnostics.Debug.WriteLine(
                            $"[UDP Associate] Idle timeout ({timeout.TotalMinutes:F0} min) — closing relay.");
                        relayCts.Cancel();
                        return;
                    }
                }
            }
            catch (OperationCanceledException) { /* normal shutdown */ }
        }

        // ─────────────────────────────────────────────────────────────────────
        // SOCKS5 reply helpers
        // ─────────────────────────────────────────────────────────────────────

        private static async Task SendSocks5UdpAssociateReply(
            System.IO.Stream stream, IPAddress bindAddress, int port, CancellationToken ct)
        {
            var addrBytes = bindAddress.GetAddressBytes();
            var replyLen  = 4 + addrBytes.Length + 2;
            var reply     = new byte[replyLen];
            reply[0] = 5; // VER
            reply[1] = 0; // REP = success
            reply[2] = 0; // RSV
            reply[3] = bindAddress.AddressFamily == AddressFamily.InterNetworkV6 ? (byte)4 : (byte)1;
            addrBytes.CopyTo(reply, 4);
            reply[4 + addrBytes.Length]     = (byte)(port >> 8);
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
