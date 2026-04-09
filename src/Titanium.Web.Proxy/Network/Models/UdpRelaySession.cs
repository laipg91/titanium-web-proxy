using System;
using System.Net;
using System.Threading;

namespace Titanium.Web.Proxy.Network.Tcp
{
    /// <summary>
    ///     Holds state for a single SOCKS5 UDP Associate relay session.
    ///     Lifetime is coupled to the controlling TCP connection.
    /// </summary>
    internal sealed class UdpRelaySession : IDisposable
    {
        private IPEndPoint? clientUdpEndPoint;
        private int disposed;
        private long lastActivityTicks;

        internal UdpRelaySession()
        {
            Touch();
        }

        /// <summary>UDP source endpoint of the SOCKS5 client.</summary>
        internal IPEndPoint? ClientUdpEndPoint => clientUdpEndPoint;

        internal void SetClientUdpEndPoint(IPEndPoint udpEndPoint)
        {
            clientUdpEndPoint = udpEndPoint;
        }

        /// <summary>Last time a datagram was relayed (used for idle cleanup).</summary>
        internal DateTime LastActivityUtc => new DateTime(Interlocked.Read(ref lastActivityTicks), DateTimeKind.Utc);

        internal void Touch()
        {
            Interlocked.Exchange(ref lastActivityTicks, DateTime.UtcNow.Ticks);
        }

        public void Dispose()
        {
            if (Interlocked.CompareExchange(ref disposed, 1, 0) == 0)
            {
                // Nothing to dispose here — sockets are owned by the relay loops.
                // This class is purely a state record.
            }
        }
    }
}
