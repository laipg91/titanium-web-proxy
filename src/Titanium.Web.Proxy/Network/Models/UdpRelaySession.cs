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
        private int disposed;

        internal UdpRelaySession(IPEndPoint clientUdpEndPoint)
        {
            ClientUdpEndPoint = clientUdpEndPoint;
            LastActivity = DateTime.UtcNow;
        }

        /// <summary>UDP source endpoint of the SOCKS5 client.</summary>
        internal IPEndPoint ClientUdpEndPoint { get; }

        /// <summary>Last time a datagram was relayed (used for idle cleanup).</summary>
        internal DateTime LastActivity { get; set; }

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
