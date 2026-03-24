using System;
using System.Net;
using System.Net.Sockets;

namespace Titanium.Web.Proxy
{
    /// <summary>
    ///     SOCKS5 UDP header (RFC 1928 §7):
    ///     RSV(2) + FRAG(1) + ATYP(1) + DST.ADDR(var) + DST.PORT(2)
    ///
    ///     All methods accept plain byte[] with offset to avoid Span on older targets.
    /// </summary>
    internal static class UdpSocks5Header
    {
        /// <summary>Minimum header (IPv4): 10 bytes.</summary>
        internal const int MinSize = 10;

        /// <summary>Maximum header (IPv6): 22 bytes.</summary>
        internal const int MaxHeaderSize = 22;

        /// <summary>
        ///     Parse a SOCKS5 UDP request header from <paramref name="buf"/>[0..<paramref name="len"/>].
        ///     Returns true on success. Allocates one IPEndPoint on success.
        ///     Fix Critical 3: For ATYP=3 (domain name), sets <paramref name="domainName"/> instead of
        ///     resolving DNS inline. <paramref name="destEp"/> will be IPAddress.Any:port as a placeholder.
        ///     Caller must resolve the domain asynchronously if domainName is not null.
        /// </summary>
        internal static bool TryParse(byte[] buf, int len, out int headerLen, out IPEndPoint destEp,
            out string domainName)
        {
            headerLen  = 0;
            destEp     = null;
            domainName = null;

            if (len < 4)            return false;
            if (buf[0] != 0 || buf[1] != 0) return false; // RSV must be 0
            if (buf[2] != 0)        return false;          // Reject fragmented datagrams

            var atyp = buf[3];
            int port;

            switch (atyp)
            {
                case 1: // IPv4
                    if (len < 10) return false;
                    var ipv4 = new byte[4];
                    Buffer.BlockCopy(buf, 4, ipv4, 0, 4);
                    port   = (buf[8] << 8) | buf[9];
                    destEp = new IPEndPoint(new IPAddress(ipv4), port);
                    headerLen = 10;
                    return true;

                case 3: // Domain name
                    if (len < 5) return false;
                    var nameLen = buf[4];
                    var needed  = 5 + nameLen + 2;
                    if (len < needed) return false;
                    var host = System.Text.Encoding.ASCII.GetString(buf, 5, nameLen);
                    port     = (buf[5 + nameLen] << 8) | buf[5 + nameLen + 1];
                    // Fix Critical 3: Return domain name to caller for async DNS resolution.
                    // Do NOT call Dns.GetHostAddresses (blocking) here.
                    domainName = host;
                    destEp     = new IPEndPoint(IPAddress.Any, port); // placeholder port carrier
                    headerLen  = needed;
                    return true;

                case 4: // IPv6
                    if (len < 22) return false;
                    var ipv6 = new byte[16];
                    Buffer.BlockCopy(buf, 4, ipv6, 0, 16);
                    port   = (buf[20] << 8) | buf[21];
                    destEp = new IPEndPoint(new IPAddress(ipv6), port);
                    headerLen = 22;
                    return true;

                default:
                    return false;
            }
        }

        /// <summary>
        ///     Write a SOCKS5 UDP response header for <paramref name="sourceEp"/> into
        ///     <paramref name="buf"/> starting at index 0.
        ///     Returns number of bytes written (≤ <see cref="MaxHeaderSize"/>).
        /// </summary>
        internal static int Write(byte[] buf, int bufSize, IPEndPoint sourceEp)
        {
            buf[0] = 0; // RSV
            buf[1] = 0; // RSV
            buf[2] = 0; // FRAG

            var addrBytes = sourceEp.Address.GetAddressBytes();
            var isIpv6    = sourceEp.AddressFamily == AddressFamily.InterNetworkV6;
            buf[3] = isIpv6 ? (byte)4 : (byte)1; // ATYP

            Buffer.BlockCopy(addrBytes, 0, buf, 4, addrBytes.Length);
            var idx = 4 + addrBytes.Length;
            buf[idx++] = (byte)(sourceEp.Port >> 8);
            buf[idx++] = (byte)(sourceEp.Port & 0xFF);

            return idx; // header length
        }
    }
}
