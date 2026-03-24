using System.Net;
using System.Net.Sockets;

namespace Titanium.Web.Proxy.Extensions
{
    internal static class IPAddressExtensions
    {
        /// <summary>
        /// Checks if the given IP format is an internal (private or loopback) network address.
        /// Covers IPv4 (10/8, 172.16/12, 192.168/16, 127/8, 169.254/16) and IPv6 loopback/local.
        /// Zero-dependency method for fast, allocation-free local IP filtration (SSRF protection).
        /// </summary>
        public static bool IsInternal(this IPAddress ip)
        {
            if (IPAddress.IsLoopback(ip)) return true;
            
            if (ip.AddressFamily == AddressFamily.InterNetwork)
            {
                byte[] bytes = ip.GetAddressBytes();
                if (bytes[0] == 10) return true; // 10.0.0.0/8
                if (bytes[0] == 172 && bytes[1] >= 16 && bytes[1] <= 31) return true; // 172.16.0.0/12
                if (bytes[0] == 192 && bytes[1] == 168) return true; // 192.168.0.0/16
                if (bytes[0] == 169 && bytes[1] == 254) return true; // 169.254.0.0/16
                // 100.64.0.0/10 (CGNAT)
                if (bytes[0] == 100 && bytes[1] >= 64 && bytes[1] <= 127) return true;

                // Multicast 224.0.0.0/4
                if (bytes[0] >= 224 && bytes[0] <= 239) return true;

                // Reserved 240.0.0.0/4
                if (bytes[0] >= 240) return true;
                // 0.0.0.0/8
                if (bytes[0] == 0) return true;
            }
            else if (ip.AddressFamily == AddressFamily.InterNetworkV6)
            {
                if (ip.IsIPv6LinkLocal || ip.IsIPv6SiteLocal || ip.IsIPv6Multicast) return true;
                // fc00::/7 (ULA)
                byte[] b = ip.GetAddressBytes();
                if ((b[0] & 0xFE) == 0xFC) return true; 
            }

            return false;
        }
    }
}
