using System.Net;

namespace Titanium.Web.Proxy
{
    /// <summary>
    /// Immutable DNS cache entry used by <see cref="UdpSocks5Header"/> resolver.
    /// Avoids C# 7 ValueTuple to remain compatible with net461 without NuGet dependency.
    /// </summary>
    internal sealed class DnsCacheEntry
    {
        internal readonly IPAddress Address;
        internal readonly long ExpiryTicks; // DateTime.UtcNow.Ticks at expiry

        internal DnsCacheEntry(IPAddress address, long expiryTicks)
        {
            Address     = address;
            ExpiryTicks = expiryTicks;
        }
    }
}
