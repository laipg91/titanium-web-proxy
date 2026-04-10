using System;
using System.Net;
using Titanium.Web.Proxy.Models;

namespace Titanium.Web.Proxy.IntegrationTests.Setup;

public sealed class TestSocksProxyServer : IDisposable
{
    public TestSocksProxyServer(bool enableUdpSsrfFilter = false)
    {
        ProxyServer = new ProxyServer();

        var socksEndPoint = new SocksProxyEndPoint(IPAddress.Any, 0, false)
        {
            EnableUdpAssociate = true,
            EnableUdpSsrfFilter = enableUdpSsrfFilter,
        };

        ProxyServer.AddEndPoint(socksEndPoint);
        ProxyServer.Start();
    }

    public ProxyServer ProxyServer { get; }

    public int ListeningPort => ProxyServer.ProxyEndPoints[0].Port;

    public void Dispose()
    {
        ProxyServer.Stop();
        ProxyServer.Dispose();
    }
}
