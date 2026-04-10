using System.Net.Http;
using Titanium.Web.Proxy.IntegrationTests.Helpers;
using Titanium.Web.Proxy.IntegrationTests.Setup;

namespace Titanium.Web.Proxy.IntegrationTests;

public class TestSuite : System.IDisposable
{
    private readonly TestServer server;
    private TestProxyServer testProxyServer;

    public TestSuite(bool requireMutualTls = false)
        : this(requireMutualTls, null)
    {
    }

    public TestSuite(bool requireMutualTls, Microsoft.AspNetCore.Server.Kestrel.Core.HttpProtocols? protocols)
    {
        var dummyProxy = new ProxyServer();
        var serverCertificate = dummyProxy.CertificateManager.CreateServerCertificate("localhost").Result;
        server = new TestServer(serverCertificate, requireMutualTls, protocols);
    }

    public TestServer GetServer()
    {
        return server;
    }

    public ProxyServer GetProxy(ProxyServer upStreamProxy = null, bool enableHttp2 = false)
    {
        if (upStreamProxy != null)
        {
            testProxyServer = new TestProxyServer(false, upStreamProxy, enableHttp2);
            return testProxyServer.ProxyServer;
        }

        testProxyServer = new TestProxyServer(false, null, enableHttp2);
        return testProxyServer.ProxyServer;
    }

    public ProxyServer GetReverseProxy(ProxyServer upStreamProxy = null)
    {
        if (upStreamProxy != null)
        {
            testProxyServer = new TestProxyServer(true, upStreamProxy);
            return testProxyServer.ProxyServer;
        }

        testProxyServer = new TestProxyServer(true);
        return testProxyServer.ProxyServer;
    }

    public void Dispose()
    {
        server?.Dispose();
        testProxyServer?.Dispose();
    }

    public HttpClient GetClient(ProxyServer proxyServer, bool enableBasicProxyAuthorization = false)
    {
        return TestHelper.GetHttpClient(proxyServer.ProxyEndPoints[0].Port, enableBasicProxyAuthorization);
    }

    public HttpClient GetReverseProxyClient()
    {
        return TestHelper.GetHttpClient();
    }
}
