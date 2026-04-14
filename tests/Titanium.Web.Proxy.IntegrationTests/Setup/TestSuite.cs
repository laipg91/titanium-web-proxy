using System;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Titanium.Web.Proxy.IntegrationTests.Helpers;
using Titanium.Web.Proxy.IntegrationTests.Setup;
using Titanium.Web.Proxy.Network;

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
        dummyProxy.CertificateManager.CertificateEngine = CertificateEngine.BouncyCastleFast;
        var serverCertificate = dummyProxy.CertificateManager.CreateServerCertificate("localhost").Result
                                ?? CreateLocalhostCertificate();
        server = new TestServer(serverCertificate, requireMutualTls, protocols);
    }

    private static X509Certificate2 CreateLocalhostCertificate()
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            "CN=localhost",
            rsa,
            HashAlgorithmName.SHA256,
            RSASignaturePadding.Pkcs1);

        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(
            X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment,
            false));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
            new OidCollection { new Oid("1.3.6.1.5.5.7.3.1") },
            false));

        var subjectAlternativeNames = new SubjectAlternativeNameBuilder();
        subjectAlternativeNames.AddDnsName("localhost");
        subjectAlternativeNames.AddIpAddress(IPAddress.Loopback);
        request.CertificateExtensions.Add(subjectAlternativeNames.Build());

        using var certificate = request.CreateSelfSigned(
            DateTimeOffset.UtcNow.AddDays(-1),
            DateTimeOffset.UtcNow.AddYears(1));
        return new X509Certificate2(
            certificate.Export(X509ContentType.Pfx),
            string.Empty,
            X509KeyStorageFlags.Exportable | X509KeyStorageFlags.MachineKeySet);
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
