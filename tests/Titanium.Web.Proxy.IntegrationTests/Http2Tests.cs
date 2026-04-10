using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Titanium.Web.Proxy.IntegrationTests.Setup;
using Titanium.Web.Proxy.IntegrationTests.Helpers;
using Titanium.Web.Proxy.Models;

namespace Titanium.Web.Proxy.IntegrationTests;

[TestClass]
public class Http2Tests
{
    private static async Task HandleRequest(HttpContext context)
    {
        if (context.Request.Path == "/chunked")
        {
            var bodyChars = await new StreamReader(context.Request.Body).ReadToEndAsync();
            context.Response.StatusCode = 200;
            // Write a chunked response just by flushing
            await context.Response.WriteAsync("Response-Chunked-");
            await context.Response.Body.FlushAsync();
            await context.Response.WriteAsync(bodyChars);
        }
        else
        {
            var bodyChars = await new StreamReader(context.Request.Body).ReadToEndAsync();
            context.Response.StatusCode = 200;
            await context.Response.WriteAsync("Response-" + bodyChars);
        }
    }

    [TestMethod]
    public async Task Can_Translate_H1_Client_To_H2_Server()
    {
        // Server supports ONLY HTTP/2
        using var testSuite = new TestSuite(false, HttpProtocols.Http2);
        var server = testSuite.GetServer();
        server.HandleRequest(HandleRequest);

        var proxy = testSuite.GetProxy(null, true);
        var proxyExceptions = new List<Exception>();
        proxy.ExceptionFunc = ex => proxyExceptions.Add(ex);

        // Client forces HTTP/1.1
        using var client = testSuite.GetClient(proxy);
        var request = new HttpRequestMessage(HttpMethod.Post, new Uri(server.ListeningHttpsUrl))
        {
            Version = HttpVersion.Version11,
            VersionPolicy = HttpVersionPolicy.RequestVersionExact,
            Content = new StringContent("H1-H2-TestData", Encoding.UTF8, "text/plain")
        };

        HttpResponseMessage response;
        try
        {
            response = await client.SendAsync(request);
        }
        catch (Exception ex)
        {
            var proxyError = proxyExceptions.Count == 0
                ? "No proxy exception captured."
                : string.Join(Environment.NewLine + "---" + Environment.NewLine, proxyExceptions);
            Assert.Fail($"Client request failed: {ex}{Environment.NewLine}Proxy exceptions:{Environment.NewLine}{proxyError}");
            throw;
        }

        Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
        var responseBody = await response.Content.ReadAsStringAsync();
        Assert.AreEqual("Response-H1-H2-TestData", responseBody);

    }

    [TestMethod]
    public async Task Can_Translate_H2_Client_To_H1_Server()
    {
        // Server supports ONLY HTTP/1.1
        using var testSuite = new TestSuite(false, HttpProtocols.Http1);
        var server = testSuite.GetServer();
        server.HandleRequest(HandleRequest);

        var proxy = testSuite.GetProxy(null, true);
        var proxyExceptions = new List<Exception>();
        proxy.ExceptionFunc = ex => proxyExceptions.Add(ex);

        // Client forces HTTP/2
        using var client = testSuite.GetClient(proxy);
        var request = new HttpRequestMessage(HttpMethod.Post, new Uri(server.ListeningHttpsUrl))
        {
            Version = HttpVersion.Version20,
            VersionPolicy = HttpVersionPolicy.RequestVersionExact,
            Content = new StringContent("H2-H1-TestData", Encoding.UTF8, "text/plain")
        };

        HttpResponseMessage response;
        try
        {
            response = await client.SendAsync(request);
        }
        catch (Exception ex)
        {
            var proxyError = proxyExceptions.Count == 0
                ? "No proxy exception captured."
                : string.Join(Environment.NewLine + "---" + Environment.NewLine, proxyExceptions);
            Assert.Fail($"Client request failed: {ex}{Environment.NewLine}Proxy exceptions:{Environment.NewLine}{proxyError}");
            throw;
        }

        Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
        var responseBody = await response.Content.ReadAsStringAsync();
        Assert.AreEqual("Response-H2-H1-TestData", responseBody);
    }

    [TestMethod]
    public async Task Can_Relay_H2_Client_To_H2_Server()
    {
        // Server supports ONLY HTTP/2
        using var testSuite = new TestSuite(false, HttpProtocols.Http2);
        var server = testSuite.GetServer();
        server.HandleRequest(HandleRequest);

        var proxy = testSuite.GetProxy(null, true);
        // Client forces HTTP/2
        using var client = testSuite.GetClient(proxy);
        var request = new HttpRequestMessage(HttpMethod.Post, new Uri(server.ListeningHttpsUrl))
        {
            Version = HttpVersion.Version20,
            VersionPolicy = HttpVersionPolicy.RequestVersionExact,
            Content = new StringContent("H2-H2-TestData", Encoding.UTF8, "text/plain")
        };

        var response = await client.SendAsync(request);

        Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
        var responseBody = await response.Content.ReadAsStringAsync();
        Assert.AreEqual("Response-H2-H2-TestData", responseBody);
        Assert.AreEqual(HttpVersion.Version20, response.Version);
    }

    [TestMethod]
    public async Task Can_Translate_H1_Client_To_H2_Server_Chunked_Request()
    {
        // Server supports ONLY HTTP/2
        using var testSuite = new TestSuite(false, HttpProtocols.Http2);
        var server = testSuite.GetServer();
        server.HandleRequest(HandleRequest);

        var proxy = testSuite.GetProxy(null, true);

        // Client forces HTTP/1.1
        using var client = testSuite.GetClient(proxy);
        var request = new HttpRequestMessage(HttpMethod.Post, new Uri(server.ListeningHttpsUrl + "/chunked"))
        {
            Version = HttpVersion.Version11,
            VersionPolicy = HttpVersionPolicy.RequestVersionExact,
        };

        // Create a streamed content without length to compel chunked encoding in H1
        var ms = new MemoryStream(Encoding.UTF8.GetBytes("Chunked-Upload"));
        request.Content = new StreamContent(ms);
        request.Headers.TransferEncodingChunked = true; 

        var response = await client.SendAsync(request);

        Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
        var responseBody = await response.Content.ReadAsStringAsync();
        Assert.AreEqual("Response-Chunked-Chunked-Upload", responseBody);
    }
    
    [TestMethod]
    public async Task Can_Translate_H2_Client_To_H1_Server_Chunked_Response()
    {
        // Server supports ONLY HTTP/1.1
        using var testSuite = new TestSuite(false, HttpProtocols.Http1);
        var server = testSuite.GetServer();
        server.HandleRequest(HandleRequest);

        var proxy = testSuite.GetProxy(null, true);

        // Client forces HTTP/2
        using var client = testSuite.GetClient(proxy);
        var request = new HttpRequestMessage(HttpMethod.Post, new Uri(server.ListeningHttpsUrl + "/chunked"))
        {
            Version = HttpVersion.Version20,
            VersionPolicy = HttpVersionPolicy.RequestVersionExact,
            Content = new StringContent("H2-H1-TestChunkedData", Encoding.UTF8, "text/plain")
        };

        var response = await client.SendAsync(request);

        Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
        var responseBody = await response.Content.ReadAsStringAsync();
        Assert.AreEqual("Response-Chunked-H2-H1-TestChunkedData", responseBody);
    }
    
    [TestMethod]
    public async Task Can_Relay_H2_To_External_NgHttp2()
    {
        // No local server needed for this external test
        using var testSuite = new TestSuite();
        var proxy = testSuite.GetProxy(null, true);
        
        int dataSentCount = 0;
        int dataReceivedCount = 0;

        var explicitEndPoint = (ExplicitProxyEndPoint)proxy.ProxyEndPoints[0];
        explicitEndPoint.BeforeTunnelConnectRequest += (sender, e) =>
        {
            if (e.HttpClient.Request.RequestUri.Host.Contains("nghttp2.org"))
            {
                e.DecryptSsl = true;
            }
            return Task.CompletedTask;
        };

        proxy.BeforeRequest += (sender, e) =>
        {
            if (e.HttpClient.Request.RequestUri.Host.Contains("nghttp2.org"))
            {
                e.DataSent += (s, args) => dataSentCount++;
                e.DataReceived += (s, args) => dataReceivedCount++;
            }
            return Task.CompletedTask;
        };

        // Custom client with certificate validation bypass for proxy fake certs
        var handler = new HttpClientHandler
        {
            Proxy = new WebProxy($"http://localhost:{proxy.ProxyEndPoints[0].Port}"),
            UseProxy = true,
            ServerCertificateCustomValidationCallback = (m, c, ch, er) => true
        };

        using var client = new HttpClient(handler);
        var request = new HttpRequestMessage(HttpMethod.Get, "https://nghttp2.org/")
        {
            Version = HttpVersion.Version20,
            VersionPolicy = HttpVersionPolicy.RequestVersionExact
        };

        var response = await client.SendAsync(request);

        Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
        
        // Data events should have fired if it was a real H2 relay inside the decrypted session
        Assert.IsTrue(dataSentCount > 0, "OnDataSent should have fired");
        Assert.IsTrue(dataReceivedCount > 0, "OnDataReceived should have fired");
    }

    [TestMethod]
    public async Task Can_Translate_H2_Client_To_External_Http1()
    {
        using var testSuite = new TestSuite();
        var proxy = testSuite.GetProxy(null, true);

        int dataSentCount = 0;
        int dataReceivedCount = 0;
        proxy.BeforeRequest += (sender, e) =>
        {
            if (e.HttpClient.Request.RequestUri.Host.Contains("neverssl.com"))
            {
                e.DataSent += (s, args) => dataSentCount++;
                e.DataReceived += (s, args) => dataReceivedCount++;
            }
            return Task.CompletedTask;
        };

        // Client forces HTTP/2
        var handler = new HttpClientHandler
        {
            Proxy = new WebProxy($"http://localhost:{proxy.ProxyEndPoints[0].Port}"),
            UseProxy = true,
            ServerCertificateCustomValidationCallback = (m, c, ch, er) => true
        };

        using var client = new HttpClient(handler);
        client.DefaultRequestHeaders.Add("User-Agent", "Mozilla/5.0");
        var request = new HttpRequestMessage(HttpMethod.Get, "http://neverssl.com/")
        {
            Version = HttpVersion.Version20,
            // neverssl.com is HTTP-1.1 accessible, so the proxy will contact it via HTTP/1.1
            // Even if the request from HttpClient is downgraded by .NET or sent as H1 over proxy, 
            // we test it to ensure no exceptions are thrown and translation / relay works.
        };

        var response = await client.SendAsync(request);

        if (response.StatusCode != HttpStatusCode.OK)
        {
            var err = await response.Content.ReadAsStringAsync();
            Assert.Fail($"Status: {response.StatusCode}, Body: {err}");
        }

        Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
        var responseBody = await response.Content.ReadAsStringAsync();
        Assert.IsTrue(responseBody.Contains("neverssl", StringComparison.OrdinalIgnoreCase), "Response should contain neverssl content");
        Assert.IsTrue(dataSentCount > 0, "OnDataSent should have fired");
        Assert.IsTrue(dataReceivedCount > 0, "OnDataReceived should have fired");
    }

    [TestMethod]
    public async Task Can_Translate_H1_Client_To_External_Http2()
    {
        using var testSuite = new TestSuite();
        var proxy = testSuite.GetProxy(null, true);

        int dataSentCount = 0;
        int dataReceivedCount = 0;
        proxy.BeforeRequest += (sender, e) =>
        {
            if (e.HttpClient.Request.RequestUri.Host.Contains("nghttp2.org"))
            {
                e.DataSent += (s, args) => dataSentCount++;
                e.DataReceived += (s, args) => dataReceivedCount++;
            }
            return Task.CompletedTask;
        };

        var explicitEndPoint = (ExplicitProxyEndPoint)proxy.ProxyEndPoints[0];
        explicitEndPoint.BeforeTunnelConnectRequest += (sender, e) =>
        {
            if (e.HttpClient.Request.RequestUri.Host.Contains("nghttp2.org"))
            {
                e.DecryptSsl = true;
            }
            return Task.CompletedTask;
        };

        // Client forces HTTP/1.1
        var handler = new HttpClientHandler
        {
            Proxy = new WebProxy($"http://localhost:{proxy.ProxyEndPoints[0].Port}"),
            UseProxy = true,
            ServerCertificateCustomValidationCallback = (m, c, ch, er) => true
        };

        using var client = new HttpClient(handler);
        var request = new HttpRequestMessage(HttpMethod.Get, "https://nghttp2.org/")
        {
            Version = HttpVersion.Version11,
            VersionPolicy = HttpVersionPolicy.RequestVersionExact
        };

        var response = await client.SendAsync(request);

        Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
        var responseBody = await response.Content.ReadAsStringAsync();
        // nghttp2.org homepage likely contains something we can assert
        Assert.IsTrue(responseBody.Contains("nghttp2", StringComparison.OrdinalIgnoreCase), "Response should contain nghttp2 content");
        Assert.IsTrue(dataSentCount > 0, "OnDataSent should have fired");
        Assert.IsTrue(dataReceivedCount > 0, "OnDataReceived should have fired");
    }
}
