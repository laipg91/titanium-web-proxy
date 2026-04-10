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
}
