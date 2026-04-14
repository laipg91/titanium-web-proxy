using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
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
    public async Task Can_Run_Multiple_Grpc_Calls_Over_H2_Tls_Proxy()
    {
        // Regression coverage for https://github.com/justcoding121/Titanium-Web-Proxy/issues/838:
        // a TLS gRPC client makes unary, server-streaming, client-streaming, then bidi-streaming calls
        // through the proxy. The original issue reports the first call succeeds and the second fails.
        using var testSuite = new TestSuite(false, HttpProtocols.Http2);
        var server = testSuite.GetServer();
        server.HandleRequest(HandleGrpcRequest);

        using var directClient = new HttpClient(new HttpClientHandler
        {
            ServerCertificateCustomValidationCallback = (m, c, ch, er) => true
        });
        await AssertGrpcSequenceAsync(directClient, server.ListeningHttpsUrl);

        var proxy = testSuite.GetProxy(null, true);
        var proxyExceptions = new List<Exception>();
        proxy.ExceptionFunc = ex => proxyExceptions.Add(ex);
        proxy.ServerCertificateValidationCallback += (sender, e) =>
        {
            e.IsValid = true;
            return Task.CompletedTask;
        };

        var explicitEndPoint = (ExplicitProxyEndPoint)proxy.ProxyEndPoints[0];
        explicitEndPoint.BeforeTunnelConnectRequest += (sender, e) =>
        {
            e.DecryptSsl = true;
            return Task.CompletedTask;
        };

        var handler = new HttpClientHandler
        {
            Proxy = new TestHelper.TestProxy($"http://localhost:{proxy.ProxyEndPoints[0].Port}", false),
            UseProxy = true,
            ServerCertificateCustomValidationCallback = (m, c, ch, er) => true
        };

        using var client = new HttpClient(handler);

        try
        {
            await AssertGrpcSequenceAsync(client, server.ListeningHttpsUrl);
        }
        catch (Exception ex)
        {
            Assert.Fail($"gRPC over H2 TLS proxy failed: {ex}{Environment.NewLine}{FormatProxyExceptions(proxyExceptions)}");
        }

        Assert.AreEqual(0, proxyExceptions.Count, FormatProxyExceptions(proxyExceptions));
    }

    private static async Task AssertGrpcSequenceAsync(HttpClient client, string baseUrl)
    {
        CollectionAssert.AreEqual(
            new[] { "unary:one" },
            await SendGrpcCallAsync(client, baseUrl, "Unary", "one"));

        CollectionAssert.AreEqual(
            new[] { "server-stream:two:1", "server-stream:two:2" },
            await SendGrpcCallAsync(client, baseUrl, "ServerStreaming", "two"));

        CollectionAssert.AreEqual(
            new[] { "client-stream:three,four,five" },
            await SendGrpcCallAsync(client, baseUrl, "ClientStreaming", "three", "four", "five"));

        CollectionAssert.AreEqual(
            new[] { "bidi:six", "bidi:seven" },
            await SendGrpcCallAsync(client, baseUrl, "BidiStreaming", "six", "seven"));
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

    private static async Task HandleGrpcRequest(HttpContext context)
    {
        Assert.AreEqual("HTTP/2", context.Request.Protocol);
        Assert.AreEqual("application/grpc", context.Request.ContentType);

        context.Response.StatusCode = 200;
        context.Response.ContentType = "application/grpc";
        context.Response.DeclareTrailer("grpc-status");

        var requestMessages = await ReadGrpcMessagesAsync(context.Request.Body);
        var method = context.Request.Path.Value?.Split('/').Last();
        var responseMessages = method switch
        {
            "Unary" => new[] { "unary:" + requestMessages.Single() },
            "ServerStreaming" => new[] { $"server-stream:{requestMessages.Single()}:1", $"server-stream:{requestMessages.Single()}:2" },
            "ClientStreaming" => new[] { "client-stream:" + string.Join(",", requestMessages) },
            "BidiStreaming" => requestMessages.Select(x => "bidi:" + x).ToArray(),
            _ => throw new InvalidOperationException("Unexpected gRPC method: " + method)
        };

        foreach (var message in responseMessages)
        {
            var frame = CreateGrpcFrame(message);
            await context.Response.Body.WriteAsync(frame, 0, frame.Length);
            await context.Response.Body.FlushAsync();
        }

        context.Response.AppendTrailer("grpc-status", "0");
    }

    private static async Task<string[]> SendGrpcCallAsync(
        HttpClient client, string baseUrl, string method, params string[] messages)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, $"{baseUrl}/grpc.integration.TestService/{method}")
        {
            Version = HttpVersion.Version20,
            VersionPolicy = HttpVersionPolicy.RequestVersionExact,
            Content = new ByteArrayContent(CreateGrpcPayload(messages))
        };
        request.Headers.TE.Add(new TransferCodingWithQualityHeaderValue("trailers"));
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/grpc");

        using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);
        Assert.AreEqual(HttpStatusCode.OK, response.StatusCode);
        Assert.AreEqual(HttpVersion.Version20, response.Version);

        await using var responseBody = await response.Content.ReadAsStreamAsync();
        var responseMessages = await ReadGrpcMessagesAsync(responseBody);

        var grpcStatus = response.TrailingHeaders.TryGetValues("grpc-status", out var values)
            ? values.Single()
            : null;
        Assert.AreEqual("0", grpcStatus, "gRPC response trailer grpc-status should be 0.");

        return responseMessages;
    }

    private static byte[] CreateGrpcPayload(params string[] messages)
    {
        using var ms = new MemoryStream();
        foreach (var message in messages)
        {
            var frame = CreateGrpcFrame(message);
            ms.Write(frame, 0, frame.Length);
        }

        return ms.ToArray();
    }

    private static byte[] CreateGrpcFrame(string message)
    {
        var payload = Encoding.UTF8.GetBytes(message);
        var frame = new byte[5 + payload.Length];
        frame[0] = 0; // uncompressed
        frame[1] = (byte)((payload.Length >> 24) & 0xff);
        frame[2] = (byte)((payload.Length >> 16) & 0xff);
        frame[3] = (byte)((payload.Length >> 8) & 0xff);
        frame[4] = (byte)(payload.Length & 0xff);
        Buffer.BlockCopy(payload, 0, frame, 5, payload.Length);
        return frame;
    }

    private static async Task<string[]> ReadGrpcMessagesAsync(Stream stream)
    {
        var messages = new List<string>();
        var header = new byte[5];

        while (true)
        {
            int headerRead = await ReadAtLeastAsync(stream, header, header.Length);
            if (headerRead == 0)
                break;

            Assert.AreEqual(header.Length, headerRead, "Incomplete gRPC message header.");
            Assert.AreEqual(0, header[0], "Compressed gRPC messages are not used by this test.");

            int length = (header[1] << 24) | (header[2] << 16) | (header[3] << 8) | header[4];
            var payload = new byte[length];
            int payloadRead = await ReadAtLeastAsync(stream, payload, length);
            Assert.AreEqual(length, payloadRead, "Incomplete gRPC message payload.");
            messages.Add(Encoding.UTF8.GetString(payload));
        }

        return messages.ToArray();
    }

    private static async Task<int> ReadAtLeastAsync(Stream stream, byte[] buffer, int count)
    {
        int total = 0;
        while (total < count)
        {
            int read = await stream.ReadAsync(buffer, total, count - total);
            if (read == 0)
                break;

            total += read;
        }

        return total;
    }

    private static string FormatProxyExceptions(IReadOnlyCollection<Exception> proxyExceptions)
    {
        return proxyExceptions.Count == 0
            ? "No proxy exception captured."
            : "Proxy exceptions:" + Environment.NewLine + string.Join(
                Environment.NewLine + "---" + Environment.NewLine, proxyExceptions);
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
