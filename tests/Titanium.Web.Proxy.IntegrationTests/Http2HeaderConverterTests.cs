using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Titanium.Web.Proxy.Http;
using Titanium.Web.Proxy.Http2.Translation;

namespace Titanium.Web.Proxy.IntegrationTests;

[TestClass]
public class Http2HeaderConverterTests
{
    [TestMethod]
    public void ToHttp2RequestHeaders_Strips_Connection_Specific_Headers_And_Normalizes_Te()
    {
        var request = new Request
        {
            Method = "GET",
            RequestUriString = "https://example.com/test?q=1",
            HttpVersion = new Version(1, 1)
        };

        request.Headers.AddHeader("Connection", "Foo, Trailer");
        request.Headers.AddHeader("Foo", "bar");
        request.Headers.AddHeader("Trailer", "Expires");
        request.Headers.AddHeader("Keep-Alive", "timeout=5");
        request.Headers.AddHeader("Transfer-Encoding", "chunked");
        request.Headers.AddHeader("Upgrade", "websocket");
        request.Headers.AddHeader("Proxy-Authorization", "Basic abc");
        request.Headers.AddHeader("Host", "override.example");
        request.Headers.AddHeader("HTTP2-Settings", "AAMAAABkAAQCAAAAAAIAAAAA");
        request.Headers.AddHeader("TE", "gzip, trailers");
        request.Headers.AddHeader("X-Test", "ok");

        var headers = Http2HeaderConverter.ToHttp2RequestHeaders(request);

        AssertHeaderPresent(headers, ":method", "GET");
        AssertHeaderPresent(headers, ":scheme", "https");
        AssertHeaderPresent(headers, ":authority", "example.com");
        AssertHeaderPresent(headers, ":path", "/test?q=1");
        AssertHeaderPresent(headers, "te", "trailers");
        AssertHeaderPresent(headers, "x-test", "ok");

        AssertHeaderMissing(headers, "connection");
        AssertHeaderMissing(headers, "foo");
        AssertHeaderMissing(headers, "trailer");
        AssertHeaderMissing(headers, "keep-alive");
        AssertHeaderMissing(headers, "transfer-encoding");
        AssertHeaderMissing(headers, "upgrade");
        AssertHeaderMissing(headers, "proxy-authorization");
        AssertHeaderMissing(headers, "host");
        AssertHeaderMissing(headers, "http2-settings");
    }

    [TestMethod]
    public void ToHttp2ResponseHeaders_Strips_Connection_Specific_And_Proxy_Response_Headers()
    {
        var response = new Response
        {
            StatusCode = 200,
            StatusDescription = "OK",
            HttpVersion = new Version(1, 1)
        };

        response.Headers.AddHeader("Connection", "Foo");
        response.Headers.AddHeader("Foo", "bar");
        response.Headers.AddHeader("Keep-Alive", "timeout=5");
        response.Headers.AddHeader("Transfer-Encoding", "chunked");
        response.Headers.AddHeader("Proxy-Authenticate", "Basic realm=test");
        response.Headers.AddHeader("Proxy-Authentication-Info", "nextnonce=abc");
        response.Headers.AddHeader("Trailer", "Expires");
        response.Headers.AddHeader("TE", "trailers");
        response.Headers.AddHeader("X-Test", "ok");

        var headers = Http2HeaderConverter.ToHttp2ResponseHeaders(response);

        AssertHeaderPresent(headers, ":status", "200");
        AssertHeaderPresent(headers, "trailer", "Expires");
        AssertHeaderPresent(headers, "x-test", "ok");

        AssertHeaderMissing(headers, "connection");
        AssertHeaderMissing(headers, "foo");
        AssertHeaderMissing(headers, "keep-alive");
        AssertHeaderMissing(headers, "transfer-encoding");
        AssertHeaderMissing(headers, "proxy-authenticate");
        AssertHeaderMissing(headers, "proxy-authentication-info");
        AssertHeaderMissing(headers, "te");
    }

    private static void AssertHeaderPresent(
        System.Collections.Generic.IReadOnlyList<(string Name, string Value)> headers,
        string expectedName,
        string expectedValue)
    {
        Assert.IsTrue(headers.Any(h => h.Name == expectedName && h.Value == expectedValue),
            $"Expected header '{expectedName}: {expectedValue}' was not found.");
    }

    private static void AssertHeaderMissing(
        System.Collections.Generic.IReadOnlyList<(string Name, string Value)> headers,
        string forbiddenName)
    {
        Assert.IsFalse(headers.Any(h => h.Name == forbiddenName),
            $"Header '{forbiddenName}' should have been stripped.");
    }
}
