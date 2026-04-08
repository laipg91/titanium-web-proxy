#if NET6_0_OR_GREATER
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using Titanium.Web.Proxy.Http;
using Titanium.Web.Proxy.Models;

namespace Titanium.Web.Proxy.Http2.Translation
{
    /// <summary>
    /// Converts HTTP/1.x headers to/from HTTP/2 pseudo-headers.
    /// RFC 7540 §8.1.2 — HTTP Header Fields.
    ///
    /// Single responsibility: header translation only.
    /// No I/O, no state, all methods are pure.
    /// </summary>
    internal static class Http2HeaderConverter
    {
        // ── Hop-by-hop headers (RFC 7540 §8.1.2.2) ───────────────────────────
        // These MUST NOT be forwarded in HTTP/2 frames.
        private static readonly HashSet<string> HopByHopHeaders =
            new(StringComparer.OrdinalIgnoreCase)
            {
                "connection",
                "keep-alive",
                "proxy-connection",
                "transfer-encoding",
                "te",
                "upgrade",
                "proxy-authorization",
            };

        // ── H1 → H2 ──────────────────────────────────────────────────────────

        /// <summary>
        /// Builds the complete list of HTTP/2 headers (pseudo-headers first, then regular)
        /// from an HTTP/1.x <see cref="Request"/>.
        /// Strips hop-by-hop headers that are forbidden in HTTP/2.
        /// </summary>
        internal static IReadOnlyList<(string Name, string Value)> ToHttp2RequestHeaders(Request request)
        {
            var uri    = request.RequestUri;
            var scheme = request.IsHttps ? "https" : "http";
            var path   = string.IsNullOrEmpty(uri.PathAndQuery) ? "/" : uri.PathAndQuery;

            var headers = new List<(string, string)>()
            {
                (":method",    request.Method),
                (":authority", uri.Authority),
                (":scheme",    scheme),
                (":path",      path),
            };

            foreach (var h in request.Headers)
            {
                if (!HopByHopHeaders.Contains(h.Name))
                    headers.Add((h.Name.ToLowerInvariant(), h.Value));
            }

            return headers;
        }

        /// <summary>
        /// Builds the complete list of HTTP/2 headers (pseudo-header first, then regular)
        /// from an HTTP/1.x <see cref="Response"/>.
        /// Strips hop-by-hop headers.
        /// </summary>
        internal static IReadOnlyList<(string Name, string Value)> ToHttp2ResponseHeaders(Response response)
        {
            var headers = new List<(string, string)>()
            {
                (":status", response.StatusCode.ToString()),
            };

            foreach (var h in response.Headers)
            {
                if (!HopByHopHeaders.Contains(h.Name))
                    headers.Add((h.Name.ToLowerInvariant(), h.Value));
            }

            return headers;
        }

        // ── H2 → H1 ──────────────────────────────────────────────────────────

        /// <summary>
        /// Applies decoded HTTP/2 headers to an HTTP/1.x <see cref="Request"/>.
        /// Pseudo-headers are mapped to request fields; regular headers are added to the collection.
        /// </summary>
        internal static void ApplyToHttp1Request(
            Request request,
            IReadOnlyList<(string Name, string Value)> headers)
        {
            string method    = string.Empty;
            string path      = "/";
            string authority = string.Empty;
            string scheme    = string.Empty;

            // First pass: extract pseudo-headers
            foreach (var (name, value) in headers)
            {
                if (name.Length == 0) continue;
                if (name[0] != ':')   continue;

                switch (name)
                {
                    case ":method":    method    = value; break;
                    case ":path":      path      = value; break;
                    case ":authority": authority = value; break;
                    case ":scheme":    scheme    = value; break;
                }
            }

            request.Method        = method;
            request.IsHttps       = string.Equals(scheme, "https", StringComparison.OrdinalIgnoreCase);
            request.HttpVersion   = HttpHeader.Version11;
            request.Authority     = (ByteString)authority;

            // Build an absolute-form URI so request.RequestUri works correctly
            var uriString = $"{scheme}://{authority}{path}";
            request.RequestUriString8 = (ByteString)uriString;

            // Second pass: regular headers (skip pseudo-headers)
            foreach (var (name, value) in headers)
            {
                if (name.Length == 0 || name[0] == ':') continue;
                request.Headers.AddHeader(new HttpHeader(name, value));
            }

            // Ensure Host header is present for HTTP/1.1 compatibility
            if (request.Headers.GetHeaderValueOrNull(KnownHeaders.Host) == null &&
                !string.IsNullOrEmpty(authority))
            {
                request.Headers.SetOrAddHeaderValue(KnownHeaders.Host, authority);
            }
        }

        /// <summary>
        /// Applies decoded HTTP/2 response headers to an HTTP/1.x <see cref="Response"/>.
        /// Pseudo-header <c>:status</c> is mapped to <see cref="Response.StatusCode"/>.
        /// </summary>
        internal static void ApplyToHttp1Response(
            Response response,
            IReadOnlyList<(string Name, string Value)> headers)
        {
            foreach (var (name, value) in headers)
            {
                if (name.Length == 0) continue;

                if (name[0] == ':')
                {
                    if (name == ":status" && int.TryParse(value, out int code))
                    {
                        response.StatusCode        = code;
                        response.StatusDescription = GetStatusDescription(code);
                    }
                    continue;
                }

                response.Headers.AddHeader(new HttpHeader(name, value));
            }

            response.HttpVersion = HttpHeader.Version11;
        }

        // ── Helpers ───────────────────────────────────────────────────────────

        private static string GetStatusDescription(int code) => code switch
        {
            200 => "OK",
            201 => "Created",
            204 => "No Content",
            206 => "Partial Content",
            301 => "Moved Permanently",
            302 => "Found",
            304 => "Not Modified",
            400 => "Bad Request",
            401 => "Unauthorized",
            403 => "Forbidden",
            404 => "Not Found",
            405 => "Method Not Allowed",
            408 => "Request Timeout",
            429 => "Too Many Requests",
            500 => "Internal Server Error",
            502 => "Bad Gateway",
            503 => "Service Unavailable",
            504 => "Gateway Timeout",
            _   => string.Empty,
        };
    }
}
#endif
