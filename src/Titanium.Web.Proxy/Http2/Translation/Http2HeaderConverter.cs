#if NET6_0_OR_GREATER
using System;
using System.Collections.Generic;
using Titanium.Web.Proxy.Http;
using Titanium.Web.Proxy.Models;

namespace Titanium.Web.Proxy.Http2.Translation
{
    /// <summary>
    /// Converts HTTP/1.x headers to/from HTTP/2 pseudo-headers.
    /// RFC 9113 forbids connection-specific fields in HTTP/2, and RFC 9110
    /// requires intermediaries to strip fields named by Connection.
    /// </summary>
    internal static class Http2HeaderConverter
    {
        private static readonly HashSet<string> CommonForbiddenHeaders =
            new(StringComparer.OrdinalIgnoreCase)
            {
                "connection",
                "keep-alive",
                "proxy-connection",
                "transfer-encoding",
                "upgrade",
            };

        private static readonly HashSet<string> RequestForbiddenHeaders =
            new(StringComparer.OrdinalIgnoreCase)
            {
                "proxy-authorization",
                "host",
                "http2-settings",
            };

        private static readonly HashSet<string> ResponseForbiddenHeaders =
            new(StringComparer.OrdinalIgnoreCase)
            {
                "proxy-authenticate",
                "proxy-authentication-info",
                "te",
            };

        internal static IReadOnlyList<(string Name, string Value)> ToHttp2RequestHeaders(Request request)
        {
            var uri = request.RequestUri;
            var scheme = request.IsHttps ? "https" : "http";
            var path = string.IsNullOrEmpty(uri.PathAndQuery) ? "/" : uri.PathAndQuery;
            var connectionTokens = GetConnectionHeaderTokens(request.Headers);

            var headers = new List<(string, string)>
            {
                (":method", request.Method),
                (":authority", uri.Authority),
                (":scheme", scheme),
                (":path", path),
            };

            foreach (var header in request.Headers)
            {
                if (ShouldSkipRequestHeader(header.Name, connectionTokens))
                    continue;

                if (header.Name.Equals("te", StringComparison.OrdinalIgnoreCase))
                {
                    if (TryNormalizeTeForHttp2(header.Value, out var normalizedTe))
                        headers.Add(("te", normalizedTe));

                    continue;
                }

                headers.Add((header.Name.ToLowerInvariant(), header.Value));
            }

            return headers;
        }

        internal static IReadOnlyList<(string Name, string Value)> ToHttp2ResponseHeaders(Response response)
        {
            var connectionTokens = GetConnectionHeaderTokens(response.Headers);

            var headers = new List<(string, string)>
            {
                (":status", response.StatusCode.ToString()),
            };

            foreach (var header in response.Headers)
            {
                if (ShouldSkipResponseHeader(header.Name, connectionTokens))
                    continue;

                headers.Add((header.Name.ToLowerInvariant(), header.Value));
            }

            return headers;
        }

        internal static void ApplyToHttp1Request(
            Request request,
            IReadOnlyList<(string Name, string Value)> headers)
        {
            string method = string.Empty;
            string path = "/";
            string authority = string.Empty;
            string scheme = string.Empty;

            foreach (var (name, value) in headers)
            {
                if (name.Length == 0 || name[0] != ':')
                    continue;

                switch (name)
                {
                    case ":method":
                        method = value;
                        break;
                    case ":path":
                        path = value;
                        break;
                    case ":authority":
                        authority = value;
                        break;
                    case ":scheme":
                        scheme = value;
                        break;
                }
            }

            request.Method = method;
            request.IsHttps = string.Equals(scheme, "https", StringComparison.OrdinalIgnoreCase);
            request.HttpVersion = HttpHeader.Version11;
            request.Authority = (ByteString)authority;
            request.RequestUriString8 = (ByteString)$"{scheme}://{authority}{path}";

            var connectionTokens = GetConnectionHeaderTokens(headers);
            foreach (var (name, value) in headers)
            {
                if (name.Length == 0 || name[0] == ':')
                    continue;

                if (ShouldSkipRequestHeader(name, connectionTokens))
                    continue;

                request.Headers.AddHeader(new HttpHeader(name, value));
            }

            if (request.Headers.GetHeaderValueOrNull(KnownHeaders.Host) == null &&
                !string.IsNullOrEmpty(authority))
            {
                request.Headers.SetOrAddHeaderValue(KnownHeaders.Host, authority);
            }
        }

        internal static void ApplyToHttp1Response(
            Response response,
            IReadOnlyList<(string Name, string Value)> headers)
        {
            var connectionTokens = GetConnectionHeaderTokens(headers);

            foreach (var (name, value) in headers)
            {
                if (name.Length == 0)
                    continue;

                if (name[0] == ':')
                {
                    if (name == ":status" && int.TryParse(value, out int code))
                    {
                        response.StatusCode = code;
                        response.StatusDescription = GetStatusDescription(code);
                    }

                    continue;
                }

                if (ShouldSkipResponseHeader(name, connectionTokens))
                    continue;

                response.Headers.AddHeader(new HttpHeader(name, value));
            }

            response.HttpVersion = HttpHeader.Version11;
        }

        private static bool ShouldSkipRequestHeader(string name, HashSet<string> connectionTokens)
        {
            return CommonForbiddenHeaders.Contains(name) ||
                   RequestForbiddenHeaders.Contains(name) ||
                   connectionTokens.Contains(name);
        }

        private static bool ShouldSkipResponseHeader(string name, HashSet<string> connectionTokens)
        {
            return CommonForbiddenHeaders.Contains(name) ||
                   ResponseForbiddenHeaders.Contains(name) ||
                   connectionTokens.Contains(name);
        }

        private static HashSet<string> GetConnectionHeaderTokens(HeaderCollection headers)
        {
            return GetConnectionHeaderTokens(headers.GetHeaderValueOrNull(KnownHeaders.Connection));
        }

        private static HashSet<string> GetConnectionHeaderTokens(IReadOnlyList<(string Name, string Value)> headers)
        {
            var tokens = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var (name, value) in headers)
            {
                if (name.Equals("connection", StringComparison.OrdinalIgnoreCase))
                    AddConnectionHeaderTokens(tokens, value);
            }

            return tokens;
        }

        private static HashSet<string> GetConnectionHeaderTokens(string? connection)
        {
            var tokens = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            AddConnectionHeaderTokens(tokens, connection);
            return tokens;
        }

        private static void AddConnectionHeaderTokens(HashSet<string> tokens, string? connection)
        {
            if (string.IsNullOrWhiteSpace(connection))
                return;

            foreach (var part in connection.Split(','))
            {
                var token = part.Trim();
                if (token.Length != 0)
                    tokens.Add(token);
            }
        }

        private static bool TryNormalizeTeForHttp2(string value, out string normalized)
        {
            normalized = string.Empty;

            if (string.IsNullOrWhiteSpace(value))
                return false;

            foreach (var part in value.Split(','))
            {
                var token = part.Trim();
                var parameterIndex = token.IndexOf(';');
                if (parameterIndex >= 0)
                    token = token.Substring(0, parameterIndex).Trim();

                if (token.Equals("trailers", StringComparison.OrdinalIgnoreCase))
                {
                    normalized = "trailers";
                    return true;
                }
            }

            return false;
        }

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
            _ => string.Empty,
        };
    }
}
#endif
