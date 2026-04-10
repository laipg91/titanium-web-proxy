#if NET6_0_OR_GREATER
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Titanium.Web.Proxy.EventArguments;
using Titanium.Web.Proxy.Exceptions;
using Titanium.Web.Proxy.Extensions;
using Titanium.Web.Proxy.Helpers;
using Titanium.Web.Proxy.Http;
using Titanium.Web.Proxy.Http2.Hpack;
using Titanium.Web.Proxy.Http2.Primitives;
using Titanium.Web.Proxy.Models;
using Decoder = Titanium.Web.Proxy.Http2.Hpack.Decoder;

namespace Titanium.Web.Proxy.Http2.Translation
{
    /// <summary>
    /// Scenario A: HTTP/1.x client ↔ HTTP/2 server.
    ///
    /// Flow:
    ///   1. Read HTTP/1.x request line + headers from <c>clientStream</c>.
    ///   2. Convert to HTTP/2 HEADERS frame via <see cref="Http2HeaderConverter"/> and send to server.
    ///   3. Stream request body as DATA frames (if present).
    ///   4. Read HTTP/2 HEADERS + DATA frames from server.
    ///   5. Convert to HTTP/1.x response and write to <c>clientStream</c>.
    ///   6. Loop for keep-alive (pipelined H1 requests over same TCP connection).
    ///
    /// Only activated when:
    ///   - <c>EnableHttp2 = true</c>, connection is HTTPS
    ///   - Client negotiated HTTP/1.x in ALPN with proxy
    ///   - Server negotiated HTTP/2 in ALPN with proxy
    /// </summary>
    internal sealed class Http1ToHttp2Translator : IHttp2Translator
    {
        /// <summary>
        /// Runs the translation loop for an HTTP/1.x client and an HTTP/2 server.
        /// </summary>
        public async Task TranslateAsync(
            HttpClientStream clientStream,
            Stream serverStream,
            SessionEventArgs? initialSession,
            Func<SessionEventArgs> sessionFactory,
            Func<SessionEventArgs, Task> onBeforeRequest,
            Func<SessionEventArgs, Task> onBeforeResponse,
            CancellationTokenSource cts,
            Guid connectionId,
            ExceptionHandler? exceptionFunc)
        {
            var ct             = cts.Token;
            var serverSettings = new Http2Settings();
            var encoderState   = new Http2EncoderState();
            var frameHeader    = new Http2FrameHeader();
            var frameHeaderBuf = new byte[9];
            var dataBuffer     = new byte[serverSettings.MaxFrameSize];

            // H2 stream IDs sent by proxy→server start at 1 and increment by 2 (client-initiated)
            int nextStreamId = 1;

            await serverStream.WriteAsync(Http2Helper.ConnectionPreface, 0, Http2Helper.ConnectionPreface.Length, ct);
            await Http2FrameWriter.SendSettingsAsync(serverStream, frameHeaderBuf, ct);

            // Wait for server's connection preface (SETTINGS frame) before sending requests.
            // This also handles any PING frames sent by the server before SETTINGS.
            await ConsumeServerPrefaceAsync(
                serverStream, serverSettings, frameHeader, frameHeaderBuf, dataBuffer, ct, exceptionFunc);

            // ── Bug #1 Fix: if caller already parsed the first request, use it directly ──
            bool firstRequest = true;

            while (!ct.IsCancellationRequested)
            {
                SessionEventArgs args;

                if (firstRequest && initialSession != null)
                {
                    // Re-use the pre-parsed session from RequestHandler.
                    // RequestHandler has already: read request line, read headers,
                    // assigned RequestUriString8, Method, HttpVersion, and fired OnBeforeRequest.
                    args = initialSession;
                    firstRequest = false;
                }
                else
                {
                    // ── Keep-Alive: parse the next request from the client stream ──
                    // Bug #2 Fix: use proxy's own parser so RequestUriString8 and Authority
                    // are populated correctly (matching what RequestHandler does).
                    firstRequest = false;

                    var requestLine = await clientStream.ReadRequestLine(ct);
                    if (requestLine.IsEmpty()) return; // client closed connection

                    args = sessionFactory();
                    var req = args.HttpClient.Request;

                    req.RequestUriString8 = requestLine.RequestUri; // Bug #2 Fix: was missing
                    req.Method            = requestLine.Method;
                    req.HttpVersion       = requestLine.Version;
                    req.IsHttps           = true; // always HTTPS in this scenario

                    // Use proxy's standard HeaderParser (handles folded headers, trims, etc.)
                    await HeaderParser.ReadHeaders(clientStream, req.Headers, ct);
                    req.SetOriginalHeaders();

                    // Sync Authority from Host header so :authority pseudo-header is set correctly
                    var host = req.Headers.GetHeaderValueOrNull(KnownHeaders.Host);
                    if (!string.IsNullOrEmpty(host))
                        req.Authority = (ByteString)host;

                    await onBeforeRequest(args);
                }

                var request = args.HttpClient.Request;

                // ── Encode and send H2 HEADERS frame to server ───────────────
                int streamId = nextStreamId;
                nextStreamId += 2;

                var h2Headers = Http2HeaderConverter.ToHttp2RequestHeaders(request);
                await SendH2RequestHeadersAsync(
                    serverStream, serverSettings, encoderState,
                    frameHeader, frameHeaderBuf,
                    h2Headers, streamId, !request.HasBody, ct);

                // ── Stream request body as DATA frames ───────────────────────
                if (request.HasBody)
                    await ForwardBodyAsDataFramesAsync(
                        clientStream, serverStream,
                        frameHeader, frameHeaderBuf, dataBuffer, args, streamId, ct);

                // ── Read H2 response from server ─────────────────────────────
                var response = args.HttpClient.Response;
                response.RequestMethod = request.Method;
                await ReadH2ResponseAsync(
                    serverStream, serverSettings, encoderState,
                    frameHeader, frameHeaderBuf, dataBuffer,
                    args, streamId, ct, exceptionFunc);

                // Fire before-response hook
                await onBeforeResponse(args);

                // ── Write HTTP/1.x response to client ────────────────────────
                await WriteH1ResponseAsync(clientStream, response, ct);

                args.Dispose();

                // If client or response says close, exit loop
                if (!response.KeepAlive) return;
            }
        }

        // ── Server preface consumer ───────────────────────────────────────────

        /// <summary>
        /// Consumes the server's connection preface (mandatory SETTINGS frame).
        /// </summary>
        private static async Task ConsumeServerPrefaceAsync(
            Stream serverStream, Http2Settings serverSettings,
            Http2FrameHeader frameHeader, byte[] headerBuffer, byte[] dataBuffer,
            CancellationToken ct, ExceptionHandler? exceptionFunc)
        {
            // Read frames until we see SETTINGS (server preface).
            // RFC 7540 §3.5: server must send SETTINGS as connection preface.
            while (true)
            {
                if (!await Http2FrameReader.TryReadFrameHeaderAsync(serverStream, headerBuffer, frameHeader, ct))
                    return;

                int length = frameHeader.Length;
                if (length > 0)
                {
                    int read = await Http2FrameReader.ForceReadAsync(serverStream, dataBuffer, 0, length, ct);
                    if (read != length) return;
                }

                if (frameHeader.Type == Http2FrameType.Settings)
                {
                    ParseSettings(serverSettings, dataBuffer, length);
                    // Send SETTINGS ACK back
                    await Http2FrameWriter.SendSettingsAckAsync(serverStream, headerBuffer, ct);
                    return; // preface done
                }

                if (frameHeader.Type == Http2FrameType.Ping)
                {
                    var payload = new byte[8];
                    Array.Copy(dataBuffer, payload, Math.Min(8, length));
                    await Http2FrameWriter.SendPingAckAsync(serverStream, headerBuffer, payload, ct);
                }
            }
        }

        // ── Send H2 request HEADERS ───────────────────────────────────────────

        /// <summary>
        /// Encodes and sends HTTP/2 request headers to the server.
        /// </summary>
        private static async Task SendH2RequestHeadersAsync(
            Stream serverStream, Http2Settings serverSettings, Http2EncoderState encoderState,
            Http2FrameHeader frameHeader, byte[] headerBuffer,
            IReadOnlyList<(string Name, string Value)> h2Headers,
            int streamId, bool endStream,
            CancellationToken ct)
        {
            // Build HPACK-encoded header block
            using var ms     = new MemoryStream();
            var       writer = new BinaryWriter(ms);

            EnsureEncoder(encoderState, serverSettings);
            var encoder = encoderState.Encoder!;

            foreach (var (name, value) in h2Headers)
            {
                var nameBytes  = System.Text.Encoding.ASCII.GetBytes(name);
                var valueBytes = System.Text.Encoding.ASCII.GetBytes(value);
                encoder.EncodeHeader(writer, (ByteString)nameBytes, (ByteString)valueBytes);
            }

            var encoded = ms.ToArray();
            var flags   = Http2FrameFlag.EndHeaders;
            if (endStream) flags |= Http2FrameFlag.EndStream;

            frameHeader.Length   = encoded.Length;
            frameHeader.Type     = Http2FrameType.Headers;
            frameHeader.Flags    = flags;
            frameHeader.StreamId = streamId;
            frameHeader.CopyToBuffer(headerBuffer);

            await serverStream.WriteAsync(headerBuffer, 0, 9,              ct);
            await serverStream.WriteAsync(encoded,      0, encoded.Length, ct);
        }

        // ── Forward request body as DATA frames ───────────────────────────────

        /// <summary>
        /// Forwards the HTTP/1.x request body to the server as a series of HTTP/2 DATA frames.
        /// </summary>
        private static async Task ForwardBodyAsDataFramesAsync(
            HttpClientStream clientStream, Stream serverStream,
            Http2FrameHeader frameHeader, byte[] headerBuffer, byte[] dataBuffer,
            SessionEventArgs args, int streamId, CancellationToken ct)
        {
            var request = args.HttpClient.Request;
            if (request.IsChunked)
            {
                await ForwardChunkedBodyAsDataFramesAsync(
                    clientStream, serverStream, frameHeader, headerBuffer, dataBuffer, args, streamId, ct);
                return;
            }

            long remaining = request.ContentLength;
            while (remaining > 0)
            {
                int toRead = Math.Min(dataBuffer.Length, (int)Math.Min(remaining, int.MaxValue));
                int read = await clientStream.ReadAsync(dataBuffer, 0, toRead, ct);
                if (read == 0)
                    throw new IOException("Unexpected end of HTTP/1.x request body.");

                remaining -= read;
                args.OnDataSent(dataBuffer, 0, read);
                await SendDataFrameAsync(
                    serverStream, frameHeader, headerBuffer, dataBuffer, read, streamId, remaining == 0, ct);
            }
        }

        private static async Task ForwardChunkedBodyAsDataFramesAsync(
            HttpClientStream clientStream, Stream serverStream,
            Http2FrameHeader frameHeader, byte[] headerBuffer, byte[] dataBuffer,
            SessionEventArgs args, int streamId, CancellationToken ct)
        {
            while (true)
            {
                var chunkHead = await clientStream.ReadLineAsync(ct);
                if (chunkHead == null)
                    throw new IOException("Unexpected end of chunked request body.");

                int separatorIndex = chunkHead.IndexOf(';');
                if (separatorIndex >= 0)
                    chunkHead = chunkHead.Substring(0, separatorIndex);

                if (!int.TryParse(chunkHead, System.Globalization.NumberStyles.HexNumber, null, out int chunkSize))
                    throw new ProxyHttpException($"Invalid chunk length: '{chunkHead}'", null, null);

                if (chunkSize == 0)
                {
                    await ConsumeChunkTrailersAsync(clientStream, ct);
                    args.OnDataSent(dataBuffer, 0, 0);
                    await SendDataFrameAsync(
                        serverStream, frameHeader, headerBuffer, dataBuffer, 0, streamId, true, ct);
                    return;
                }

                int remaining = chunkSize;
                while (remaining > 0)
                {
                    int toRead = Math.Min(dataBuffer.Length, remaining);
                    int read = await clientStream.ReadAsync(dataBuffer, 0, toRead, ct);
                    if (read == 0)
                        throw new IOException("Unexpected end of chunked request data.");

                    remaining -= read;
                    args.OnDataSent(dataBuffer, 0, read);
                    await SendDataFrameAsync(
                        serverStream, frameHeader, headerBuffer, dataBuffer, read, streamId, false, ct);
                }

                var chunkTerminator = await clientStream.ReadLineAsync(ct);
                if (chunkTerminator == null)
                    throw new IOException("Unexpected end of chunked request terminator.");
            }
        }

        private static async Task ConsumeChunkTrailersAsync(HttpClientStream clientStream, CancellationToken ct)
        {
            while (true)
            {
                var trailerLine = await clientStream.ReadLineAsync(ct);
                if (trailerLine == null || trailerLine.Length == 0)
                    return;
            }
        }

        private static async Task SendDataFrameAsync(
            Stream serverStream, Http2FrameHeader frameHeader, byte[] headerBuffer, byte[] dataBuffer,
            int payloadLength, int streamId, bool endStream, CancellationToken ct)
        {
            frameHeader.Length = payloadLength;
            frameHeader.Type = Http2FrameType.Data;
            frameHeader.Flags = endStream ? Http2FrameFlag.EndStream : (Http2FrameFlag)0;
            frameHeader.StreamId = streamId;
            frameHeader.CopyToBuffer(headerBuffer);

            await serverStream.WriteAsync(headerBuffer, 0, 9, ct);
            if (payloadLength > 0)
                await serverStream.WriteAsync(dataBuffer, 0, payloadLength, ct);
        }

        // ── Read H2 response from server ──────────────────────────────────────

        /// <summary>
        /// Reads the HTTP/2 response from the server and populates the <see cref="Response"/> object.
        /// </summary>
        private static async Task ReadH2ResponseAsync(
            Stream serverStream, Http2Settings serverSettings, Http2EncoderState encoderState,
            Http2FrameHeader frameHeader, byte[] headerBuffer, byte[] dataBuffer,
            SessionEventArgs args, int expectedStreamId,
            CancellationToken ct, ExceptionHandler? exceptionFunc)
        {
            var         response   = args.HttpClient.Response;
            var         bodyAccum  = new MemoryStream();
            Decoder?    decoder    = null;
            int         tableSize  = 0;
            var         headers    = new List<(string, string)>();

            while (true)
            {
                if (!await Http2FrameReader.TryReadFrameHeaderAsync(serverStream, headerBuffer, frameHeader, ct))
                    return;

                int  length   = frameHeader.Length;
                int  streamId = frameHeader.StreamId;
                var  type     = frameHeader.Type;
                var  flags    = frameHeader.Flags;

                if (length > 0)
                {
                    int read = await Http2FrameReader.ForceReadAsync(serverStream, dataBuffer, 0, length, ct);
                    if (read != length) return;
                }

                // Handle connection-level control frames regardless of stream
                if (type == Http2FrameType.Settings)
                {
                    if ((flags & Http2FrameFlag.Ack) == 0)
                    {
                        ParseSettings(serverSettings, dataBuffer, length);
                        await Http2FrameWriter.SendSettingsAckAsync(serverStream, headerBuffer, ct);
                    }
                    continue;
                }

                if (type == Http2FrameType.Ping)
                {
                    var payload = new byte[8];
                    Array.Copy(dataBuffer, payload, Math.Min(8, length));
                    await Http2FrameWriter.SendPingAckAsync(serverStream, headerBuffer, payload, ct);
                    continue;
                }

                if (type == Http2FrameType.WindowUpdate) continue; // just acknowledge locally

                // Only process frames for our stream
                if (streamId != expectedStreamId) continue;

                if (type == Http2FrameType.Headers)
                {
                    // Decode HPACK response headers
                    if (decoder == null || tableSize < serverSettings.HeaderTableSize)
                    {
                        tableSize = serverSettings.HeaderTableSize;
                        decoder   = new Decoder(8192, tableSize);
                    }

                    headers.Clear();
                    var listener = new SimpleHeaderListener(headers);
                    try
                    {
                        decoder.Decode(new BinaryReader(new MemoryStream(dataBuffer, 0, length)), listener);
                        decoder.EndHeaderBlock();
                    }
                    catch (Exception ex)
                    {
                        exceptionFunc?.Invoke(new ProxyHttpException("Failed to decode H2 response headers", ex, null));
                        return;
                    }

                    Http2HeaderConverter.ApplyToHttp1Response(response, headers);
                    response.SetOriginalHeaders();

                    if ((flags & Http2FrameFlag.EndStream) != 0) break;
                }
                else if (type == Http2FrameType.Data)
                {
                    args.OnDataReceived(dataBuffer, 0, length);
                    bodyAccum.Write(dataBuffer, 0, length);
                    if ((flags & Http2FrameFlag.EndStream) != 0) break;
                }
                else if (type == Http2FrameType.RstStream)
                {
                    uint errorCode = ((uint)dataBuffer[0] << 24) | ((uint)dataBuffer[1] << 16)
                                   | ((uint)dataBuffer[2] << 8)  |  dataBuffer[3];
                    exceptionFunc?.Invoke(new ProxyHttpException(
                        $"Server reset H2 stream {expectedStreamId} with error {errorCode}", null, null));
                    return;
                }
            }

            if (bodyAccum.Length > 0)
            {
                response.Body        = bodyAccum.ToArray();
                response.IsBodyRead  = true;
            }
        }

        // ── Write HTTP/1.x response to client ────────────────────────────────

        private static async Task WriteH1ResponseAsync(HttpClientStream clientStream, Response response, CancellationToken ct)
        {
            response.HttpVersion = HttpHeader.Version11;
            await clientStream.WriteResponseAsync(response, ct);
        }

        // ── SETTINGS parser (shared) ────────────────────────────────────────────────

        /// <summary>
        /// Parses an HTTP/2 SETTINGS frame payload.
        /// </summary>
        internal static void ParseSettings(Http2Settings settings, byte[] buffer, int length)
        {
            if (length % 6 != 0) return;
            int pos = 0;
            while (pos < length)
            {
                int id    = (buffer[pos++] << 8)  | buffer[pos++];
                int value = (buffer[pos++] << 24) | (buffer[pos++] << 16)
                          | (buffer[pos++] << 8)  |  buffer[pos++];
                switch (id)
                {
                    case 1: settings.HeaderTableSize     = value; break;
                    case 2: settings.EnablePush          = value; break;
                    case 3: settings.MaxConcurrentStreams = value < 0 ? int.MaxValue : value; break;
                    case 4: settings.InitialWindowSize   = value; break;
                    case 5: settings.MaxFrameSize        = value; break;
                    case 6: settings.MaxHeaderListSize   = value < 0 ? int.MaxValue : value; break;
                }
            }
        }

        // ── HPACK encoder management ──────────────────────────────────────────

        private static void EnsureEncoder(Http2EncoderState state, Http2Settings settings)
        {
            if (state.Encoder == null || settings.HeaderTableSize < state.HeaderTableSize)
            {
                state.HeaderTableSize = settings.HeaderTableSize;
                state.Encoder = new Hpack.Encoder(settings.HeaderTableSize);
            }
            else if (settings.HeaderTableSize > state.HeaderTableSize)
            {
                state.HeaderTableSize = settings.HeaderTableSize;
            }
        }

        // ── Lightweight HPACK header listener ────────────────────────────────

        private sealed class SimpleHeaderListener : IHeaderListener
        {
            private readonly List<(string, string)> _list;
            public SimpleHeaderListener(List<(string, string)> list) => _list = list;

            public void AddHeader(ByteString name, ByteString value, bool sensitive)
                => _list.Add((name.GetString(), value.GetString()));
        }
    }
}
#endif
