#if NET6_0_OR_GREATER
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Titanium.Web.Proxy.EventArguments;
using Titanium.Web.Proxy.Exceptions;
using Titanium.Web.Proxy.Extensions;
using Titanium.Web.Proxy.Helpers;
using Titanium.Web.Proxy.Http;
using Titanium.Web.Proxy.Http2.Hpack;
using Titanium.Web.Proxy.Http2.Primitives;
using Titanium.Web.Proxy.Http2.WebSocket;
using Titanium.Web.Proxy.Models;
using Decoder = Titanium.Web.Proxy.Http2.Hpack.Decoder;

namespace Titanium.Web.Proxy.Http2.Translation
{
    /// <summary>
    /// Scenario B: HTTP/2 client to HTTP/1.x server.
    /// Requests are serialized onto the HTTP/1.x connection while the client side
    /// still speaks HTTP/2 to the proxy.
    /// </summary>
    internal sealed class Http2ToHttp1Translator : IHttp2Translator
    {
        /// <summary>
        /// Runs the translation loop for an HTTP/2 client and an HTTP/1.x server.
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
            _ = initialSession;
            _ = connectionId;

            var queue = Channel.CreateUnbounded<Http2StreamContext>(
                new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });

            var clientSettings = new Http2Settings();
            var clientWriteLock = new SemaphoreSlim(1, 1);
            var serverFrameHeaderBuffer = new byte[9];

            // Send the server connection preface first, advertising ENABLE_CONNECT_PROTOCOL=1
            // (RFC 8441 §3) so that RFC 8441-compliant clients (e.g. Chrome) know they may
            // send CONNECT+:protocol=websocket requests to this proxy.
            // ACKs and stream frames must not overtake this SETTINGS frame.
            await Http2FrameWriter.SendSettingsWithExtendedConnectAsync(clientStream, serverFrameHeaderBuffer, cts.Token);

            var readTask = ReadLoopAsync(
                clientStream, clientSettings, new Http2FrameHeader(), new byte[9],
                sessionFactory, onBeforeRequest, queue.Writer, clientWriteLock,
                cts, exceptionFunc);

            var writeTask = WriteLoopAsync(
                serverStream, clientStream, clientSettings, new Http2FrameHeader(), new byte[9],
                onBeforeResponse, queue.Reader, clientWriteLock, cts, exceptionFunc);

            await Task.WhenAny(readTask, writeTask);
            cts.Cancel();
            await Task.WhenAll(readTask, writeTask);
        }

        /// <summary>
        /// Continuously reads HTTP/2 frames from the client and handles them (HEADERS, DATA, SETTINGS, etc.).
        /// </summary>
        private static async Task ReadLoopAsync(
            Stream clientStream,
            Http2Settings clientSettings,
            Http2FrameHeader frameHeader,
            byte[] headerBuffer,
            Func<SessionEventArgs> sessionFactory,
            Func<SessionEventArgs, Task> onBeforeRequest,
            ChannelWriter<Http2StreamContext> writer,
            SemaphoreSlim clientWriteLock,
            CancellationTokenSource cts,
            ExceptionHandler? exceptionFunc)
        {
            var ct = cts.Token;
            var dataBuffer = new byte[clientSettings.MaxFrameSize];
            var pendingStreams = new Dictionary<int, Http2StreamContext>();
            var pendingHeaderBlocks = new Dictionary<int, PendingHeaderBlock>();

            var decoderState = new HeaderDecoderState();

            try
            {
                while (!ct.IsCancellationRequested)
                {
                    if (dataBuffer.Length < clientSettings.MaxFrameSize)
                        dataBuffer = new byte[clientSettings.MaxFrameSize];

                    if (!await Http2FrameReader.TryReadFrameHeaderAsync(clientStream, headerBuffer, frameHeader, ct))
                        break;

                    int length = frameHeader.Length;
                    int streamId = frameHeader.StreamId;
                    var type = frameHeader.Type;
                    var flags = frameHeader.Flags;

                    if (length > 0)
                    {
                        int read = await Http2FrameReader.ForceReadAsync(clientStream, dataBuffer, 0, length, ct);
                        if (read != length)
                            break;
                    }

                    if (type == Http2FrameType.Settings)
                    {
                        if ((flags & Http2FrameFlag.Ack) == 0)
                        {
                            Http1ToHttp2Translator.ParseSettings(clientSettings, dataBuffer, length);
                            await WriteToClientAsync(clientWriteLock,
                                () => Http2FrameWriter.SendSettingsAckAsync(clientStream, headerBuffer, ct));
                        }
                        continue;
                    }

                    if (type == Http2FrameType.Ping)
                    {
                        if ((flags & Http2FrameFlag.Ack) == 0 && streamId == 0 && length == 8)
                        {
                            var pingPayload = new byte[8];
                            Array.Copy(dataBuffer, pingPayload, 8);
                            await WriteToClientAsync(clientWriteLock,
                                () => Http2FrameWriter.SendPingAckAsync(clientStream, headerBuffer, pingPayload, ct));
                        }
                        continue;
                    }

                    if (type == Http2FrameType.WindowUpdate ||
                        type == Http2FrameType.Priority)
                        continue;

                    if (type == Http2FrameType.GoAway)
                        break;

                    if (type == Http2FrameType.Headers)
                    {
                        bool endHeaders = (flags & Http2FrameFlag.EndHeaders) != 0;
                        bool endStream = (flags & Http2FrameFlag.EndStream) != 0;
                        bool padded = (flags & Http2FrameFlag.Padded) != 0;
                        bool priority = (flags & Http2FrameFlag.Priority) != 0;

                        int offset = 0;
                        int padLength = padded ? dataBuffer[offset++] : 0;
                        if (priority) offset += 5;

                        int blockLength = length - offset - padLength;
                        if (blockLength < 0)
                        {
                            exceptionFunc?.Invoke(new ProxyHttpException(
                                "Invalid HTTP/2 HEADERS frame length.", null, null));
                            break;
                        }

                        if (!pendingHeaderBlocks.TryGetValue(streamId, out var pendingHeader))
                        {
                            pendingHeader = new PendingHeaderBlock();
                            pendingHeaderBlocks[streamId] = pendingHeader;
                        }

                        pendingHeader.EndStream = endStream;
                        pendingHeader.Buffer.Write(dataBuffer, offset, blockLength);

                        if (endHeaders)
                        {
                            await FinalizeHeadersAsync(
                                streamId, pendingHeaderBlocks, pendingStreams, sessionFactory, onBeforeRequest, writer,
                                decoderState, clientSettings, ct, exceptionFunc);
                        }

                        continue;
                    }

                    if (type == Http2FrameType.Continuation)
                    {
                        bool endHeaders = (flags & Http2FrameFlag.EndHeaders) != 0;
                        if (!pendingHeaderBlocks.TryGetValue(streamId, out var pendingHeader))
                        {
                            exceptionFunc?.Invoke(new ProxyHttpException(
                                $"Unexpected HTTP/2 CONTINUATION on stream {streamId}.", null, null));
                            break;
                        }

                        pendingHeader.Buffer.Write(dataBuffer, 0, length);

                        if (endHeaders)
                        {
                            await FinalizeHeadersAsync(
                                streamId, pendingHeaderBlocks, pendingStreams, sessionFactory, onBeforeRequest, writer,
                                decoderState, clientSettings, ct, exceptionFunc);
                        }

                        continue;
                    }

                    if (type == Http2FrameType.Data && pendingStreams.TryGetValue(streamId, out var streamContext))
                    {
                        bool padded = (flags & Http2FrameFlag.Padded) != 0;
                        bool endStream = (flags & Http2FrameFlag.EndStream) != 0;
                        int offset = 0;
                        int padLength = padded ? dataBuffer[offset++] : 0;
                        int dataLength = length - offset - padLength;

                        if (dataLength < 0)
                        {
                            exceptionFunc?.Invoke(new ProxyHttpException(
                                $"Invalid HTTP/2 DATA frame on stream {streamId}.", null, streamContext.Args));
                            break;
                        }

                        streamContext.RequestBody ??= new MemoryStream();
                        streamContext.Args.OnDataSent(dataBuffer, offset, dataLength);
                        streamContext.RequestBody.Write(dataBuffer, offset, dataLength);

                        if (endStream)
                        {
                            streamContext.RequestBodyComplete = true;
                            pendingStreams.Remove(streamId);
                            await writer.WriteAsync(streamContext, ct);
                        }

                        continue;
                    }

                    if (type == Http2FrameType.RstStream)
                    {
                        pendingStreams.Remove(streamId);
                        pendingHeaderBlocks.Remove(streamId);
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
            finally
            {
                writer.TryComplete();
            }
        }

        /// <summary>
        /// Finalizes the decoding of headers for a stream and triggers the before-request hook.
        /// Detects RFC 8441 extended-CONNECT WebSocket requests via :protocol=websocket
        /// and marks the stream context accordingly so the write loop can route it correctly.
        /// </summary>
        private static async Task FinalizeHeadersAsync(
            int streamId,
            IDictionary<int, PendingHeaderBlock> pendingHeaderBlocks,
            IDictionary<int, Http2StreamContext> pendingStreams,
            Func<SessionEventArgs> sessionFactory,
            Func<SessionEventArgs, Task> onBeforeRequest,
            ChannelWriter<Http2StreamContext> writer,
            HeaderDecoderState decoderState,
            Http2Settings clientSettings,
            CancellationToken ct,
            ExceptionHandler? exceptionFunc)
        {
            var pendingHeader = pendingHeaderBlocks[streamId];
            pendingHeaderBlocks.Remove(streamId);

            var decodedHeaders = DecodeHeaders(decoderState, clientSettings, pendingHeader.Buffer.ToArray(), exceptionFunc);

            var context = new Http2StreamContext(streamId, sessionFactory());
            Http2HeaderConverter.ApplyToHttp1Request(context.Args.HttpClient.Request, decodedHeaders);
            context.Args.HttpClient.Request.SetOriginalHeaders();

            // Detect RFC 8441 extended-CONNECT WebSocket: :method=CONNECT + :protocol=websocket
            // Http2HeaderConverter.ApplyToHttp1Request already sets request.Method; we look for
            // :protocol in the raw decoded list (it is NOT a standard request header).
            foreach (var (name, value) in decodedHeaders)
            {
                if (name.Equals(":protocol", StringComparison.OrdinalIgnoreCase))
                {
                    context.Args.HttpClient.Request.Http2Protocol = value;
                    break;
                }
            }

            context.IsWebSocket = context.Args.HttpClient.Request.UpgradeToWebSocket;

            await onBeforeRequest(context.Args);

            // WebSocket streams have no body before the tunnel opens.
            if (context.IsWebSocket)
            {
                context.RequestBodyComplete = true;
                await writer.WriteAsync(context, ct);
                return;
            }

            if (pendingHeader.EndStream)
            {
                context.RequestBodyComplete = true;
                await writer.WriteAsync(context, ct);
            }
            else
            {
                pendingStreams[streamId] = context;
            }
        }

        /// <summary>
        /// Reads captured HTTP/2 streams from the queue and executes them against the HTTP/1.x server.
        /// WebSocket streams are routed to <see cref="HandleWebSocketTunnelAsync"/>;
        /// all other streams follow the normal HTTP/1.1 request/response cycle.
        /// </summary>
        private static async Task WriteLoopAsync(
            Stream serverStream,
            Stream clientStream,
            Http2Settings clientSettings,
            Http2FrameHeader frameHeader,
            byte[] headerBuffer,
            Func<SessionEventArgs, Task> onBeforeResponse,
            ChannelReader<Http2StreamContext> reader,
            SemaphoreSlim clientWriteLock,
            CancellationTokenSource cts,
            ExceptionHandler? exceptionFunc)
        {
            var ct = cts.Token;
            var encoderState = new Http2EncoderState();
            var dataBuffer = new byte[clientSettings.MaxFrameSize];

            try
            {
                await foreach (var context in reader.ReadAllAsync(ct))
                {
                    try
                    {
                        // RFC 8441: route WebSocket streams to the tunnel handler
                        if (context.IsWebSocket)
                        {
                            await HandleWebSocketTunnelAsync(
                                serverStream, clientStream, clientSettings,
                                frameHeader, headerBuffer, dataBuffer,
                                context, encoderState, clientWriteLock, cts, exceptionFunc);
                            continue;
                        }

                        var request  = context.Args.HttpClient.Request;
                        var response = context.Args.HttpClient.Response;

                        await SendH1RequestAsync(serverStream, request, context.RequestBody, ct);

                        response.RequestMethod = request.Method;
                        await ReadH1ResponseAsync(serverStream, context.Args, ct);

                        await onBeforeResponse(context.Args);

                        var h2ResponseHeaders = Http2HeaderConverter.ToHttp2ResponseHeaders(response);
                        bool hasBody = response.IsBodyRead && response.Body?.Length > 0;

                        await WriteToClientAsync(clientWriteLock,
                            () => SendH2HeadersToClientAsync(
                                clientStream, clientSettings, encoderState,
                                frameHeader, headerBuffer,
                                h2ResponseHeaders, context.StreamId, !hasBody, ct));

                        if (hasBody)
                        {
                            await WriteToClientAsync(clientWriteLock,
                                () => SendH2DataToClientAsync(
                                    clientStream, clientSettings,
                                    frameHeader, headerBuffer, dataBuffer,
                                    response.Body!, context.StreamId, ct));
                        }
                    }
                    catch (Exception ex) when (!(ex is OperationCanceledException))
                    {
                        exceptionFunc?.Invoke(new ProxyHttpException(
                            $"Error translating H2 to H1 stream {context.StreamId}", ex, context.Args));

                        await WriteToClientAsync(clientWriteLock,
                            () => Http2FrameWriter.SendRstStreamAsync(
                                clientStream, headerBuffer, context.StreamId, 0x2, ct));
                    }
                    finally
                    {
                        context.Args.Dispose();
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
        }

        // ── WebSocket tunnel (H2 client ↔ H1 server) ────────────────────────────

        /// <summary>
        /// Handles an RFC 8441 WebSocket tunnel when the H2 client sends
        /// CONNECT+:protocol=websocket but the upstream server speaks HTTP/1.1.
        ///
        /// Protocol translation:
        ///   H2 client  ─CONNECT+:protocol:websocket─▶  proxy
        ///   proxy  ─GET Upgrade:websocket──────────▶  H1 server
        ///   H1 server  ─101 Switching Protocols───▶  proxy  (validates Accept hash)
        ///   proxy  ─:status 200─────────────────▶  H2 client
        ///   ── bidirectional DATA frame relay ──
        /// </summary>
        private static async Task HandleWebSocketTunnelAsync(
            Stream serverStream,
            Stream clientStream,
            Http2Settings clientSettings,
            Http2FrameHeader frameHeader,
            byte[] headerBuffer,
            byte[] dataBuffer,
            Http2StreamContext context,
            Http2EncoderState encoderState,
            SemaphoreSlim clientWriteLock,
            CancellationTokenSource cts,
            ExceptionHandler? exceptionFunc)
        {
            var ct      = cts.Token;
            var request = context.Args.HttpClient.Request;

            // ── Step 1: Send HTTP/1.1 GET Upgrade to the H1 server ──────────
            // RFC 8441 §5: proxy generates its own Sec-WebSocket-Key so the
            // H1 server can complete the challenge-response handshake.
            var proxyKey = WebSocketHandshakeHelper.GenerateClientKey();

            var upgradeBuilder = new StringBuilder();
            upgradeBuilder.Append($"GET {request.RequestUri.PathAndQuery} HTTP/1.1\r\n");
            upgradeBuilder.Append($"Host: {request.RequestUri.Authority}\r\n");
            upgradeBuilder.Append("Upgrade: websocket\r\n");
            upgradeBuilder.Append("Connection: Upgrade\r\n");
            upgradeBuilder.Append($"Sec-WebSocket-Key: {proxyKey}\r\n");
            upgradeBuilder.Append("Sec-WebSocket-Version: 13\r\n");

            // Forward any non-forbidden application headers (e.g. Sec-WebSocket-Protocol)
            foreach (var header in request.Headers)
            {
                if (!Http2HeaderConverter.IsH2ForbiddenRequestHeader(header.Name))
                    upgradeBuilder.Append($"{header.Name}: {header.Value}\r\n");
            }

            upgradeBuilder.Append("\r\n");
            var upgradeBytes = Encoding.ASCII.GetBytes(upgradeBuilder.ToString());
            await serverStream.WriteAsync(upgradeBytes, 0, upgradeBytes.Length, ct);
            await serverStream.FlushAsync(ct);

            // ── Step 2: Read and validate H1 101 response ───────────────────
            string? statusLine = await ReadLineFromStreamAsync(serverStream, ct);
            if (string.IsNullOrEmpty(statusLine))
            {
                await WriteToClientAsync(clientWriteLock,
                    () => Http2FrameWriter.SendRstStreamAsync(
                        clientStream, headerBuffer, context.StreamId, 0x2 /*INTERNAL_ERROR*/, ct));
                return;
            }

            var parts      = statusLine.Split(' ');
            var statusCode = parts.Length > 1 && int.TryParse(parts[1], out var sc) ? sc : 0;

            // Drain H1 response headers (we need Sec-WebSocket-Accept for validation)
            string? acceptHeader = null;
            while (true)
            {
                var line = await ReadLineFromStreamAsync(serverStream, ct);
                if (string.IsNullOrEmpty(line)) break;

                int colon = line.IndexOf(':');
                if (colon > 0)
                {
                    var hName  = line.Substring(0, colon).Trim();
                    var hValue = line.Substring(colon + 1).Trim();
                    if (hName.Equals("Sec-WebSocket-Accept", StringComparison.OrdinalIgnoreCase))
                        acceptHeader = hValue;
                }
            }

            if (statusCode != 101)
            {
                // H1 server refused; signal RST_STREAM to the H2 client
                exceptionFunc?.Invoke(new ProxyHttpException(
                    $"H1 server rejected WebSocket upgrade with status {statusCode}", null, context.Args));
                await WriteToClientAsync(clientWriteLock,
                    () => Http2FrameWriter.SendRstStreamAsync(
                        clientStream, headerBuffer, context.StreamId, 0x2 /*INTERNAL_ERROR*/, ct));
                return;
            }

            // Validate the accept hash (lenient: log but continue on mismatch)
            WebSocketHandshakeHelper.ValidateServerAccept(
                proxyKey, acceptHeader,
                msg => exceptionFunc?.Invoke(
                    new ProxyHttpException($"WS handshake warning on stream {context.StreamId}: {msg}", null, context.Args)));

            // ── Step 3: Reply :status 200 to the H2 client ─────────────────
            // RFC 8441 §4: successful WebSocket upgrade over HTTP/2 returns 200, NOT 101
            var status200 = new List<(string, string)> { (":status", "200") };
            await WriteToClientAsync(clientWriteLock,
                () => SendH2HeadersToClientAsync(
                    clientStream, clientSettings, encoderState,
                    frameHeader, headerBuffer,
                    status200, context.StreamId, endStream: false, ct));

            // ── Step 4: Bidirectional DATA relay until END_STREAM or reset ──
            // Relay H2 DATA frames from client → raw bytes to H1 server, and
            // H1 server bytes → H2 DATA frames to client.
            var clientToServer = RelayH2DataToH1Async(
                clientStream, serverStream, dataBuffer, context.StreamId, ct);
            var serverToClient = RelayH1DataToH2Async(
                serverStream, clientStream, clientSettings,
                dataBuffer, frameHeader, headerBuffer,
                context, encoderState, clientWriteLock, ct);

            await Task.WhenAny(clientToServer, serverToClient);
            cts.Cancel();
            await Task.WhenAll(clientToServer, serverToClient);
        }

        /// <summary>
        /// Reads H2 DATA frames from the client and writes raw WebSocket bytes
        /// to the H1 server connection until END_STREAM is signalled.
        /// </summary>
        internal static async Task RelayH2DataToH1Async(
            Stream clientStream,
            Stream serverStream,
            byte[] buffer,
            int streamId,
            CancellationToken ct)
        {
            var headerBuffer = new byte[9];
            var frameHeader  = new Http2FrameHeader();

            try
            {
                while (!ct.IsCancellationRequested)
                {
                    if (!await Http2FrameReader.TryReadFrameHeaderAsync(clientStream, headerBuffer, frameHeader, ct))
                        return;

                    int length  = frameHeader.Length;
                    var type    = frameHeader.Type;
                    var flags   = frameHeader.Flags;

                    if (length > 0)
                    {
                        if (buffer.Length < length)
                            buffer = new byte[length];

                        int read = await Http2FrameReader.ForceReadAsync(clientStream, buffer, 0, length, ct);
                        if (read != length) return;
                    }

                    // Only forward DATA frames for the current WebSocket stream
                    if (type == Http2FrameType.Data && frameHeader.StreamId == streamId)
                    {
                        bool padded    = (flags & Http2FrameFlag.Padded) != 0;
                        int  offset    = padded ? 1 : 0;
                        int  padLen    = padded ? buffer[0] : 0;
                        int  dataLen   = length - offset - padLen;

                        if (dataLen > 0)
                            await serverStream.WriteAsync(buffer, offset, dataLen, ct);

                        await serverStream.FlushAsync(ct);

                        // Client signalled end of WebSocket stream
                        if ((flags & Http2FrameFlag.EndStream) != 0)
                            return;
                    }
                    else if (type == Http2FrameType.RstStream && frameHeader.StreamId == streamId)
                    {
                        return;
                    }
                    // Other control frames (PING, WINDOW_UPDATE, etc.) are discarded;
                    // the main Http2ToHttp1 read loop is no longer running for this connection.
                }
            }
            catch (OperationCanceledException) { }
            catch (IOException) { }
        }

        /// <summary>
        /// Reads raw WebSocket bytes from the H1 server and wraps them into
        /// H2 DATA frames sent back to the H2 client.
        /// </summary>
        internal static async Task RelayH1DataToH2Async(
            Stream serverStream,
            Stream clientStream,
            Http2Settings clientSettings,
            byte[] buffer,
            Http2FrameHeader frameHeader,
            byte[] headerBuffer,
            Http2StreamContext context,
            Http2EncoderState encoderState,
            SemaphoreSlim clientWriteLock,
            CancellationToken ct)
        {
            // Allocate a private small buffer to avoid racing with the headerBuffer reused elsewhere.
            var privateHeader = new byte[9];

            try
            {
                while (!ct.IsCancellationRequested)
                {
                    // Resize buffer if needed (server may send large WebSocket frames)
                    var readBuf = buffer.Length >= clientSettings.MaxFrameSize
                        ? buffer
                        : new byte[clientSettings.MaxFrameSize];

                    int read = await serverStream.ReadAsync(readBuf, 0, readBuf.Length, ct);
                    if (read == 0)
                        break; // server closed connection

                    context.Args.OnDataReceived(readBuf, 0, read);

                    // Wrap data in H2 DATA frame (no END_STREAM — tunnel stays open)
                    var dataFrameHeader = new Http2FrameHeader
                    {
                        Length   = read,
                        Type     = Http2FrameType.Data,
                        Flags    = (Http2FrameFlag)0,
                        StreamId = context.StreamId
                    };
                    dataFrameHeader.CopyToBuffer(privateHeader);

                    await WriteToClientAsync(clientWriteLock, async () =>
                    {
                        await clientStream.WriteAsync(privateHeader, 0, 9, ct);
                        await clientStream.WriteAsync(readBuf,       0, read, ct);
                    });
                }

                // Server closed — send DATA+END_STREAM to H2 client
                var endHeader = new Http2FrameHeader
                {
                    Length   = 0,
                    Type     = Http2FrameType.Data,
                    Flags    = Http2FrameFlag.EndStream,
                    StreamId = context.StreamId
                };
                endHeader.CopyToBuffer(privateHeader);
                await WriteToClientAsync(clientWriteLock, async () =>
                    await clientStream.WriteAsync(privateHeader, 0, 9, ct));
            }
            catch (OperationCanceledException) { }
            catch (IOException) { }
        }

        /// <summary>
        /// Formats and sends an HTTP/1.x request to the server.
        /// </summary>
        private static async Task SendH1RequestAsync(
            Stream server, Request request, MemoryStream? body, CancellationToken ct)
        {
            if (body != null)
            {
                request.Headers.RemoveHeader(KnownHeaders.TransferEncoding);
                request.ContentLength = body.Length;
            }

            var headerBuilder = new HeaderBuilder();
            headerBuilder.WriteRequestLine(request.Method, request.RequestUri.PathAndQuery, HttpHeader.Version11);
            headerBuilder.WriteHeaders(request.Headers);
            var headerBuffer = headerBuilder.GetBuffer();
            await server.WriteAsync(headerBuffer.Array!, headerBuffer.Offset, headerBuffer.Count, ct);

            if (body != null && body.Length > 0)
            {
                var bodyBytes = body.ToArray();
                await server.WriteAsync(bodyBytes, 0, bodyBytes.Length, ct);
            }

            await server.FlushAsync(ct);
        }

        /// <summary>
        /// Reads an HTTP/1.x response from the server and populates the <see cref="Response"/> object.
        /// </summary>
        private static async Task ReadH1ResponseAsync(Stream server, SessionEventArgs args, CancellationToken ct)
        {
            var response = args.HttpClient.Response;
            string? statusLine = await ReadLineFromStreamAsync(server, ct);
            if (string.IsNullOrEmpty(statusLine))
                return;

            var parts = statusLine.Split(new[] { ' ' }, 3);
            response.HttpVersion = parts[0].EndsWith("1.0", StringComparison.Ordinal)
                ? HttpHeader.Version10
                : HttpHeader.Version11;
            response.StatusCode = parts.Length > 1 && int.TryParse(parts[1], out var statusCode) ? statusCode : 200;
            response.StatusDescription = parts.Length > 2 ? parts[2] : string.Empty;

            while (true)
            {
                var line = await ReadLineFromStreamAsync(server, ct);
                if (string.IsNullOrEmpty(line))
                    break;

                int colon = line.IndexOf(':');
                if (colon < 0)
                    continue;

                response.Headers.AddHeader(new HttpHeader(
                    line.Substring(0, colon).Trim(),
                    line.Substring(colon + 1).Trim()));
            }

            response.SetOriginalHeaders();

            if (!response.HasBody)
                return;

            byte[] bodyBytes;
            if (response.IsChunked)
            {
                bodyBytes = await ReadChunkedBodyAsync(server, args, ct);
            }
            else if (response.ContentLength >= 0)
            {
                bodyBytes = await ReadFixedLengthBodyAsync(server, args, response.ContentLength, ct);
            }
            else if (!response.KeepAlive || response.HttpVersion == HttpHeader.Version10)
            {
                bodyBytes = await ReadUntilEofAsync(server, args, ct);
            }
            else
            {
                bodyBytes = Array.Empty<byte>();
            }

            response.Body = bodyBytes;
            response.IsBodyRead = true;
        }

        private static async Task<byte[]> ReadFixedLengthBodyAsync(Stream stream, SessionEventArgs args, long contentLength, CancellationToken ct)
        {
            if (contentLength <= 0)
                return Array.Empty<byte>();

            var body = new byte[contentLength];
            int totalRead = 0;
            while (totalRead < body.Length)
            {
                int read = await stream.ReadAsync(body, totalRead, body.Length - totalRead, ct);
                if (read == 0)
                    throw new IOException("Unexpected end of HTTP/1.x response body.");

                args.OnDataReceived(body, totalRead, read);
                totalRead += read;
            }

            return body;
        }

        private static async Task<byte[]> ReadChunkedBodyAsync(Stream stream, SessionEventArgs args, CancellationToken ct)
        {
            using var body = new MemoryStream();

            while (true)
            {
                var chunkHead = await ReadLineFromStreamAsync(stream, ct);
                if (chunkHead == null)
                    throw new IOException("Unexpected end of chunked response body.");

                int separatorIndex = chunkHead.IndexOf(';');
                if (separatorIndex >= 0)
                    chunkHead = chunkHead.Substring(0, separatorIndex);

                if (!int.TryParse(chunkHead, System.Globalization.NumberStyles.HexNumber, null, out int chunkSize))
                    throw new ProxyHttpException($"Invalid chunk length: '{chunkHead}'", null, null);

                if (chunkSize == 0)
                {
                    await ConsumeChunkTrailersAsync(stream, ct);
                    break;
                }

                var chunk = await ReadFixedLengthBodyAsync(stream, args, chunkSize, ct);
                await body.WriteAsync(chunk, 0, chunk.Length, ct);

                var chunkTerminator = await ReadLineFromStreamAsync(stream, ct);
                if (chunkTerminator == null)
                    throw new IOException("Unexpected end of chunked response terminator.");
            }

            return body.ToArray();
        }

        private static async Task<byte[]> ReadUntilEofAsync(Stream stream, SessionEventArgs args, CancellationToken ct)
        {
            using var body = new MemoryStream();
            var buffer = new byte[8192];

            while (true)
            {
                int read = await stream.ReadAsync(buffer, 0, buffer.Length, ct);
                if (read == 0)
                    break;

                args.OnDataReceived(buffer, 0, read);
                await body.WriteAsync(buffer, 0, read, ct);
            }

            return body.ToArray();
        }

        private static async Task ConsumeChunkTrailersAsync(Stream stream, CancellationToken ct)
        {
            while (true)
            {
                var trailerLine = await ReadLineFromStreamAsync(stream, ct);
                if (trailerLine == null || trailerLine.Length == 0)
                    return;
            }
        }

        private static async Task<string?> ReadLineFromStreamAsync(Stream stream, CancellationToken ct)
        {
            var bytes = new List<byte>(256);
            var oneByte = new byte[1];

            while (true)
            {
                int read = await stream.ReadAsync(oneByte, 0, 1, ct);
                if (read == 0)
                    return bytes.Count == 0 ? null : System.Text.Encoding.ASCII.GetString(bytes.ToArray());

                if (oneByte[0] == '\n')
                    break;

                if (oneByte[0] != '\r')
                    bytes.Add(oneByte[0]);
            }

            return System.Text.Encoding.ASCII.GetString(bytes.ToArray());
        }

        private static async Task SendH2HeadersToClientAsync(
            Stream clientStream, Http2Settings clientSettings, Http2EncoderState encoderState,
            Http2FrameHeader frameHeader, byte[] headerBuffer,
            IReadOnlyList<(string Name, string Value)> headers,
            int streamId, bool endStream,
            CancellationToken ct)
        {
            EnsureEncoder(encoderState, clientSettings);
            var encoder = encoderState.Encoder!;

            using var ms = new MemoryStream();
            var writer = new BinaryWriter(ms);

            foreach (var (name, value) in headers)
            {
                encoder.EncodeHeader(writer,
                    (ByteString)System.Text.Encoding.ASCII.GetBytes(name),
                    (ByteString)System.Text.Encoding.ASCII.GetBytes(value));
            }

            var encoded = ms.ToArray();
            var flags = Http2FrameFlag.EndHeaders;
            if (endStream) flags |= Http2FrameFlag.EndStream;

            frameHeader.Length = encoded.Length;
            frameHeader.Type = Http2FrameType.Headers;
            frameHeader.Flags = flags;
            frameHeader.StreamId = streamId;
            frameHeader.CopyToBuffer(headerBuffer);

            await clientStream.WriteAsync(headerBuffer, 0, 9, ct);
            await clientStream.WriteAsync(encoded, 0, encoded.Length, ct);
        }

        private static async Task SendH2DataToClientAsync(
            Stream clientStream, Http2Settings clientSettings,
            Http2FrameHeader frameHeader, byte[] headerBuffer, byte[] dataBuffer,
            byte[] body, int streamId, CancellationToken ct)
        {
            int position = 0;
            while (position < body.Length)
            {
                int chunkLength = Math.Min(dataBuffer.Length, body.Length - position);
                Buffer.BlockCopy(body, position, dataBuffer, 0, chunkLength);
                position += chunkLength;

                frameHeader.Length = chunkLength;
                frameHeader.Type = Http2FrameType.Data;
                frameHeader.Flags = position >= body.Length ? Http2FrameFlag.EndStream : (Http2FrameFlag)0;
                frameHeader.StreamId = streamId;
                frameHeader.CopyToBuffer(headerBuffer);

                await clientStream.WriteAsync(headerBuffer, 0, 9, ct);
                await clientStream.WriteAsync(dataBuffer, 0, chunkLength, ct);
            }
        }

        /// <summary>
        /// Decodes a block of HPACK-encoded headers.
        /// </summary>
        private static IReadOnlyList<(string, string)> DecodeHeaders(
            HeaderDecoderState decoderState,
            Http2Settings settings, byte[] block,
            ExceptionHandler? exceptionFunc)
        {
            var list = new List<(string, string)>();
            try
            {
                if (decoderState.Decoder == null || decoderState.TableSize < settings.HeaderTableSize)
                {
                    decoderState.TableSize = settings.HeaderTableSize;
                    decoderState.Decoder = new Decoder(8192, decoderState.TableSize);
                }

                var listener = new SimpleHeaderListener(list);
                decoderState.Decoder.Decode(new BinaryReader(new MemoryStream(block)), listener);
                decoderState.Decoder.EndHeaderBlock();
            }
            catch (Exception ex)
            {
                exceptionFunc?.Invoke(new ProxyHttpException("Failed to decode H2 request headers", ex, null));
            }

            return list;
        }

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

        private static async Task WriteToClientAsync(SemaphoreSlim clientWriteLock, Func<Task> writeFunc)
        {
            await clientWriteLock.WaitAsync();
            try
            {
                await writeFunc();
            }
            finally
            {
                clientWriteLock.Release();
            }
        }

        private sealed class SimpleHeaderListener : IHeaderListener
        {
            private readonly List<(string, string)> _list;

            public SimpleHeaderListener(List<(string, string)> list)
            {
                _list = list;
            }

            public void AddHeader(ByteString name, ByteString value, bool sensitive)
            {
                _list.Add((name.GetString(), value.GetString()));
            }
        }

        internal sealed class Http2StreamContext
        {
            public int StreamId { get; }
            public SessionEventArgs Args { get; }
            public MemoryStream? RequestBody { get; set; }
            public bool RequestBodyComplete { get; set; }

            /// <summary>
            /// True when this stream carries a WebSocket tunnel
            /// (RFC 8441: CONNECT + :protocol=websocket).
            /// </summary>
            public bool IsWebSocket { get; set; }

            public Http2StreamContext(int streamId, SessionEventArgs args)
            {
                StreamId = streamId;
                Args = args;
            }
        }

        private sealed class PendingHeaderBlock
        {
            public MemoryStream Buffer { get; } = new MemoryStream();
            public bool EndStream { get; set; }
        }

        private sealed class HeaderDecoderState
        {
            public Decoder? Decoder { get; set; }
            public int TableSize { get; set; }
        }
    }
}
#endif
