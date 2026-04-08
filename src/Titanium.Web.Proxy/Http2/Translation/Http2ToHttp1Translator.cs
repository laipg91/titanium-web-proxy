#if NET6_0_OR_GREATER
using System;
using System.Collections.Generic;
using System.IO;
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
using Titanium.Web.Proxy.Models;
using Decoder = Titanium.Web.Proxy.Http2.Hpack.Decoder;

namespace Titanium.Web.Proxy.Http2.Translation
{
    /// <summary>
    /// Scenario B: HTTP/2 client ↔ HTTP/1.x server.
    ///
    /// Strategy: <b>serialize</b> (Q3 decision).
    /// H2 streams are queued and dispatched one-at-a-time to the H1 server.
    /// This avoids head-of-line issues at the cost of not exploiting H2 multiplexing
    /// on the server side — acceptable because Scenario B is an edge case.
    ///
    /// Architecture:
    ///   ReadLoop  — reads H2 frames from client, enqueues completed requests.
    ///   WriteLoop — dequeues requests, sends H1 to server, converts response back to H2.
    ///
    /// Both loops run concurrently; the queue decouples them.
    /// </summary>
    internal sealed class Http2ToHttp1Translator : IHttp2Translator
    {
        public async Task TranslateAsync(
            HttpClientStream clientStream,
            Stream serverStream,
            SessionEventArgs? initialSession,   // not used in Scenario B — H2 client sends preface directly
            Func<SessionEventArgs> sessionFactory,
            Func<SessionEventArgs, Task> onBeforeRequest,
            Func<SessionEventArgs, Task> onBeforeResponse,
            CancellationTokenSource cts,
            Guid connectionId,
            ExceptionHandler? exceptionFunc)
        {
            // Unbounded channel: ReadLoop produces, WriteLoop consumes.
            var queue = Channel.CreateUnbounded<Http2StreamContext>(
                new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });

            // Channel used by ReadLoop to notify WriteLoop that a SETTINGS ACK must be sent.
            // We keep writes on clientStream serialized in WriteLoop to avoid concurrent writes.
            var settingsAckChannel = Channel.CreateUnbounded<bool>(
                new UnboundedChannelOptions { SingleReader = true, SingleWriter = true });

            var ct             = cts.Token;
            var clientSettings = new Http2Settings();
            var frameHeaderBuf = new byte[9];
            var frameHeader    = new Http2FrameHeader();

            var readTask  = ReadLoopAsync(clientStream, clientSettings, frameHeader, frameHeaderBuf,
                                          sessionFactory, onBeforeRequest, queue.Writer, settingsAckChannel.Writer,
                                          cts, exceptionFunc);

            var writeTask = WriteLoopAsync(serverStream, clientStream, clientSettings,
                                           frameHeader, frameHeaderBuf,
                                           onBeforeResponse, queue.Reader, settingsAckChannel.Reader,
                                           cts, exceptionFunc);

            await Task.WhenAny(readTask, writeTask);
            cts.Cancel();
            await Task.WhenAll(readTask, writeTask);
        }

        // ── Read Loop: H2 client → queue ──────────────────────────────────────

        private static async Task ReadLoopAsync(
            Stream clientStream,
            Http2Settings clientSettings,
            Http2FrameHeader frameHeader,
            byte[] headerBuffer,
            Func<SessionEventArgs> sessionFactory,
            Func<SessionEventArgs, Task> onBeforeRequest,
            ChannelWriter<Http2StreamContext> writer,
            ChannelWriter<bool> settingsAckWriter,
            CancellationTokenSource cts,
            ExceptionHandler? exceptionFunc)
        {
            var ct = cts.Token;
            var dataBuffer = new byte[clientSettings.MaxFrameSize];

            // Active streams being assembled (waiting for EndHeaders / body frames)
            var pendingStreams  = new Dictionary<int, Http2StreamContext>();
            var pendingHeaders  = new Dictionary<int, MemoryStream>();

            Decoder? decoder   = null;
            int tableSize      = 0;

            try
            {
                while (!ct.IsCancellationRequested)
                {
                    // Resize data buffer if settings changed
                    if (dataBuffer.Length < clientSettings.MaxFrameSize)
                        dataBuffer = new byte[clientSettings.MaxFrameSize];

                    if (!await Http2FrameReader.TryReadFrameHeaderAsync(clientStream, headerBuffer, frameHeader, ct))
                        break;

                    int length   = frameHeader.Length;
                    int streamId = frameHeader.StreamId;
                    var type     = frameHeader.Type;
                    var flags    = frameHeader.Flags;

                    if (length > 0)
                    {
                        int read = await Http2FrameReader.ForceReadAsync(clientStream, dataBuffer, 0, length, ct);
                        if (read != length) break;
                    }

                    // ── Connection-level frames ───────────────────────────────
                    if (type == Http2FrameType.Settings)
                    {
                        if ((flags & Http2FrameFlag.Ack) == 0)
                        {
                            Http1ToHttp2Translator.ParseSettings(clientSettings, dataBuffer, length);
                            // Signal WriteLoop to send SETTINGS ACK (keeps clientStream writes on one Task)
                            settingsAckWriter.TryWrite(true);
                        }
                        continue;
                    }

                    if (type == Http2FrameType.Ping)
                    {
                        // Ping ACK must go back to client — handled by WriteLoop via a dedicated channel
                        // For simplicity, ignore ping in ReadLoop (WriteLoop can ACK).
                        continue;
                    }

                    if (type == Http2FrameType.WindowUpdate ||
                        type == Http2FrameType.Priority)
                        continue;

                    if (type == Http2FrameType.GoAway) break;

                    // ── Stream HEADERS ────────────────────────────────────────
                    if (type == Http2FrameType.Headers)
                    {
                        bool endHeaders  = (flags & Http2FrameFlag.EndHeaders)  != 0;
                        bool endStream   = (flags & Http2FrameFlag.EndStream)   != 0;
                        bool padded      = (flags & Http2FrameFlag.Padded)      != 0;
                        bool priority    = (flags & Http2FrameFlag.Priority)    != 0;

                        int offset   = 0;
                        int padLen   = padded   ? dataBuffer[offset++] : 0;
                        if (priority) offset += 5; // skip 5-byte priority block

                        int blockLen = length - offset - padLen;

                        if (!pendingHeaders.TryGetValue(streamId, out var ms))
                            pendingHeaders[streamId] = ms = new MemoryStream();
                        ms.Write(dataBuffer, offset, blockLen);

                        if (endHeaders)
                        {
                            var headerBlock = ms.ToArray();
                            pendingHeaders.Remove(streamId);

                            var decodedHeaders = DecodeHeaders(
                                ref decoder, ref tableSize, clientSettings, headerBlock, exceptionFunc);

                            var ctx  = new Http2StreamContext(streamId, sessionFactory());
                            Http2HeaderConverter.ApplyToHttp1Request(ctx.Args.HttpClient.Request, decodedHeaders);
                            ctx.Args.HttpClient.Request.SetOriginalHeaders();

                            await onBeforeRequest(ctx.Args);

                            if (endStream)
                            {
                                ctx.RequestBodyComplete = true;
                                await writer.WriteAsync(ctx, ct);
                            }
                            else
                            {
                                pendingStreams[streamId] = ctx;
                            }
                        }
                        continue;
                    }

                    // ── CONTINUATION ──────────────────────────────────────────
                    if (type == Http2FrameType.Continuation)
                    {
                        bool endHeaders = (flags & Http2FrameFlag.EndHeaders) != 0;
                        if (pendingHeaders.TryGetValue(streamId, out var ms))
                        {
                            ms.Write(dataBuffer, 0, length);
                            if (endHeaders)
                            {
                                var headerBlock   = ms.ToArray();
                                pendingHeaders.Remove(streamId);
                                var decodedHeaders = DecodeHeaders(
                                    ref decoder, ref tableSize, clientSettings, headerBlock, exceptionFunc);

                                if (pendingStreams.TryGetValue(streamId, out var ctx))
                                    Http2HeaderConverter.ApplyToHttp1Request(ctx.Args.HttpClient.Request, decodedHeaders);
                            }
                        }
                        continue;
                    }

                    // ── DATA ──────────────────────────────────────────────────
                    if (type == Http2FrameType.Data && pendingStreams.TryGetValue(streamId, out var stream))
                    {
                        bool padded2  = (flags & Http2FrameFlag.Padded)    != 0;
                        bool endStream = (flags & Http2FrameFlag.EndStream) != 0;
                        int  offset2  = 0;
                        int  padLen2  = padded2 ? dataBuffer[offset2++] : 0;
                        int  dataLen  = length - offset2 - padLen2;

                        stream.RequestBody ??= new MemoryStream();
                        stream.RequestBody.Write(dataBuffer, offset2, dataLen);

                        if (endStream)
                        {
                            stream.RequestBodyComplete = true;
                            pendingStreams.Remove(streamId);
                            await writer.WriteAsync(stream, ct);
                        }
                        continue;
                    }

                    // ── RST_STREAM ────────────────────────────────────────────
                    if (type == Http2FrameType.RstStream && pendingStreams.ContainsKey(streamId))
                    {
                        pendingStreams.Remove(streamId);
                    }
                }
            }
            catch (OperationCanceledException) { /* expected on shutdown */ }
            finally
            {
                writer.TryComplete();
                settingsAckWriter.TryComplete();
            }
        }

        // ── Write Loop: queue → H1 server → H2 client ────────────────────────

        private static async Task WriteLoopAsync(
            Stream serverStream,
            Stream clientStream,
            Http2Settings clientSettings,
            Http2FrameHeader frameHeader,
            byte[] headerBuffer,
            Func<SessionEventArgs, Task> onBeforeResponse,
            ChannelReader<Http2StreamContext> reader,
            ChannelReader<bool> settingsAckReader,
            CancellationTokenSource cts,
            ExceptionHandler? exceptionFunc)
        {
            var ct           = cts.Token;
            var encoderState = new Http2EncoderState();
            var dataBuffer   = new byte[clientSettings.MaxFrameSize];

            // Bug #4 Fix: send empty SETTINGS (connection preface), NOT SETTINGS ACK.
            // RFC 7540 §3.5: when acting as an HTTP/2 server, the preface is a SETTINGS frame.
            await Http2FrameWriter.SendSettingsAsync(clientStream, headerBuffer, ct);

            // Helper: flush any pending SETTINGS ACK signals from ReadLoop.
            // All clientStream writes are serialized here in WriteLoop.
            async Task SendPendingSettingsAcksAsync()
            {
                while (settingsAckReader.TryRead(out _))
                    await Http2FrameWriter.SendSettingsAckAsync(clientStream, headerBuffer, ct);
            }

            // Send ACK for the client's initial SETTINGS (sent right after its preface).
            // The ReadLoop will have signalled us via settingsAckChannel.
            // Allow a short cooperative yield so ReadLoop can process the SETTINGS frame first.
            await Task.Yield();
            await SendPendingSettingsAcksAsync();

            try
            {
                await foreach (var ctx in reader.ReadAllAsync(ct))
                {
                    try
                    {
                        var request  = ctx.Args.HttpClient.Request;
                        var response = ctx.Args.HttpClient.Response;

                        // ── Send H1 request to server ─────────────────────────
                        await SendH1RequestAsync(serverStream, request, ctx.RequestBody, ct);

                        // ── Read H1 response from server ──────────────────────
                        await ReadH1ResponseAsync(serverStream, response, ct);

                        // Fire before-response hook
                        await onBeforeResponse(ctx.Args);

                        // ── Convert H1 response to H2 HEADERS + DATA frames ───
                        var h2ResponseHeaders = Http2HeaderConverter.ToHttp2ResponseHeaders(response);
                        bool hasBody = response.IsBodyRead && response.Body?.Length > 0;

                        await SendH2HeadersToClientAsync(
                            clientStream, clientSettings, encoderState,
                            frameHeader, headerBuffer,
                            h2ResponseHeaders, ctx.StreamId, !hasBody, ct);

                        if (hasBody)
                        {
                            await SendH2DataToClientAsync(
                                clientStream, clientSettings,
                                frameHeader, headerBuffer, dataBuffer,
                                response.Body!, ctx.StreamId, ct);
                        }

                        // ACK any SETTINGS frames received while we were processing this request
                        await SendPendingSettingsAcksAsync();
                    }
                    catch (Exception ex) when (!(ex is OperationCanceledException))
                    {
                        exceptionFunc?.Invoke(new ProxyHttpException(
                            $"Error translating H2→H1 stream {ctx.StreamId}", ex, ctx.Args));
                        await Http2FrameWriter.SendRstStreamAsync(
                            clientStream, headerBuffer, ctx.StreamId, 0x2 /* INTERNAL_ERROR */, ct);
                    }
                    finally
                    {
                        ctx.Args.Dispose();
                    }
                }
            }
            catch (OperationCanceledException) { /* shutdown */ }
        }


        // ── H1 send/receive helpers ───────────────────────────────────────────

        private static async Task SendH1RequestAsync(Stream server, Request request, MemoryStream? body, CancellationToken ct)
        {
            var hb = new HeaderBuilder();
            hb.WriteRequestLine(request.Method, request.RequestUri.PathAndQuery, HttpHeader.Version11);
            hb.WriteHeaders(request.Headers);
            var buf = hb.GetBuffer();
            await server.WriteAsync(buf.Array!, buf.Offset, buf.Count, ct);

            if (body != null && body.Length > 0)
            {
                var bodyBytes = body.ToArray();
                await server.WriteAsync(bodyBytes, 0, bodyBytes.Length, ct);
            }

            await server.FlushAsync(ct);
        }

        private static async Task ReadH1ResponseAsync(Stream server, Response response, CancellationToken ct)
        {
            // Read status line
            string? statusLine = await ReadLineFromStreamAsync(server, ct);
            if (string.IsNullOrEmpty(statusLine)) return;

            // Parse status line manually: "HTTP/1.1 200 OK"
            var parts = statusLine.Split(new[] { ' ' }, 3);
            response.HttpVersion       = parts[0].EndsWith("1.0") ? HttpHeader.Version10 : HttpHeader.Version11;
            response.StatusCode        = parts.Length > 1 && int.TryParse(parts[1], out var sc) ? sc : 200;
            response.StatusDescription = parts.Length > 2 ? parts[2] : string.Empty;

            // Read headers
            while (true)
            {
                var line = await ReadLineFromStreamAsync(server, ct);
                if (string.IsNullOrEmpty(line)) break;

                int colon = line.IndexOf(':');
                if (colon < 0) continue;
                response.Headers.AddHeader(new HttpHeader(
                    line.Substring(0, colon).Trim(),
                    line.Substring(colon + 1).Trim()));
            }

            // Read body based on content-length
            if (response.ContentLength > 0)
            {
                var bodyBytes = new byte[response.ContentLength];
                int totalRead = 0;
                while (totalRead < bodyBytes.Length)
                {
                    int read = await server.ReadAsync(bodyBytes, totalRead, (int)(bodyBytes.Length - totalRead), ct);
                    if (read == 0) break;
                    totalRead += read;
                }
                response.Body       = bodyBytes;
                response.IsBodyRead = true;
            }
        }

        private static async Task<string?> ReadLineFromStreamAsync(Stream stream, CancellationToken ct)
        {
            var buf     = new List<byte>(256);
            var oneByte = new byte[1];
            while (true)
            {
                int read = await stream.ReadAsync(oneByte, 0, 1, ct);
                if (read == 0) return null;
                if (oneByte[0] == '\n') break;
                if (oneByte[0] != '\r') buf.Add(oneByte[0]);
            }
            return System.Text.Encoding.ASCII.GetString(buf.ToArray());
        }

        // ── H2 response send helpers ──────────────────────────────────────────

        private static async Task SendH2HeadersToClientAsync(
            Stream clientStream, Http2Settings clientSettings, Http2EncoderState encoderState,
            Http2FrameHeader frameHeader, byte[] headerBuffer,
            IReadOnlyList<(string Name, string Value)> headers,
            int streamId, bool endStream,
            CancellationToken ct)
        {
            EnsureEncoder(encoderState, clientSettings);
            var encoder = encoderState.Encoder!;

            using var ms     = new MemoryStream();
            var       writer = new BinaryWriter(ms);

            foreach (var (name, value) in headers)
            {
                encoder.EncodeHeader(writer,
                    (ByteString)System.Text.Encoding.ASCII.GetBytes(name),
                    (ByteString)System.Text.Encoding.ASCII.GetBytes(value));
            }

            var encoded = ms.ToArray();
            var flags   = Http2FrameFlag.EndHeaders;
            if (endStream) flags |= Http2FrameFlag.EndStream;

            frameHeader.Length   = encoded.Length;
            frameHeader.Type     = Http2FrameType.Headers;
            frameHeader.Flags    = flags;
            frameHeader.StreamId = streamId;
            frameHeader.CopyToBuffer(headerBuffer);

            await clientStream.WriteAsync(headerBuffer, 0, 9,              ct);
            await clientStream.WriteAsync(encoded,      0, encoded.Length, ct);
        }

        private static async Task SendH2DataToClientAsync(
            Stream clientStream, Http2Settings clientSettings,
            Http2FrameHeader frameHeader, byte[] headerBuffer, byte[] dataBuffer,
            byte[] body, int streamId, CancellationToken ct)
        {
            int pos = 0;
            while (pos < body.Length)
            {
                int chunkLen = Math.Min(dataBuffer.Length, body.Length - pos);
                Buffer.BlockCopy(body, pos, dataBuffer, 0, chunkLen);
                pos += chunkLen;

                frameHeader.Length   = chunkLen;
                frameHeader.Type     = Http2FrameType.Data;
                frameHeader.Flags    = pos >= body.Length ? Http2FrameFlag.EndStream : (Http2FrameFlag)0;
                frameHeader.StreamId = streamId;
                frameHeader.CopyToBuffer(headerBuffer);

                await clientStream.WriteAsync(headerBuffer, 0, 9,         ct);
                await clientStream.WriteAsync(dataBuffer,   0, chunkLen,  ct);
            }
        }

        // ── HPACK helpers ─────────────────────────────────────────────────────

        private static IReadOnlyList<(string, string)> DecodeHeaders(
            ref Decoder? decoder, ref int tableSize,
            Http2Settings settings, byte[] block,
            ExceptionHandler? exceptionFunc)
        {
            var list = new List<(string, string)>();
            try
            {
                if (decoder == null || tableSize < settings.HeaderTableSize)
                {
                    tableSize = settings.HeaderTableSize;
                    decoder   = new Decoder(8192, tableSize);
                }
                var listener = new SimpleHeaderListener(list);
                decoder.Decode(new BinaryReader(new MemoryStream(block)), listener);
                decoder.EndHeaderBlock();
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

        private sealed class SimpleHeaderListener : IHeaderListener
        {
            private readonly List<(string, string)> _list;
            public SimpleHeaderListener(List<(string, string)> list) => _list = list;
            public void AddHeader(ByteString name, ByteString value, bool sensitive)
                => _list.Add((name.GetString(), value.GetString()));
        }

        // ── Stream context ────────────────────────────────────────────────────

        private sealed class Http2StreamContext
        {
            public int              StreamId            { get; }
            public SessionEventArgs Args                { get; }
            public MemoryStream?    RequestBody         { get; set; }
            public bool             RequestBodyComplete { get; set; }

            public Http2StreamContext(int streamId, SessionEventArgs args)
            {
                StreamId = streamId;
                Args     = args;
            }
        }
    }
}
#endif
