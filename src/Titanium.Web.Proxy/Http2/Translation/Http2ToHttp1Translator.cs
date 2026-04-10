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

            // Send the server connection preface first.
            // ACKs and stream frames must not overtake this SETTINGS frame.
            await Http2FrameWriter.SendSettingsAsync(clientStream, serverFrameHeaderBuffer, cts.Token);

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

            await onBeforeRequest(context.Args);

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
                        var request = context.Args.HttpClient.Request;
                        var response = context.Args.HttpClient.Response;

                        await SendH1RequestAsync(serverStream, request, context.RequestBody, ct);

                        response.RequestMethod = request.Method;
                        await ReadH1ResponseAsync(serverStream, response, ct);

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
        private static async Task ReadH1ResponseAsync(Stream server, Response response, CancellationToken ct)
        {
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
                bodyBytes = await ReadChunkedBodyAsync(server, ct);
            }
            else if (response.ContentLength >= 0)
            {
                bodyBytes = await ReadFixedLengthBodyAsync(server, response.ContentLength, ct);
            }
            else if (!response.KeepAlive || response.HttpVersion == HttpHeader.Version10)
            {
                bodyBytes = await ReadUntilEofAsync(server, ct);
            }
            else
            {
                bodyBytes = Array.Empty<byte>();
            }

            response.Body = bodyBytes;
            response.IsBodyRead = true;
        }

        private static async Task<byte[]> ReadFixedLengthBodyAsync(Stream stream, long contentLength, CancellationToken ct)
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

                totalRead += read;
            }

            return body;
        }

        private static async Task<byte[]> ReadChunkedBodyAsync(Stream stream, CancellationToken ct)
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

                var chunk = await ReadFixedLengthBodyAsync(stream, chunkSize, ct);
                await body.WriteAsync(chunk, 0, chunk.Length, ct);

                var chunkTerminator = await ReadLineFromStreamAsync(stream, ct);
                if (chunkTerminator == null)
                    throw new IOException("Unexpected end of chunked response terminator.");
            }

            return body.ToArray();
        }

        private static async Task<byte[]> ReadUntilEofAsync(Stream stream, CancellationToken ct)
        {
            using var body = new MemoryStream();
            var buffer = new byte[8192];

            while (true)
            {
                int read = await stream.ReadAsync(buffer, 0, buffer.Length, ct);
                if (read == 0)
                    break;

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

        private sealed class Http2StreamContext
        {
            public int StreamId { get; }
            public SessionEventArgs Args { get; }
            public MemoryStream? RequestBody { get; set; }
            public bool RequestBodyComplete { get; set; }

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
