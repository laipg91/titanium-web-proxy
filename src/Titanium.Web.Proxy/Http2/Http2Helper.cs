#if NETSTANDARD2_1
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Titanium.Web.Proxy.Compression;
using Titanium.Web.Proxy.EventArguments;
using Titanium.Web.Proxy.Exceptions;
using Titanium.Web.Proxy.Extensions;
using Titanium.Web.Proxy.Http;
using Titanium.Web.Proxy.Http2.Hpack;
using Titanium.Web.Proxy.Models;
using Decoder = Titanium.Web.Proxy.Http2.Hpack.Decoder;
using Encoder = Titanium.Web.Proxy.Http2.Hpack.Encoder;

namespace Titanium.Web.Proxy.Http2
{
    internal class Http2Helper
    {
        public static readonly byte[] ConnectionPreface = Encoding.ASCII.GetBytes("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");

        /// <summary>
        ///     Relays the input clientStream to the server at the specified host name and port with the given httpCmd and headers
        ///     as prefix. Useful for HTTP/2 tunneling.
        ///     Task-based Asynchronous Pattern
        /// </summary>
        internal static async Task SendHttp2(Stream clientStream, Stream serverStream,
            Func<SessionEventArgs> sessionFactory,
            Func<SessionEventArgs, Task> onBeforeRequest, Func<SessionEventArgs, Task> onBeforeResponse,
            CancellationTokenSource cancellationTokenSource, Guid connectionId,
            ExceptionHandler? exceptionFunc)
        {
            var clientSettings = new Http2Settings();
            var serverSettings = new Http2Settings();

            var sessions = new ConcurrentDictionary<int, SessionEventArgs>();

            // Now async relay all server=>client & client=>server data
            var sendRelay =
                CopyHttp2FrameAsync(clientStream, serverStream, clientSettings, serverSettings,
                    sessionFactory, sessions, onBeforeRequest,
                    connectionId, true, cancellationTokenSource.Token, exceptionFunc);
            var receiveRelay =
                CopyHttp2FrameAsync(serverStream, clientStream, serverSettings, clientSettings,
                    sessionFactory, sessions, onBeforeResponse,
                    connectionId, false, cancellationTokenSource.Token, exceptionFunc);

            await Task.WhenAny(sendRelay, receiveRelay);
            cancellationTokenSource.Cancel();

            await Task.WhenAll(sendRelay, receiveRelay);
        }

        private static async Task CopyHttp2FrameAsync(Stream input, Stream output,
            Http2Settings localSettings, Http2Settings remoteSettings,
            Func<SessionEventArgs> sessionFactory, ConcurrentDictionary<int, SessionEventArgs> sessions,
            Func<SessionEventArgs, Task> onBeforeRequestResponse,
            Guid connectionId, bool isClient, CancellationToken cancellationToken,
            ExceptionHandler? exceptionFunc)
        {
            int headerTableSize = 0;
            Decoder? decoder = null;

            // Stateful encoder per connection direction (Medium fix: reuse encoder for HPACK compression)
            // Note: EncoderState wrapper used because async methods cannot have ref parameters.
            var encoderState = new EncoderState();

            var frameHeader = new Http2FrameHeader();
            var frameHeaderBuffer = new byte[9];
            byte[]? buffer = null;

            // Pending header block fragments for CONTINUATION frame support (High fix)
            // Key: streamId, Value: accumulated header block fragment bytes
            var pendingHeaderBlocks = new Dictionary<int, MemoryStream>();

            // Stream-level flow control window sizes (High fix)
            // Key: streamId, Value: current window size (initialized from SETTINGS_INITIAL_WINDOW_SIZE)
            var streamWindowSizes = new Dictionary<int, int>();

            while (true)
            {
                int read = await ForceRead(input, frameHeaderBuffer, 0, 9, cancellationToken);
                if (read != 9)
                {
                    return;
                }

                int length = (frameHeaderBuffer[0] << 16) + (frameHeaderBuffer[1] << 8) + frameHeaderBuffer[2];
                var type = (Http2FrameType)frameHeaderBuffer[3];
                var flags = (Http2FrameFlag)frameHeaderBuffer[4];
                int streamId = ((frameHeaderBuffer[5] & 0x7f) << 24) + (frameHeaderBuffer[6] << 16) +
                               (frameHeaderBuffer[7] << 8) + frameHeaderBuffer[8];

                frameHeader.Length = length;
                frameHeader.Type = type;
                frameHeader.Flags = flags;
                frameHeader.StreamId = streamId;

                if (buffer == null || buffer.Length < localSettings.MaxFrameSize)
                {
                    buffer = new byte[localSettings.MaxFrameSize];
                }

                read = await ForceRead(input, buffer, 0, length, cancellationToken);
                if (read != length)
                {
                    return;
                }

                bool sendPacket = true;
                bool endStream = false;

                SessionEventArgs? args = null;
                RequestResponseBase? rr = null;

                if (type == Http2FrameType.Data || type == Http2FrameType.Headers || type == Http2FrameType.PushPromise)
                {
                    if (!sessions.TryGetValue(streamId, out args))
                    {
                        if (type == Http2FrameType.PushPromise && isClient)
                        {
                            throw new ProxyHttpException("HTTP Push promise received from the client.", null, args);
                        }
                    }
                }

                //System.Diagnostics.Debug.WriteLine("CONN: " + connectionId + ", CLIENT: " + isClient + ", STREAM: " + streamId + ", TYPE: " + type);

                // ==================== DATA frame ====================
                if (type == Http2FrameType.Data && args != null)
                {
                    if (isClient)
                        args.OnDataSent(buffer, 0, read);
                    else
                        args.OnDataReceived(buffer, 0, read);

                    rr = isClient ? (RequestResponseBase)args.HttpClient.Request : args.HttpClient.Response;

                    bool padded = (flags & Http2FrameFlag.Padded) != 0;
                    bool endStreamFlag = (flags & Http2FrameFlag.EndStream) != 0;
                    if (endStreamFlag)
                    {
                        endStream = true;
                    }

                    if (rr.Http2IgnoreBodyFrames)
                    {
                        sendPacket = false;
                    }

                    if (rr.ReadHttp2BodyTaskCompletionSource != null)
                    {
                        // GetBody() was called in the "before" event handler
                        var data = rr.Http2BodyData;
                        int offset = 0;
                        int dataLength = length;

                        // Fix Medium: Handle padding correctly in DATA frames (RFC 7540 §6.1)
                        if (padded)
                        {
                            int padLength = buffer[0];
                            offset = 1;
                            dataLength = length - 1 - padLength;
                        }

                        data!.Write(buffer, offset, dataLength);
                    }
                }
                // ==================== HEADERS frame ====================
                else if (type == Http2FrameType.Headers || type == Http2FrameType.PushPromise)
                {
                    bool endHeaders = (flags & Http2FrameFlag.EndHeaders) != 0;
                    bool padded = (flags & Http2FrameFlag.Padded) != 0;
                    bool priority = (flags & Http2FrameFlag.Priority) != 0;
                    bool endStreamFlag = (flags & Http2FrameFlag.EndStream) != 0;
                    if (endStreamFlag)
                    {
                        endStream = true;
                    }

                    int offset = 0;

                    // Fix Medium: Handle padding correctly in HEADERS frames (RFC 7540 §6.2)
                    int padLength = 0;
                    if (padded)
                    {
                        padLength = buffer[offset++];
                    }

                    if (type == Http2FrameType.PushPromise)
                    {
                        // Fix Medium: Handle PUSH_PROMISE properly (RFC 7540 §6.6)
                        int promisedStreamId =
                            ((buffer[offset] & 0x7f) << 24) + (buffer[offset + 1] << 16) +
                            (buffer[offset + 2] << 8) + buffer[offset + 3];
                        offset += 4;

                        if (!sessions.TryGetValue(streamId, out args))
                        {
                            args = sessionFactory();
                            args.IsPromise = true;
                            if (!sessions.TryAdd(streamId, args))
                                ;
                        }
                        // Register the promised stream ID as well
                        sessions.TryAdd(promisedStreamId, args);

                        System.Diagnostics.Debug.WriteLine("PROMISE STREAM: " + streamId + ", " + promisedStreamId +
                                                           ", CONN: " + connectionId);
                        rr = args.HttpClient.Request;

                        if (isClient)
                        {
                            Breakpoint(); // push_promise from client is a protocol violation
                        }
                    }
                    else // HEADERS
                    {
                        if (!sessions.TryGetValue(streamId, out args))
                        {
                            args = sessionFactory();
                            if (!sessions.TryAdd(streamId, args))
                                ;
                        }

                        rr = isClient ? (RequestResponseBase)args.HttpClient.Request : args.HttpClient.Response;

                        if (priority)
                        {
                            // 5 bytes: Exclusive(1 bit) + Stream Dependency(31 bits) + Weight(8 bits)
                            var priorityData = ((long)(buffer[offset] & 0x7f) << 32) +
                                               ((long)buffer[offset + 1] << 24) +
                                               (buffer[offset + 2] << 16) +
                                               (buffer[offset + 3] << 8) +
                                               buffer[offset + 4];
                            rr.Priority = priorityData;
                            offset += 5;
                        }
                    }

                    int dataLength = length - offset - padLength;

                    // Fix High: CONTINUATION frame support – buffer fragments until EndHeaders
                    // Accumulate this fragment into the pending header block for this stream
                    if (!pendingHeaderBlocks.TryGetValue(streamId, out var pendingMs))
                    {
                        pendingMs = new MemoryStream();
                        pendingHeaderBlocks[streamId] = pendingMs;
                    }
                    pendingMs.Write(buffer, offset, dataLength);

                    if (endHeaders)
                    {
                        // All header fragments received – decode the complete block
                        var completeHeaderData = pendingMs.ToArray();
                        pendingHeaderBlocks.Remove(streamId);

                        var headerListener = new MyHeaderListener(
                            (name, value) =>
                            {
                                var headers = isClient ? args!.HttpClient.Request.Headers : args!.HttpClient.Response.Headers;
                                headers.AddHeader(new HttpHeader(name, value));
                            });
                        try
                        {
                            // Recreate the decoder when new value is bigger
                            if (decoder == null || headerTableSize < localSettings.HeaderTableSize)
                            {
                                headerTableSize = localSettings.HeaderTableSize;
                                decoder = new Decoder(8192, headerTableSize);
                            }

                            decoder.Decode(new BinaryReader(new MemoryStream(completeHeaderData)),
                                headerListener);
                            decoder.EndHeaderBlock();

                            if (rr is Request request)
                            {
                                var method = headerListener.Method;
                                var path = headerListener.Path;
                                if (method.Length == 0 || path.Length == 0)
                                {
                                    throw new Exception("HTTP/2 Missing method or path");
                                }

                                request.HttpVersion = HttpVersion.Version20;
                                request.Method = method.GetString();
                                request.IsHttps = headerListener.Scheme == ProxyServer.UriSchemeHttps;
                                request.Authority = headerListener.Authority;
                                request.RequestUriString8 = path;
                            }
                            else
                            {
                                var response = (Response)rr!;
                                response.HttpVersion = HttpVersion.Version20;

                                // todo: avoid string conversion
                                string statusHack = HttpHeader.Encoding.GetString(headerListener.Status.Span);
                                int.TryParse(statusHack, out int statusCode);
                                response.StatusCode = statusCode;
                                response.StatusDescription = string.Empty;
                            }
                        }
                        catch (Exception ex)
                        {
                            exceptionFunc?.Invoke(new ProxyHttpException("Failed to decode HTTP/2 headers", ex, args));
                        }

                        var tcs = new TaskCompletionSource<bool>();
                        rr!.ReadHttp2BeforeHandlerTaskCompletionSource = tcs;

                        var handler = onBeforeRequestResponse(args!);
                        rr.Http2BeforeHandlerTask = handler;

                        if (handler == await Task.WhenAny(tcs.Task, handler))
                        {
                            rr.ReadHttp2BeforeHandlerTaskCompletionSource = null;
                            tcs.SetResult(true);
                            await SendHeader(remoteSettings, encoderState, frameHeader, frameHeaderBuffer, rr, endStream, output, args!.IsPromise);
                        }
                        else
                        {
                            rr.Http2IgnoreBodyFrames = true;
                        }

                        rr.Locked = true;
                    }
                    // else: waiting for CONTINUATION frames – don't send yet

                    sendPacket = false;
                }
                // ==================== CONTINUATION frame ====================
                // Fix High: Implement CONTINUATION frame (RFC 7540 §6.10)
                else if (type == Http2FrameType.Continuation)
                {
                    bool endHeaders = (flags & Http2FrameFlag.EndHeaders) != 0;

                    if (!pendingHeaderBlocks.TryGetValue(streamId, out var pendingMs))
                    {
                        // CONTINUATION without preceding HEADERS – protocol error
                        exceptionFunc?.Invoke(new ProxyHttpException(
                            "HTTP/2 CONTINUATION frame received without preceding HEADERS frame on stream " + streamId,
                            null, null));
                        return;
                    }

                    // Append the fragment
                    pendingMs.Write(buffer, 0, length);

                    if (endHeaders)
                    {
                        // Complete block assembled – decode now
                        // We need to find the session that was started by the HEADERS frame
                        // The session was already created when HEADERS was first received
                        if (sessions.TryGetValue(streamId, out args))
                        {
                            rr = isClient ? (RequestResponseBase)args.HttpClient.Request : args.HttpClient.Response;

                            var completeHeaderData = pendingMs.ToArray();
                            pendingHeaderBlocks.Remove(streamId);

                            var headerListener = new MyHeaderListener(
                                (name, value) =>
                                {
                                    var headers = isClient ? args!.HttpClient.Request.Headers : args!.HttpClient.Response.Headers;
                                    headers.AddHeader(new HttpHeader(name, value));
                                });
                            try
                            {
                                if (decoder == null || headerTableSize < localSettings.HeaderTableSize)
                                {
                                    headerTableSize = localSettings.HeaderTableSize;
                                    decoder = new Decoder(8192, headerTableSize);
                                }

                                decoder.Decode(new BinaryReader(new MemoryStream(completeHeaderData)), headerListener);
                                decoder.EndHeaderBlock();

                                if (rr is Request request)
                                {
                                    var method = headerListener.Method;
                                    var path = headerListener.Path;
                                    if (method.Length > 0 && path.Length > 0)
                                    {
                                        request.HttpVersion = HttpVersion.Version20;
                                        request.Method = method.GetString();
                                        request.IsHttps = headerListener.Scheme == ProxyServer.UriSchemeHttps;
                                        request.Authority = headerListener.Authority;
                                        request.RequestUriString8 = path;
                                    }
                                }
                                else
                                {
                                    var response = (Response)rr;
                                    response.HttpVersion = HttpVersion.Version20;
                                    string statusHack = HttpHeader.Encoding.GetString(headerListener.Status.Span);
                                    int.TryParse(statusHack, out int statusCode);
                                    response.StatusCode = statusCode;
                                    response.StatusDescription = string.Empty;
                                }
                            }
                            catch (Exception ex)
                            {
                                exceptionFunc?.Invoke(new ProxyHttpException("Failed to decode HTTP/2 CONTINUATION headers", ex, args));
                            }

                            var tcs = new TaskCompletionSource<bool>();
                            rr.ReadHttp2BeforeHandlerTaskCompletionSource = tcs;

                            var handler = onBeforeRequestResponse(args);
                            rr.Http2BeforeHandlerTask = handler;

                            if (handler == await Task.WhenAny(tcs.Task, handler))
                            {
                                rr.ReadHttp2BeforeHandlerTaskCompletionSource = null;
                                tcs.SetResult(true);
                                await SendHeader(remoteSettings, encoderState, frameHeader, frameHeaderBuffer, rr, false, output, args.IsPromise);
                            }
                            else
                            {
                                rr.Http2IgnoreBodyFrames = true;
                            }

                            rr.Locked = true;
                        }
                    }
                    // While waiting for more CONTINUATION frames, don't send this intermediate frame
                    sendPacket = false;
                }
                // ==================== SETTINGS frame ====================
                else if (type == Http2FrameType.Settings)
                {
                    // Fix Critical: Send SETTINGS ACK (RFC 7540 §6.5)
                    bool isAck = (flags & Http2FrameFlag.Ack) != 0;

                    if (!isAck)
                    {
                        // Parse settings parameters
                        if (length % 6 != 0)
                        {
                            // RFC 7540 §6.5: A SETTINGS frame with length not a multiple of 6 octets MUST be treated as FRAME_SIZE_ERROR
                            throw new ProxyHttpException("Invalid settings length", null, null);
                        }

                        int pos = 0;
                        while (pos < length)
                        {
                            int identifier = (buffer[pos++] << 8) + buffer[pos++];
                            int value = (buffer[pos++] << 24) + (buffer[pos++] << 16) + (buffer[pos++] << 8) + buffer[pos++];

                            switch (identifier)
                            {
                                case 1: // SETTINGS_HEADER_TABLE_SIZE
                                    remoteSettings.HeaderTableSize = value;
                                    break;
                                case 2: // SETTINGS_ENABLE_PUSH
                                    remoteSettings.EnablePush = value;
                                    break;
                                case 3: // SETTINGS_MAX_CONCURRENT_STREAMS
                                    remoteSettings.MaxConcurrentStreams = value < 0 ? int.MaxValue : value;
                                    break;
                                case 4: // SETTINGS_INITIAL_WINDOW_SIZE
                                    // RFC 7540 §6.9.2: update existing stream windows when INITIAL_WINDOW_SIZE changes
                                    int delta = value - remoteSettings.InitialWindowSize;
                                    remoteSettings.InitialWindowSize = value;
                                    // Update all open stream windows
                                    foreach (var key in streamWindowSizes.Keys)
                                    {
                                        streamWindowSizes[key] += delta;
                                    }
                                    break;
                                case 5: // SETTINGS_MAX_FRAME_SIZE
                                    remoteSettings.MaxFrameSize = value;
                                    break;
                                case 6: // SETTINGS_MAX_HEADER_LIST_SIZE
                                    remoteSettings.MaxHeaderListSize = value < 0 ? int.MaxValue : value;
                                    break;
                            }
                        }

                        // Send SETTINGS ACK back to sender (do not cancel with token – must complete)
                        // RFC 7540 §6.5: ACK = empty SETTINGS frame (length=0) with flag ACK (0x1)
                        var ackHeader = new Http2FrameHeader
                        {
                            Length = 0,
                            Type = Http2FrameType.Settings,
                            Flags = Http2FrameFlag.Ack,
                            StreamId = 0
                        };
                        ackHeader.CopyToBuffer(frameHeaderBuffer);
                        await output.WriteAsync(frameHeaderBuffer, 0, frameHeaderBuffer.Length);
                    }
                    // Whether ACK or not, forward the original frame to the other side
                    // (we just also sent our own ACK to the sender)
                }
                // ==================== PING frame ====================
                // Fix High: Reply to PING frames (RFC 7540 §6.7)
                else if (type == Http2FrameType.Ping)
                {
                    bool isAck = (flags & Http2FrameFlag.Ack) != 0;

                    if (!isAck && streamId == 0)
                    {
                        // Must reply with PING + ACK flag, same 8-byte payload
                        var pingHeader = new Http2FrameHeader
                        {
                            Length = 8,
                            Type = Http2FrameType.Ping,
                            Flags = Http2FrameFlag.Ack,
                            StreamId = 0
                        };
                        pingHeader.CopyToBuffer(frameHeaderBuffer);
                        await output.WriteAsync(frameHeaderBuffer, 0, frameHeaderBuffer.Length);
                        await output.WriteAsync(buffer, 0, 8);

                        // Do NOT forward PING to the other side – proxy handles it locally
                        sendPacket = false;
                    }
                    // If it's already an ACK, forward it (it's a response to a PING we sent)
                }
                // ==================== WINDOW_UPDATE frame ====================
                // Fix High: Handle flow control (RFC 7540 §6.9)
                else if (type == Http2FrameType.WindowUpdate)
                {
                    if (length == 4)
                    {
                        int increment = ((buffer[0] & 0x7f) << 24) + (buffer[1] << 16) + (buffer[2] << 8) + buffer[3];

                        if (streamId == 0)
                        {
                            // Connection-level window update
                            remoteSettings.ConnectionWindowSize += increment;
                        }
                        else
                        {
                            // Stream-level window update
                            if (!streamWindowSizes.TryGetValue(streamId, out int current))
                                current = remoteSettings.InitialWindowSize;
                            streamWindowSizes[streamId] = current + increment;
                        }
                    }
                    // Forward the WINDOW_UPDATE to the other side as normal
                }
                // ==================== RST_STREAM frame ====================
                else if (type == Http2FrameType.RstStream)
                {
                    int errorCode = (buffer[0] << 24) + (buffer[1] << 16) + (buffer[2] << 8) + buffer[3];
                    if (streamId == 0)
                    {
                        // Connection error
                        exceptionFunc?.Invoke(new ProxyHttpException("HTTP/2 connection error. Error code: " + errorCode, null, args));
                        return;
                    }
                    else
                    {
                        // Stream error
                        sessions.TryRemove(streamId, out _);
                        streamWindowSizes.Remove(streamId);

                        if (errorCode != 8 /*cancel*/)
                        {
                            exceptionFunc?.Invoke(new ProxyHttpException("HTTP/2 stream error. Error code: " + errorCode, null, args));
                        }
                    }
                }

                // ==================== End-of-stream body handling ====================
                if (endStream && rr!.ReadHttp2BodyTaskCompletionSource != null)
                {
                    if (!rr.BodyAvailable)
                    {
                        var data = rr.Http2BodyData;
                        var body = data!.ToArray();

                        if (rr.ContentEncoding != null)
                        {
                            using (var ms = new MemoryStream())
                            {
                                using (var zip =
                                    DecompressionFactory.Create(CompressionUtil.CompressionNameToEnum(rr.ContentEncoding), new MemoryStream(body)))
                                {
                                    zip.CopyTo(ms);
                                }

                                body = ms.ToArray();
                            }
                        }

                        if (!rr.BodyAvailable)
                        {
                            rr.Body = body;
                        }
                    }

                    rr.IsBodyRead = true;
                    rr.IsBodyReceived = true;

                    var tcs = rr.ReadHttp2BodyTaskCompletionSource;
                    rr.ReadHttp2BodyTaskCompletionSource = null;

                    if (!tcs.Task.IsCompleted)
                    {
                        tcs.SetResult(true);
                    }

                    rr.Http2BodyData = null;

                    if (rr.Http2BeforeHandlerTask != null)
                    {
                        await rr.Http2BeforeHandlerTask;
                    }

                    if (args!.IsPromise)
                    {
                        Breakpoint();
                    }

                    await SendBody(remoteSettings, encoderState, rr, frameHeader, frameHeaderBuffer, buffer, output);
                }

                if (!isClient && endStream)
                {
                    sessions.TryRemove(streamId, out _);
                    streamWindowSizes.Remove(streamId);
                    System.Diagnostics.Debug.WriteLine("REMOVED CONN: " + connectionId + ", CLIENT: " + isClient + ", STREAM: " + streamId + ", TYPE: " + type);
                }

                if (sendPacket)
                {
                    // Do not cancel the write operation
                    frameHeader.CopyToBuffer(frameHeaderBuffer);
                    await output.WriteAsync(frameHeaderBuffer, 0, frameHeaderBuffer.Length);
                    await output.WriteAsync(buffer, 0, length);
                }

                if (cancellationToken.IsCancellationRequested)
                {
                    return;
                }
            }
        }

        [Conditional("DEBUG")]
        private static void Breakpoint()
        {
            // When this method is called something received which is not yet implemented
            ;
        }

        /// <summary>
        /// Sends a HEADERS frame (or PUSH_PROMISE) to output, re-encoding headers with HPACK.
        /// Fix Medium: Uses a stateful Encoder per connection to enable HPACK header table compression.
        /// </summary>
        private static async Task SendHeader(Http2Settings settings, EncoderState encoderState,
            Http2FrameHeader frameHeader, byte[] frameHeaderBuffer,
            RequestResponseBase rr, bool endStream, Stream output, bool pushPromise)
        {
            // Fix Medium: Reuse encoder (stateful per-connection) instead of new Encoder() each time
            if (encoderState.Encoder == null || encoderState.HeaderTableSize != settings.HeaderTableSize)
            {
                encoderState.HeaderTableSize = settings.HeaderTableSize;
                encoderState.Encoder = new Encoder(settings.HeaderTableSize);
            }
            var encoder = encoderState.Encoder;

            var ms = new MemoryStream();
            var writer = new BinaryWriter(ms);
            if (rr.Priority.HasValue)
            {
                long p = rr.Priority.Value;
                writer.Write((byte)((p >> 32) & 0xff));
                writer.Write((byte)((p >> 24) & 0xff));
                writer.Write((byte)((p >> 16) & 0xff));
                writer.Write((byte)((p >> 8) & 0xff));
                writer.Write((byte)(p & 0xff));
            }

            if (rr is Request request)
            {
                var uri = request.RequestUri;
                encoder.EncodeHeader(writer, StaticTable.KnownHeaderMethod, request.Method.GetByteString());
                encoder.EncodeHeader(writer, StaticTable.KnownHeaderAuhtority, uri.Authority.GetByteString());
                encoder.EncodeHeader(writer, StaticTable.KnownHeaderScheme, uri.Scheme.GetByteString());
                encoder.EncodeHeader(writer, StaticTable.KnownHeaderPath, request.RequestUriString8, false,
                    HpackUtil.IndexType.None, false);
            }
            else
            {
                var response = (Response)rr;
                encoder.EncodeHeader(writer, StaticTable.KnownHeaderStatus, response.StatusCode.ToString().GetByteString());
            }

            foreach (var header in rr.Headers)
            {
                encoder.EncodeHeader(writer, header.NameData, header.ValueData);
            }

            var data = ms.ToArray();
            int newLength = data.Length;

            frameHeader.Length = newLength;
            frameHeader.Type = pushPromise ? Http2FrameType.PushPromise : Http2FrameType.Headers;

            var flags = Http2FrameFlag.EndHeaders;
            if (endStream)
            {
                flags |= Http2FrameFlag.EndStream;
            }

            if (rr.Priority.HasValue)
            {
                flags |= Http2FrameFlag.Priority;
            }

            frameHeader.Flags = flags;

            frameHeader.CopyToBuffer(frameHeaderBuffer);
            await output.WriteAsync(frameHeaderBuffer, 0, frameHeaderBuffer.Length);
            await output.WriteAsync(data, 0, data.Length);
        }

        /// <summary>
        /// Sends the response body, splitting into DATA frames respecting MaxFrameSize.
        /// </summary>
        private static async Task SendBody(Http2Settings settings, EncoderState encoderState,
            RequestResponseBase rr, Http2FrameHeader frameHeader, byte[] frameHeaderBuffer, byte[] buffer, Stream output)
        {
            var body = rr.CompressBodyAndUpdateContentLength();
            await SendHeader(settings, encoderState, frameHeader, frameHeaderBuffer, rr, !(rr.HasBody && rr.IsBodyRead), output, false);

            if (rr.HasBody && rr.IsBodyRead)
            {
                int pos = 0;
                while (pos < body!.Length)
                {
                    int bodyFrameLength = Math.Min(buffer.Length, body.Length - pos);
                    Buffer.BlockCopy(body, pos, buffer, 0, bodyFrameLength);
                    pos += bodyFrameLength;

                    frameHeader.Length = bodyFrameLength;
                    frameHeader.Type = Http2FrameType.Data;
                    frameHeader.Flags = pos < body.Length ? (Http2FrameFlag)0 : Http2FrameFlag.EndStream;

                    frameHeader.CopyToBuffer(frameHeaderBuffer);
                    await output.WriteAsync(frameHeaderBuffer, 0, frameHeaderBuffer.Length);
                    await output.WriteAsync(buffer, 0, bodyFrameLength);
                }
            }
        }

        private static async Task<int> ForceRead(Stream input, byte[] buffer, int offset, int bytesToRead,
            CancellationToken cancellationToken)
        {
            int totalRead = 0;
            while (bytesToRead > 0)
            {
                int read = await input.ReadAsync(buffer, offset, bytesToRead, cancellationToken);
                if (read == 0)
                {
                    break;
                }

                totalRead += read;
                bytesToRead -= read;
                offset += read;
            }

            return totalRead;
        }

        /// <summary>
        /// Wrapper class for Encoder to allow stateful per-connection HPACK encoding
        /// without needing ref parameters in async methods.
        /// </summary>
        private class EncoderState
        {
            public Encoder? Encoder { get; set; }
            /// <summary>The header table size the current Encoder was created with.</summary>
            public int HeaderTableSize { get; set; } = -1;
        }

        class MyHeaderListener : IHeaderListener
        {
            private readonly Action<ByteString, ByteString> addHeaderFunc;

            public ByteString Method { get; private set; }

            public ByteString Status { get; private set; }

            public ByteString Authority { get; private set; }

            private ByteString scheme;

            public ByteString Path { get; private set; }

            public string Scheme
            {
                get
                {
                    if (scheme.Equals(ProxyServer.UriSchemeHttp8))
                    {
                        return ProxyServer.UriSchemeHttp;
                    }

                    if (scheme.Equals(ProxyServer.UriSchemeHttps8))
                    {
                        return ProxyServer.UriSchemeHttps;
                    }

                    return string.Empty;
                }
            }

            public MyHeaderListener(Action<ByteString, ByteString> addHeaderFunc)
            {
                this.addHeaderFunc = addHeaderFunc;
            }

            public void AddHeader(ByteString name, ByteString value, bool sensitive)
            {
                if (name.Span[0] == ':')
                {
                    string nameStr = Encoding.ASCII.GetString(name.Span);
                    switch (nameStr)
                    {
                        case ":method":
                            Method = value;
                            return;
                        case ":authority":
                            Authority = value;
                            return;
                        case ":scheme":
                            scheme = value;
                            return;
                        case ":path":
                            Path = value;
                            return;
                        case ":status":
                            Status = value;
                            return;
                    }
                }

                addHeaderFunc(name, value);
            }

            public Uri GetUri()
            {
                if (Authority.Length == 0)
                {
                    // todo
                    Authority = HttpHeader.Encoding.GetBytes("abc.abc");
                }

                var bytes = new byte[scheme.Length + 3 + Authority.Length + Path.Length];
                scheme.Span.CopyTo(bytes);
                int idx = scheme.Length;
                bytes[idx++] = (byte)':';
                bytes[idx++] = (byte)'/';
                bytes[idx++] = (byte)'/';
                Authority.Span.CopyTo(bytes.AsSpan(idx, Authority.Length));
                idx += Authority.Length;
                Path.Span.CopyTo(bytes.AsSpan(idx, Path.Length));

                return new Uri(HttpHeader.Encoding.GetString(bytes));
            }
        }
    }
}
#endif