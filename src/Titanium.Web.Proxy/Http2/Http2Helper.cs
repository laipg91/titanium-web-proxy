#if NET6_0_OR_GREATER
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
using Titanium.Web.Proxy.Http2.Primitives;
using Titanium.Web.Proxy.Models;
using Decoder = Titanium.Web.Proxy.Http2.Hpack.Decoder;
using Encoder = Titanium.Web.Proxy.Http2.Hpack.Encoder;

namespace Titanium.Web.Proxy.Http2
{
    /// <summary>
    /// Core helper for HTTP/2 protocol handling. 
    /// Manages connection-level state, frame relaying, and hop-local control frame processing.
    /// </summary>
    internal class Http2Helper
    {
        /// <summary>
        /// The fixed connection preface that must be sent by both endpoints (RFC 7540 Section 3.5).
        /// </summary>
        public static readonly byte[] ConnectionPreface = Encoding.ASCII.GetBytes("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n");

        /// <summary>
        /// Relays the input clientStream to the server at the specified host name and port.
        /// Useful for HTTP/2 tunneling.
        /// </summary>
        internal static async Task SendHttp2(Stream clientStream, Stream serverStream,
            Func<SessionEventArgs> sessionFactory,
            Func<SessionEventArgs, Task> onBeforeRequest, Func<SessionEventArgs, Task> onBeforeResponse,
            CancellationTokenSource cancellationTokenSource, Guid connectionId,
            ExceptionHandler? exceptionFunc, bool enableWebSocketOverHttp2 = false)
        {
            var clientSettings = new Http2Settings();
            var serverSettings = new Http2Settings();

            var sessions = new ConcurrentDictionary<int, SessionEventArgs>();
            var clientWriteLock = new SemaphoreSlim(1, 1);
            var serverWriteLock = new SemaphoreSlim(1, 1);
            var clientFrameHeaderBuffer = new byte[9];
            var serverFrameHeaderBuffer = new byte[9];

            // HTTP/2 control frames are hop-local. The proxy establishes its own
            // SETTINGS state with both peers instead of relaying connection prefaces.
            // Advertise SETTINGS_ENABLE_CONNECT_PROTOCOL=1 (RFC 8441 §3) only if
            // WebSocket-over-HTTP/2 is explicitly enabled via ProxyServer.EnableWebSocketOverHttp2.
            if (enableWebSocketOverHttp2)
            {
                await WithWriteLockAsync(clientWriteLock,
                    () => Http2FrameWriter.SendSettingsWithExtendedConnectAsync(clientStream, clientFrameHeaderBuffer, cancellationTokenSource.Token));
                await WithWriteLockAsync(serverWriteLock,
                    () => Http2FrameWriter.SendSettingsWithExtendedConnectAsync(serverStream, serverFrameHeaderBuffer, cancellationTokenSource.Token));
            }
            else
            {
                // Send standard empty SETTINGS frame (no extended-CONNECT support)
                await WithWriteLockAsync(clientWriteLock,
                    () => Http2FrameWriter.SendSettingsAsync(clientStream, clientFrameHeaderBuffer, cancellationTokenSource.Token));
                await WithWriteLockAsync(serverWriteLock,
                    () => Http2FrameWriter.SendSettingsAsync(serverStream, serverFrameHeaderBuffer, cancellationTokenSource.Token));
            }

            // Now async relay all server=>client & client=>server data
            var sendRelay =
                CopyHttp2FrameAsync(clientStream, serverStream, clientSettings, serverSettings,
                    sessionFactory, sessions, onBeforeRequest,
                    clientWriteLock, serverWriteLock, connectionId, true, cancellationTokenSource.Token, exceptionFunc);
            var receiveRelay =
                CopyHttp2FrameAsync(serverStream, clientStream, serverSettings, clientSettings,
                    sessionFactory, sessions, onBeforeResponse,
                    serverWriteLock, clientWriteLock, connectionId, false, cancellationTokenSource.Token, exceptionFunc);

            await Task.WhenAny(sendRelay, receiveRelay);
            cancellationTokenSource.Cancel();

            await Task.WhenAll(sendRelay, receiveRelay);
        }

        private static async Task CopyHttp2FrameAsync(Stream input, Stream output,
            Http2Settings inputPeerSettings, Http2Settings outputPeerSettings,
            Func<SessionEventArgs> sessionFactory, ConcurrentDictionary<int, SessionEventArgs> sessions,
            Func<SessionEventArgs, Task> onBeforeRequestResponse,
            SemaphoreSlim inputWriteLock, SemaphoreSlim outputWriteLock,
            Guid connectionId, bool isClient, CancellationToken cancellationToken,
            ExceptionHandler? exceptionFunc)
        {
            int headerTableSize = 0;
            Decoder? decoder = null;

            // Stateful encoder per connection direction (Medium fix: reuse encoder for HPACK compression)
            // Note: EncoderState wrapper used because async methods cannot have ref parameters.
            var encoderState = new Http2EncoderState();

            var frameHeader = new Http2FrameHeader();
            var frameHeaderBuffer = new byte[9];
            byte[]? buffer = null;

            // Pending header block fragments for CONTINUATION frame support (High fix)
            // Key: streamId, Value: accumulated header block fragment bytes
            var pendingHeaderBlocks = new Dictionary<int, MemoryStream>();
            var ignoredPushStreams = new HashSet<int>();

            // Stream-level flow control window sizes (High fix)
            // Key: streamId, Value: current window size (initialized from SETTINGS_INITIAL_WINDOW_SIZE)
            var streamWindowSizes = new Dictionary<int, int>();

            // Fix Critical 2: Proxy-local flow control windows.
            // These track how many bytes the proxy has consumed but not yet acknowledged to the source.
            // When consumed bytes reach a threshold we send WINDOW_UPDATE back to the source so it can
            // keep sending, without letting the source flood the proxy's RAM unconstrained.
            const int WindowUpdateThreshold = 32768; // Send WINDOW_UPDATE after consuming ~32 KB
            int localConnConsumed = 0;               // bytes consumed from source at connection level
            var localStreamConsumed = new Dictionary<int, int>(); // per-stream consumed bytes

            while (true)
            {
                int read = await Http2FrameReader.ForceReadAsync(input, frameHeaderBuffer, 0, 9, cancellationToken);
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

                // Ensure buffer can hold the larger of: the negotiated MaxFrameSize and the
                // actual declared frame length. A peer that sends a frame larger than the
                // negotiated MaxFrameSize is committing a protocol violation, but we must
                // not overflow our buffer — allocate defensively rather than crash.
                int requiredSize = Math.Max(inputPeerSettings.MaxFrameSize, length);
                if (buffer == null || buffer.Length < requiredSize)
                {
                    buffer = new byte[requiredSize];
                }

                read = await Http2FrameReader.ForceReadAsync(input, buffer, 0, length, cancellationToken);
                if (read != length)
                {
                    return;
                }

                bool sendPacket = true;
                bool endStream = false;

                SessionEventArgs? args = null;
                RequestResponseBase? rr = null;

                if (!isClient && ignoredPushStreams.Contains(streamId))
                {
                    if (type == Http2FrameType.Data)
                    {
                        localConnConsumed += length;
                    }

                    if (type == Http2FrameType.RstStream ||
                        (flags & Http2FrameFlag.EndStream) != 0)
                    {
                        ignoredPushStreams.Remove(streamId);
                        streamWindowSizes.Remove(streamId);
                    }

                    if (type == Http2FrameType.Data && localConnConsumed >= WindowUpdateThreshold)
                    {
                        await WithWriteLockAsync(inputWriteLock,
                            () => Http2FrameWriter.SendWindowUpdateAsync(input, frameHeaderBuffer, 0,
                                localConnConsumed, cancellationToken));
                        localConnConsumed = 0;
                    }

                    continue;
                }

                if (!isClient && type == Http2FrameType.PushPromise)
                {
                    int offset = 0;
                    if ((flags & Http2FrameFlag.Padded) != 0)
                    {
                        offset++;
                    }

                    if (length - offset < 4)
                    {
                        throw new ProxyHttpException("HTTP/2 PUSH_PROMISE frame is missing the promised stream id.", null, null);
                    }

                    int promisedStreamId =
                        ((buffer[offset] & 0x7f) << 24) + (buffer[offset + 1] << 16) +
                        (buffer[offset + 2] << 8) + buffer[offset + 3];

                    ignoredPushStreams.Add(promisedStreamId);

                    await WithWriteLockAsync(inputWriteLock,
                        () => Http2FrameWriter.SendRstStreamAsync(input, frameHeaderBuffer, promisedStreamId, 8, cancellationToken));

                    continue;
                }

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

                    // Fix Critical 2: Track proxy-local consumed bytes for flow control.
                    // After the DATA bytes are consumed (written to rr.Http2BodyData or forwarded),
                    // accumulate them and send WINDOW_UPDATE back to the source once we cross
                    // the threshold, so the source knows the proxy is ready for more data.
                    localConnConsumed += length;
                    if (!localStreamConsumed.TryGetValue(streamId, out int streamConsumed))
                        streamConsumed = 0;
                    localStreamConsumed[streamId] = streamConsumed + length;
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
                            sessions.TryAdd(streamId, args);
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
                            sessions.TryAdd(streamId, args);
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
                            if (decoder == null || headerTableSize < inputPeerSettings.HeaderTableSize)
                            {
                                headerTableSize = inputPeerSettings.HeaderTableSize;
                                decoder = new Decoder(8192, headerTableSize);
                            }

                            decoder.Decode(new BinaryReader(new MemoryStream(completeHeaderData)),
                                headerListener);
                            decoder.EndHeaderBlock();

                            if (rr is Request request)
                            {
                                var method = headerListener.Method;
                                var path   = headerListener.Path;

                                // RFC 8441 §4: extended CONNECT requests include :protocol
                                // and MUST still carry :scheme and :path, but guard against
                                // misbehaving clients by only requiring at least a method.
                                if (method.Length == 0)
                                {
                                    throw new Exception("HTTP/2 HEADERS frame is missing :method.");
                                }

                                request.HttpVersion = HttpVersion.Version20;
                                request.Method      = method.GetString();
                                request.IsHttps     = headerListener.Scheme == ProxyServer.UriSchemeHttps;
                                request.Authority   = headerListener.Authority;

                                // For extended CONNECT, :path may be present (RFC 8441 §4 requires it).
                                // Fall back to "/" only if genuinely absent to avoid a crash.
                                request.RequestUriString8 = path.Length > 0 ? path : (ByteString)"/";

                                // Store :protocol so RequestHandler / translators can detect
                                // WebSocket-over-H2 without scanning regular headers.
                                if (headerListener.Protocol.Length > 0)
                                    request.Http2Protocol = headerListener.Protocol.GetString();
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
                                Action<byte[], int, int> onHeadersForwarded = isClient
                                    ? args!.OnDataSent
                                    : args!.OnDataReceived;
                                await WithWriteLockAsync(outputWriteLock,
                                    () => Http2FrameWriter.SendHeadersAsync(
                                        outputPeerSettings, encoderState, frameHeader, frameHeaderBuffer, rr, endStream, output, onHeadersForwarded, cancellationToken));
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
                                if (decoder == null || headerTableSize < inputPeerSettings.HeaderTableSize)
                                {
                                    headerTableSize = inputPeerSettings.HeaderTableSize;
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

                                    // RFC 8441 §4: :protocol may arrive in a CONTINUATION fragment.
                                    // Mirror the same assignment done in the primary HEADERS branch.
                                    if (headerListener.Protocol.Length > 0)
                                        request.Http2Protocol = headerListener.Protocol.GetString();
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
                                Action<byte[], int, int> onHeadersForwarded = isClient
                                    ? args.OnDataSent
                                    : args.OnDataReceived;
                                await WithWriteLockAsync(outputWriteLock,
                                    () => Http2FrameWriter.SendHeadersAsync(
                                        outputPeerSettings, encoderState, frameHeader, frameHeaderBuffer, rr, false, output, onHeadersForwarded, cancellationToken));
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
                    // SETTINGS are hop-local. Apply them to this peer state and ACK locally.
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
                                    inputPeerSettings.HeaderTableSize = value;
                                    break;
                                case 2: // SETTINGS_ENABLE_PUSH
                                    inputPeerSettings.EnablePush = value;
                                    break;
                                case 3: // SETTINGS_MAX_CONCURRENT_STREAMS
                                    inputPeerSettings.MaxConcurrentStreams = value < 0 ? int.MaxValue : value;
                                    break;
                                case 4: // SETTINGS_INITIAL_WINDOW_SIZE
                                    // RFC 7540 §6.9.2: update existing stream windows when INITIAL_WINDOW_SIZE changes
                                    int delta = value - inputPeerSettings.InitialWindowSize;
                                    inputPeerSettings.InitialWindowSize = value;
                                    // Update all open stream windows
                                    foreach (var key in streamWindowSizes.Keys)
                                    {
                                        streamWindowSizes[key] += delta;
                                    }
                                    break;
                                case 5: // SETTINGS_MAX_FRAME_SIZE
                                    inputPeerSettings.MaxFrameSize = value;
                                    break;
                                case 6: // SETTINGS_MAX_HEADER_LIST_SIZE
                                    inputPeerSettings.MaxHeaderListSize = value < 0 ? int.MaxValue : value;
                                    break;
                                case 8: // SETTINGS_ENABLE_CONNECT_PROTOCOL (RFC 8441 §3)
                                    // Record that the peer supports extended CONNECT.
                                    // We do not gate outgoing WebSocket streams on this flag
                                    // (the proxy already advertised 1 in our own SETTINGS),
                                    // but we store it for diagnostic / interop checks.
                                    inputPeerSettings.EnableConnectProtocol = value;
                                    break;
                            }
                        }

                        // Send SETTINGS ACK back (RFC 7540 §6.5)
                        await WithWriteLockAsync(inputWriteLock,
                            () => Http2FrameWriter.SendSettingsAckAsync(input, frameHeaderBuffer, cancellationToken));
                    }
                    sendPacket = false;
                }
                // ==================== PING frame ====================
                // PING is hop-local (RFC 7540 §6.7): proxy replies with PING+ACK to the
                // sender and never relays PING or PING ACK to the other side.
                else if (type == Http2FrameType.Ping)
                {
                    bool isAck = (flags & Http2FrameFlag.Ack) != 0;

                    if (!isAck && streamId == 0)
                    {
                        // Reply with PING + ACK flag, same 8-byte payload
                        var pingPayload = new byte[8];
                        Array.Copy(buffer, pingPayload, Math.Min(8, length));
                        await WithWriteLockAsync(inputWriteLock,
                            () => Http2FrameWriter.SendPingAckAsync(input, frameHeaderBuffer, pingPayload, cancellationToken));
                    }

                    // Never relay PING or PING ACK to the other peer — always consumed hop-locally.
                    sendPacket = false;
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
                            inputPeerSettings.ConnectionWindowSize += increment;
                        }
                        else
                        {
                            // Stream-level window update
                            if (!streamWindowSizes.TryGetValue(streamId, out int current))
                                current = inputPeerSettings.InitialWindowSize;
                            streamWindowSizes[streamId] = current + increment;
                        }
                    }
                    sendPacket = false;
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

                    await WithWriteLockAsync(outputWriteLock,
                        () => Http2FrameWriter.SendBodyAsync(
                            outputPeerSettings, encoderState, rr, frameHeader, frameHeaderBuffer, buffer, output, cancellationToken));
                }

                if (!isClient && endStream)
                {
                    sessions.TryRemove(streamId, out _);
                    streamWindowSizes.Remove(streamId);
                    System.Diagnostics.Debug.WriteLine("REMOVED CONN: " + connectionId + ", CLIENT: " + isClient + ", STREAM: " + streamId + ", TYPE: " + type);
                }

                if (sendPacket)
                {
                    await WithWriteLockAsync(outputWriteLock, async () =>
                    {
                        frameHeader.CopyToBuffer(frameHeaderBuffer);
                        await output.WriteAsync(frameHeaderBuffer, 0, frameHeaderBuffer.Length);
                        await output.WriteAsync(buffer, 0, length);
                    });
                }

                // Fix Critical 2: After writing DATA out, send WINDOW_UPDATE back to source
                // to tell it the proxy has room for more data (backpressure control).
                // We batch updates: only send when consumed >= threshold to avoid per-frame overhead.
                if (type == Http2FrameType.Data && localConnConsumed >= WindowUpdateThreshold)
                {
                    // Connection-level WINDOW_UPDATE (stream id = 0)
                    await WithWriteLockAsync(inputWriteLock,
                        () => Http2FrameWriter.SendWindowUpdateAsync(input, frameHeaderBuffer, 0, localConnConsumed, cancellationToken));
                    localConnConsumed = 0;
                }

                // Stream-level WINDOW_UPDATE: flush per-stream consumed bytes together with end-of-frame
                if (type == Http2FrameType.Data && streamId != 0 &&
                    localStreamConsumed.TryGetValue(streamId, out int toAckStream) &&
                    toAckStream >= WindowUpdateThreshold)
                {
                    await WithWriteLockAsync(inputWriteLock,
                        () => Http2FrameWriter.SendWindowUpdateAsync(input, frameHeaderBuffer, streamId, toAckStream, cancellationToken));
                    localStreamConsumed[streamId] = 0;
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

        private static async Task WithWriteLockAsync(SemaphoreSlim writeLock, Func<Task> writeFunc)
        {
            await writeLock.WaitAsync();
            try
            {
                await writeFunc();
            }
            finally
            {
                writeLock.Release();
            }
        }

        class MyHeaderListener : IHeaderListener
        {
            private readonly Action<ByteString, ByteString> addHeaderFunc;

            public ByteString Method { get; private set; }

            public ByteString Status { get; private set; }

            public ByteString Authority { get; private set; }

            private ByteString scheme;

            public ByteString Path { get; private set; }

            /// <summary>
            /// Value of the :protocol pseudo-header (RFC 8441 §4).
            /// Non-empty only for extended-CONNECT WebSocket streams.
            /// </summary>
            public ByteString Protocol { get; private set; }

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
                        case ":protocol":
                            // RFC 8441 §4: the :protocol pseudo-header identifies the
                            // application protocol to be tunnelled (e.g. "websocket").
                            // Stored separately so Request.Http2Protocol has a typed value
                            // without needing to scan regular headers.
                            Protocol = value;
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
