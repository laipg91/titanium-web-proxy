#if NET6_0_OR_GREATER
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Titanium.Web.Proxy.Extensions;
using Titanium.Web.Proxy.Http;
using Titanium.Web.Proxy.Http2.Hpack;
using Titanium.Web.Proxy.Models;
using Decoder = Titanium.Web.Proxy.Http2.Hpack.Decoder;
using Encoder = Titanium.Web.Proxy.Http2.Hpack.Encoder;

namespace Titanium.Web.Proxy.Http2.Primitives
{
    /// <summary>
    /// Low-level HTTP/2 frame writing utilities.
    /// All methods are stateless helpers that write directly to a destination stream.
    /// HPACK state is owned by the caller via <see cref="Http2EncoderState"/>.
    /// </summary>
    internal static class Http2FrameWriter
    {
        // ── WINDOW_UPDATE ─────────────────────────────────────────────────────

        /// <summary>
        /// Writes a WINDOW_UPDATE frame to <paramref name="destination"/>.
        /// Sends backpressure acknowledgement for <paramref name="increment"/> bytes.
        /// </summary>
        internal static async Task SendWindowUpdateAsync(
            Stream destination, byte[] headerBuffer,
            int streamId, int increment,
            CancellationToken cancellationToken)
        {
            if (increment <= 0) return;

            // 4-byte payload, high bit reserved (RFC 7540 §6.9)
            var payload = new byte[4];
            payload[0] = (byte)((increment >> 24) & 0x7f);
            payload[1] = (byte)((increment >> 16) & 0xff);
            payload[2] = (byte)((increment >>  8) & 0xff);
            payload[3] = (byte)( increment        & 0xff);

            var header = new Http2FrameHeader
            {
                Length   = 4,
                Type     = Http2FrameType.WindowUpdate,
                Flags    = (Http2FrameFlag)0,
                StreamId = streamId
            };
            header.CopyToBuffer(headerBuffer);

            await destination.WriteAsync(headerBuffer, 0, 9, cancellationToken);
            await destination.WriteAsync(payload,      0, 4, cancellationToken);
        }

        // ── SETTINGS ──────────────────────────────────────────────────────────

        /// <summary>
        /// Writes an empty SETTINGS frame (no flags) as the connection preface
        /// when acting as an HTTP/2 server toward the client (RFC 9113 §3.4 / §6.5).
        /// </summary>
        internal static async Task SendSettingsAsync(
            Stream destination, byte[] headerBuffer,
            CancellationToken cancellationToken)
        {
            var header = new Http2FrameHeader
            {
                Length   = 0,
                Type     = Http2FrameType.Settings,
                Flags    = (Http2FrameFlag)0,
                StreamId = 0
            };
            header.CopyToBuffer(headerBuffer);
            await destination.WriteAsync(headerBuffer, 0, 9, cancellationToken);
        }

        /// <summary>
        /// Writes a SETTINGS frame that advertises SETTINGS_ENABLE_CONNECT_PROTOCOL=1
        /// (RFC 8441 §3) in addition to the standard empty preface.
        /// Call this instead of <see cref="SendSettingsAsync"/> when the proxy must
        /// inform the peer that extended CONNECT (WebSocket-over-H2) is supported.
        /// </summary>
        /// <remarks>
        /// The SETTINGS payload is 6 bytes: 2-byte identifier (0x0008) + 4-byte value (0x00000001).
        /// </remarks>
        internal static async Task SendSettingsWithExtendedConnectAsync(
            Stream destination, byte[] headerBuffer,
            CancellationToken cancellationToken)
        {
            // 6-byte payload: id=0x0008, value=0x00000001
            var payload = new byte[6];
            payload[0] = 0x00;
            payload[1] = 0x08;   // identifier = SETTINGS_ENABLE_CONNECT_PROTOCOL
            payload[2] = 0x00;
            payload[3] = 0x00;
            payload[4] = 0x00;
            payload[5] = 0x01;   // value = 1 (enabled)

            var header = new Http2FrameHeader
            {
                Length   = payload.Length,
                Type     = Http2FrameType.Settings,
                Flags    = (Http2FrameFlag)0,
                StreamId = 0
            };
            header.CopyToBuffer(headerBuffer);

            await destination.WriteAsync(headerBuffer, 0, 9, cancellationToken);
            await destination.WriteAsync(payload,      0, payload.Length, cancellationToken);
        }

        // ── SETTINGS ACK ──────────────────────────────────────────────────────

        /// <summary>
        /// Writes an empty SETTINGS frame with the ACK flag set (RFC 7540 §6.5).
        /// </summary>
        internal static async Task SendSettingsAckAsync(
            Stream destination, byte[] headerBuffer,
            CancellationToken cancellationToken)
        {
            var header = new Http2FrameHeader
            {
                Length   = 0,
                Type     = Http2FrameType.Settings,
                Flags    = Http2FrameFlag.Ack,
                StreamId = 0
            };
            header.CopyToBuffer(headerBuffer);
            await destination.WriteAsync(headerBuffer, 0, 9, cancellationToken);
        }

        // ── PING ACK ──────────────────────────────────────────────────────────

        /// <summary>
        /// Writes a PING frame with the ACK flag and the same 8-byte payload (RFC 7540 §6.7).
        /// </summary>
        internal static async Task SendPingAckAsync(
            Stream destination, byte[] headerBuffer, byte[] pingPayload,
            CancellationToken cancellationToken)
        {
            var header = new Http2FrameHeader
            {
                Length   = 8,
                Type     = Http2FrameType.Ping,
                Flags    = Http2FrameFlag.Ack,
                StreamId = 0
            };
            header.CopyToBuffer(headerBuffer);
            await destination.WriteAsync(headerBuffer,  0, 9, cancellationToken);
            await destination.WriteAsync(pingPayload,   0, 8, cancellationToken);
        }

        // ── RST_STREAM ────────────────────────────────────────────────────────

        /// <summary>
        /// Writes a RST_STREAM frame with the given error code (RFC 7540 §6.4).
        /// </summary>
        internal static async Task SendRstStreamAsync(
            Stream destination, byte[] headerBuffer,
            int streamId, uint errorCode,
            CancellationToken cancellationToken)
        {
            var payload = new byte[4];
            payload[0] = (byte)(errorCode >> 24);
            payload[1] = (byte)(errorCode >> 16);
            payload[2] = (byte)(errorCode >>  8);
            payload[3] = (byte) errorCode;

            var header = new Http2FrameHeader
            {
                Length   = 4,
                Type     = Http2FrameType.RstStream,
                Flags    = (Http2FrameFlag)0,
                StreamId = streamId
            };
            header.CopyToBuffer(headerBuffer);
            await destination.WriteAsync(headerBuffer, 0, 9, cancellationToken);
            await destination.WriteAsync(payload,      0, 4, cancellationToken);
        }

        // ── GOAWAY ────────────────────────────────────────────────────────────       

        /// <summary>
        /// Writes a GOAWAY frame with the given last processed stream id and error code (RFC 9113 §6.8).
        /// </summary>
        internal static async Task SendGoAwayAsync(
            Stream destination, byte[] headerBuffer,
            int lastStreamId, uint errorCode,
            CancellationToken cancellationToken)
        {
            var payload = new byte[8];
            payload[0] = (byte)((lastStreamId >> 24) & 0x7f);
            payload[1] = (byte)((lastStreamId >> 16) & 0xff);
            payload[2] = (byte)((lastStreamId >>  8) & 0xff);
            payload[3] = (byte)( lastStreamId        & 0xff);
            payload[4] = (byte)(errorCode >> 24);
            payload[5] = (byte)(errorCode >> 16);
            payload[6] = (byte)(errorCode >>  8);
            payload[7] = (byte) errorCode;

            var header = new Http2FrameHeader
            {
                Length   = payload.Length,
                Type     = Http2FrameType.GoAway,
                Flags    = (Http2FrameFlag)0,
                StreamId = 0
            };
            header.CopyToBuffer(headerBuffer);
            await destination.WriteAsync(headerBuffer, 0, 9, cancellationToken);
            await destination.WriteAsync(payload,      0, payload.Length, cancellationToken);
        }

        // ── HEADERS ───────────────────────────────────────────────────────────

        /// <summary>
        /// HPACK-encodes and writes a HEADERS frame for a request or response.
        /// Recreates the encoder per header block to avoid cross-stream state bleed.
        /// </summary>
        internal static async Task SendHeadersAsync(
            Http2Settings remoteSettings,
            Http2EncoderState encoderState,
            Http2FrameHeader frameHeader,
            byte[] headerBuffer,
            RequestResponseBase rr,
            bool endStream,
            Stream destination,
            Action<byte[], int, int>? onWritten,
            CancellationToken cancellationToken)
        {
            // Favor correctness over compression ratio: a fresh encoder per header block
            // avoids carrying dynamic-table state across unrelated streams.
            encoderState.HeaderTableSize = remoteSettings.HeaderTableSize;
            encoderState.Encoder = new Encoder(remoteSettings.HeaderTableSize);
            var encoder = encoderState.Encoder;
            using var ms = new MemoryStream();
            var writer  = new BinaryWriter(ms);

            // Priority prefix if present
            if (rr.Priority.HasValue)
            {
                long p = rr.Priority.Value;
                writer.Write((byte)((p >> 32) & 0xff));
                writer.Write((byte)((p >> 24) & 0xff));
                writer.Write((byte)((p >> 16) & 0xff));
                writer.Write((byte)((p >>  8) & 0xff));
                writer.Write((byte)( p        & 0xff));
            }

            if (rr is Request request)
            {
                var uri = request.RequestUri;
                encoder.EncodeHeader(writer, StaticTable.KnownHeaderMethod,    request.Method.GetByteString());
                encoder.EncodeHeader(writer, StaticTable.KnownHeaderAuthority, uri.Authority.GetByteString());
                encoder.EncodeHeader(writer, StaticTable.KnownHeaderScheme,    uri.Scheme.GetByteString());
                encoder.EncodeHeader(writer, StaticTable.KnownHeaderPath,      request.RequestUriString8, false,
                    HpackUtil.IndexType.None, false);

                // RFC 8441 §4: extended-CONNECT WebSocket streams carry :protocol.
                // Must be encoded after the four mandatory pseudo-headers and before
                // regular headers so the peer can identify the tunnelled protocol.
                if (!string.IsNullOrEmpty(request.Http2Protocol))
                {
                    encoder.EncodeHeader(writer, StaticTable.KnownHeaderProtocol,
                        request.Http2Protocol!.GetByteString(), false,
                        HpackUtil.IndexType.None, false);
                }
            }
            else
            {
                var response = (Response)rr;
                // RFC 7540 §8.1.2.4: Pseudo-headers MUST NOT be emitted in trailer headers
                // Only encode :status for the initial HEADERS frame, not for trailers
                if (!endStream)
                {
                    encoder.EncodeHeader(writer, StaticTable.KnownHeaderStatus,
                        response.StatusCode.ToString().GetByteString());
                }
                else
                {
                    System.Diagnostics.Debug.WriteLine($"[H2] SendHeadersAsync TRAILER: Skipping :status encoding for endStream=true");
                }
            }

            IEnumerable<HttpHeader> headersToEncode = rr.Headers;
            if (rr is Response trailerResponse && endStream)
            {
                headersToEncode = trailerResponse.Http2TrailerHeaders;
            }

            foreach (var header in headersToEncode)
                encoder.EncodeHeader(writer, header.NameData, header.ValueData);

            var encoded  = ms.ToArray();
            var flags    = Http2FrameFlag.EndHeaders;
            if (endStream)         flags |= Http2FrameFlag.EndStream;
            if (rr.Priority.HasValue) flags |= Http2FrameFlag.Priority;

            frameHeader.Length = encoded.Length;
            frameHeader.Type   = Http2FrameType.Headers;
            frameHeader.Flags  = flags;

            // Debug: trace headers being sent (especially trailers)
            if (endStream && rr is Response)
            {
                var headerList = new System.Collections.Generic.List<(string, string)>();
                var response = (Response)rr;
                foreach (var h in response.Http2TrailerHeaders)
                    headerList.Add((h.Name, h.Value));
                System.Diagnostics.Debug.WriteLine($"[H2] SendHeadersAsync: StreamId={frameHeader.StreamId}, EndStream=true (TRAILER HEADERS), " +
                    $"StatusCode={response.StatusCode}, " +
                    $"Headers={{{string.Join(", ", headerList.Select(x => $"{x.Item1}:{x.Item2}"))}}}, " +
                    $"EncodedLen={encoded.Length}, Flags=0x{((byte)flags):X2}");
            }

            frameHeader.CopyToBuffer(headerBuffer);
            await destination.WriteAsync(headerBuffer, 0, 9,              cancellationToken);
            await destination.WriteAsync(encoded,      0, encoded.Length, cancellationToken);

            if (onWritten != null)
            {
                var frameBytes = new byte[9 + encoded.Length];
                Buffer.BlockCopy(headerBuffer, 0, frameBytes, 0, 9);
                Buffer.BlockCopy(encoded, 0, frameBytes, 9, encoded.Length);
                onWritten(frameBytes, 0, frameBytes.Length);
            }
        }

        // ── DATA ──────────────────────────────────────────────────────────────

        /// <summary>
        /// Compresses (if needed) and writes DATA frames for a response body,
        /// splitting into chunks of at most <see cref="Http2Settings.MaxFrameSize"/>.
        /// Also writes the updated HEADERS frame beforehand.
        /// </summary>
        internal static async Task SendBodyAsync(
            Http2Settings remoteSettings,
            Http2EncoderState encoderState,
            RequestResponseBase rr,
            Http2FrameHeader frameHeader,
            byte[] headerBuffer,
            byte[] dataBuffer,
            Stream destination,
            CancellationToken cancellationToken)
        {
            var body = rr.CompressBodyAndUpdateContentLength();
            await SendHeadersAsync(remoteSettings, encoderState, frameHeader, headerBuffer,
                rr, !(rr.HasBody && rr.IsBodyRead), destination, null, cancellationToken);

            if (!rr.HasBody || !rr.IsBodyRead || body == null) return;

            int pos = 0;
            while (pos < body.Length)
            {
                int chunkLen = Math.Min(dataBuffer.Length, body.Length - pos);
                Buffer.BlockCopy(body, pos, dataBuffer, 0, chunkLen);
                pos += chunkLen;

                frameHeader.Length = chunkLen;
                frameHeader.Type   = Http2FrameType.Data;
                frameHeader.Flags  = (pos < body.Length)
                    ? (Http2FrameFlag)0
                    : Http2FrameFlag.EndStream;

                frameHeader.CopyToBuffer(headerBuffer);
                await destination.WriteAsync(headerBuffer, 0, 9,         cancellationToken);
                await destination.WriteAsync(dataBuffer,   0, chunkLen,  cancellationToken);
            }
        }
    }

    // ── EncoderState ──────────────────────────────────────────────────────────

    /// <summary>
    /// Holds the current encoder settings container for one connection direction.
    /// Needed because async methods cannot use ref parameters.
    /// </summary>
    internal sealed class Http2EncoderState
    {
        public Encoder? Encoder      { get; set; }
        public int      HeaderTableSize { get; set; } = -1;
    }
}
#endif
