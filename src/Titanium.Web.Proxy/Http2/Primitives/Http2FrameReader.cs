#if NET6_0_OR_GREATER
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Titanium.Web.Proxy.Http2.Primitives
{
    /// <summary>
    /// Low-level HTTP/2 frame reading utilities.
    /// Responsible only for reliable byte-level I/O from a stream.
    /// </summary>
    internal static class Http2FrameReader
    {
        /// <summary>
        /// Reads exactly <paramref name="bytesToRead"/> bytes from <paramref name="input"/>,
        /// looping until the buffer is full or the stream signals EOF.
        /// </summary>
        /// <returns>Total bytes actually read. Less than <paramref name="bytesToRead"/> means EOF.</returns>
        internal static async Task<int> ForceReadAsync(
            Stream input, byte[] buffer, int offset, int bytesToRead,
            CancellationToken cancellationToken)
        {
            int totalRead = 0;
            while (bytesToRead > 0)
            {
                int read = await input.ReadAsync(buffer, offset, bytesToRead, cancellationToken);
                if (read == 0)
                    break; // EOF

                totalRead  += read;
                bytesToRead -= read;
                offset      += read;
            }

            return totalRead;
        }

        /// <summary>
        /// Attempts to read a complete 9-byte HTTP/2 frame header from <paramref name="input"/>
        /// and populates <paramref name="frameHeader"/>.
        /// </summary>
        /// <returns>
        /// <c>true</c> if a complete header was read; <c>false</c> on EOF / short read.
        /// </returns>
        internal static async Task<bool> TryReadFrameHeaderAsync(
            Stream input, byte[] headerBuffer, Http2FrameHeader frameHeader,
            CancellationToken cancellationToken)
        {
            int read = await ForceReadAsync(input, headerBuffer, 0, 9, cancellationToken);
            if (read != 9)
                return false;

            int length = (headerBuffer[0] << 16) | (headerBuffer[1] << 8) | headerBuffer[2];
            var type   = (Http2FrameType)headerBuffer[3];
            var flags  = (Http2FrameFlag)headerBuffer[4];
            int streamId = ((headerBuffer[5] & 0x7f) << 24)
                         | (headerBuffer[6] << 16)
                         | (headerBuffer[7] << 8)
                         |  headerBuffer[8];

            frameHeader.Length   = length;
            frameHeader.Type     = type;
            frameHeader.Flags    = flags;
            frameHeader.StreamId = streamId;

            return true;
        }
    }
}
#endif
