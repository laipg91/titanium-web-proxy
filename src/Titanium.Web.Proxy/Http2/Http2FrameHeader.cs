namespace Titanium.Web.Proxy.Http2
{

    /// <summary>
    /// Represents an 9-byte HTTP/2 frame header as defined in RFC 7540 Section 4.1.
    /// </summary>
    internal class Http2FrameHeader
    {
        /// <summary>
        /// The frame flags.
        /// </summary>
        public Http2FrameFlag Flags;

        /// <summary>
        /// The length of the frame payload in bytes.
        /// </summary>
        public int Length;

        /// <summary>
        /// The stream identifier (31-bit unsigned integer).
        /// </summary>
        public int StreamId;

        /// <summary>
        /// The frame type.
        /// </summary>
        public Http2FrameType Type;

        /// <summary>
        /// Serializes the frame header into the provided byte buffer (must be at least 9 bytes).
        /// </summary>
        /// <param name="buf">Target buffer.</param>
        public void CopyToBuffer(byte[] buf)
        {
            var length = Length;
            buf[0] = (byte)((length >> 16) & 0xff);
            buf[1] = (byte)((length >> 8) & 0xff);
            buf[2] = (byte)(length & 0xff);
            buf[3] = (byte)Type;
            buf[4] = (byte)Flags;
            var streamId = StreamId;
            buf[5] = (byte)((streamId >> 24) & 0xff);
            buf[6] = (byte)((streamId >> 16) & 0xff);
            buf[7] = (byte)((streamId >> 8) & 0xff);
            buf[8] = (byte)(streamId & 0xff);
        }
    }
}