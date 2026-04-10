using System;

namespace Titanium.Web.Proxy.Http2
{

    /// <summary>
    /// HTTP/2 frame flags as defined in RFC 7540 Section 6.
    /// </summary>
    [Flags]
    internal enum Http2FrameFlag : byte
    {
        /// <summary>
        /// SETTINGS frames (0x4) and PING frames (0x6) use the ACK flag (0x1) to acknowledge receipt.
        /// </summary>
        Ack = 0x01,

        /// <summary>
        /// DATA frames (0x0) and HEADERS frames (0x1) use the END_STREAM flag (0x1) to indicate the end of a stream.
        /// </summary>
        EndStream = 0x01,

        /// <summary>
        /// HEADERS frames (0x1), PUSH_PROMISE frames (0x5), and CONTINUATION frames (0x9) use the END_HEADERS flag (0x4).
        /// </summary>
        EndHeaders = 0x04,

        /// <summary>
        /// DATA frames (0x0), HEADERS frames (0x1), and PUSH_PROMISE frames (0x5) use the PADDED flag (0x8).
        /// </summary>
        Padded = 0x08,

        /// <summary>
        /// HEADERS frames (0x1) use the PRIORITY flag (0x20) to indicate the presence of priority information.
        /// </summary>
        Priority = 0x20
    }
}