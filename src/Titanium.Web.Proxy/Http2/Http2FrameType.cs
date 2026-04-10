namespace Titanium.Web.Proxy.Http2
{

    /// <summary>
    /// HTTP/2 frame types as defined in RFC 7540 Section 6.
    /// </summary>
    internal enum Http2FrameType : byte
    {
        /// <summary>
        /// DATA frames (0x0) are used to convey arbitrary, variable-length sequences of octets associated with a stream.
        /// </summary>
        Data = 0x00,

        /// <summary>
        /// HEADERS frames (0x1) are used to open a stream and additionally carry a header block fragment.
        /// </summary>
        Headers = 0x01,

        /// <summary>
        /// PRIORITY frames (0x2) specify the sender-advised priority of a stream.
        /// </summary>
        Priority = 0x02,

        /// <summary>
        /// RST_STREAM frames (0x3) allow for immediate termination of a stream.
        /// </summary>
        RstStream = 0x03,

        /// <summary>
        /// SETTINGS frames (0x4) convey configuration parameters that affect how endpoints communicate.
        /// </summary>
        Settings = 0x04,

        /// <summary>
        /// PUSH_PROMISE frames (0x5) are used to notify the peer endpoint in advance of streams the sender intends to initiate.
        /// </summary>
        PushPromise = 0x05,

        /// <summary>
        /// PING frames (0x6) are a mechanism for measuring a minimal round-trip time from the sender.
        /// </summary>
        Ping = 0x06,

        /// <summary>
        /// GOAWAY frames (0x7) are used to initiate shutdown of a connection or to signal serious error conditions.
        /// </summary>
        GoAway = 0x07,

        /// <summary>
        /// WINDOW_UPDATE frames (0x8) are used to implement flow control.
        /// </summary>
        WindowUpdate = 0x08,

        /// <summary>
        /// CONTINUATION frames (0x9) are used to continue a sequence of header block fragments.
        /// </summary>
        Continuation = 0x09
    }
}