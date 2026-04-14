#if NET6_0_OR_GREATER
namespace Titanium.Web.Proxy.Http2
{
    /// <summary>
    /// Holds the HTTP/2 connection settings as defined in RFC 9113 Section 6.5.2
    /// and RFC 8441 (Bootstrapping WebSockets with HTTP/2).
    /// Each instance represents one peer's settings — either what the proxy
    /// advertises to that peer, or the last settings received from it.
    /// </summary>
    internal class Http2Settings
    {
        /// <summary>
        /// SETTINGS_HEADER_TABLE_SIZE (0x1).
        /// Maximum size of the HPACK compression table in bytes.
        /// Default: 4096 bytes.
        /// </summary>
        public int HeaderTableSize { get; set; } = 4096;

        /// <summary>
        /// SETTINGS_ENABLE_PUSH (0x2).
        /// Whether server push is permitted (1 = enabled, 0 = disabled).
        /// Default: 1 (enabled).
        /// </summary>
        public int EnablePush { get; set; } = 1;

        /// <summary>
        /// SETTINGS_MAX_CONCURRENT_STREAMS (0x3).
        /// Maximum number of concurrent streams. int.MaxValue means unlimited.
        /// Default: unlimited.
        /// </summary>
        public int MaxConcurrentStreams { get; set; } = int.MaxValue;

        /// <summary>
        /// SETTINGS_INITIAL_WINDOW_SIZE (0x4).
        /// Initial flow-control window size for new streams.
        /// Default: 65535 bytes (64 KB - 1).
        /// </summary>
        public int InitialWindowSize { get; set; } = 65535;

        /// <summary>
        /// SETTINGS_MAX_FRAME_SIZE (0x5).
        /// Maximum allowed size of a frame payload.
        /// Default: 16384 bytes (16 KB). Valid range: 16384–16777215.
        /// </summary>
        public int MaxFrameSize { get; set; } = 16384;

        /// <summary>
        /// SETTINGS_MAX_HEADER_LIST_SIZE (0x6).
        /// Advisory limit on the maximum size of a header block.
        /// Default: int.MaxValue (unlimited).
        /// </summary>
        public int MaxHeaderListSize { get; set; } = int.MaxValue;

        /// <summary>
        /// SETTINGS_ENABLE_CONNECT_PROTOCOL (0x8) — RFC 8441 §3.
        /// When 1, the extended CONNECT method is permitted: a CONNECT request
        /// MAY include a :protocol pseudo-header to bootstrap a WebSocket (or
        /// other application protocol) tunnel over an HTTP/2 stream.
        /// The proxy advertises this to both client and server so that browsers
        /// and RFC 8441-aware backends can negotiate WebSocket over HTTP/2.
        /// Default: 0 (advertising disabled until negotiated).
        /// </summary>
        public int EnableConnectProtocol { get; set; } = 0;

        /// <summary>
        /// Current connection-level flow-control window size.
        /// Initialised to the default 65535 and updated by WINDOW_UPDATE frames
        /// on stream 0. Not a SETTINGS parameter; managed separately.
        /// </summary>
        public int ConnectionWindowSize { get; set; } = 65535;
    }
}
#endif
