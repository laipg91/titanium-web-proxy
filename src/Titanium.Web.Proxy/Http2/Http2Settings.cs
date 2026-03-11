#if NETSTANDARD2_1
namespace Titanium.Web.Proxy.Http2
{
    /// <summary>
    /// Holds the HTTP/2 connection settings as defined in RFC 7540 Section 6.5.2.
    /// </summary>
    internal class Http2Settings
    {
        /// <summary>
        /// SETTINGS_HEADER_TABLE_SIZE (0x1).
        /// The maximum size of the header compression table in bytes.
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
        /// The maximum number of concurrent streams. int.MaxValue means unlimited.
        /// Default: unlimited.
        /// </summary>
        public int MaxConcurrentStreams { get; set; } = int.MaxValue;

        /// <summary>
        /// SETTINGS_INITIAL_WINDOW_SIZE (0x4).
        /// The initial flow-control window size for new streams.
        /// Default: 65535 bytes (64 KB - 1).
        /// </summary>
        public int InitialWindowSize { get; set; } = 65535;

        /// <summary>
        /// SETTINGS_MAX_FRAME_SIZE (0x5).
        /// The maximum allowed size of a frame payload.
        /// Default: 16384 bytes (16 KB). Must be between 16384 and 16777215.
        /// </summary>
        public int MaxFrameSize { get; set; } = 16384;

        /// <summary>
        /// SETTINGS_MAX_HEADER_LIST_SIZE (0x6).
        /// Advisory limit on the maximum size of a header list.
        /// Default: int.MaxValue (unlimited).
        /// </summary>
        public int MaxHeaderListSize { get; set; } = int.MaxValue;

        /// <summary>
        /// The current flow-control window size for the entire connection.
        /// Starts at the default initial window size.
        /// </summary>
        public int ConnectionWindowSize { get; set; } = 65535;
    }
}
#endif
