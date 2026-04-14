#if NET6_0_OR_GREATER
using System;
using System.Security.Cryptography;
using System.Text;

namespace Titanium.Web.Proxy.Http2.WebSocket
{
    /// <summary>
    /// Utilities for computing and validating the WebSocket handshake challenge
    /// used during cross-protocol translation (HTTP/1.1 ↔ HTTP/2).
    ///
    /// Background
    /// ----------
    /// RFC 8441 removes the Sec-WebSocket-Key / Sec-WebSocket-Accept challenge
    /// from HTTP/2 WebSocket handshakes, because HTTP/2 streams are already
    /// authenticated and framed by the TLS layer.  However, when the proxy
    /// bridges an HTTP/1.1 client to an HTTP/2 backend (or vice-versa), it must
    /// terminate the challenge on behalf of the legacy side:
    ///
    ///   H1-client → proxy → H2-server
    ///     • Client sends  Sec-WebSocket-Key
    ///     • Proxy drops key before forwarding H2 CONNECT
    ///     • Server responds :status 200 (no accept hash)
    ///     • Proxy GENERATES the correct Sec-WebSocket-Accept hash and returns
    ///       "101 Switching Protocols" to the H1 client
    ///
    ///   H2-client → proxy → H1-server
    ///     • Client sends :protocol websocket (no key)
    ///     • Proxy GENERATES a random Sec-WebSocket-Key and sends H1 Upgrade
    ///     • Server responds 101 + Sec-WebSocket-Accept
    ///     • Proxy validates the hash then forwards :status 200 to H2 client
    /// </summary>
    internal static class WebSocketHandshakeHelper
    {
        // RFC 6455 §1.3 — fixed GUID concatenated with the client key before SHA-1
        private const string WebSocketGuid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

        // Length of the random key bytes before Base64 encoding (RFC 6455 §4.1)
        private const int KeyByteLength = 16;

        /// <summary>
        /// Computes the <c>Sec-WebSocket-Accept</c> header value for the given
        /// <paramref name="clientKey"/> using the RFC 6455 §1.3 algorithm:
        ///   SHA-1( key + <c>258EAFA5-E914-47DA-95CA-C5AB0DC85B11</c> ) → Base64.
        /// </summary>
        /// <param name="clientKey">
        ///     Raw value of the <c>Sec-WebSocket-Key</c> header, as received
        ///     from the HTTP/1.1 client (Base64-encoded 16-byte nonce).
        /// </param>
        /// <returns>The <c>Sec-WebSocket-Accept</c> value to return to the client.</returns>
        /// <exception cref="ArgumentNullException">
        ///     Thrown when <paramref name="clientKey"/> is null or empty.
        /// </exception>
        public static string ComputeAcceptKey(string clientKey)
        {
            if (string.IsNullOrEmpty(clientKey))
                throw new ArgumentNullException(nameof(clientKey),
                    "Sec-WebSocket-Key must not be null or empty.");

            // Concatenate key + GUID using ASCII (RFC 6455 §4.1)
            var combined = clientKey + WebSocketGuid;
            var inputBytes = Encoding.ASCII.GetBytes(combined);

            // SHA-1 hash — SHA1.Create() is thread-safe to construct; reuse is not required
            byte[] hashBytes;
#if NET5_0_OR_GREATER
            hashBytes = SHA1.HashData(inputBytes);
#else
            using var sha1 = SHA1.Create();
            hashBytes = sha1.ComputeHash(inputBytes);
#endif
            return Convert.ToBase64String(hashBytes);
        }

        /// <summary>
        /// Generates a cryptographically random <c>Sec-WebSocket-Key</c> value
        /// (16 random bytes encoded as Base64) that the proxy can use when
        /// initiating an HTTP/1.1 WebSocket handshake on behalf of an HTTP/2 client.
        /// </summary>
        /// <returns>A valid Base64-encoded 16-byte random nonce.</returns>
        public static string GenerateClientKey()
        {
            var keyBytes = new byte[KeyByteLength];
            using var rng = RandomNumberGenerator.Create();
            rng.GetBytes(keyBytes);
            return Convert.ToBase64String(keyBytes);
        }

        /// <summary>
        /// Validates that the <paramref name="receivedAccept"/> value returned by an
        /// HTTP/1.1 server matches the expected hash for <paramref name="sentKey"/>.
        /// Logs a warning (via <paramref name="onMismatch"/>) but does NOT abort the
        /// connection — some HTTP/1.1 servers misbehave and production proxies must
        /// be lenient.
        /// </summary>
        /// <param name="sentKey">The <c>Sec-WebSocket-Key</c> that the proxy sent.</param>
        /// <param name="receivedAccept">
        ///     The <c>Sec-WebSocket-Accept</c> value returned by the HTTP/1.1 server.
        /// </param>
        /// <param name="onMismatch">
        ///     Optional callback invoked when the hashes do not match.
        ///     Receives a human-readable diagnostic string.
        /// </param>
        /// <returns>
        ///     <c>true</c> if the hashes match; <c>false</c> otherwise.
        /// </returns>
        public static bool ValidateServerAccept(
            string sentKey,
            string? receivedAccept,
            Action<string>? onMismatch = null)
        {
            if (string.IsNullOrEmpty(receivedAccept))
            {
                onMismatch?.Invoke(
                    "H1 server returned no Sec-WebSocket-Accept; continuing anyway.");
                return false;
            }

            var expected = ComputeAcceptKey(sentKey);
            if (!string.Equals(expected, receivedAccept, StringComparison.Ordinal))
            {
                onMismatch?.Invoke(
                    $"Sec-WebSocket-Accept mismatch. Expected: {expected}, Got: {receivedAccept}");
                return false;
            }

            return true;
        }
    }
}
#endif
