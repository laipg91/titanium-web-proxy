using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Titanium.Web.Proxy.EventArguments;
using Titanium.Web.Proxy.IntegrationTests.Setup;
using Titanium.Web.Proxy.Http2.WebSocket;
using Titanium.Web.Proxy.Models;

namespace Titanium.Web.Proxy.IntegrationTests
{
    /// <summary>
    /// WebSocket unit tests — RFC 6455 handshake helper functions.
    /// These are pure unit tests with no network I/O.
    /// </summary>
    [TestClass]
    public class WebSocketHandshakeHelperTests
    {
        [TestMethod]
        public void Http2_ComputesCorrectAcceptHash()
        {
            // The example client key from RFC 6455 §1.3 example
            string clientKey = "dGhlIHNhbXBsZSBub25jZQ==";
            string expectedAccept = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";

            string computedAccept = WebSocketHandshakeHelper.ComputeAcceptKey(clientKey);

            Assert.AreEqual(expectedAccept, computedAccept);
        }

        [TestMethod]
        public void Http2_GeneratesValidClientKey()
        {
            string clientKey = WebSocketHandshakeHelper.GenerateClientKey();
            
            Assert.IsFalse(string.IsNullOrEmpty(clientKey));
            
            byte[] rawKey = Convert.FromBase64String(clientKey);
            Assert.AreEqual(16, rawKey.Length);
        }

        [TestMethod]
        public void Http2_ValidatesServerAcceptCorrectly()
        {
            string proxyKey = WebSocketHandshakeHelper.GenerateClientKey();
            string validAccept = WebSocketHandshakeHelper.ComputeAcceptKey(proxyKey);
            
            bool validationResult = WebSocketHandshakeHelper.ValidateServerAccept(proxyKey, validAccept);
            Assert.IsTrue(validationResult);

            bool failResult = WebSocketHandshakeHelper.ValidateServerAccept(proxyKey, "invalid_hash");
            Assert.IsFalse(failResult);
        }
    }

    /// <summary>
    /// HTTP/1.1 WebSocket integration tests via proxy.
    /// Tests WebSocket upgrade from HTTP/1.1 client through proxy to a remote echo server.
    /// </summary>
    [TestClass]
    public class Http1WebSocketProxyTests
    {
        private TestProxyServer? _proxyServer;

        [TestInitialize]
        public void Setup()
        {
            _proxyServer = new TestProxyServer(isReverseProxy: false, enableHttp2: false);
        }

        [TestCleanup]
        public void Teardown()
        {
            _proxyServer?.Dispose();
        }

        /// <summary>
        /// Test WebSocket HTTP/1.1 client → proxy → echo server (wss://echo.websocket.org/).
        /// Sends a message, validates that echo server returns the same message.
        /// 
        /// Note: This test requires internet connectivity to reach echo.websocket.org.
        /// If the test server is not available, this test will be skipped or fail gracefully.
        /// </summary>
        [TestMethod]
        [Timeout(15000)] // 15 seconds
        public async Task Http1_WebSocketEchoViaProxyReturnsMatchingMessage()
        {
            const string testMessage = "Hello, WebSocket!";
            string? echoedMessage = null;

            try
            {
                // Create a ClientWebSocket and connect through the proxy
                using var webSocket = new ClientWebSocket();
                
                // Set proxy to connect through Titanium.Web.Proxy
                var proxyPort = _proxyServer!.ListeningPort;
                webSocket.Options.Proxy = new WebProxy($"http://127.0.0.1:{proxyPort}");
                
                // Add User-Agent header
                webSocket.Options.SetRequestHeader("User-Agent", "Titanium.Web.Proxy.Test/1.0");

                // Connect to public echo server through proxy
                // Note: wss://echo.websocket.org/ is a public echo WebSocket server for testing
                await webSocket.ConnectAsync(
                    new Uri("wss://echo.websocket.org/"),
                    CancellationToken.None);

                // Verify connection state
                Assert.AreEqual(WebSocketState.Open, webSocket.State,
                    "WebSocket should be in Open state after successful connection");

                // Echo server sends a welcome message upon connection — consume it first
                byte[] welcomeBuffer = new byte[1024];
                WebSocketReceiveResult welcomeResult = await webSocket.ReceiveAsync(
                    new ArraySegment<byte>(welcomeBuffer),
                    CancellationToken.None);
                string welcomeMessage = Encoding.UTF8.GetString(welcomeBuffer, 0, welcomeResult.Count);
                Assert.IsTrue(welcomeMessage.StartsWith("Request served by"),
                    $"Expected welcome message, got: '{welcomeMessage}'");

                // Send test message
                byte[] sendBuffer = Encoding.UTF8.GetBytes(testMessage);
                await webSocket.SendAsync(
                    new ArraySegment<byte>(sendBuffer),
                    WebSocketMessageType.Text,
                    endOfMessage: true,
                    CancellationToken.None);

                // Receive echo response
                byte[] receiveBuffer = new byte[1024];
                WebSocketReceiveResult result = await webSocket.ReceiveAsync(
                    new ArraySegment<byte>(receiveBuffer),
                    CancellationToken.None);

                // Decode response
                echoedMessage = Encoding.UTF8.GetString(receiveBuffer, 0, result.Count);

                // Verify echo matches
                Assert.AreEqual(testMessage, echoedMessage,
                    $"Echo server should return the same message. Sent: '{testMessage}', Received: '{echoedMessage}'");

                // Gracefully close connection
                await webSocket.CloseAsync(
                    WebSocketCloseStatus.NormalClosure,
                    "Test complete",
                    CancellationToken.None);
            }
            catch (WebSocketException ex)
            {
                Assert.Fail($"WebSocket error during echo test: {ex.Message}\nReceived message: {echoedMessage}");
            }
            catch (HttpRequestException ex)
            {
                Assert.Fail($"HTTP error (possible proxy issue): {ex.Message}");
            }
        }

        /// <summary>
        /// Test multiple messages in sequence to verify proxy maintains connection state.
        /// </summary>
        [TestMethod]
        [Timeout(20000)] // 20 seconds
        public async Task Http1_WebSocketMultipleEchoMessagesViaProxy()
        {
            var testMessages = new[] { "Message 1", "Message 2", "Message 3" };

            try
            {
                using var webSocket = new ClientWebSocket();
                var proxyPort = _proxyServer!.ListeningPort;
                webSocket.Options.Proxy = new WebProxy($"http://127.0.0.1:{proxyPort}");
                webSocket.Options.SetRequestHeader("User-Agent", "Titanium.Web.Proxy.Test/1.0");

                await webSocket.ConnectAsync(
                    new Uri("wss://echo.websocket.org/"),
                    CancellationToken.None);

                // Consume welcome message
                byte[] welcomeBuffer = new byte[1024];
                WebSocketReceiveResult welcomeResult = await webSocket.ReceiveAsync(
                    new ArraySegment<byte>(welcomeBuffer),
                    CancellationToken.None);

                foreach (var testMessage in testMessages)
                {
                    // Send
                    byte[] sendBuffer = Encoding.UTF8.GetBytes(testMessage);
                    await webSocket.SendAsync(
                        new ArraySegment<byte>(sendBuffer),
                        WebSocketMessageType.Text,
                        endOfMessage: true,
                        CancellationToken.None);

                    // Receive
                    byte[] receiveBuffer = new byte[1024];
                    WebSocketReceiveResult result = await webSocket.ReceiveAsync(
                        new ArraySegment<byte>(receiveBuffer),
                        CancellationToken.None);

                    string echoedMessage = Encoding.UTF8.GetString(receiveBuffer, 0, result.Count);

                    Assert.AreEqual(testMessage, echoedMessage,
                        $"Echo mismatch for message '{testMessage}'");
                }

                await webSocket.CloseAsync(
                    WebSocketCloseStatus.NormalClosure,
                    "Test complete",
                    CancellationToken.None);
            }
            catch (Exception ex)
            {
                Assert.Fail($"Error during multi-message echo test: {ex.Message}");
            }
        }
    }

    /// <summary>
    /// HTTP/2 WebSocket integration tests using a local HTTP/2 echo server.
    /// Tests RFC 8441 extended-CONNECT WebSocket over HTTP/2.
    /// </summary>
    [TestClass]
    public class Http2WebSocketProxyTests
    {
        private TestProxyServer? _proxyServer;
        private Http2WebSocketEchoServer? _h2EchoServer;

        [TestInitialize]
        public async Task Setup()
        {
            _proxyServer = new TestProxyServer(isReverseProxy: false, enableHttp2: true);
            _h2EchoServer = new Http2WebSocketEchoServer();
            await _h2EchoServer.StartAsync();
        }

        [TestCleanup]
        public async Task Teardown()
        {
            if (_h2EchoServer != null)
            {
                await _h2EchoServer.StopAsync();
            }
            _proxyServer?.Dispose();
        }

        /// <summary>
        /// Test HTTP/2 WebSocket (RFC 8441 extended-CONNECT) through proxy.
        /// Client: HTTP/1.1 → Proxy: HTTP/2 → Server: HTTP/2 with WebSocket echo.
        /// 
        /// Flow:
        ///   1. HTTP/1.1 client sends WebSocket upgrade request through proxy
        ///   2. Proxy translates to HTTP/2 CONNECT+:protocol=websocket to backend
        ///   3. Backend (local H2 WebSocket server) accepts and echoes messages
        ///   4. Verify echo messages match sent messages
        /// </summary>
        [TestMethod]
        [Timeout(15000)] // 15 seconds
        public async Task Http2_WebSocketEchoViaProxyH1ToH2Translation()
        {
            const string testMessage = "Hello, HTTP/2 WebSocket!";
            string? echoedMessage = null;

            try
            {
                var proxyPort = _proxyServer!.ListeningPort;
                var h2ServerUri = _h2EchoServer!.GetWebSocketUri();

                // Create ClientWebSocket and connect through proxy to H2 backend
                using var webSocket = new ClientWebSocket();
                webSocket.Options.Proxy = new WebProxy($"http://127.0.0.1:{proxyPort}");
                webSocket.Options.SetRequestHeader("User-Agent", "Titanium.Web.Proxy.Test/1.0");

                // Connect to HTTP/2 backend through proxy
                await webSocket.ConnectAsync(h2ServerUri, CancellationToken.None);

                Assert.AreEqual(WebSocketState.Open, webSocket.State,
                    "WebSocket should be Open after connection");

                // Send message
                byte[] sendBuffer = Encoding.UTF8.GetBytes(testMessage);
                await webSocket.SendAsync(
                    new ArraySegment<byte>(sendBuffer),
                    WebSocketMessageType.Text,
                    endOfMessage: true,
                    CancellationToken.None);

                // Receive echo
                byte[] receiveBuffer = new byte[1024];
                WebSocketReceiveResult result = await webSocket.ReceiveAsync(
                    new ArraySegment<byte>(receiveBuffer),
                    CancellationToken.None);

                echoedMessage = Encoding.UTF8.GetString(receiveBuffer, 0, result.Count);

                // Verify echo
                Assert.AreEqual(testMessage, echoedMessage,
                    $"Echo mismatch. Sent: '{testMessage}', Received: '{echoedMessage}'");

                await webSocket.CloseAsync(
                    WebSocketCloseStatus.NormalClosure,
                    "Test complete",
                    CancellationToken.None);
            }
            catch (Exception ex)
            {
                Assert.Fail($"H2 WebSocket test failed: {ex.Message}\nEchoed: {echoedMessage}");
            }
        }
    }

    /// <summary>
    /// Simple HTTP/2 WebSocket echo server for testing RFC 8441 support.
    /// Listens on HTTP/2 with WebSocket support and echoes messages.
    /// 
    /// Note: For testing HTTP/2 support, we use cleartext HTTP/2 (h2c) with unencrypted connection.
    /// In production, this should always be over TLS (https with h2 ALPN negotiation).
    /// </summary>
    internal class Http2WebSocketEchoServer : IDisposable
    {
        private IWebHost? _host;
        private int _port = 5001;

        public async Task StartAsync()
        {
            _host = new WebHostBuilder()
                .UseKestrel(options =>
                {
                    // Configure HTTP/2 support on cleartext connection (h2c).
                    // This is for testing only; production should use HTTPS.
                    options.ListenLocalhost(_port, listenOptions =>
                    {
                        listenOptions.Protocols = HttpProtocols.Http2;
                    });
                })
                .Configure(app =>
                {
                    app.UseWebSockets(new WebSocketOptions
                    {
                        KeepAliveInterval = TimeSpan.FromSeconds(30)
                    });

                    app.Use(async (context, next) =>
                    {
                        if (context.WebSockets.IsWebSocketRequest)
                        {
                            try
                            {
                                using var webSocket = await context.WebSockets.AcceptWebSocketAsync();
                                await EchoWebSocketMessagesAsync(webSocket);
                            }
                            catch (Exception ex)
                            {
                                System.Diagnostics.Debug.WriteLine($"WebSocket accept error: {ex.Message}");
                            }
                        }
                        else
                        {
                            await next();
                        }
                    });
                })
                .Build();

            var hostTask = _host.RunAsync();
            await Task.Delay(500); // Give server time to start
        }

        public async Task StopAsync()
        {
            if (_host != null)
            {
                await _host.StopAsync();
                _host.Dispose();
            }
        }

        public Uri GetWebSocketUri()
        {
            // Return HTTP (not HTTPS) WebSocket URI for local h2c testing
            return new Uri($"ws://127.0.0.1:{_port}/ws");
        }

        private async Task EchoWebSocketMessagesAsync(WebSocket webSocket)
        {
            byte[] buffer = new byte[1024];

            try
            {
                while (webSocket.State == WebSocketState.Open)
                {
                    WebSocketReceiveResult result = await webSocket.ReceiveAsync(
                        new ArraySegment<byte>(buffer),
                        CancellationToken.None);

                    if (result.MessageType == WebSocketMessageType.Close)
                    {
                        await webSocket.CloseAsync(
                            WebSocketCloseStatus.NormalClosure,
                            "Closing",
                            CancellationToken.None);
                    }
                    else if (result.MessageType == WebSocketMessageType.Text)
                    {
                        // Echo back the same message
                        await webSocket.SendAsync(
                            new ArraySegment<byte>(buffer, 0, result.Count),
                            WebSocketMessageType.Text,
                            result.EndOfMessage,
                            CancellationToken.None);
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine($"WebSocket error: {ex.Message}");
            }
        }

        public void Dispose()
        {
            _host?.Dispose();
        }
    }
}

