# Implementation Plan - WebSocket over HTTP/2 (RFC 8441)

This plan outlines the steps required to implement support for bootstrapping WebSockets with HTTP/2 as defined in RFC 8441 and clarified in RFC 9113. 

## User Review Required

> [!IMPORTANT]
> This implementation focuses on the **handshake, transport, and multiplexing** layer of WebSockets over HTTP/2. Unlike HTTP/1.1 WebSocket which takes over the TCP socket natively, HTTP/2 WebSockets are multiplexed over a single stream.
> The proxy will **relay** WebSocket data frames natively within the H2 `DATA` frame tunnel without blocking other H2 streams.

> [!WARNING]
> RFC 8441 requires cross-version protocol translation (H1 ↔ H2). The `Sec-WebSocket-Key` mechanism is obsolete in H2. The proxy must synthesize and simulate this handshake when communicating cross-protocol to prevent connection drops.

## Proposed Changes

### 1. Architectural Changes: Multiplexing Loop

#### [MODIFY] [RequestHandler.cs](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/RequestHandler.cs)
- Prevent HTTP/2 WebSockets from falling into `HandleWebSocketUpgrade`.
- **Reason:** `HandleWebSocketUpgrade` cuts the connection from the HTTP loop and uses raw TCP tunneling (`TcpHelper.SendRaw`), which violates HTTP/2 multiplexing and will crash the HTTP/2 connection.
- H2 WebSockets must be kept inside the `Http2Helper.cs` or translator loops, processing as continuous `DATA` frames.

- ---

### 2. HTTP/2 settings and Primitives (Enforcement)

#### [MODIFY] [Http2Settings.cs](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/Http2/Http2Settings.cs)
- Add `EnableConnectProtocol` property (mapped to identifier 0x8).

#### [MODIFY] [Http2FrameWriter.cs](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/Http2/Primitives/Http2FrameWriter.cs)
- Include `SETTINGS_ENABLE_CONNECT_PROTOCOL = 1` in the initial SETTINGS frame.

#### [MODIFY] [Http2Helper.cs](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/Http2/Http2Helper.cs)
- **Settings Parsing:** Ensure we capture if the upstream server returns `ENABLE_CONNECT_PROTOCOL`. If the client requests upgrading but the server doesn't support it, we must reject the upgrade or fallback properly.
- **EndStream Handling:** Ensure the stream is kept alive for continuous WebSocket relay until a proper `EndStream` data frame or `RST_STREAM` is sent by either party.

- ---

### 3. Protocol Translation: Sec-WebSocket-Key & Sec-WebSocket-Accept

#### [MODIFY] [Http1ToHttp2Translator.cs](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/Http2/Translation/Http1ToHttp2Translator.cs)
- **H1 Client ↔ H2 Server:**
    - Parse the `Upgrade: websocket` from an H1 Client and translate it to an H2 `CONNECT` method with `:protocol: websocket`.
    - Drop the `Sec-WebSocket-Key` header before sending to H2 backend.
    - When the H2 Server responds with `:status: 200`, the proxy **MUST** generate the `Sec-WebSocket-Accept` hash based on the client's original `Sec-WebSocket-Key` string (`base64(SHA1(key + specific_guid))`).
    - Return `101 Switching Protocols` to the H1 Client with the generated Accept Hash.

#### [NEW] Http2ToHttp1Translator.cs (or inline logic)
- **H2 Client ↔ H1 Server:**
    - Parse the incoming `:protocol: websocket` H2 stream.
    - Generate a random `Sec-WebSocket-Key` for the upstream H1 Server.
    - Translate the request to `GET` + `Upgrade: websocket` and send to backend.
    - Validate the `Sec-WebSocket-Accept` from the H1 backend's `101` response.
    - Send a `:status: 200` response without the WebSocket Accept keys back to the H2 client.

- ---

### 4. HPACK and Header Handling

#### [MODIFY] [StaticTable.cs](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/Http2/Hpack/StaticTable.cs)
- Add `KnownHeaderProtocol = (ByteString)":protocol"`.

#### [MODIFY] [Request.cs](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/Http/Request.cs)
- Add `public string? Http2Protocol { get; set; }` property.
- Update `UpgradeToWebSocket` property to also return true if `Http2Protocol == "websocket"`.

- ---

## Verification Plan

### Automated Tests
- Create a new integration test suite `Http2WebSocketTests.cs`:
    - **Test Case 1**: H2 Client -> Proxy -> H2 Server (Verify continuous multiplexed DATA flow without END_STREAM cut-off).
    - **Test Case 2**: H1 Client -> Proxy -> H2 Server (Verify `Sec-WebSocket-Accept` hash generation by the proxy).
    - **Test Case 3**: Verify SETTINGS frame contains `SETTINGS_ENABLE_CONNECT_PROTOCOL`.

### Manual Verification
- Use an H2-WebSocket-enabled client (e.g., modern Chrome testing against an RFC 8441 server) to verify full-duplex communication over a multiplexed HTTP/2 Proxy session without breaking the other concurrent HTTP/2 streams.
