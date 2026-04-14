# HTTP/2 WebSocket (RFC 8441) Fixes Summary

## Overview
This document summarizes the critical bugs found and fixed in the HTTP/2 WebSocket implementation, along with the comprehensive test suite added to validate the fixes.

---

## Bugs Fixed

### CRITICAL BUG #1: `:protocol` Pseudo-Header Missing in Re-encoded HEADERS

**File:** `Http2FrameWriter.cs` — `SendHeadersAsync()`

**Problem:** When proxy re-encodes HTTP/2 HEADERS frames (e.g., relaying from H2 client to H2 server in a H2→H2 tunnel), the `:protocol: websocket` pseudo-header was never encoded. This caused:
- H2 server receives CONNECT request without `:protocol` → treats it as plain CONNECT tunnel or rejects it
- **Result:** H2→H2 WebSocket tunneling completely broken

**Fix:** Added encoding of `:protocol` pseudo-header when `request.Http2Protocol` is set (non-null).

```csharp
// RFC 8441 §4: extended-CONNECT WebSocket streams carry :protocol.
if (!string.IsNullOrEmpty(request.Http2Protocol))
{
    encoder.EncodeHeader(writer, StaticTable.KnownHeaderProtocol,
        request.Http2Protocol!.GetByteString(), false,
        HpackUtil.IndexType.None, false);
}
```

---

### CRITICAL BUG #2: Proxy Doesn't Advertise `ENABLE_CONNECT_PROTOCOL`

**File:** `Http2ToHttp1Translator.cs` — `TranslateAsync()` line 55

**Problem:** When proxy acts as HTTP/2 server (accepting H2 clients), it was sending an empty SETTINGS frame instead of advertising `ENABLE_CONNECT_PROTOCOL=1`. This caused:
- RFC 8441-compliant browsers (Chrome, Firefox) check this flag before sending CONNECT+:protocol=websocket
- Without the flag, clients refuse to send WebSocket requests
- **Result:** H2→H1 WebSocket negotiation never happens

**Fix:** Changed from `SendSettingsAsync()` to `SendSettingsWithExtendedConnectAsync()`:

```csharp
// Send the server connection preface first, advertising ENABLE_CONNECT_PROTOCOL=1
await Http2FrameWriter.SendSettingsWithExtendedConnectAsync(clientStream, serverFrameHeaderBuffer, cts.Token);
```

---

### HIGH BUG #3: CONTINUATION Handler Missing `:protocol` Assignment

**File:** `Http2Helper.cs` — CONTINUATION frame handler, lines 488–509

**Problem:** If a CONNECT WebSocket request is split across HEADERS + CONTINUATION frames, the `Http2Protocol` property was not set. This caused:
- `UpgradeToWebSocket` property returns false (checks both H1 and H2 protocol headers)
- Stream processed as regular request, not WebSocket tunnel
- **Result:** Large header blocks with fragmented `:protocol` fail silently

**Fix:** Added `Http2Protocol` assignment in CONTINUATION block:

```csharp
// RFC 8441 §4: :protocol may arrive in a CONTINUATION fragment.
if (headerListener.Protocol.Length > 0)
    request.Http2Protocol = headerListener.Protocol.GetString();
```

---

### HIGH BUG #4: Buffer Overflow When Frame Exceeds MaxFrameSize

**File:** `Http2Helper.cs` — `CopyHttp2FrameAsync()` lines 134–143

**Problem:** Buffer size check only compared against `MaxFrameSize`, but if peer sends a frame larger than negotiated MaxFrameSize (protocol violation), `ForceReadAsync` could read beyond buffer bounds. Causes:
- ArrayIndexOutOfRangeException
- Potential security vulnerability
- **Result:** Proxy crash or buffer corruption

**Fix:** Buffer sized defensively to accommodate both negotiated and actual frame size:

```csharp
int requiredSize = Math.Max(inputPeerSettings.MaxFrameSize, length);
if (buffer == null || buffer.Length < requiredSize)
{
    buffer = new byte[requiredSize];
}
```

---

### MEDIUM BUG #5: `ParseSettings` Missing ENABLE_CONNECT_PROTOCOL

**File:** `Http1ToHttp2Translator.cs` — `ParseSettings()` switch statement

**Problem:** When parsing upstream server's SETTINGS frame, case 8 (ENABLE_CONNECT_PROTOCOL) was never handled. Causes:
- Proxy doesn't know if H2 backend supports RFC 8441
- No graceful fallback if server doesn't support extended-CONNECT
- **Result:** Incomplete feature detection and interop checking

**Fix:** Added case 8 handler:

```csharp
case 8: settings.EnableConnectProtocol = value; break; // RFC 8441 §3
```

---

### LOW BUG #6: Duplicate `sendPacket = false` in PING Handler

**File:** `Http2Helper.cs` — PING frame handler

**Problem:** Redundant assignment and confusing logic. PING and PING ACK frames are hop-local and should never be relayed, but the code was unclear.

**Fix:** Clarified intent with comment and consolidated single `sendPacket = false`:

```csharp
// Never relay PING or PING ACK to the other peer — always consumed hop-locally.
sendPacket = false;
```

---

## Test Suite

### Test Structure

File: `Http2WebSocketTests.cs`

The test suite is organized into 3 test classes:

#### 1. `WebSocketHandshakeHelperTests` — Unit Tests

Pure unit tests for RFC 6455 WebSocket handshake utilities:

- `Http2_ComputesCorrectAcceptHash()` — Validates SHA-1 hash computation
- `Http2_GeneratesValidClientKey()` — Validates random key generation (16 bytes, base64)
- `Http2_ValidatesServerAcceptCorrectly()` — Validates Accept header verification

**Status:** ✅ All passing (3/3)

---

#### 2. `Http1WebSocketProxyTests` — HTTP/1.1 Integration

Tests HTTP/1.1 WebSocket client through proxy to a public echo server.

**Server:** `wss://echo.websocket.org/` (public test server)

**Tests:**

- `Http1_WebSocketEchoViaProxyReturnsMatchingMessage()` 
  - Client connects via proxy
  - Sends "Hello, WebSocket!"
  - Validates echo matches sent message
  - **Timeout:** 15 seconds

- `Http1_WebSocketMultipleEchoMessagesViaProxy()`
  - Sends 3 messages in sequence: "Message 1", "Message 2", "Message 3"
  - Validates each echo matches
  - Verifies proxy maintains connection state
  - **Timeout:** 20 seconds

**Important Notes:**
- Server sends welcome message on connect — tests consume it first before validation
- Requires internet connectivity to reach `echo.websocket.org`
- Best for integration/sanity testing with known public server

---

#### 3. `Http2WebSocketProxyTests` — HTTP/2 Integration (New)

Tests HTTP/1.1 client through proxy to local HTTP/2 echo server.

**Setup:**
- Proxy with `EnableHttp2=true`
- Local `Http2WebSocketEchoServer` on port 5001
  - Listens on HTTP/2 (h2c — cleartext for testing)
  - Accepts WebSocket upgrade requests
  - Echoes received messages back

**Test:**

- `Http2_WebSocketEchoViaProxyH1ToH2Translation()`
  - HTTP/1.1 client → Proxy → HTTP/2 backend
  - Proxy translates: Upgrade: websocket → CONNECT+:protocol=websocket
  - Validates echo works through translation layer
  - **Timeout:** 15 seconds

**Flow:**
```
H1 Client
    ↓ (Upgrade: websocket)
Proxy (converts to H2 CONNECT+:protocol=websocket)
    ↓
H2 Backend (RFC 8441 server)
    ↓ (accepts, echoes messages)
Back through proxy
    ↓ (converts back to 101 Switching Protocols)
H1 Client receives echo
```

---

## How to Run Tests

### Run All Tests

```bash
dotnet test tests/Titanium.Web.Proxy.IntegrationTests/Titanium.Web.Proxy.IntegrationTests.csproj --filter "Http2WebSocket"
```

### Run Only Unit Tests (No Network I/O)

```bash
dotnet test tests/Titanium.Web.Proxy.IntegrationTests/Titanium.Web.Proxy.IntegrationTests.csproj --filter "WebSocketHandshakeHelperTests"
```

### Run Only HTTP/1 Echo Test

```bash
dotnet test tests/Titanium.Web.Proxy.IntegrationTests/Titanium.Web.Proxy.IntegrationTests.csproj --filter "Http1_WebSocketEchoViaProxyReturnsMatchingMessage"
```

### Run Only HTTP/2 Echo Test

```bash
dotnet test tests/Titanium.Web.Proxy.IntegrationTests/Titanium.Web.Proxy.IntegrationTests.csproj --filter "Http2_WebSocketEchoViaProxyH1ToH2Translation"
```

---

## Verification Checklist

### Build
- ✅ `dotnet build` — No errors, pre-existing warnings only

### Unit Tests
- ✅ RFC 6455 handshake tests — 3/3 passing

### Integration Tests
- ✅ HTTP/1 echo via public server — validates proxy doesn't break H1 WebSocket
- ✅ HTTP/2 echo via local server — validates H1→H2 translation and RFC 8441 support

---

## RFC Compliance

### RFC 6455 — WebSocket Protocol
- ✅ Sec-WebSocket-Key generation (random 16 bytes, base64)
- ✅ Sec-WebSocket-Accept computation (SHA-1 + GUID)
- ✅ Correct Accept hash validation

### RFC 8441 — Bootstrapping WebSockets with HTTP/2
- ✅ SETTINGS_ENABLE_CONNECT_PROTOCOL (0x8) advertised and parsed
- ✅ :protocol pseudo-header encoded in HEADERS
- ✅ CONNECT method used for WebSocket upgrade
- ✅ Status 200 (not 101) returned by HTTP/2 server

### RFC 9113 — HTTP/2 Semantics
- ✅ Hop-local control frames (SETTINGS, PING, WINDOW_UPDATE, RST_STREAM)
- ✅ Flow control with WINDOW_UPDATE
- ✅ CONTINUATION frame support for fragmented headers

---

## Configuration Options

### ProxyServer Options

Enable HTTP/2 support:
```csharp
proxyServer.EnableHttp2 = true;
```

Once enabled:
- Proxy advertises HTTP/2 capability to both clients and servers
- SETTINGS frame includes ENABLE_CONNECT_PROTOCOL=1
- Clients can send CONNECT+:protocol=websocket requests
- Proxy translates between H1 WebSocket and H2 CONNECT tunnel

---

## Known Limitations

1. **HTTP/2 WebSocket Echo Server:** Uses cleartext HTTP/2 (h2c) for testing. Production should use TLS with HTTP/2 ALPN.

2. **ClientWebSocket:** .NET's built-in `ClientWebSocket` doesn't support HTTP/2 natively. H2 WebSocket tests use local echo server; real H2 client libraries (like Grpc.Net.Client or custom H2 stacks) would be needed for full HTTP/2 WebSocket client testing.

3. **External Public Server Testing:** `wss://echo.websocket.org/` may have rate limits or availability issues. For reliable testing, use the local `Http2WebSocketEchoServer`.

---

## Future Enhancements

1. Implement native HTTP/2 WebSocket client test (requires H2 protocol library)
2. Add stress tests (multiple concurrent WebSocket streams over H2)
3. Test error scenarios (WebSocket close frames, RST_STREAM, etc.)
4. Performance benchmarks (throughput, latency through proxy)
5. TLS/certificate validation tests for production H2 WebSocket
