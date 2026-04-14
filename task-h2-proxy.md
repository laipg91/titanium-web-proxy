# Task: WebSocket over HTTP/2 (RFC 8441) Implementation

## Phase 1 — Primitives & Data Model
- [x] `Http2Settings.cs` — thêm `EnableConnectProtocol`
- [x] `Http2FrameWriter.cs` — `SendSettingsWithExtendedConnectAsync` gửi SETTINGS kèm 0x8
- [x] `StaticTable.cs` — thêm `KnownHeaderProtocol = ":protocol"`
- [x] `Request.cs` — thêm `Http2Protocol`, cập nhật `UpgradeToWebSocket`

## Phase 2 — WebSocket Handshake Utility
- [x] `WebSocketHandshakeHelper.cs` (NEW) — tính Sec-WebSocket-Accept hash, generate random key

## Phase 3 — Core H2 Logic (Http2Helper.cs)
- [x] Settings: parse identifier 0x8 → `EnableConnectProtocol`
- [x] MyHeaderListener: capture `:protocol` pseudo-header
- [x] HEADERS decode: relax CONNECT validation, set `Http2Protocol`
- [x] WebSocket stream detection: route WS streams thành tunnel DATA relay

## Phase 4 — Translation Layer
- [x] `Http2HeaderConverter.cs` — strip/inject WebSocket headers
- [x] `Http2ToHttp1Translator.cs` — detect CONNECT+websocket, emit proper H1 GET Upgrade, relay tunnel
- [x] `Http1ToHttp2Translator.cs` — detect H1 Upgrade:websocket, emit H2 CONNECT, relay tunnel

## Phase 5 — RequestHandler
- [x] Guard H2 WebSocket từ falling vào `HandleWebSocketUpgrade` TCP raw path

## Phase 6 — Verification
- [ ] Build & compile check
- [ ] Integration test stub `Http2WebSocketTests.cs`
