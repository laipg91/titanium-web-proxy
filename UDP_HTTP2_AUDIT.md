# UDP and HTTP/2 Audit

Date: 2026-04-09
Repo: `titanium-web-proxy`
Scope:
- SOCKS5 `UDP ASSOCIATE`
- HTTP/2 proxy flows
- Code-path audit plus runtime/testability check

## Executive Summary

Current status:
- `UDP ASSOCIATE` is not production-ready.
- `HTTP/2` is not production-ready.
- Both features compile on `net6.0`, but neither has working end-to-end evidence in the current repo.

High-level conclusion:
- `UDP ASSOCIATE` contains a real runtime race, a bind/addressing bug that breaks non-local clients, and session timeout state bugs.
- `HTTP/2` contains protocol-level mistakes in all three meaningful scenarios:
  - `H2 client -> H1 server`
  - `H1 client -> H2 server`
  - `H2 <-> H2 relay`

## What Was Checked

Reviewed main code paths:
- `src/Titanium.Web.Proxy/SocksClientHandler.cs`
- `src/Titanium.Web.Proxy/UdpAssociateHandler.cs`
- `src/Titanium.Web.Proxy/Helpers/UdpSocks5Header.cs`
- `src/Titanium.Web.Proxy/Network/Udp/UdpSocketAwaitable.cs`
- `src/Titanium.Web.Proxy/ExplicitClientHandler.cs`
- `src/Titanium.Web.Proxy/RequestHandler.cs`
- `src/Titanium.Web.Proxy/Http2/Http2Helper.cs`
- `src/Titanium.Web.Proxy/Http2/Translation/Http1ToHttp2Translator.cs`
- `src/Titanium.Web.Proxy/Http2/Translation/Http2ToHttp1Translator.cs`
- `src/Titanium.Web.Proxy/Http2/Translation/Http2HeaderConverter.cs`
- `src/Titanium.Web.Proxy/Http2/Primitives/Http2FrameWriter.cs`
- `src/Titanium.Web.Proxy/Http2/Primitives/Http2FrameReader.cs`
- `src/Titanium.Web.Proxy/Models/SocksProxyEndPoint.cs`

Checked tests and runtime verification:
- Searched `tests/` for any `HTTP/2` or `UDP ASSOCIATE` coverage.
- Built library for `net6.0`.
- Ran test assemblies directly with `dotnet vstest`.

## Findings

### 1. Critical: `H2 client -> H1 server` path is broken before translation starts

Files:
- `src/Titanium.Web.Proxy/ExplicitClientHandler.cs:327`
- `src/Titanium.Web.Proxy/ExplicitClientHandler.cs:333`
- `src/Titanium.Web.Proxy/ExplicitClientHandler.cs:352`

Problem:
- The proxy writes the HTTP/2 connection preface to the upstream server before checking whether the server actually negotiated `h2`.
- If the upstream server negotiated HTTP/1.1, the stream is already polluted by `PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n`.
- The later fallback translator (`Http2ToHttp1Translator`) then runs on an already corrupted upstream connection.

Impact:
- Real `H2 client -> H1 server` fallback does not work correctly.
- This is a hard wire-level failure, not just a missing optimization.

### 2. Critical: `H2 <-> H2` relay sends `SETTINGS` and `PING` ACK to the wrong side

Files:
- `src/Titanium.Web.Proxy/Http2/Http2Helper.cs:453`
- `src/Titanium.Web.Proxy/Http2/Http2Helper.cs:505`
- `src/Titanium.Web.Proxy/Http2/Http2Helper.cs:512`
- `src/Titanium.Web.Proxy/Http2/Http2Helper.cs:519`

Problem:
- In `CopyHttp2FrameAsync`, when a `SETTINGS` or `PING` frame is read from `input`, the code sends the ACK to `output`.
- Per HTTP/2, ACK must be sent back to the peer that sent the original control frame.

Impact:
- Any real HTTP/2 peer exchange using these control frames will become protocol-invalid.
- Even if data forwarding seems to continue for a while, the connection is logically broken.

### 3. High: `H1 client -> H2 server` does not send the mandatory client `SETTINGS` frame

Files:
- `src/Titanium.Web.Proxy/RequestHandler.cs:313`
- `src/Titanium.Web.Proxy/RequestHandler.cs:316`
- `src/Titanium.Web.Proxy/Http2/Translation/Http1ToHttp2Translator.cs:58`

Problem:
- The proxy sends only the HTTP/2 client connection preface bytes to the server.
- It does not send the mandatory client `SETTINGS` frame before starting request/response translation.
- The translator waits for server preface, but never establishes the proxy side as a fully valid HTTP/2 client.

Impact:
- Standards-compliant HTTP/2 servers may reject the connection or stall.
- This path cannot be considered reliable on the wire.

### 4. High: `H1 -> H2` request body forwarding is wrong for chunked requests

Files:
- `src/Titanium.Web.Proxy/Http2/Translation/Http1ToHttp2Translator.cs:223`
- `src/Titanium.Web.Proxy/Http2/Translation/Http1ToHttp2Translator.cs:228`
- `src/Titanium.Web.Proxy/Http2/Translation/Http1ToHttp2Translator.cs:238`

Problem:
- The translator reads raw bytes from the HTTP/1.x client stream and forwards them as HTTP/2 DATA.
- For chunked requests, this forwards chunk framing bytes instead of decoded entity bytes.
- It also never marks `END_STREAM` for chunked request bodies.

Impact:
- Uploads or streaming request bodies to H2 upstream can hang or arrive corrupted.
- This affects practical use, not edge behavior.

### 5. High: `UDP ASSOCIATE` binds `0.0.0.0` listeners to loopback for the reply address

Files:
- `src/Titanium.Web.Proxy/UdpAssociateHandler.cs:58`
- `src/Titanium.Web.Proxy/UdpAssociateHandler.cs:60`

Problem:
- If the SOCKS endpoint is listening on `IPAddress.Any` or `IPAddress.IPv6Any`, the code rewrites the UDP relay bind address to `127.0.0.1`.
- That address is then sent back in the SOCKS5 UDP Associate reply.

Impact:
- Remote clients cannot use the UDP relay.
- Only local clients can realistically work in this common configuration.
- This is a functional correctness bug, not just a configuration issue.

### 6. High: `UdpSocketAwaitable` has a completion race that can hang relay loops

Files:
- `src/Titanium.Web.Proxy/Network/Udp/UdpSocketAwaitable.cs:45`
- `src/Titanium.Web.Proxy/Network/Udp/UdpSocketAwaitable.cs:73`
- `src/Titanium.Web.Proxy/Network/Udp/UdpSocketAwaitable.cs:76`
- `src/Titanium.Web.Proxy/Network/Udp/UdpSocketAwaitable.cs:100`
- `src/Titanium.Web.Proxy/Network/Udp/UdpSocketAwaitable.cs:103`

Problem:
- `TaskCompletionSource` is assigned after calling `Socket.ReceiveFromAsync` or `Socket.SendToAsync`.
- If the async completion callback fires before `_recvTcs` or `_sendTcs` is assigned, the completion signal is lost.
- The awaiting code can then wait forever.

Impact:
- UDP relay can intermittently deadlock.
- This is a real concurrency bug and makes the feature unreliable under load or timing-sensitive conditions.

### 7. Medium: UDP idle-timeout logic uses global shared state and ignores endpoint config

Files:
- `src/Titanium.Web.Proxy/UdpAssociateHandler.cs:106`
- `src/Titanium.Web.Proxy/UdpAssociateHandler.cs:238`
- `src/Titanium.Web.Proxy/UdpAssociateHandler.cs:251`
- `src/Titanium.Web.Proxy/UdpAssociateHandler.cs:320`
- `src/Titanium.Web.Proxy/Models/SocksProxyEndPoint.cs:67`

Problem:
- `UdpLastActivityTicks` is `static`, shared across sessions.
- It is updated only on `client -> remote`, not on `remote -> client`.
- `WatchIdleTimeout` hardcodes 3 minutes and does not use `UdpAssociateTimeoutSeconds`.

Impact:
- One active session can keep another session alive.
- Download-only or response-heavy traffic may time out incorrectly.
- Public configuration is misleading because the timeout property is never honored.

### 8. Medium: `H2 -> H1` translator still cannot be trusted for realistic traffic

Files:
- `src/Titanium.Web.Proxy/Http2/Translation/Http2ToHttp1Translator.cs:133`
- `src/Titanium.Web.Proxy/Http2/Translation/Http2ToHttp1Translator.cs:266`
- `src/Titanium.Web.Proxy/Http2/Translation/Http2ToHttp1Translator.cs:358`
- `src/Titanium.Web.Proxy/Http2/Translation/Http2ToHttp1Translator.cs:383`

Problems:
- Client `PING` is ignored by the read loop rather than being ACKed promptly.
- H1 response reading only handles bodies with `Content-Length`.
- Chunked responses and EOF-terminated responses are not handled.

Impact:
- Even after fixing the preface bug in `ExplicitClientHandler`, this scenario still would not be robust enough for real traffic.

## Test and Runtime Evidence

### Build result

Command used:
- `dotnet build src/Titanium.Web.Proxy/Titanium.Web.Proxy.csproj -c Debug -f net6.0`

Result:
- Build succeeded.
- This confirms the `net6.0` HTTP/2 code is actually compiled.

### Coverage search result

Search result summary:
- No test files in `tests/` were found that explicitly exercise `HTTP/2` or `UDP ASSOCIATE`.

Interpretation:
- There is currently no direct automated evidence in the repo that these features work end-to-end.

### Integration tests

Command used:
- `dotnet vstest tests/Titanium.Web.Proxy.IntegrationTests/bin/Debug/net60/Titanium.Web.Proxy.IntegrationTests.dll`

Observed result:
- Integration suite failed before useful proxy feature validation.
- All visible failures were blocked by test HTTPS server setup receiving a null certificate.

Relevant files:
- `tests/Titanium.Web.Proxy.IntegrationTests/Setup/TestSuite.cs:14`
- `tests/Titanium.Web.Proxy.IntegrationTests/Setup/TestServer.cs:56`

What this means:
- The current integration environment does not provide runtime evidence that proxy HTTPS paths are working cleanly.
- It also means there is no usable integration safety net for validating HTTP/2 changes right now.

### Unit tests

Command used:
- `dotnet vstest tests/Titanium.Web.Proxy.UnitTests/bin/Debug/net461/Titanium.Web.Proxy.UnitTests.dll`

Observed result:
- Some tests passed, but failures exist in certificate generation and system-proxy related areas.
- This does not directly validate or invalidate `UDP ASSOCIATE` or `HTTP/2`, but it confirms the test baseline is not clean.

## Practical Verdict

### UDP ASSOCIATE

Verdict:
- Not safe to claim as working in general.

Reason:
- It may work for local-only use in a narrow setup.
- It is not trustworthy for remote clients.
- It contains a genuine relay hang race.
- Timeout/session state handling is also flawed.

### HTTP/2

Verdict:
- Not safe to claim as working.

Scenario breakdown:
- `H2 client -> H1 server`: broken.
- `H1 client -> H2 server`: incomplete protocol implementation.
- `H2 <-> H2 relay`: ACK logic is wrong at protocol level.

## Recommended Fix Order

### UDP

1. Fix `UdpSocketAwaitable` completion race.
2. Stop rewriting bind reply address to loopback when listening on `Any`.
3. Move idle timestamp from static global state to per-session state.
4. Update last-activity on both directions.
5. Honor `UdpAssociateTimeoutSeconds`.
6. Add integration tests with:
   - local client
   - non-local bind/reply behavior
   - concurrent sessions
   - timeout behavior

### HTTP/2

1. Fix `Http2Helper` ACK direction for `SETTINGS` and `PING`.
2. In `ExplicitClientHandler`, only send H2 preface upstream after confirming server negotiated `h2`.
3. In `H1 -> H2`, send the required client `SETTINGS` frame.
4. Rework request-body handling to correctly decode HTTP/1 chunked bodies before emitting H2 DATA.
5. Rework `H2 -> H1` response reading to support chunked and EOF-terminated H1 responses.
6. Add dedicated integration tests for:
   - `H1 client -> H2 server`
   - `H2 client -> H1 server`
   - `H2 client -> H2 server`
   - body upload/download
   - `PING` / `SETTINGS` behavior

## Final Conclusion

The codebase contains substantial implementation work for both features, but neither feature currently has enough correctness, protocol compliance, or test evidence to be considered truly working.

Short version:
- `UDP ASSOCIATE`: partially implemented, but still broken in important real deployments.
- `HTTP/2`: implemented in several branches, but not currently reliable on real traffic.
