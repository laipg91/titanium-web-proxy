# Task: Fix Critical Bugs - HTTP/2 & UDP Associate

## 🔴 Critical Fixes (Sprint 1 - ✅ DONE)

### HTTP/2
- [x] **Fix 1: HPACK Encoder Stateful** — Chỉ `new Encoder()` khi size giảm, không new mỗi lần thay đổi
- [x] **Fix 2: Flow Control WINDOW_UPDATE** — Thêm proxy-local `localConnConsumed`/`localStreamConsumed` tracking + `SendWindowUpdate()` backpressure

### UDP Associate
- [x] **Fix 3: Async DNS** — Tách `TryParse` trả về `domainName`, `LoopClientToRemote` dùng `await GetHostAddressesAsync` + `DnsCacheEntry` cache 5 phút
- [x] **Fix 4: UDP Idle Timeout** — Thêm `WatchIdleTimeout(3 phút)` + `UdpLastActivityTicks` vào `HandleUdpAssociate`

> **Build:** ✅ `dotnet build` 0 errors (net461 + net6+)

> **Sprint 2 Build:** ✅ `dotnet build` 0 errors — Fix 7 + Refactor 8 Primitives complete

---

## 🟠 Important Fixes (Sprint 2)

- [x] **Fix 5: SSRF filter** — Blacklist private IP range khi forward UDP
- [x] **Fix 6: Zero-Alloc UDP** — Nâng `UdpSocketHelper` lên `ReceiveFromAsync(Memory<byte>)` / SAEA
- [x] **Fix 7: HTTP/1.x ↔ HTTP/2 Protocol Translation** — Adapter riêng (`Http1ToHttp2Translator`, `Http2ToHttp1Translator`, `Http2HeaderConverter`, `IHttp2Translator`)

---

## 📐 Long-term Refactor (Sprint 3 - Chưa làm)

- [x] **Refactor 8: Phân rã Monolithic `Http2Helper.cs`** → `Http2FrameReader`, `Http2FrameWriter` (Primitives layer — done as part of Fix 7)
- [ ] **Refactor 9: UDP Socket Multiplexing** — Ghép nhiều session chia sẻ ít socket hơn
- [ ] **Refactor 10: UDP Hijacking fix** — Kiểm tra Source IP/Port chặt hơn
- [ ] **Refactor 11: PUSH_PROMISE support** (Low priority — Chrome deprecated)
