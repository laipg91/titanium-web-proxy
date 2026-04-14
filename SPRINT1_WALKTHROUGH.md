# Walkthrough — Sprint 1: Critical Fixes Completed

**Ngày thực hiện:** 13/03/2026  
**Build kết quả:** ✅ `dotnet build` — 0 errors (net461 + net6+)

---

## Những gì đã làm

### Fix 1 — HPACK Encoder Stateful ✅
**File:** [Http2Helper.cs](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/Http2/Http2Helper.cs)

**Vấn đề gốc:** `SendHeader()` gọi `new Encoder(headerTableSize)` **mỗi khi** `HeaderTableSize` thay đổi — kể cả khi tăng — khiến HPACK dynamic table bị reset liên tục, vô hiệu hóa tính năng nén header và gây GC pressure.

**Fix:** Chỉ `new Encoder()` khi:
- a) Lần đầu (`encoder == null`), hoặc
- b) HeaderTableSize **giảm** (RFC 7541 §4.3 yêu cầu discard dynamic table khi shrink)

Khi size tăng: giữ encoder cũ, chỉ update `encoderState.HeaderTableSize`.

---

### Fix 2 — Flow Control (WINDOW_UPDATE Backpressure) ✅
**File:** [Http2Helper.cs](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/Http2/Http2Helper.cs)

**Vấn đề gốc:** Proxy forward `WINDOW_UPDATE` thẳng giữa client/server mà không có local window tracking. Server xả DATA tới tấp làm RAM proxy OOM.

**Fix:** Thêm:
- `localConnConsumed` — đếm bytes đã consumed từ source ở connection level
- `localStreamConsumed[streamId]` — per-stream consumed bytes
- `SendWindowUpdate(sourceStream, ...)` — gửi `WINDOW_UPDATE` ngược lại source sau khi đã write DATA ra output (mỗi khi >= 32KB = `WindowUpdateThreshold`)

Proxy "phanh" source lại theo nhịp nó có thể xử lý, thay vì để source xả tự do.

---

### Fix 3 — Async DNS với Cache ✅
**File:** [UdpAssociateHandler.cs](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/UdpAssociateHandler.cs)

**Vấn đề gốc:** `UdpSocks5Header.TryParse` gọi `Dns.GetHostAddresses(host)` — **blocking I/O** trong async hot-path, gây Thread Pool Starvation.

**Fix:**
1. `TryParse` refactored thêm `out string domainName` — trả về hostname thay vì resolve ngay
2. `LoopClientToRemote` kiểm tra `domainName != null` → `await Dns.GetHostAddressesAsync(domainName)`
3. DNS Cache (`ConcurrentDictionary<string, DnsCacheEntry>`) TTL 5 phút — tránh lookup lặp lại cùng domain
4. `DnsCacheEntry` là class riêng (không dùng C# 7 ValueTuple) → **tương thích net461**

---

### Fix 4 — UDP Idle Timeout ✅
**File:** [UdpAssociateHandler.cs](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/UdpAssociateHandler.cs)

**Vấn đề gốc:** Vòng đời UDP relay phụ thuộc hoàn toàn vào TCP. TCP half-open có thể giữ relay sống ~2 giờ trước khi OS detect ra.

**Fix:**
- `WatchIdleTimeout(TimeSpan.FromMinutes(3), relayCts)` — chạy song song trong `Task.WhenAny`
- `UdpLastActivityTicks` (static long) — update bằng `Interlocked.Exchange` mỗi khi `LoopClientToRemote` forward packet thành công
- Check mỗi 30 giây: nếu `idleFor >= 3 phút` → `relayCts.Cancel()` + log debug

---

## Validation

```
dotnet build src\Titanium.Web.Proxy\Titanium.Web.Proxy.csproj -c Debug
→ Build succeeded. 0 Error(s)
```

---

## Việc còn lại (Sprint 2 & 3)

| # | Fix | Mức độ |
|---|-----|--------|
| 5 | SSRF filter — Blacklist private IP cho UDP | ✅ Done |
| 6 | Zero-Alloc UDP — Nâng `UdpSocketHelper` lên SAEA | ✅ Done |
| 7 | HTTP/1.x ↔ HTTP/2 Protocol Translation Adapter | 🟠 Important |
| 8 | Phân rã `Http2Helper.cs` monolithic | 📐 Refactor |
| 9 | UDP Socket Multiplexing | 📐 Refactor |
| 10 | UDP Hijacking fix | 📐 Refactor |
| 11 | PUSH_PROMISE support | 📐 Low priority |

> Xem checklist đầy đủ tại [UPGRADE_CHECKLIST.md](./UPGRADE_CHECKLIST.md)
