# Code Review: Titanium Web Proxy (C#)

## Tổng quan dự án

**Titanium Web Proxy** là một thư viện proxy server C# hỗ trợ:
- HTTP/HTTPS explicit proxy (với SSL decryption via certificate spoofing)
- Transparent proxy
- SOCKS proxy (SOCKS4/SOCKS5)
- WebSocket tunneling
- HTTP/2 (giới hạn)
- Windows Authentication (NTLM/Kerberos)

Codebase sử dụng pattern `partial class` để tách `ProxyServer` thành nhiều file theo chức năng: `ProxyServer.cs`, `RequestHandler.cs`, `ResponseHandler.cs`, `ExplicitClientHandler.cs`, v.v.

---

## ✅ Điểm mạnh

### 1. Kiến trúc rõ ràng, event-driven API
Code sử dụng event handler cho phép người dùng intercept request/response một cách linh hoạt:

```csharp
public event AsyncEventHandler<SessionEventArgs>? BeforeRequest;
public event AsyncEventHandler<SessionEventArgs>? BeforeResponse;
public event AsyncEventHandler<SessionEventArgs>? AfterResponse;
```

> [!TIP]
> Pattern này cho phép người dùng modify/inspect traffic mà không cần subclass, rất convenient và flexible.

### 2. Connection Pooling được thiết kế tốt
`TcpConnectionFactory` dùng `ConcurrentDictionary<string, ConcurrentQueue<TcpServerConnection>>` kết hợp với một cleanup task chạy nền mỗi 3 giây, tránh memory leak cho idle connections.

### 3. Connection Prefetching thông minh
Khi nhận CONNECT request, proxy lập tức khởi động kết nối tới server (prefetch) song song với việc parse request — giảm latency đáng kể.

```csharp
// don't pass cancellation token here — it could cause floating server connections when client exits
prefetchConnectionTask = TcpConnectionFactory.GetServerConnection(..., CancellationToken.None);
```

### 4. Retry Policy generic và tái sử dụng được
`RetryPolicy<T>` cho phép retry với bất kỳ exception type cụ thể, tránh nuốt các exception không liên quan.

### 5. Xử lý IDisposable đúng chuẩn
`ProxyServer`, `TcpConnectionFactory` đều implement dispose pattern đầy đủ với finalizer và `GC.SuppressFinalize`.

### 6. TimeLine tracing built-in
`args.TimeLine` lưu timestamp từng bước (DNS, Connection, Request Sent, Response Received), rất hữu ích để debug/profiling.

---

## ⚠️ Vấn đề tiềm ẩn

### 1. SSL Protocol mặc định lỗi thời (Nghiêm trọng)

**File:** [`ProxyServer.cs:249`](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/ProxyServer.cs)

```csharp
#pragma warning disable 618
public SslProtocols SupportedSslProtocols { get; set; } =
    SslProtocols.Ssl3 | SslProtocols.Tls | SslProtocols.Tls11 | SslProtocols.Tls12;
#pragma warning restore 618
```

> [!CAUTION]
> **SSLv3 và TLS 1.0/1.1 đã bị deprecated và có lỗ hổng bảo mật nghiêm trọng** (POODLE, BEAST). Default nên là `TlsProtocols.Tls12 | TlsProtocols.Tls13`. Việc dùng `#pragma warning disable 618` để tắt cảnh báo là dấu hiệu rõ ràng rằng vấn đề này đã biết nhưng chưa được xử lý.

### 2. Sử dụng `goto` trong production code

**File:** [`TcpConnectionFactory.cs:615, 630`](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/Network/TcpConnection/TcpConnectionFactory.cs)

```csharp
retry:
try { ... }
catch (IOException ex) when (...)
{
    retry = false;
    goto retry;  // 🚨
}
```

> [!WARNING]
> `goto` trong C# làm code khó đọc và dễ gây nhầm lẫn. Đây là trường hợp retry khi SSL negotiation thất bại để thử lại với protocol cũ hơn. Có thể refactor thành vòng lặp `while(retry)` hoặc tách thành method riêng.

### 3. Lock không nhất quán (Race condition tiềm ẩn)

**File:** [`TcpConnectionFactory.cs`](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/Network/TcpConnection/TcpConnectionFactory.cs)

Khi **đọc** từ cache (GetServerConnection), code dùng `lock(existingConnections)`:
```csharp
lock (existingConnections)
{
    while (existingConnections.Count > 0)
        if (existingConnections.TryDequeue(...)) { ... }
}
```

Nhưng khi **ghi** vào cache (Release), code dùng `SemaphoreSlim @lock`:
```csharp
await @lock.WaitAsync();
// ... enqueue connection
@lock.Release();
```

> [!CAUTION]
> Hai cơ chế locking khác nhau cho cùng một data structure. `ConcurrentQueue` về mặt kỹ thuật là thread-safe cho từng operation riêng lẻ, nhưng kết hợp check-then-act (Count > 0 rồi mới Dequeue) trong một `lock` trong khi nơi khác dùng semaphore có thể gây race condition.

### 4. Blocking call trong async code

**File:** [`TcpConnectionFactory.cs:775`](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/Network/TcpConnection/TcpConnectionFactory.cs)

```csharp
protected virtual void Dispose(bool disposing)
{
    // ...
    @lock.Wait(); // ⚠️ Blocking call của async SemaphoreSlim
}
```

> [!WARNING]
> Gọi `.Wait()` thay vì `await WaitAsync()` trong Dispose có thể gây deadlock trong một số context async. Tuy nhiên đây là vấn đề khó tránh trong Dispose vì Dispose không nên là async.

### 5. Connection pool dequeue không kiểm tra lại sau đóng

**File:** [`TcpConnectionFactory.cs:282-296`](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/Network/TcpConnection/TcpConnectionFactory.cs)

```csharp
if (recentConnection.LastAccess > cutOff && recentConnection.TcpSocket.IsGoodConnection())
    return recentConnection; // ← Trả về ngay trong lock
```

Sau khi lấy connection từ pool, có khoảng thời gian ngắn trước khi thực sự dùng nó, connection có thể đã bị server đóng. May mắn là có `RetryPolicy` để handle trường hợp này.

### 6. Typos trong error messages

**File:** `ProxyServer.cs` và các file khác

```csharp
// Line 571: "confugure" → "configure"
throw new NotSupportedException(
    "...Please manually confugure you operating system...");

// Line 363: "occured" → "occurred"
OnException(clientStream, new Exception("Error occured in whilst handling the client", e));

// RequestHandler.cs Line 220: "occured" → "occurred"
throw new ProxyHttpException("Error occured whilst handling session request", e, args);
```

### 7. Thiếu TLS 1.3 support

Tìm kiếm trong codebase không thấy `Tls13` được đề cập trong default SSL protocols, trong khi TLS 1.3 đã được hỗ trợ từ .NET 5+.

### 8. TODO comments chưa được giải quyết

**File:** `ExplicitClientHandler.cs:125`
```csharp
connectRequest.IsHttps = true; // todo: move this line to the previous "if"
```

**File:** `TcpConnectionFactory.cs:447, 453`
```csharp
// todo: resolve only once when the SOCKS proxy has multiple addresses
// todo: use the 2nd, 3rd... remote addresses when first fails
```

> [!NOTE]
> Có ít nhất 5 `TODO` comments trong codebase, một số ảnh hưởng đến correctness (SOCKS proxy failover).

### 9. Xử lý exception quá rộng trong `OnAcceptConnection`

**File:** [`ProxyServer.cs:731-734`](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/ProxyServer.cs)

```csharp
catch
{
    // Other errors are discarded to keep proxy running
}
```

> [!WARNING]
> Nuốt tất cả exception (kể cả `OutOfMemoryException`, `StackOverflowException`) mà không log hoặc invoke `ExceptionFunc`. Có thể gây khó debug khi có lỗi thực sự.

---

## 📊 Phân tích theo module

| Module | Chất lượng | Ghi chú |
|--------|-----------|---------|
| `ProxyServer.cs` | ⭐⭐⭐⭐ | Tốt, API rõ ràng, có vài typo nhỏ |
| `RequestHandler.cs` | ⭐⭐⭐⭐ | Logic rõ, xử lý keep-alive tốt |
| `ResponseHandler.cs` | ⭐⭐⭐⭐⭐ | Ngắn gọn, rõ ràng, body streaming tốt |
| `ExplicitClientHandler.cs` | ⭐⭐⭐⭐ | SSL intercept logic phức tạp nhưng được comment tốt |
| `TcpConnectionFactory.cs` | ⭐⭐⭐ | Phức tạp, có race condition tiềm ẩn, dùng `goto` |
| `RetryPolicy.cs` | ⭐⭐⭐⭐⭐ | Generic, clean, tốt |

---

## 💡 Gợi ý cải thiện

1. **Nâng cấp default SSL**: Đổi default thành `TlsProtocols.Tls12 | TlsProtocols.Tls13`
2. **Refactor `goto`**: Thay bằng `while(retry)` loop hoặc recursive method
3. **Thêm structured logging**: Tích hợp `ILogger` (Microsoft.Extensions.Logging) thay vì chỉ dùng `ExceptionFunc`
4. **Fix typos**: `confugure` → `configure`, `occured` → `occurred`
5. **Bổ sung testing**: Xem thêm thư mục `tests/` để đánh giá coverage
6. **Documentation HTTP/2**: Comment rõ hơn các giới hạn của HTTP/2 support
