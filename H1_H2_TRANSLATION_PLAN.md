# H1 <=> H2 Protocol Translation Audit

Sau khi audit lại toàn bộ luồng H1 <=> H2 (đặc biệt là Scenario A: H1 Client → H2 Server), tôi đã phát hiện **2 lỗi nghiêm trọng (Critical Bugs)** do "miss calls" và thiết kế sai lệch so với vòng đời Request/Response của Titanium Web Proxy:

## 🔴 1. Bug: Mất hoàn toàn Request đầu tiên (H1 Client → H2 Server)
Trong `RequestHandler.cs`, hàm `HandleClient()` **đã đọc và parse xong** toàn bộ Request Line và Headers của request đầu tiên (vào `args.HttpClient.Request`), thực thi cả sự kiện `OnBeforeRequest(args)`.
Tuy nhiên, khi gọi sang `Http1ToHttp2Translator.TranslateAsync`:
- Nó truyền vào một `sessionFactory` để tạo ra một `SessionEventArgs` **trắng tinh mới tinh**.
- Translator bắt đầu bằng vòng lặp `while (true)`: đâm đầu vào gọi `ReadLineAsync(clientStream)` để đọc Request mới.
- **Hậu quả:** Nó bỏ qua/đánh rơi hoàn toàn cái Request đã được parse từ trước. Nếu Request có Body, `ReadLineAsync` sẽ đọc nhầm Body thành Header. Nếu Request không có Body, nó sẽ `treo (hang)` vĩnh viễn ở `clientStream` để chờ Keep-Alive request tiếp theo!

*Cách sửa (Plan)*: Đổi tham số của `IHttp2Translator.TranslateAsync` để nhận trực tiếp `SessionEventArgs initialSession` (Request đầu tiên đã được parse). Translator sẽ xử lý thẳng `initialSession`, sau khi xong xuôi 1 chu kỳ Response (reply lại cho H1 client) thì mới vòng lại loop để parse tiếp các request Keep-Alive (sử dụng HeaderParser của Proxy thay vì tự parse tay).

## 🔴 2. Bug: Thiếu thông tin phân giải Request.RequestUri cho Keep-Alive Requests
Khi Translator xử lý các request Keep-Alive tiếp theo, nó tự viết logic parse `ParseRequestLine` và `ReadH1HeadersAsync` rất sơ sài.
- Nó trích xuất được `uri` từ dòng Request line nhưng... **bỏ quên không gán** vào `request.RequestUriString8`.
- Cấu trúc Header bị thiếu `Host` mapping.
- **Hậu quả:** Khi gọi `Http2HeaderConverter.ToHttp2RequestHeaders(request)` để chuyển sang H2 pseudo-headers (`:authority`, `:path`), nó sẽ ném ra `NullReferenceException` do `request.RequestUri` bị rỗng.

*Cách sửa (Plan)*: 
- Xoá bỏ đoạn parse tay sơ sài (`ParseRequestLine`, `ReadH1HeadersAsync`).
- Tái sử dụng helper chuẩn của Proxy: `HeaderParser.ReadHeaders(clientStream, ...)` để parse chính xác và đầy đủ các object `RequestUri`.

## 🟡 3. Nhận xét về Initial SETTINGS (Đã Fix trước đó)
Theo RFC 7540, proxy khi đóng vai trò Client (nối với server H2) thì Connection Preface bao gồm chuỗi `PRI * HTTP/2.0...` **VÀ** phải kèm theo 1 frame `SETTINGS` ban đầu.
- Ở bước trước tôi đã thêm gọi `SendSettingsAsync` trước vòng lặp của translator. Việc này đã đúng chuẩn protocol.

---

## 🛠️ Proposed Changes (Kế hoạch sửa chữa Code)

### 1. `IHttp2Translator.cs`
- Sửa hàm `TranslateAsync`: Thêm tham số `SessionEventArgs initialSession`, loại bỏ `Func<SessionEventArgs, Task> onBeforeRequest` (Sự kiện đã chạy xong từ RequestHandler hoặc sẽ chạy trong HeaderParser nếu cần).

### 2. `RequestHandler.cs` / `ExplicitClientHandler.cs`
- Khi gọi `translator.TranslateAsync`, **không pass sessionFactory mới** nữa, mà pass thẳng biến `args` hiện tại làm `initialSession`.

### 3. `Http1ToHttp2Translator.cs` (Kịch bản Client H1 → Server H2)
- Cập nhật vòng lặp lại thành:
  - Nếu có `initialSession`, trích xuất `request` từ đó và không đọc `clientStream`.
  - Nếu `initialSession` bằng null (nghĩa là đang ở chu kỳ Keep-Alive tiếp theo), sử dụng `HeaderParser.ReadHeaders(clientStream, request.Headers...)` để parse Request header kế tiếp giống hệt như RequestHandler.

### 4. `Http2ToHttp1Translator.cs` (Kịch bản Client H2 → Server H1)
- Vẫn duy trì loop hiện tại chạy qua `reader.ReadAllAsync()`.
- Chữ ký interface bị đổi, nên bên H2 Client phải thay thế `initialSession` (có thể bằng null vì Client gửi H2 PREFACE ngay từ đầu và không có parsed Request chờ sẵn).
