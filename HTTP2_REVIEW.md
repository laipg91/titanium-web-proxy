# Báo cáo đánh giá: HTTP/2 Implementation

**Ngày review:** 11/03/2026
**Mục tiêu:** Đánh giá độ tin cậy và khả năng triển khai lên môi trường High-Load Production của bản build HTTP/2 hiện tại (sau những đợt fix trước đây).
**Trạng thái chung:** HTTP/2 (enable qua `#if NETSTANDARD2_1`) đã hoạt động ở mức cơ bản nhưng vẫn mang tính chất "thử nghiệm" (experimental). Nó chưa đủ vững vàng để chạy Production quy mô lớn nếu không xử lý triệt để các hạn chế kiến trúc.

---

## 1. Workflow hiện tại của HTTP/2 Proxy
- Khi có kết nối TLS, lớp Ssl nhận diện client ALPN là `h2`.
- Proxy nhảy vào luồng giả lập HTTP/2: `CopyHttp2FrameAsync` làm nhiệm vụ đọc liên tục (while-loop) các frame HTTP/2 (DATA, HEADERS, SETTINGS, PING...) theo chuẩn RFC 7540.
- Nếu là Frame HEADERS → decode = HPACK `Decoder`, tách header ra map cho `Request/Response` object rồi gọi qua event `OnBeforeRequest/OnBeforeResponse` như HTTP/1.x, sau đó encode = HPACK `Encoder` để forward tiếp đi.
- Hỗ trợ giải nén body inline của DATA frames.
- Sử dụng `ConcurrentDictionary` để map `StreamId` với một `SessionEventArgs` duy nhất trên mỗi kết nối, cho phép Client-Proxy-Server Multiplexing thực thụ.

---

## 2. Những vấn đề tồn đọng & Rủi ro trên Production

Mặc dù đợt trước chúng ta đã vá một số lỗi chí mạng (như Stream ID = 0, PING ACK, Settings Padding, Continuation Buffer), phần HTTP/2 vẫn tiềm ẩn 5 nhóm rủi ro lớn sau:

### 🔴 2.1. Không quản lý Flow Control (WINDOW_UPDATE) cho toàn tuyến
- **Mô tả:** HTTP/2 có khái niệm Cửa sổ luồng (Flow Control Window). Hai bên (Client, Server) dùng frame `WINDOW_UPDATE` để báo cho nhau "tôi còn trống X bytes RAM để nhận tiếp".
- **Rủi ro Production:** Hiện tại proxy forward bừa bãi `WINDOW_UPDATE` giữa Client và Server mà không có state local control cho chính nó. Nếu Proxy bị nghẽn CPU hoặc nghẽn băng thông ra ngoài, Server vẫn cứ xả frame DATA tới tấp làm Proxy "nổ tung" RAM (OOM - Out of Memory exception).
- **Cách fix:** Viết một bộ đếm `LocalWindowSize` cho connection và per-stream, chủ động gửi WINDOW_UPDATE cho cả Client và Server để "phanh" lưu lượng nếu proxy đọc chậm.

### 🔴 2.2. HPACK Encoder cắn bộ nhớ rác liên tục (GC Pressure)
- **Mô tả:** Compression Table của HTTP/2 HPACK phải là loại **Connection-Stateful** (giữ nguyên bảng cho cả ngàn streams của 1 user). Mặc dù Decoder đã được làm Stateful an toàn, nhưng **Encoder** của proxy (khi gửi Headers đi) cứ mỗi lần gọi `SendHeader()` đều khởi tạo một instance `new Encoder()` mới toanh.
- **Rủi ro Production:**
  - Vừa làm mất công dụng nén của HTTP/2 (Header size phình to trở lại do không có Dynamic Table lookup).
  - Vừa xả rác cho Garbage Collector đọn dẹp liên tục với mỗi request.
- **Cách fix:** Khởi tạo một `hpackEncoder` duy nhất bám theo `TcpConnection` (giống cách `hpackDecoder` đang làm).

### 🟠 2.3. Thiếu cơ chế đẩy song song Server Push (PUSH_PROMISE)
- **Mô tả:** Frame `PUSH_PROMISE` bị bỏ qua thẳng tay (hoặc bị block) nếu có xử lý. Proxy không hiểu khái niệm "Server muốn đẩy thêm file CSS/JS kèm theo HTML".
- **Rủi ro Production:** Nếu Web Server mục tiêu (ngân hàng, báo chí) cố tình xài `PUSH_PROMISE` rất nhiều, client sẽ bị mất kết nối những asset tĩnh quan trọng đó, làm vỡ giao diện UI của website khi đi qua proxy. Rất may hiện nay các trình duyệt Chrome/Firefox đang dần deprecate (khai tử) ServerPush, nhưng Proxy vẫn nên forward frame này chuẩn xác.

### 🟠 2.4. HTTP/1.x ↔ HTTP/2 Protocol Translation bị hổng
- **Mô tả:** Nếu Client xài HTTP/2 nhưng Server xài HTTP/1.1 (hoặc ngược lại). Logic của Titanium hiện tại chưa cover mượt mà việc gỡ rã (demux) HTTP/2 streams ép thành từng dòng text HTTP/1.1 và ngược lại đóng gói text HTTP/1.1 thành HTTP/2 Headers + Data frames.
- **Rủi ro Production:** Thường dẫn tới lỗi treo phiên (hung sessions) do mất dấu `EndStream` hoặc HTTP/1 `Transfer-Encoding: chunked` đánh nhau với HTTP/2 DATA format.
- **Cách khắc phục:** Cần lớp `Http2ToHttp1Handler` độc lập làm Adapter riêng thay vì lồng ghép dính vào `TcpConnection`.

### 🟡 2.5. Hạn chế bởi TARGET Framework `#if NETSTANDARD2_1`
- **Mô tả:** Toàn bộ code HTTP/2 trong project chỉ active khi build với `NETSTANDARD2_1` (và được inherit sang `NET6.0`). Còn nếu Proxy chạy trên môi trường .NET Framework truyền thống (`net461`), HTTP/2 coi như biến mất hoàn toàn.
- **Cách khắc phục:** .NET 6 là lý tưởng, vừa có struct/memory/span hiệu năng cao, vừa support ALPN qua SslStream. Phải ép client xài .NET 6/8 trở lên nếu muốn HTTP/2 Proxy ổn định.

---

## 3. Đánh giá sự tuân thủ chuẩn Kiến trúc (Architecture Guidelines)

So với mã nguồn gốc của Core Proxy, việc thiết kế HTTP/2 bộc lộ vài nhược điểm do áp dụng tư duy "Feature-Driven" (Chạy đạt tính năng) thay vì "Framework-Driven" (Xây dựng thư viện chuẩn):

### 🟢 Những điểm tuân thủ tốt (Core Standard)
- **Buffer Pooling:** Khắc phục được lỗi cấp phát `new byte[]` bừa bãi và dùng đúng hệ thống `BufferPool` cho việc đọc các frame data.
- **Cancellation Token & Async:** Các vòng lặp while-loop chặn bắt Frame của HTTP/2 đều nhạy bén với Cancellation Token, không bị hanging thread.

### 🔴 Lệch chuẩn (Cần refactor)
1. **Thiết kế khối nguyên khối (Monolithic Pattern):** Toàn bộ logic giải mã, định hướng của HTTP/2 bị nhồi chung vào một hàm khổng lồ `CopyHttp2FrameAsync` trong `Http2Helper.cs`. Tác giả gốc phân lớp HTTP/1.x rất gọn gàng (`HeaderParser`, `HttpHelper`). Code HTTP/2 hiện tại khó mở rộng, cần tuân thủ coding rule và phong cách viết giống core HTTP/1.
2. **Vi phạm tiêu chuẩn State Caching (Zero-Allocation):** GC Pressure là rủi ro bị đe dọa nhất ở Titanium. Việc đối tượng thư viện `Encoder` của HTTP/2 (HPACK) lại bị khởi tạo "mới toanh" cho MỖI request/response đi ra là một sự lãng phí trầm trọng (phá vỡ luôn tính năng giữ Size Context của HPACK) và ngược dòng với kỹ thuật Stateful connection của Core Proxy.

---
**Kết luận:** 
UDP thiết kế theo kiểu pass-through (nông), nên chỉ cần fix các nút thắt CPU. Nhưng HTTP/2 thì Proxy vừa phải **"decode toàn bộ"** vừa phải **"encode toàn bộ"** (làm Man-in-the-Middle thực sự của HPACK và Frames). Nên hiện tại nó chỉ chạy tạm ổn cho Test/Dev. Để đem lên Production, việc fix HPACK Encoder và Flow Control (Window Update) là bắt buộc.
