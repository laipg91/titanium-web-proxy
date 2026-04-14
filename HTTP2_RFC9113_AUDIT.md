# Báo cáo Audit: HTTP/2 Proxy Code vs RFC 9113

Thực tế là **RFC 9113** (được phát hành vào năm 2022) đã thay thế và obsolete **RFC 7540**. Tuy nhiên, RFC 9113 không thay đổi quá nhiều về mặt giao thức (wire format) mà chủ yếu là tổ chức lại, phân tách rạch ròi ngữ nghĩa (semantics) sang RFC 9110, loại bỏ một số tính năng ít được sử dụng và làm rõ các tài liệu.

Sau khi review toàn bộ module `Http2` (`Http1ToHttp2Translator`, `Http2HeaderConverter`, `Http2Helper`, `Http2FrameWriter`, v.v.), dưới đây là các điểm tương thích và một số điểm **còn thiếu sót (mismatch)** so với chuẩn RFC 9113.

## ✅ Những gì đã làm ĐÚNG theo RFC 9113

1. **Loại bỏ `h2c` (Cleartext HTTP/2) - (RFC 9113 Section 3.1):**
    RFC 9113 chính thức loại bỏ cơ chế Upgrade token `h2c`. Trong `RequestHandler.cs` của bạn, kết nối HTTP/2 được chặn rõ ràng: `EnableHttp2 && args.IsHttps` — tức là bạn chỉ cấp phát HTTP/2 qua kết nối TLS (h2). Điều này hoàn toàn tuân thủ RFC mới.
2. **Loại bỏ Connection-Specific Headers - (RFC 9113 Section 8.2.1):**
    RFC 9113 quy định nghiêm ngặt rằng proxy trung gian KHÔNG ĐƯỢC phép forward các header như `Connection`, `Keep-Alive`, `Transfer-Encoding`, `Upgrade`. 
    Trong `Http2HeaderConverter.cs`, bộ lọc `CommonForbiddenHeaders` của bạn đã làm rất tốt việc này (xóa các header trước khi mã hóa HPACK).
3. **Chuyển đổi Header sang Lowercase - (RFC 9113 Section 8.2.1):**
    Tên Header (Header field names) phải luôn luôn là chữ thường (lowercase). Nếu bạn để chữ hoa, connection có thể bị đối tác gửi `PROTOCOL_ERROR`. Thuộc tính `ToLowerInvariant()` của bạn trong bộ Converter đã làm đúng.
4. **Xử lý `TE` Header - (RFC 9113 Section 8.2.1):**
    Thuộc tính `TE` không được chứa bất kỳ giá trị nào ngoại trừ `trailers`. Hàm `TryNormalizeTeForHttp2` đã xử lý chuẩn xác.
5. **Deprecation luồng Priority - (RFC 9113 Section 5.3):**
    Tính năng Priority Frame (ví dụ cờ gài trong HEADERS hoặc frame PRIORITY) bị **deprecated** trong RFC 9113. Trong code `Http2Helper` và `Http2FrameWriter`, bạn có cấu trúc phân tích dữ liệu 5-byte priority nhưng sau đó dường như nó không chi phối tài nguyên (không có logic scheduling phức tạp). Điều này lại rẽ vào việc vô tình... "chuẩn" theo RFC 9113 vì bạn được khuyên là bỏ qua semantic của chúng.

---

## ❌ Những điểm Mismatch (Trái/Thiếu so với RFC 9113)

### 1. Thiếu sót về Trailers (Trailing Headers) (RFC 9113 Section 8.1)
RFC 9113 tái khẳng định HTTP/2 hỗ trợ đầy đủ các **Trailers** (header được gửi sau Request Body chunk). 
- **Lỗi code của bạn hiện tại:** Khi client HTTP/1.1 gửi request dạng Chunked với Trailer, trong `Http1ToHttp2Translator.cs`, phương thức `ConsumeChunkTrailersAsync()` của bạn chỉ.. đọc bỏ qua (discard line) rồi lập tức gửi 1 DATA frame rỗng với cờ `EndStream = true`. Điều này làm mất dữ liệu ngữ nghĩa Trailers.
- **Để chuẩn hóa:** Nó phải được gửi thành một `HEADERS frame` sau body DATA array (frame HEADERS này sẽ kèm theo cờ `EndStream`).

### 2. Sự Validation lỏng lẻo đối với pseudo-headers (RFC 9113 Section 8.3 & 8.3.1)
RFC 9113 yêu cầu tất cả các pseudo-header (bắt đầu bằng `:`) phải được thiết lập trật tự trước (trước các header thông thường khác).
- **Lỗi code:** Mặc dù `ToHttp2RequestHeaders` đã đẩy 4 headers `:method`, `:authority`, `:scheme`, `:path` lên trước, nhưng code thiếu xác thực (validation) chiều từ Server về. Nếu một HTTP/2 Server độc hại giả mạo việc trả về pseudo-header ở giữa các header bình thường, `Http2Helper.cs` (MyHeaderListener) sẽ chấp nhận mặc dù đáng lẽ phải bị báo `PROTOCOL_ERROR`.
- **Validation `:authority` vs `Host`:** RFC 9113 cảnh báo request bị coi là malformed nếu bạn truyền qua một `Host` header có giá trị khác với `:authority`. Hiện tại `Http2HeaderConverter` xóa `Host` header và tự tạo `:authority` vì `Host` được thêm vào danh sách Cấm (`RequestForbiddenHeaders`). Động thái này giúp cho proxy không bao giờ gửi song song hai value lệch nhau lên Server, nhưng nếu bạn muốn convert ngược h2 → h1, có thể dẫn đến việc thiếu `Host` mapping chính xác với cổng.

### 3. Vấn đề Padding độ dài trong Data Frames (RFC 9113 Section 6.1)
- Padding của `DATA` và `HEADERS` frame trong `Http2Helper` được bóc tách bằng biến `padLength`. Tuy nhiên, biến này chưa được tính toán kỹ lưỡng vào kích thước của Connection Window Limit (cho WindowUpdate flow control). Bạn nhận byte đệm (padded) => bạn phải tiêu thụ (consume) chúng, và phải hoàn trả (WINDOW_UPDATE) cho chính xác số Byte đã nhận (Bao gồm cả Padding Byte, chứ không chỉ length thuần). Cần rà soát lại `localConnConsumed` và `localStreamConsumed` để đảm bảo Padding độ dài `+1` (độ dài của Byte chỉ định PadLength) và padding payload đã được cộng gộp chuẩn hay bị trượt (drift window limits).

### 4. Bỏ qua Strict WindowUpdate Boundaries (RFC 9113 Section 6.9.1)
- HTTP/2 có giới hạn Max Window size là `2^31 - 1`. Nếu proxy nhận hoặc update WINDOW_UPDATE vượt quá ngưỡng này, proxy phải trigger `RST_STREAM` hoặc `GOAWAY` với mã `FLOW_CONTROL_ERROR`. Hiện tại `Http2Helper.cs` chỉ thực hiện phép cộng `inputPeerSettings.ConnectionWindowSize += increment;` mà không tiến hành kiểm tra chống tràn số đối với `increment == 0` (lỗi) hoặc cửa sổ > Int32 max.

---

### Tổng kết

Architecture HTTP/2 của bạn rất nguyên bản, ổn định và **không gặp rủi ro lớn** về khả năng tương thích khi làm cầu nối proxy so với các rule của RFC 9113. 

**3 việc duy nhất nếu bạn muốn codebase hoàn toàn RFC 9113 compliance là:**
1. Áp dụng chuẩn **Trailers** (Xử lý trailing headers frame logic sau Body).
2. Xử lý triệt để validation cho `WINDOW_UPDATE` (báo lỗi nếu size increment = 0).
3. Đảm bảo flow control cộng dồn cả **padding bytes** khi tính toán ngưỡng xả `WINDOW_UPDATE`.
