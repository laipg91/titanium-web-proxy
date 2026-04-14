# Báo cáo đánh giá: Kiến trúc Proxy Lõi (SOCKS5, Explicit, Transparent)

**Ngày review:** 11/03/2026
**Mục tiêu:** Đánh giá các Handler xử lý kết nối đầu vào của ProxyServer nhằm tìm ra các điểm yếu, nguy cơ nghẽn cổ chai và rủi ro bảo mật trên môi trường High-Load Production.

---

## 1. Workflow hiện tại (Cách hoạt động)
Titanium Web Proxy xử lý kết nối gốc qua 3 Handler chính tương ứng với 3 loại EndPoint:

1. **`SocksClientHandler` (SOCKS4 / SOCKS5):**
   - Đọc handshake byte đầu tiên `0x04` hoặc `0x05` để phân loại.
   - Xử lý xác thực User/Password hoặc check IP/Whitelist.
   - Nếu Client gửi lệnh CONNECT (0x01) → giả lập tạo `ExplicitClientHandler` luồng TCP.
   - Nếu Client gửi UDP ASSOCIATE (0x03) → chẻ nhánh sang `HandleUdpAssociate`.

2. **`ExplicitClientHandler` (HTTP Proxy tiêu chuẩn):**
   - Lắng nghe Request Line. Nếu thấy `CONNECT host:443 HTTP/1.1` → biết là HTTPS.
   - Gửi lại `200 Connection Established`.
   - Phân tích gói `ClientHello` đầu tiên. Nếu `DecryptSsl = true`, Proxy tự ký (BouncyCastle) một chứng chỉ giả mạo (Fake X509) và trở thành Server HTTPS đối với Client (Man-in-the-Middle).
   - Nếu tắt Decrypt, tạo kết nối thẳng (TCP Relay) tới đích và bơm RAW bytes 2 chiều tốc độ cao (`TcpHelper.SendRaw`).

3. **`TransparentClientHandler` (Transparent Proxy/SNI Intercept):**
   - Không có thủ tục HTTP CONNECT. Bắt thẳng gói TCP đầu tiên, thường là TLS `ClientHello`.
   - Lấy tên miền từ SNI (Server Name Indication). 
   - Tương tự như Explicit: ký chứng chỉ chặn bắt hoặc Relay RAW.

---

## 2. Các rủi ro và Bottlenecks trên Production

Core Proxy xử lý tốt, linh hoạt, tính năng phong phú, nhưng mang trong mình 5 rủi ro cực kỳ lớn khi đem triển khai cho hệ thống tải cao (hàng chục nghìn Requests/second):

### 🔴 2.1. Cổ chai tạo chứng chỉ giả mạo (Fake Certificate Generation Bottleneck)
- **Vấn đề:** Khi `DecryptSsl = true`, proxy gọi `CertificateManager.CreateServerCertificate(certName)` để sinh chứng chỉ X509 dỏm bằng BouncyCastle (RSA/ECDSA + SHA256).
- **Rủi ro:** Thuật toán mật mã này tốn **rất nhiều CPU**. Mặc dù đã có Cache chứng chỉ trong `CertificateManager` (dùng ConcurrentDictionary) cho các domain tĩnh, nhưng nếu bị kẻ tấn công xả vào 100,000 domain ngẫu nhiên (hoặc wildcards), CPU sẽ chạm 100% ngay lập tức.
- **Cách fix:** 
  - Đặt cấu hình giới hạn cấp phát bộ nhớ/CPU cho Certificate Engine.
  - Chuyển BouncyCastle sang dùng CryptoAPI của Hệ điều hành (nếu Windows) hoặc OpenSSL native (nếu Linux) để tăng tốc độ ký. (Chúng ta vừa update BouncyCastle.Cryptography 2.3.1 - điều này đã giúp cải thiện một phần rất lớn).

### 🔴 2.2. Vung tiền ném qua cửa sổ ở HTTP/2 Discovery Hack (Explicit Proxy)
- **Vấn đề:** Trong `ExplicitClientHandler`, khi thấy Client hỗ trợ `h2` trong ALPN, code tạo thử **một kết nối TCP riêng** (`TcpConnectionFactory.GetServerConnection`) tới Server chỉ để test xem Server đích có thực sự hỗ trợ HTTP/2 hay không, xong đóng lại/ném vào pool!
- **Rủi ro:** Gây **tăng gấp đôi (x2) lượng TCP connections** mở ra ngoài, x2 độ trễ (Latency) cho Client, x2 lượng SYN/ACK gói tin. Đây là một hack vô cùng tốn kém tài nguyên hệ thống.
- **Cách fix:** Lưu trạng thái (Cache) dạng `Dictionary<Host, bool>` xem domain đó có HTTP/2 không thay vì phải mở thử mỗi lần có người hỏi.

### 🟠 2.3. Lỗ hổng SSRF qua "CONNECT / Explicit Proxy" (Bypass mạng nội bộ)
- **Vấn đề:** `ExplicitClientHandler` đọc Host:Port từ lệnh CONNECT và tạo TCP tới đích hoàn toàn bừa bãi.
- **Rủi ro:** Kẻ xấu cấu hình hệ thống của chúng trỏ đến Proxy của bạn, sau đó gửi: `CONNECT 127.0.0.1:3306 HTTP/1.1` hoặc `CONNECT 192.168.1.5:22 HTTP/1.1`. Proxy biến thành công cụ hack mạng nội bộ (SSRF), rà quét cổng (Port scanner).
- **Cách fix:** Bắt buộc phải có Regex Filter/Blacklist IP (loại trừ các dải IP Private RFC1918) áp dụng vô sự kiện `OnBeforeTunnelConnectRequest`. (Mặc định Titanium không tự filter, System Admin phải tự code sự kiện này).

### 🟠 2.4. Khả năng cạn kiệt TLS/Thread do đọc Stream đồng bộ một phần
- **Vấn đề:** Ở một số chỗ (như Parse HTTP header, đọc PeekClientHello), Titanium dùng `BufferPool` gán vào luồng đọc. Nhưng thư viện `SslStream.AuthenticateAsServerAsync` là cơ chế cấp phát ngầm khổng lồ của Microsoft (.NET).
- **Rủi ro:** TLS Handshake tốn trung bình 2-3 RTT (round-trips). Nếu hàng ngàn kết nối ảo (Slowloris attack) chỉ mở TCP nhưng cố tình không gửi ClientHello, hoặc gửi siêu chậm từng byte, Proxy sẽ giam hàng ngàn Task/Thread dẫn đến chết đói ThreadPool (Thread Starvation).
- **Cách fix:** Bắt buộc **thiết lập Cancellation Token / Timeout (Ví dụ 5-10 giây)** chẽ nhỏ cho quy trình Peak Ssl/AuthenticateAsServer.

### 🟡 2.5. Cache Poisoning bằng DNS ngầm (Transparent Proxy)
- **Vấn đề:** Trong Trực tiếp (Transparent), SNI chứa `google.com` nhưng Destination IP thật sự có thể là IP của Hacker. Gói fake cert cấp phát cho `google.com` (tồn tại trong cache server). 
- **Rủi ro:** Khi HostName (SNI) và IP thực sự không tương xứng, hệ thống tracking/audit của Proxy sẽ bị qua mặt. Các module lọc mã độc (DPI - Deep packet inspect) cấu hình dựa trên Domain List sẽ bị mù hoàn toàn do logic proxy chỉ lấy HostName ảo trong Data mà không ghim cứng với Real IP dưới tầng dBase TCP.

---
**Kết luận chung:** 
Với tư cách là một phần mềm proxy cấp thấp (.NET), Core Connection của Titanium Proxy viết cực kỳ chặt chẽ ở lớp tái sử dụng RAM (`BufferPool`). Nhưng vì nó được thiết kế dạng Library mở, tác giả đã **"cởi mở"** rất nhiều rào chắn bảo mật (Ví dụ SSRF, timeout). Để lên High-Load Production bảo mật, Developer phải tự đè tay vào các event `OnBeforeTunnelConnect` để khóa mạng nội bộ, giới hạn thời gian (Timeout), và chặn các tấn công cạn kiệt CPU (như tạo SSL wildcard).
