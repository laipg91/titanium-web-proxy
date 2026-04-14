# Kế hoạch Triển khai (Implementation Plan)

Dựa trên phân tích mã nguồn hiện tại của nhánh CORE Proxy, dưới đây là kế hoạch chi tiết để cập nhật và refactor 2 module **UDP Associate** ([UdpAssociateHandler.cs](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/UdpAssociateHandler.cs), [SocksClientHandler.cs](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/SocksClientHandler.cs)) và **HTTP/2** ([Http2Helper.cs](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/Http2/Http2Helper.cs)) nhằm đáp ứng chuẩn High-Load Production.

## 1. Mục tiêu (Goal Description)
Nâng cấp `UdpAssociateHandler` và [Http2Helper](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/Http2/Http2Helper.cs#23-874) tuân thủ nghiêm ngặt 5 phong cách code của Titanium Proxy, đặc biệt là:
1. **Zero-Allocation**: Dọn dẹp các điểm sinh rác mảng (GC pressure) ở Network I/O.
2. **Event Hooking**: Móc (Hook) luồng UDP vào `SessionEventArgs` để kích hoạt `OnDataSent` và `OnDataReceived`.
3. **Timeout & Security**: Bổ sung cơ chế Idle Timeout cho UDP và xử lý triệt để Flow Control Window cho HTTP/2.

---

## 2. Chi tiết thay đổi (Proposed Changes)

### 2.1. Cập nhật SOCKS5 Handler
Chuyển tiếp `SessionEventArgs` (đại diện cho vòng đời kết nối TCP) xuống cho `UdpAssociateHandler` để nó có ngữ cảnh bắn sự kiện.

#### [MODIFY] [SocksClientHandler.cs](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/SocksClientHandler.cs)
- Sửa chữ ký hàm [HandleUdpAssociate](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/UdpAssociateHandler.cs#26-111) để nhận thêm tham số `SessionEventArgs sessionEventArgs`.
- Cập nhật chỗ gọi hàm [HandleUdpAssociate](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/UdpAssociateHandler.cs#26-111) (khoảng dòng 191) để pass biến `sessionEventArgs` vào thay vì bỏ qua.

### 2.2. Xây dựng lại Zero-Allocation UDP Associate
Giữ nguyên logic SOCKS5, nhưng thay thế tầng Transport dưới cùng.

#### [MODIFY] [UdpAssociateHandler.cs](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/UdpAssociateHandler.cs)
- **Tích hợp Session Event:** 
  - Trong [LoopClientToRemote](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/UdpAssociateHandler.cs#135-177): Sau khi đọc xong biến payload, gọi `sessionEventArgs.OnDataSent(buf, headerLen, dataLen)`.
  - Trong [LoopRemoteToClient](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/UdpAssociateHandler.cs#178-226): Sau khi thêm SOCKS5 header, trước khi gửi cho Client, gọi `sessionEventArgs.OnDataReceived(buf, 0, totalLen)`.
- **Zero-Allocation Network API:**
  - Bỏ class [UdpSocketHelper](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/UdpAssociateHandler.cs#285-320) cũ đang dùng `TaskCompletionSource` và `BeginReceiveFrom`.
  - Thay bằng API [ReceiveFromAsync(Memory<byte>, SocketFlags, EndPoint)](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/UdpAssociateHandler.cs#293-308) của .NET 6+ (Proxy chạy trên NET6_0_OR_GREATER) hoặc sử dụng `SocketAsyncEventArgs` (SAEA) nếu cần tương thích đa nền tảng.
- **Xử lý Async DNS Bottleneck:**
  - Trong hàm `UdpSocks5Header.TryParse`: Tách phần xử lý Domain name (ATYP=3) ra ngoài. Đổi [TryParse](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/UdpAssociateHandler.cs#339-392) chỉ parse string host, sau đó bên ngoài dùng `await Dns.GetHostAddressesAsync(host)` để tránh block thread.
- **Thêm Idle Timeout & Security Filter:**
  - Cập nhật CancellationToken/Task.Delay cho loop A/B để kiểm tra: Nếu 3 phút không có I/O nào chạy -> Tự động hủy token đóng UDP Relay.
  - Bổ sung filter kiểm tra IP mục tiêu: Chặn các dải IP Private (Loopback 127.0.0.1, Local) nếu không được cho phép, tránh lỗi SSRF nội bộ.

### 2.3. Nâng cấp bộ máy HTTP/2 (Khắc phục cục mịch nguyên khối)
Như phân tích tại Code Review, [Http2Helper.cs](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/Http2/Http2Helper.cs) dài gần 900 dòng, nhồi nhét ngột ngạt toàn bộ việc đọc/ghi Frame, Parse Header, Flow Control vào trong một vòng lặp `while(true)` khổng lồ của hàm [CopyHttp2FrameAsync](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/Http2/Http2Helper.cs#59-647). Điều này hoàn toàn đi ngược lại với phong cách **Event-Driven & Module hóa** của Titanium Proxy (như cách HTTP/1.xchia nhỏ ra `HeaderParser`, `HttpHelper`, `HttpBase`).

#### [Decompose] Phân rã [Http2Helper.cs](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/Http2/Http2Helper.cs) (Monolithic → Event-Driven Modules)
- **Tạo `Http2FrameReader.cs`:** 
  - Đóng gói logic đọc Frame từ Stream ([ForceRead](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/Http2/Http2Helper.cs#756-775), bóc tách 9-byte Frame Header).
  - Sử dụng cơ chế `IObservable<Http2Frame>` hoặc `Event (OnFrameReceived)` để bắn các Frame ra ngoài thay vì xử lý trực tiếp.
- **Tạo `Http2FrameWriter.cs`:**
  - Chịu trách nhiệm Serialize các Frame Header và mảng byte payload xuống `NetworkStream`.
- **Tạo `Http2SessionHandler.cs` (hoặc `Http2StreamManager`):**
  - Chịu trách nhiệm duy trì Map `ConcurrentDictionary<int, SessionEventArgs>`.
  - Phân luồng các Frame (DATA, HEADERS, SETTINGS, PING, WINDOW_UPDATE) nhận được từ `Http2FrameReader` vào các phương thức chuyên biệt xử lý loại Frame đó.
  - Gọi các Delegate/Sự kiện Hook chuẩn xác như `args.OnDataReceived()`.

#### [MODIFY] [Http2Helper.cs](file:///d:/Aeg%20Project/titanium-web-proxy/titanium-web-proxy/src/Titanium.Web.Proxy/Http2/Http2Helper.cs) (nằm trong thư mục `src/Titanium.Web.Proxy/Http2/`)
- Mỏng hóa file này thành một Orchestrator:
  - Khởi tạo `Reader`, `Writer`, `StreamManager`.
  - Liên kết event `Reader.OnFrameReceived += StreamManager.HandleFrame`.
- **Quản lý Cửa sổ luồng đệm (Local Flow Control Window):**
  - Bổ sung tracking cho vùng đệm local. Proxy hiện tại nhận frame DATA từ Server và nhồi thẳng vào `ReadHttp2BodyTaskCompletionSource` (RAM).
  - Cần viết logic gửi `WINDOW_UPDATE` từ Proxy ra cho Server/Client nếu proxy đã kịp đọc hết khối đệm, tránh trường hợp Client xả dữ liệu quá nhanh làm tràn RAM proxy.
- **Bảo toàn HPACK Encoder (Zero-Allocation Fix):**
  - Mặc dù tác giả đã dùng `EncoderState` để cache lại, nhưng việc khởi tạo mới `new Encoder()` mỗi khi Client có sự thăng giáng về `HeaderTableSize` vẫn gây rác.
  - Sửa logic để chỉ Reset kích cỡ bảng (nếu API `Encoder` hỗ trợ) hoặc hạn chế việc `new` không cần thiết.

---

## 3. Kế hoạch Kiểm thử (Verification Plan)

### Automated Tests
- Build lại toàn bộ project (`dotnet build`) để đảm bảo không gãy (break) code ở NET 4.6.1 lẫn NET 6+.

### Manual Verification
1. **Test UDP OnDataSent/Received Hook:** 
   - Đăng ký sự kiện `ProxyServer.BeforeResponse` hoặc `ProxyServer.AfterResponse` (nếu có thể hook custom logic đong đếm băng thông).
   - Thiết lập proxy cho một Client SOCKS5 (như Telegram hoặc cURL dạng socks5h).
   - Dùng cURL gửi UDP DNS query qua Proxy: `curl --socks5 127.0.0.1:8000 URL`.
   - Kiểm tra log console xem sự kiện có bắn ra số bytes gửi/nhận hay không.
2. **Test HTTP/2 Memory stability:**
   - Dùng lệnh `h2load` hoặc `nghttp` để thực hiện flood request vào 1 website HTTPS qua Proxy. Nhìn Task Manager để xác nhận RAM không tăng nhảy vọt và GC không phải dọn rác liên tục.
3. **Test UDP Idle Timeout:**
   - Mở 1 kết nối SOCKS5 UDP nhưng không gửi packet nào. Đợi 3 phút để xem proxy có tự dọn dẹp (Dispose) UDP socket và TCP Control channel có được nhả ra không.
