# gRPC over HTTP/2 - Detailed Debug Findings

## Test Case: `Can_Run_Multiple_Grpc_Calls_Over_H2_Tls_Proxy`

### Test Setup
- **Client**: .NET HttpClient with HTTP/2
- **Proxy**: Titanium Web Proxy with HTTP/2 support
- **Server**: ASP.NET Core Kestrel with gRPC test handler

### Frame Sequence Observed

From debug trace, we observe this frame sequence for streamId=1:

1. **Client → Proxy (HEADERS)**
   - Length: 92 bytes
   - Flags: EndHeaders=Yes, EndStream=No
   - Content: POST request to `/grpc.integration.TestService/Unary`

2. **Server → Proxy (HEADERS)**
   - Length: 80 bytes  
   - Flags: EndHeaders=Yes, EndStream=No
   - Content: `:status:200` + response headers

3. ⚠️ **MISSING: DATA frame(s)**
   - Expected: gRPC message frame (~30 bytes) with the response data
   - **NOT OBSERVED in logs**

4. **Server → Proxy (HEADERS)**
   - Length: 15 bytes
   - Flags: EndHeaders=Yes, EndStream=Yes
   - Content: Trailer headers (grpc-status:0 and others)

### Hypothesis about Missing DATA Frame

The ASP.NET Core server code (lines 271-276 of Http2Tests.cs) writes response messages:

```csharp
foreach (var message in responseMessages)
{
    var frame = CreateGrpcFrame(message);
    await context.Response.Body.WriteAsync(frame, 0, frame.Length);
    await context.Response.Body.FlushAsync();
}
```

Expected behavior: This should result in HTTP/2 DATA frame(s) being sent to the proxy.

**Possible explanations:**
1. The DATA frame is being sent but not logged (logging filter issue)
2. The DATA frame is being absorbed somewhere in the proxy code
3. ASP.NET Core is buffering the data until the response is fully written (including trailers)
4. The test server isn't actually sending the data properly through the proxy connection

### Issues Fixed

✅ **Fixed**: Pseudo-header `:status` in trailer frames (RFC 7540 violation)
- Trailer frames were including `:status` which violates RFC 7540 §8.1.2.4
- Fixed by skipping `:status` encoding when `endStream=true`

✅ **Fixed**: Status code overwriting to 0
- Trailer frame processing was overwriting the valid 200 status with 0
- Fixed by preserving existing status code for trailer frames

### Current Error

**Error**: "The response ended prematurely while waiting for the next frame from the server"

**When**: In `HttpClient.SendAsync()` at `ReadResponseHeadersAsync()`

**Why**: After successfully sending the request, the client is waiting for complete response headers. It appears the connection is being closed prematurely or a frame is malformed.

### Next Investigation Steps

1. **Verify DATA frame encoding**: Check if DATA frames are being received from server at all
2. **Check trailer frame format**: Verify the trailer HEADERS frame is properly encoded
3. **Frame flow control**: Check if window updates are being sent properly
4. **Stream state management**: Verify the stream isn't being marked as complete prematurely

### Debug Output Traces

Initial HEADERS decoding:
```
[H2] CopyHttp2FrameAsync: Decoded :status header: streamId=1, endStream=False, statusHack='200', statusCode=200
```

Trailer HEADERS decoding:
```
[H2] CopyHttp2FrameAsync: SKIPPING :status update (trailer frame): streamId=1, response.StatusCode=200
[H2] SendHeadersAsync TRAILER: Skipping :status encoding for endStream=true
```

Trailer HEADERS being sent:
```
[H2] SendHeadersAsync: StreamId=1, EndStream=true (TRAILER HEADERS), StatusCode=200, ...
EncodedLen=16, Flags=0x05
```

Note: Encoded length is 16 bytes (compared to 19 bytes with `:status:0`), confirming `:status` is not being encoded.

### RFC References

- RFC 7540 §8.1.2.4 - Pseudo-headers must not appear in trailer headers
- RFC 7540 §6.10 - HEADERS frame format
- gRPC over HTTP/2 specification

### Files Modified

1. `src/Titanium.Web.Proxy/Http2/Primitives/Http2FrameWriter.cs` - Skip :status in trailers
2. `src/Titanium.Web.Proxy/Http2/Http2Helper.cs` - Preserve status code, preserve status code in CONTINUATION
3. `src/Titanium.Web.Proxy/Http2/Translation/Http1ToHttp2Translator.cs` - Similar logic for CONTINUATION frames
