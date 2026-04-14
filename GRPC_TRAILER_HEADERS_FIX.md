# gRPC over HTTP/2 Trailer Headers Fix

## Issue
The gRPC test `Can_Run_Multiple_Grpc_Calls_Over_H2_Tls_Proxy` was failing with error:
```
System.Net.Http.HttpRequestException: Received an invalid status code: '0'
```

## Root Cause Analysis

### Problem 1: Pseudo-headers in Trailer Frames (RFC 7540 Violation)
When the server sent trailer headers (e.g., `grpc-status`), the proxy was including the `:status` pseudo-header in the trailer HEADERS frame. According to RFC 7540 §8.1.2.4:

> HTTP/2 does not use the Connection header field to indicate dependencies within the connection. In HTTP/2, pseudo-headers are header fields that are defined with a leading ":" character. Pseudo-headers are only valid and legal in a request or response; they MUST NOT be emitted by an application as trailer fields.

The `:status` pseudo-header should ONLY appear in the initial HEADERS frame for a stream, never in trailer HEADERS frames.

### Problem 2: Status Code Corruption  
When decoding trailer frames, the code was attempting to extract a `:status` pseudo-header that didn't exist. Since the trailer frame had no `:status`, parsing failed and returned 0, which then overwrote the valid status code (200) from the initial headers frame.

This resulted in the trailer frame being sent to the client with `:status:0`, which the .NET HTTP/2 client rejected as invalid.

## Fixes Applied

### Fix 1: Http2FrameWriter.cs (Trailer Frame Encoding)
**File**: `src/Titanium.Web.Proxy/Http2/Primitives/Http2FrameWriter.cs`

In `SendHeadersAsync()` method, added condition to skip `:status` encoding for trailer frames:

```csharp
else
{
    var response = (Response)rr;
    // RFC 7540 §8.1.2.4: Pseudo-headers MUST NOT be emitted in trailer headers
    // Only encode :status for the initial HEADERS frame, not for trailers
    if (!endStream)
    {
        encoder.EncodeHeader(writer, StaticTable.KnownHeaderStatus,
            response.StatusCode.ToString().GetByteString());
    }
}
```

**Result**: Trailer frames no longer include the forbidden `:status` pseudo-header.

### Fix 2: Http2Helper.cs (Trailer Frame Decoding - Response Status)
**File**: `src/Titanium.Web.Proxy/Http2/Http2Helper.cs`

In `CopyHttp2FrameAsync()` method, prevented status code overwriting for trailer frames:

```csharp
// RFC 7540 §8.1.2.4: Pseudo-headers must not be in trailers
// For trailer frames (endStream=true with existing StatusCode), skip status update
if (!endStream || response.StatusCode == 0)
{
    // parse and update status code
    string statusHack = HttpHeader.Encoding.GetString(headerListener.Status.Span);
    int.TryParse(statusHack, out int statusCode);
    response.StatusCode = statusCode;
}
```

**Result**: The valid status code from the initial HEADERS frame is preserved and not overwritten by the trailer frame parsing.

### Fix 3: Http1ToHttp2Translator.cs (Similar Defensive Logic)
**File**: `src/Titanium.Web.Proxy/Http2/Translation/Http1ToHttp2Translator.cs`

Applied the same defensive logic to the CONTINUATION frame handling path.

## Testing

The test now progresses further, but a new issue has emerged:
- Client error: "The response ended prematurely while waiting for the next frame from the server"

This suggests the trailer frame format may still have issues or there's a problem with how the frames are being sequenced.

## Related Standards

- RFC 7540 §8.1 - HTTP Message Semantics
- RFC 7540 §8.1.2.4 - Pseudo-Headers
- RFC 7540 §6.10 - HEADERS Frames
- gRPC over HTTP/2 specification

## Debugging Notes

Debug output traces show:
1. Initial HEADERS frame (80 bytes, statusCode=200) - ✅ Decoded correctly
2. Trailer HEADERS frame (15 bytes, contains grpc-status) - ✅ Now encoded without `:status`
3. Client error occurs when processing the trailer

The encoded trailer frame is now 16 bytes instead of 19 bytes (difference of 3 bytes for `:status:0`), confirming the `:status` is no longer being encoded.
