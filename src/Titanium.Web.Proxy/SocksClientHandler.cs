using System;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Titanium.Web.Proxy.EventArguments;
using Titanium.Web.Proxy.Extensions;
using Titanium.Web.Proxy.Helpers;
using Titanium.Web.Proxy.Models;
using Titanium.Web.Proxy.Network.Tcp;

namespace Titanium.Web.Proxy
{

    public partial class ProxyServer
    {
        /// <summary>
        ///     This is called when this proxy acts as a reverse proxy (like a real http server).
        ///     So for HTTPS requests we would start SSL negotiation right away without expecting a CONNECT request from client
        /// </summary>
        /// <param name="endPoint">The transparent endpoint.</param>
        /// <param name="clientConnection">The client connection.</param>
        /// <returns></returns>
        private async Task HandleClient(SocksProxyEndPoint endPoint, TcpClientConnection clientConnection)
        {
            var cancellationTokenSource = new CancellationTokenSource();
            var cancellationToken = cancellationTokenSource.Token;

            var stream = clientConnection.GetStream();
            var buffer = BufferPool.GetBuffer();
            var port = 0;
            var isUdpAssociate = false;
            SessionEventArgs sessionEventArgs =null;

            // Handshake timeout to prevent infinite wait on laggy/malicious clients
            cancellationTokenSource.CancelAfter(TimeSpan.FromSeconds(ConnectionTimeOutSeconds));
            try
            {
                // Read at least 2 bytes (VER, NMETHODS/CMD)
                var read = await ForceReadAsync(stream, buffer, 0, 2, cancellationToken);
                if (read < 2) return;

                sessionEventArgs = new SessionEventArgs(this, endPoint,
                    new HttpClientStream(this, clientConnection, stream, BufferPool, cancellationToken), null,
                    cancellationTokenSource);

                if (buffer[0] == 4) //Socks client version 4
                {
                    if (buffer[1] != 1)
                        // not a connect request (only CONNECT=0x01 supported in SOCKS4)
                        return;

                    // SOCKS4 CONNECT layout: VER(1) CMD(1) PORT(2) IP(4) USERID(var) NULL(1)
                    // We already have VER+CMD in buffer[0..1]. Read PORT(2) + IP(4) = 6 bytes.
                    read = await ForceReadAsync(stream, buffer, 2, 6, cancellationToken);
                    if (read < 6) return;

                    port = (buffer[2] << 8) + buffer[3];

                    // Drain the NULL-terminated USERID field from the stream.
                    // Without this, leftover bytes corrupt subsequent HTTP traffic.
                    // Read in chunks using the existing buffer (offset 8+) to find the null terminator.
                    bool foundNull = false;
                    while (!foundNull)
                    {
                        read = await stream.ReadAsync(buffer, 8, buffer.Length - 8, cancellationToken);
                        if (read == 0) return; // EOF before null terminator
                        for (int i = 8; i < 8 + read; i++)
                        {
                            if (buffer[i] == 0) { foundNull = true; break; }
                        }
                    }

                    buffer[0] = 0;
                    buffer[1] = 90; // request granted
                    
                    if (ProxyAuthenticationSchemes.Contains("IP-Address") && ProxySchemeAuthenticateFunc != null)
                    {
                        //Socks4 doesnt support authentication, so if set authentication we can verify by IP if needed
                        var authResult = await ProxySchemeAuthenticateFunc.Invoke(sessionEventArgs, "IP-Address", string.Empty);
                        if (authResult.Result != ProxyAuthenticationResult.Success)
                        {
                            buffer[1] = 91;//request rejected or failed
                        }

                    }

                    await stream.WriteAsync(buffer, 0, 8, cancellationToken);
                    if (buffer[1]!=90) //denied
                        return;
                }
                else if (buffer[0] == 5) //Socks client version 5
                {
                    int authenticationMethodCount = buffer[1];
                    read = await ForceReadAsync(stream, buffer, 2, authenticationMethodCount, cancellationToken);
                    if (read < authenticationMethodCount) return;

                    var acceptedMethod = 255;
                    for (var i = 0; i < authenticationMethodCount; i++)
                    {
                        int method = buffer[i + 2];
                        if (method == 0)
                        {
                            bool success = true;
                            if (ProxyAuthenticationSchemes.Contains("IP-Address") && ProxySchemeAuthenticateFunc != null)
                            {
                                //client send no authentication but we need verify by IP then we need parse an empty username/password
                                var authResult = await ProxySchemeAuthenticateFunc.Invoke(sessionEventArgs, "IP-Address", string.Empty);
                                if (authResult.Result != ProxyAuthenticationResult.Success)
                                    success = false;
                            }
                            if(success)
                                acceptedMethod = 0;

                            break;
                        }

                        if (method == 2)
                        {
                            acceptedMethod = 2;
                            break;
                        }
                    }

                    buffer[1] = (byte)acceptedMethod;
                    await stream.WriteAsync(buffer, 0, 2, cancellationToken);

                    if (acceptedMethod == 255)
                        // no acceptable method
                        return;
                    
                    if (acceptedMethod == 2)
                    {
                        // Read Version and Username Length
                        read = await ForceReadAsync(stream, buffer, 0, 2, cancellationToken);
                        if (read < 2 || buffer[0] != 1)
                            // authentication version should be 1
                            return;

                        int userNameLength = buffer[1];
                        // Read Username + Password Length byte
                        read = await ForceReadAsync(stream, buffer, 2, userNameLength + 1, cancellationToken);
                        if (read < userNameLength + 1) return;

                        var userName = Encoding.ASCII.GetString(buffer, 2, userNameLength);

                        int passwordLength = buffer[2 + userNameLength];
                        // Read Password
                        read = await ForceReadAsync(stream, buffer, 3 + userNameLength, passwordLength, cancellationToken);
                        if (read < passwordLength) return;

                        var password = Encoding.ASCII.GetString(buffer, 3 + userNameLength, passwordLength);
                        var success = true;
                        if (ProxyBasicAuthenticateFunc != null)
                        {
                            success = await ProxyBasicAuthenticateFunc.Invoke(sessionEventArgs, userName, password);
                        }

                        buffer[1] = success ? (byte)0 : (byte)1;
                        await stream.WriteAsync(buffer, 0, 2, cancellationToken);
                        if (!success) return;
                    }

                    // 1. Read Header (VER, CMD, RSV, ATYP)
                    read = await ForceReadAsync(stream, buffer, 0, 4, cancellationToken);
                    if (read < 4) return;
                    var cmd = buffer[1];
                    if (cmd != 1 && cmd != 3) return;

                        int addrLen;
                        switch (buffer[3])
                        {
                            case 1:
                                // IPv4: 4 bytes IP + 2 bytes Port
                                addrLen = 6;
                                break;
                            case 3:
                                // Domain: 1 byte Len + Len bytes Name + 2 bytes Port
                                read = await ForceReadAsync(stream, buffer, 4, 1, cancellationToken);
                                if (read < 1) return;
                                addrLen = buffer[4] + 2;
                                break;
                            case 4:
                                // IPv6: 16 bytes IP + 2 bytes Port
                                addrLen = 18;
                                break;
                            default:
                                return;
                        }

                        var offset = buffer[3] == 3 ? 5 : 4;
                        read = await ForceReadAsync(stream, buffer, offset, addrLen, cancellationToken);
                        if (read < addrLen) return;

                        if (cmd == 3) // UDP ASSOCIATE
                        {
                            isUdpAssociate = true;
                        }
                        else // CMD = CONNECT (0x01)
                        {
                            port = (buffer[offset + addrLen - 2] << 8) + buffer[offset + addrLen - 1];
                            buffer[1] = 0; // succeeded
                            await stream.WriteAsync(buffer, 0, offset + addrLen, cancellationToken);
                        } // end cmd == 1 block
                }
                else
                {
                    return;
                }
            }
            finally
            {
                BufferPool.ReturnBuffer(buffer);
                sessionEventArgs?.Dispose();
            }

            // Handshake successful, reset timeout to let subsequent handlers manage their own lifetimes
            cancellationTokenSource.CancelAfter(Timeout.Infinite);

            if (isUdpAssociate)
            {
                await HandleUdpAssociate(endPoint, clientConnection, stream, cancellationToken);
                return;
            }

            await HandleClient(endPoint, clientConnection, port, cancellationTokenSource, cancellationToken);
        }

        /// <summary>
        /// Reads exactly <paramref name="bytesToRead"/> bytes from <paramref name="input"/>,
        /// looping until the buffer is full or the stream signals EOF.
        /// </summary>
        private static async Task<int> ForceReadAsync(System.IO.Stream input, byte[] buffer, int offset, int bytesToRead,
            CancellationToken cancellationToken)
        {
            int totalRead = 0;
            while (bytesToRead > 0)
            {
                int read = await input.ReadAsync(buffer, offset, bytesToRead, cancellationToken);
                if (read == 0)
                    break; // EOF

                totalRead += read;
                bytesToRead -= read;
                offset += read;
            }

            return totalRead;
        }
    }
}
