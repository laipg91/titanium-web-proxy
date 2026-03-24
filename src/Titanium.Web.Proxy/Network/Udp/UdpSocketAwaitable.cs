using System;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace Titanium.Web.Proxy
{
    internal struct RecvResult
    {
        internal int ReceivedBytes;
        internal IPEndPoint RemoteEp;
    }

    /// <summary>
    ///     Fix 6: Zero-Alloc UDP — SAEA-based wrapper.
    ///     Uses <see cref="SocketAsyncEventArgs"/> pre-allocated once per direction,
    ///     reused across all packets in a session.
    ///     On synchronous completion (common for UDP — data already in kernel buffer):
    ///       → ZERO heap allocation.
    ///     On async completion (pending I/O):
    ///       → One <see cref="TaskCompletionSource{T}"/> allocation.
    ///     Works on both net461 and net6.0+.
    /// </summary>
    internal sealed class UdpSocketAwaitable : IDisposable
    {
        private readonly SocketAsyncEventArgs _recvSaea;
        private readonly SocketAsyncEventArgs _sendSaea;
        private TaskCompletionSource<bool> _recvTcs;
        private TaskCompletionSource<bool> _sendTcs;

        internal UdpSocketAwaitable(AddressFamily af)
        {
            _recvSaea = new SocketAsyncEventArgs();
            _recvSaea.Completed += OnRecvCompleted;

            _sendSaea = new SocketAsyncEventArgs();
            _sendSaea.Completed += OnSendCompleted;

            // Set initial remote endpoint for ReceiveFrom
            _recvSaea.RemoteEndPoint = af == AddressFamily.InterNetworkV6
                ? (EndPoint)new IPEndPoint(IPAddress.IPv6Any, 0)
                : new IPEndPoint(IPAddress.Any, 0);
        }

        private void OnRecvCompleted(object sender, SocketAsyncEventArgs e)
        {
            var tcs = _recvTcs;
            if (tcs == null) return;
            if (e.SocketError == SocketError.Success)
                tcs.TrySetResult(true);
            else
                tcs.TrySetException(new SocketException((int)e.SocketError));
        }

        private void OnSendCompleted(object sender, SocketAsyncEventArgs e)
        {
            var tcs = _sendTcs;
            if (tcs == null) return;
            if (e.SocketError == SocketError.Success)
                tcs.TrySetResult(true);
            else
                tcs.TrySetException(new SocketException((int)e.SocketError));
        }

        /// <summary>
        /// SAEA-based receive. On sync completion (common for UDP), zero alloc.
        /// </summary>
        internal async Task<RecvResult> ReceiveFromAsync(
            Socket socket, byte[] buf, int offset, int size)
        {
            _recvSaea.SetBuffer(buf, offset, size);

            if (socket.ReceiveFromAsync(_recvSaea))
            {
                // Async path — need TCS (allocated only when pending)
                _recvTcs = new TaskCompletionSource<bool>();
                await _recvTcs.Task;
            }

            // Sync or async completed — read result from SAEA (no alloc)
            if (_recvSaea.SocketError != SocketError.Success)
                throw new SocketException((int)_recvSaea.SocketError);

            return new RecvResult
            {
                ReceivedBytes = _recvSaea.BytesTransferred,
                RemoteEp = (IPEndPoint)_recvSaea.RemoteEndPoint
            };
        }

        /// <summary>
        /// SAEA-based send. On sync completion (common for UDP), zero alloc.
        /// </summary>
        internal async Task SendToAsync(
            Socket socket, byte[] buf, int offset, int size, EndPoint remoteEp)
        {
            _sendSaea.SetBuffer(buf, offset, size);
            _sendSaea.RemoteEndPoint = remoteEp;

            if (socket.SendToAsync(_sendSaea))
            {
                // Async path — need TCS
                _sendTcs = new TaskCompletionSource<bool>();
                await _sendTcs.Task;
            }

            if (_sendSaea.SocketError != SocketError.Success)
                throw new SocketException((int)_sendSaea.SocketError);
        }

        public void Dispose()
        {
            _recvSaea.Dispose();
            _sendSaea.Dispose();
        }
    }
}
