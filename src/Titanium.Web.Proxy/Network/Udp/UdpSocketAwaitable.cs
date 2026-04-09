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
    ///     SAEA-based UDP helper that reuses one receive and one send args object.
    /// </summary>
    internal sealed class UdpSocketAwaitable : IDisposable
    {
        private readonly SocketAsyncEventArgs _recvSaea;
        private readonly SocketAsyncEventArgs _sendSaea;
        private TaskCompletionSource<bool>? _recvTcs;
        private TaskCompletionSource<bool>? _sendTcs;

        internal UdpSocketAwaitable(AddressFamily af)
        {
            _recvSaea = new SocketAsyncEventArgs();
            _recvSaea.Completed += OnRecvCompleted;

            _sendSaea = new SocketAsyncEventArgs();
            _sendSaea.Completed += OnSendCompleted;

            _recvSaea.RemoteEndPoint = af == AddressFamily.InterNetworkV6
                ? (EndPoint)new IPEndPoint(IPAddress.IPv6Any, 0)
                : new IPEndPoint(IPAddress.Any, 0);
        }

        private void OnRecvCompleted(object? sender, SocketAsyncEventArgs e)
        {
            var tcs = _recvTcs;
            if (tcs == null) return;

            if (e.SocketError == SocketError.Success)
                tcs.TrySetResult(true);
            else
                tcs.TrySetException(new SocketException((int)e.SocketError));
        }

        private void OnSendCompleted(object? sender, SocketAsyncEventArgs e)
        {
            var tcs = _sendTcs;
            if (tcs == null) return;

            if (e.SocketError == SocketError.Success)
                tcs.TrySetResult(true);
            else
                tcs.TrySetException(new SocketException((int)e.SocketError));
        }

        internal async Task<RecvResult> ReceiveFromAsync(
            Socket socket, byte[] buf, int offset, int size)
        {
            _recvSaea.SetBuffer(buf, offset, size);
            var recvTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _recvTcs = recvTcs;

            try
            {
                if (socket.ReceiveFromAsync(_recvSaea))
                    await recvTcs.Task;
            }
            finally
            {
                _recvTcs = null;
            }

            if (_recvSaea.SocketError != SocketError.Success)
                throw new SocketException((int)_recvSaea.SocketError);

            return new RecvResult
            {
                ReceivedBytes = _recvSaea.BytesTransferred,
                RemoteEp = (IPEndPoint)_recvSaea.RemoteEndPoint!
            };
        }

        internal async Task SendToAsync(
            Socket socket, byte[] buf, int offset, int size, EndPoint remoteEp)
        {
            _sendSaea.SetBuffer(buf, offset, size);
            _sendSaea.RemoteEndPoint = remoteEp;
            var sendTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _sendTcs = sendTcs;

            try
            {
                if (socket.SendToAsync(_sendSaea))
                    await sendTcs.Task;
            }
            finally
            {
                _sendTcs = null;
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
