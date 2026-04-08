#if NET6_0_OR_GREATER
using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Titanium.Web.Proxy.EventArguments;
using Titanium.Web.Proxy.Exceptions;
using Titanium.Web.Proxy.Helpers;
using Titanium.Web.Proxy.Models;

namespace Titanium.Web.Proxy.Http2.Translation
{
    /// <summary>
    /// Translates between a client-side stream and a server-side stream
    /// that use different HTTP versions (H1 ↔ H2).
    /// </summary>
    internal interface IHttp2Translator
    {
        /// <summary>
        /// Runs the translation loop until the connection is closed,
        /// an unrecoverable error occurs, or <paramref name="cts"/> is cancelled.
        /// </summary>
        /// <param name="clientStream">Stream connected to the client.</param>
        /// <param name="serverStream">Stream connected to the upstream server.</param>
        /// <param name="initialSession">
        /// The already-parsed first <see cref="SessionEventArgs"/> (including request line,
        /// headers, and fired <c>OnBeforeRequest</c>).  Pass <see langword="null"/> when the
        /// caller has not pre-parsed any request (e.g. Scenario B — H2 client).
        /// </param>
        /// <param name="sessionFactory">Factory that creates a new <see cref="SessionEventArgs"/> per request.</param>
        /// <param name="onBeforeRequest">Hook invoked before forwarding each request.</param>
        /// <param name="onBeforeResponse">Hook invoked before forwarding each response.</param>
        /// <param name="cts">Cancellation source shared across both sides of the connection.</param>
        /// <param name="connectionId">Unique ID of the parent connection (for diagnostics).</param>
        /// <param name="exceptionFunc">Called with non-fatal exceptions instead of throwing.</param>
        Task TranslateAsync(
            HttpClientStream clientStream,
            Stream serverStream,
            SessionEventArgs? initialSession,
            Func<SessionEventArgs> sessionFactory,
            Func<SessionEventArgs, Task> onBeforeRequest,
            Func<SessionEventArgs, Task> onBeforeResponse,
            CancellationTokenSource cts,
            Guid connectionId,
            ExceptionHandler? exceptionFunc);
    }
}
#endif
