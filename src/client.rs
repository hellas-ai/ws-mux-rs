use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};

#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;

use prost::Message;
use tokio::sync::{mpsc, oneshot};

/// Stream dispatch table: `Mutex` on native (multi-threaded), `RefCell` on wasm32
/// (single-threaded — avoids async locking overhead and `spawn_local` re-entrancy).
#[cfg(not(target_arch = "wasm32"))]
type StreamMap = tokio::sync::Mutex<HashMap<u32, mpsc::Sender<Frame>>>;
#[cfg(target_arch = "wasm32")]
type StreamMap = std::cell::RefCell<HashMap<u32, mpsc::Sender<Frame>>>;
use tracing::Instrument;

use crate::error;
use crate::error::Error;
use crate::frame::{self, Frame, TraceContext, flags};

/// Maximum buffered bytes for a unary response payload.
const MAX_UNARY_RESPONSE_PAYLOAD: usize = 4 * 1024 * 1024;
/// Per-stream frame queue bound to avoid unbounded memory growth.
const STREAM_QUEUE_CAPACITY: usize = 64;
/// Retries when stream-id allocation collides after wrap-around.
const MAX_STREAM_ID_ALLOCATION_ATTEMPTS: usize = 1024;

/// Platform-appropriate shared pointer (Arc on native, Rc on wasm32).
#[cfg(not(target_arch = "wasm32"))]
type Shared<T> = Arc<T>;
#[cfg(target_arch = "wasm32")]
type Shared<T> = std::rc::Rc<T>;

/// Type-erased async function for sending raw bytes over a WebSocket.
#[cfg(not(target_arch = "wasm32"))]
pub type SendFn =
    Arc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<(), Error>> + Send>> + Send + Sync>;

/// Type-erased async function for sending raw bytes over a WebSocket (wasm32 variant, no Send).
#[cfg(target_arch = "wasm32")]
pub type SendFn = std::rc::Rc<dyn Fn(Vec<u8>) -> Pin<Box<dyn Future<Output = Result<(), Error>>>>>;

/// A multiplexing client channel over a single WebSocket connection.
///
/// Clone-able — all clones share the same underlying connection.
/// Each RPC call allocates a new odd-numbered stream ID.
#[derive(Clone)]
pub struct MuxChannel {
    inner: Shared<Inner>,
}

struct Inner {
    /// Next client-initiated stream ID (odd, starts at 1, increments by 2).
    next_stream_id: AtomicU32,
    /// Write side of the WebSocket (type-erased).
    send_fn: SendFn,
    /// Per-stream dispatch: incoming frames are routed here by the background reader.
    streams: StreamMap,
}

impl MuxChannel {
    /// Create a channel from a send function.
    ///
    /// Allocates odd stream IDs (1, 3, 5, …).  The caller must arrange for
    /// incoming frames to be dispatched via [`route_frame`](Self::route_frame)
    /// (typically from a background reader or `websocket_message` callback).
    pub fn new(send_fn: SendFn) -> Self {
        Self {
            inner: Shared::new(Inner {
                next_stream_id: AtomicU32::new(1),
                send_fn,
                streams: StreamMap::new(HashMap::new()),
            }),
        }
    }

    /// Create a channel that allocates **even** stream IDs (2, 4, 6, …).
    ///
    /// Use this for the "server-initiated" side of a bidirectional connection
    /// so that stream IDs never collide with the odd-numbered client side.
    pub fn new_even(send_fn: SendFn) -> Self {
        Self {
            inner: Shared::new(Inner {
                next_stream_id: AtomicU32::new(2),
                send_fn,
                streams: StreamMap::new(HashMap::new()),
            }),
        }
    }

    /// Route an incoming frame to the correct stream receiver.
    ///
    /// Called by the background reader task for each received WebSocket message.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn route_frame(&self, frame: Frame) {
        let stream_id = frame.stream_id;
        let tx = {
            let streams = self.inner.streams.lock().await;
            streams.get(&stream_id).cloned()
        };
        self.dispatch_frame(stream_id, tx, frame).await;
    }

    /// Route an incoming frame synchronously (wasm32 only).
    ///
    /// Avoids `spawn_local` overhead in the browser `onmessage` callback.
    /// Safe because wasm32 is single-threaded — no lock contention.
    #[cfg(target_arch = "wasm32")]
    pub fn route_frame(&self, frame: Frame) {
        let stream_id = frame.stream_id;
        let tx = self.inner.streams.borrow().get(&stream_id).cloned();
        // For the full-queue RST path we need async send_raw, but this is
        // rare enough that spawn_local is acceptable here.
        if let Some(tx) = tx {
            match tx.try_send(frame) {
                Ok(()) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    tracing::debug!(stream_id, "receiver dropped for stream");
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!(stream_id, "response stream queue full, cancelling stream");
                    self.inner.streams.borrow_mut().remove(&stream_id);
                }
            }
        } else {
            tracing::debug!(stream_id, "no receiver for frame, dropping");
        }
    }

    /// Shared frame dispatch logic (native only — wasm inlines this).
    #[cfg(not(target_arch = "wasm32"))]
    async fn dispatch_frame(&self, stream_id: u32, tx: Option<mpsc::Sender<Frame>>, frame: Frame) {
        if let Some(tx) = tx {
            match tx.try_send(frame) {
                Ok(()) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    tracing::debug!(stream_id, "receiver dropped for stream");
                }
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    tracing::warn!(stream_id, "response stream queue full, cancelling stream");
                    if self.deregister_stream(stream_id).await {
                        let _ = self
                            .send_raw(
                                Self::cancellation_frame(stream_id, "response stream queue full")
                                    .encode(),
                            )
                            .await;
                    }
                }
            }
        } else {
            tracing::debug!(stream_id, "no receiver for frame, dropping");
        }
    }

    /// Decode a raw WebSocket message and route it to the correct stream.
    ///
    /// Convenience wrapper around [`Frame::decode`] + [`route_frame`](Self::route_frame).
    /// Designed for push-based transports such as the Cloudflare Workers
    /// `websocket_message` hibernation callback.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn receive(&self, data: &[u8]) -> Result<(), Error> {
        let frame = Frame::decode(data)?;
        self.route_frame(frame).await;
        Ok(())
    }

    /// Decode a raw WebSocket message and route it synchronously (wasm32).
    #[cfg(target_arch = "wasm32")]
    pub fn receive(&self, data: &[u8]) -> Result<(), Error> {
        let frame = Frame::decode(data)?;
        self.route_frame(frame);
        Ok(())
    }

    /// Allocate a new client-initiated stream ID.
    fn alloc_stream_id(&self) -> u32 {
        self.inner.next_stream_id.fetch_add(2, Ordering::Relaxed)
    }

    /// Allocate and register a new stream receiver, retrying collisions.
    #[cfg(not(target_arch = "wasm32"))]
    async fn alloc_and_register_stream(&self) -> Result<(u32, mpsc::Receiver<Frame>), Error> {
        for _ in 0..MAX_STREAM_ID_ALLOCATION_ATTEMPTS {
            let stream_id = self.alloc_stream_id();
            let (tx, rx) = mpsc::channel(STREAM_QUEUE_CAPACITY);
            let mut streams = self.inner.streams.lock().await;
            if let std::collections::hash_map::Entry::Vacant(entry) = streams.entry(stream_id) {
                entry.insert(tx);
                return Ok((stream_id, rx));
            }
            tracing::warn!(stream_id, "stream ID collision, retrying allocation");
        }
        Err(Error::Protocol(
            "unable to allocate stream ID without collision".into(),
        ))
    }

    /// Allocate and register a new stream receiver (wasm32 — synchronous RefCell).
    ///
    /// Wrapped in an async fn so callers don't need cfg gates.
    #[cfg(target_arch = "wasm32")]
    async fn alloc_and_register_stream(&self) -> Result<(u32, mpsc::Receiver<Frame>), Error> {
        for _ in 0..MAX_STREAM_ID_ALLOCATION_ATTEMPTS {
            let stream_id = self.alloc_stream_id();
            let (tx, rx) = mpsc::channel(STREAM_QUEUE_CAPACITY);
            let mut streams = self.inner.streams.borrow_mut();
            if let std::collections::hash_map::Entry::Vacant(entry) = streams.entry(stream_id) {
                entry.insert(tx);
                return Ok((stream_id, rx));
            }
            tracing::warn!(stream_id, "stream ID collision, retrying allocation");
        }
        Err(Error::Protocol(
            "unable to allocate stream ID without collision".into(),
        ))
    }

    /// Return true when `stream_id` was initiated by this channel.
    ///
    /// The channel parity is invariant because IDs always increment by 2.
    #[cfg(feature = "server")]
    pub(crate) fn is_local_stream_id(&self, stream_id: u32) -> bool {
        let parity = self.inner.next_stream_id.load(Ordering::Relaxed) & 1;
        (stream_id & 1) == parity
    }

    /// Remove a stream from the dispatch table.
    #[cfg(not(target_arch = "wasm32"))]
    async fn deregister_stream(&self, stream_id: u32) -> bool {
        self.inner.streams.lock().await.remove(&stream_id).is_some()
    }

    /// Remove a stream from the dispatch table (wasm32 — synchronous).
    #[cfg(target_arch = "wasm32")]
    async fn deregister_stream(&self, stream_id: u32) -> bool {
        self.inner.streams.borrow_mut().remove(&stream_id).is_some()
    }

    fn cancellation_frame(stream_id: u32, message: &'static str) -> Frame {
        Frame {
            stream_id,
            flags: flags::RST,
            payload: frame::build_rst_payload(error::code::CANCELLED, message),
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn cleanup_stream_on_drop(&self, stream_id: u32, send_rst: bool) {
        let channel = self.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                if channel.deregister_stream(stream_id).await && send_rst {
                    let _ = channel
                        .send_raw(Self::cancellation_frame(stream_id, "cancelled").encode())
                        .await;
                }
            });
        } else if let Ok(mut streams) = self.inner.streams.try_lock() {
            streams.remove(&stream_id);
        }
    }

    #[cfg(target_arch = "wasm32")]
    fn cleanup_stream_on_drop(&self, stream_id: u32, _send_rst: bool) {
        self.inner.streams.borrow_mut().remove(&stream_id);
    }

    /// Send raw bytes through the WebSocket sink.
    pub async fn send_raw(&self, data: Vec<u8>) -> Result<(), Error> {
        (self.inner.send_fn)(data).await
    }

    /// Build a channel from a [`crate::server::WsSink`] without spawning a background reader.
    ///
    /// The caller is responsible for feeding incoming messages via
    /// [`receive`](Self::receive) or [`route_frame`](Self::route_frame).
    ///
    /// Allocates odd stream IDs (1, 3, 5, …). Use [`from_sink_even`](Self::from_sink_even)
    /// for the server-initiated side of a bidirectional connection.
    #[cfg(all(feature = "server", not(target_arch = "wasm32")))]
    pub fn from_sink(sink: impl crate::server::WsSink + Clone + Send + Sync + 'static) -> Self {
        let send_fn: SendFn = Arc::new(move |data: Vec<u8>| {
            let sink = sink.clone();
            Box::pin(async move { sink.send(data).await })
        });
        Self::new(send_fn)
    }

    /// Like [`from_sink`](Self::from_sink) but allocates **even** stream IDs
    /// (2, 4, 6, …) for the server-initiated side of a bidirectional connection.
    #[cfg(all(feature = "server", not(target_arch = "wasm32")))]
    pub fn from_sink_even(
        sink: impl crate::server::WsSink + Clone + Send + Sync + 'static,
    ) -> Self {
        let send_fn: SendFn = Arc::new(move |data: Vec<u8>| {
            let sink = sink.clone();
            Box::pin(async move { sink.send(data).await })
        });
        Self::new_even(send_fn)
    }

    /// Build a channel from a [`crate::server::WsSink`] without installing callbacks.
    ///
    /// The caller is responsible for feeding incoming messages via
    /// [`receive`](Self::receive) or [`route_frame`](Self::route_frame).
    ///
    /// Allocates odd stream IDs (1, 3, 5, …). Use [`from_sink_even`](Self::from_sink_even)
    /// for the server-initiated side of a bidirectional connection.
    #[cfg(all(feature = "server", target_arch = "wasm32"))]
    pub fn from_sink(sink: impl crate::server::WsSink + Clone + 'static) -> Self {
        let send_fn: SendFn = std::rc::Rc::new(move |data: Vec<u8>| {
            let sink = sink.clone();
            Box::pin(async move { sink.send(data).await })
        });
        Self::new(send_fn)
    }

    /// Like [`from_sink`](Self::from_sink) but allocates **even** stream IDs
    /// (2, 4, 6, …) for the server-initiated side of a bidirectional connection.
    #[cfg(all(feature = "server", target_arch = "wasm32"))]
    pub fn from_sink_even(sink: impl crate::server::WsSink + Clone + 'static) -> Self {
        let send_fn: SendFn = std::rc::Rc::new(move |data: Vec<u8>| {
            let sink = sink.clone();
            Box::pin(async move { sink.send(data).await })
        });
        Self::new_even(send_fn)
    }

    /// Notify the channel that the underlying transport has closed.
    ///
    /// This wakes all pending RPC receivers with [`Error::Closed`]. Embedders
    /// using custom transports should call this when their WebSocket or
    /// equivalent transport reaches EOF or a terminal error state.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn transport_closed(&self) {
        self.inner.streams.lock().await.clear();
    }

    #[cfg(target_arch = "wasm32")]
    pub async fn transport_closed(&self) {
        self.inner.streams.borrow_mut().clear();
    }

    /// Perform a unary RPC call.
    pub async fn unary<Req: Message, Resp: Message + Default>(
        &self,
        method: u8,
        req: &Req,
    ) -> Result<Resp, Error> {
        let span = tracing::info_span!(
            "ws_mux.call",
            stream_id = tracing::field::Empty,
            method,
            kind = "unary",
        );

        async {
            let (stream_id, mut rx) = self.alloc_and_register_stream().await?;
            tracing::Span::current().record("stream_id", stream_id);

            // Send OPEN|END with method index + trace context + encoded request.
            let trace_ctx = TraceContext::current();
            let payload = frame::build_open_payload(method, &trace_ctx, &req.encode_to_vec());
            let frame = Frame {
                stream_id,
                flags: flags::OPEN | flags::END,
                payload,
            };
            if let Err(e) = self.send_raw(frame.encode()).await {
                let _ = self.deregister_stream(stream_id).await;
                return Err(e);
            }

            // Wait for the response frame(s).
            let resp = recv_unary_response::<Resp>(&mut rx).await;
            let _ = self.deregister_stream(stream_id).await;
            resp
        }
        .instrument(span)
        .await
    }

    /// Initiate a server-streaming RPC call.
    ///
    /// Returns a [`Streaming`] that yields decoded response messages.
    pub async fn server_streaming<Req: Message>(
        &self,
        method: u8,
        req: &Req,
    ) -> Result<Streaming, Error> {
        let span = tracing::info_span!(
            "ws_mux.call",
            stream_id = tracing::field::Empty,
            method,
            kind = "server_streaming",
        );

        async {
            let (stream_id, rx) = self.alloc_and_register_stream().await?;
            tracing::Span::current().record("stream_id", stream_id);

            // Send OPEN|END with method index + trace context + encoded request.
            let trace_ctx = TraceContext::current();
            let payload = frame::build_open_payload(method, &trace_ctx, &req.encode_to_vec());
            let frame = Frame {
                stream_id,
                flags: flags::OPEN | flags::END,
                payload,
            };
            if let Err(e) = self.send_raw(frame.encode()).await {
                let _ = self.deregister_stream(stream_id).await;
                return Err(e);
            }

            Ok(Streaming {
                channel: self.clone(),
                stream_id,
                rx,
                ended: false,
            })
        }
        .instrument(span)
        .await
    }

    /// Initiate a client-streaming RPC call.
    ///
    /// Returns a [`StreamingSender`] for sending request messages and a
    /// [`ResponseFuture`] for awaiting the single response.
    pub async fn client_streaming(
        &self,
        method: u8,
    ) -> Result<(StreamingSender, ResponseFuture), Error> {
        let span = tracing::info_span!(
            "ws_mux.call",
            stream_id = tracing::field::Empty,
            method,
            kind = "client_streaming",
        );

        async {
            let (stream_id, rx) = self.alloc_and_register_stream().await?;
            tracing::Span::current().record("stream_id", stream_id);

            // Send OPEN (without END) with method index + trace context and empty payload.
            let trace_ctx = TraceContext::current();
            let payload = frame::build_open_payload(method, &trace_ctx, &[]);
            let frame = Frame {
                stream_id,
                flags: flags::OPEN,
                payload,
            };
            if let Err(e) = self.send_raw(frame.encode()).await {
                let _ = self.deregister_stream(stream_id).await;
                return Err(e);
            }

            let (done_tx, done_rx) = oneshot::channel();

            let sender = StreamingSender {
                channel: self.clone(),
                stream_id,
                done: Some(done_tx),
            };

            let resp_future = ResponseFuture {
                channel: self.clone(),
                stream_id,
                rx,
                sender_done: Some(done_rx),
            };

            Ok((sender, resp_future))
        }
        .instrument(span)
        .await
    }
}

/// Receive a unary response: collect DATA frames until END or RST.
async fn recv_unary_response<Resp: Message + Default>(
    rx: &mut mpsc::Receiver<Frame>,
) -> Result<Resp, Error> {
    let mut buf = Vec::new();
    loop {
        let frame = rx.recv().await.ok_or(Error::Closed)?;

        if frame.is_rst() {
            let (code, msg) = frame::parse_rst_payload(&frame.payload)?;
            return Err(Error::Status {
                code,
                message: msg.to_owned(),
            });
        }

        if frame.is_open() {
            return Err(Error::Protocol(
                "received OPEN frame on unary response stream".into(),
            ));
        }

        if frame.is_data() {
            if buf.len().saturating_add(frame.payload.len()) > MAX_UNARY_RESPONSE_PAYLOAD {
                return Err(Error::Protocol(format!(
                    "unary response exceeded max payload of {MAX_UNARY_RESPONSE_PAYLOAD} bytes"
                )));
            }
            buf.extend_from_slice(&frame.payload);

            if frame.is_end() {
                return Resp::decode(buf.as_slice()).map_err(Error::from);
            }
            continue;
        }

        if frame.is_end() {
            return Err(Error::Protocol(
                "received END without DATA on unary response stream".into(),
            ));
        }

        return Err(Error::Protocol(format!(
            "unexpected unary response frame flags: 0x{:02x}",
            frame.flags
        )));
    }
}

/// A stream of server-sent response messages for a server-streaming RPC.
#[must_use = "dropping Streaming early cancels the RPC"]
pub struct Streaming {
    channel: MuxChannel,
    stream_id: u32,
    rx: mpsc::Receiver<Frame>,
    ended: bool,
}

impl Streaming {
    async fn finish(&mut self) {
        self.ended = true;
        let _ = self.channel.deregister_stream(self.stream_id).await;
    }

    /// Receive the next message from the stream.
    ///
    /// Returns `Ok(None)` when the stream ends normally.
    pub async fn message<T: Message + Default>(&mut self) -> Result<Option<T>, Error> {
        if self.ended {
            return Ok(None);
        }

        loop {
            let frame = self.rx.recv().await.ok_or(Error::Closed)?;

            if frame.is_rst() {
                let (code, msg) = frame::parse_rst_payload(&frame.payload)?;
                let message = msg.to_owned();
                self.finish().await;
                return Err(Error::Status { code, message });
            }

            if frame.is_open() {
                return Err(Error::Protocol(
                    "received OPEN frame on server-stream response stream".into(),
                ));
            }

            if frame.is_data() {
                if frame.is_end() && frame.payload.is_empty() {
                    self.finish().await;
                    return Ok(None);
                }
                if frame.payload.is_empty() {
                    continue;
                }
                let msg = T::decode(frame.payload.as_slice())?;
                if frame.is_end() {
                    self.finish().await;
                }
                return Ok(Some(msg));
            }

            if frame.is_end() {
                return Err(Error::Protocol(
                    "received END without DATA on server-stream response stream".into(),
                ));
            }

            return Err(Error::Protocol(format!(
                "unexpected server-stream response frame flags: 0x{:02x}",
                frame.flags
            )));
        }
    }
}

impl Drop for Streaming {
    fn drop(&mut self) {
        self.channel
            .cleanup_stream_on_drop(self.stream_id, !self.ended);
    }
}

/// Sender for a client-streaming RPC.
#[must_use = "dropping StreamingSender early cancels the RPC"]
pub struct StreamingSender {
    channel: MuxChannel,
    stream_id: u32,
    done: Option<oneshot::Sender<()>>,
}

impl StreamingSender {
    /// Send a request message on the stream.
    pub async fn send<T: Message>(&self, msg: &T) -> Result<(), Error> {
        let frame = Frame {
            stream_id: self.stream_id,
            flags: flags::DATA,
            payload: msg.encode_to_vec(),
        };
        self.channel.send_raw(frame.encode()).await
    }

    /// Close the client side of the stream (sends END).
    pub async fn close(mut self) -> Result<(), Error> {
        let frame = Frame {
            stream_id: self.stream_id,
            flags: flags::DATA | flags::END,
            payload: vec![],
        };
        self.channel.send_raw(frame.encode()).await?;
        if let Some(done) = self.done.take() {
            let _ = done.send(());
        }
        Ok(())
    }
}

impl Drop for StreamingSender {
    fn drop(&mut self) {
        if let Some(done) = self.done.take() {
            let _ = done.send(());
            self.channel.cleanup_stream_on_drop(self.stream_id, true);
        }
    }
}

/// Future that resolves to the single response of a client-streaming RPC.
#[must_use = "dropping ResponseFuture early cancels the RPC"]
pub struct ResponseFuture {
    channel: MuxChannel,
    stream_id: u32,
    rx: mpsc::Receiver<Frame>,
    sender_done: Option<oneshot::Receiver<()>>,
}

impl ResponseFuture {
    /// Await the response. The sender must be closed first (via
    /// [`StreamingSender::close`]).
    pub async fn response<T: Message + Default>(mut self) -> Result<T, Error> {
        // Wait for the sender to signal it's done.
        if let Some(done) = self.sender_done.take() {
            let _ = done.await;
        }

        let result = recv_unary_response::<T>(&mut self.rx).await;
        let _ = self.channel.deregister_stream(self.stream_id).await;
        result
    }
}

impl Drop for ResponseFuture {
    fn drop(&mut self) {
        self.channel.cleanup_stream_on_drop(self.stream_id, true);
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use prost::Message;
    use std::sync::Arc;

    #[derive(Clone, PartialEq, Message)]
    struct TestMsg {
        #[prost(uint32, tag = "1")]
        value: u32,
    }

    #[tokio::test]
    async fn unary_send_failure_deregisters_stream() {
        let send_fn: SendFn = Arc::new(|_data: Vec<u8>| Box::pin(async { Err(Error::Closed) }));
        let channel = MuxChannel::new(send_fn);
        let req = TestMsg { value: 1 };

        let result: Result<TestMsg, Error> = channel.unary(0, &req).await;
        assert!(matches!(result, Err(Error::Closed)));
        assert!(channel.inner.streams.lock().await.is_empty());
    }

    #[tokio::test]
    async fn close_all_streams_wakes_pending_unary() {
        let send_fn: SendFn = Arc::new(|_data: Vec<u8>| Box::pin(async { Ok(()) }));
        let channel = MuxChannel::new(send_fn);
        let req = TestMsg { value: 42 };
        let ch = channel.clone();

        let call = tokio::spawn(async move {
            let result: Result<TestMsg, Error> = ch.unary(0, &req).await;
            result
        });

        for _ in 0..100 {
            if !channel.inner.streams.lock().await.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
        channel.transport_closed().await;

        let result = call.await.expect("task join");
        assert!(matches!(result, Err(Error::Closed)));
        assert!(channel.inner.streams.lock().await.is_empty());
    }

    #[tokio::test]
    async fn dropping_response_future_deregisters_stream() {
        let send_fn: SendFn = Arc::new(|_data: Vec<u8>| Box::pin(async { Ok(()) }));
        let channel = MuxChannel::new(send_fn);

        let (sender, resp) = channel.client_streaming(2).await.expect("open stream");
        drop(sender);
        drop(resp);

        for _ in 0..100 {
            if channel.inner.streams.lock().await.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(channel.inner.streams.lock().await.is_empty());
    }

    #[tokio::test]
    async fn dropping_streaming_sender_sends_rst() {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let send_fn: SendFn = {
            let sent = sent.clone();
            Arc::new(move |data: Vec<u8>| {
                let sent = sent.clone();
                Box::pin(async move {
                    let frame = Frame::decode(&data)?;
                    sent.lock().await.push(frame);
                    Ok(())
                })
            })
        };
        let channel = MuxChannel::new(send_fn);

        let (sender, _resp) = channel.client_streaming(2).await.expect("open stream");
        drop(sender);

        for _ in 0..100 {
            if sent.lock().await.iter().any(Frame::is_rst) {
                break;
            }
            tokio::task::yield_now().await;
        }

        assert!(sent.lock().await.iter().any(Frame::is_rst));
    }

    #[tokio::test]
    async fn dropping_response_future_sends_rst() {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let send_fn: SendFn = {
            let sent = sent.clone();
            Arc::new(move |data: Vec<u8>| {
                let sent = sent.clone();
                Box::pin(async move {
                    let frame = Frame::decode(&data)?;
                    sent.lock().await.push(frame);
                    Ok(())
                })
            })
        };
        let channel = MuxChannel::new(send_fn);

        let (sender, resp) = channel.client_streaming(2).await.expect("open stream");
        sender.close().await.expect("close stream");
        drop(resp);

        for _ in 0..100 {
            if sent.lock().await.iter().any(Frame::is_rst) {
                break;
            }
            tokio::task::yield_now().await;
        }

        assert!(sent.lock().await.iter().any(Frame::is_rst));
    }

    #[tokio::test]
    async fn dropping_streaming_sends_rst() {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let send_fn: SendFn = {
            let sent = sent.clone();
            Arc::new(move |data: Vec<u8>| {
                let sent = sent.clone();
                Box::pin(async move {
                    sent.lock().await.push(Frame::decode(&data)?);
                    Ok(())
                })
            })
        };
        let channel = MuxChannel::new(send_fn);
        let (stream_id, rx) = channel
            .alloc_and_register_stream()
            .await
            .expect("register stream");

        let stream = Streaming {
            channel: channel.clone(),
            stream_id,
            rx,
            ended: false,
        };
        drop(stream);

        for _ in 0..100 {
            if sent.lock().await.iter().any(Frame::is_rst) {
                break;
            }
            tokio::task::yield_now().await;
        }

        assert!(sent.lock().await.iter().any(Frame::is_rst));
    }

    #[tokio::test]
    async fn recv_unary_rejects_open_response_frame() {
        let (tx, mut rx) = mpsc::channel(4);
        tx.send(Frame {
            stream_id: 1,
            flags: flags::OPEN | flags::END,
            payload: frame::build_open_payload(0, &TraceContext::default(), &[]),
        })
        .await
        .expect("send test frame");
        drop(tx);

        let result: Result<TestMsg, Error> = recv_unary_response(&mut rx).await;
        assert!(matches!(result, Err(Error::Protocol(_))));
    }

    #[tokio::test]
    async fn streaming_message_rejects_unexpected_flags() {
        let send_fn: SendFn = Arc::new(|_data: Vec<u8>| Box::pin(async { Ok(()) }));
        let channel = MuxChannel::new(send_fn);
        let (tx, rx) = mpsc::channel(4);
        tx.send(Frame {
            stream_id: 1,
            flags: flags::END,
            payload: vec![1],
        })
        .await
        .expect("send test frame");
        drop(tx);

        let mut stream = Streaming {
            channel,
            stream_id: 1,
            rx,
            ended: false,
        };
        let result: Result<Option<TestMsg>, Error> = stream.message().await;
        assert!(matches!(result, Err(Error::Protocol(_))));
    }

    #[tokio::test]
    async fn streaming_end_deregisters_stream_immediately() {
        let send_fn: SendFn = Arc::new(|_data: Vec<u8>| Box::pin(async { Ok(()) }));
        let channel = MuxChannel::new(send_fn);
        let (stream_id, rx) = channel
            .alloc_and_register_stream()
            .await
            .expect("register stream");

        let tx = channel
            .inner
            .streams
            .lock()
            .await
            .get(&stream_id)
            .cloned()
            .expect("stream sender");

        tx.send(Frame {
            stream_id,
            flags: flags::DATA,
            payload: TestMsg { value: 7 }.encode_to_vec(),
        })
        .await
        .expect("send data frame");
        tx.send(Frame {
            stream_id,
            flags: flags::DATA | flags::END,
            payload: vec![],
        })
        .await
        .expect("send end frame");

        let mut stream = Streaming {
            channel: channel.clone(),
            stream_id,
            rx,
            ended: false,
        };

        let msg: TestMsg = stream
            .message()
            .await
            .expect("first message result")
            .expect("first message payload");
        assert_eq!(msg.value, 7);
        assert!(channel.inner.streams.lock().await.contains_key(&stream_id));

        let end: Option<TestMsg> = stream.message().await.expect("stream end");
        assert!(end.is_none());
        assert!(!channel.inner.streams.lock().await.contains_key(&stream_id));
    }

    #[tokio::test]
    async fn full_stream_queue_does_not_block_other_routes() {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let send_fn: SendFn = {
            let sent = sent.clone();
            Arc::new(move |data: Vec<u8>| {
                let sent = sent.clone();
                Box::pin(async move {
                    sent.lock().await.push(Frame::decode(&data)?);
                    Ok(())
                })
            })
        };
        let channel = MuxChannel::new(send_fn);
        let (slow_stream_id, _slow_rx) = channel
            .alloc_and_register_stream()
            .await
            .expect("register slow stream");
        let (fast_stream_id, mut fast_rx) = channel
            .alloc_and_register_stream()
            .await
            .expect("register fast stream");

        for value in 0..STREAM_QUEUE_CAPACITY {
            channel
                .route_frame(Frame {
                    stream_id: slow_stream_id,
                    flags: flags::DATA,
                    payload: TestMsg {
                        value: value as u32,
                    }
                    .encode_to_vec(),
                })
                .await;
        }

        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            channel.route_frame(Frame {
                stream_id: slow_stream_id,
                flags: flags::DATA,
                payload: TestMsg { value: 999 }.encode_to_vec(),
            }),
        )
        .await
        .expect("slow stream overflow should not block routing");

        channel
            .route_frame(Frame {
                stream_id: fast_stream_id,
                flags: flags::DATA | flags::END,
                payload: TestMsg { value: 7 }.encode_to_vec(),
            })
            .await;

        let fast_frame = tokio::time::timeout(std::time::Duration::from_secs(1), fast_rx.recv())
            .await
            .expect("fast stream should still receive frames")
            .expect("fast frame");
        let fast_msg = TestMsg::decode(fast_frame.payload.as_slice()).expect("decode fast message");

        assert_eq!(fast_msg.value, 7);
        assert!(
            !channel
                .inner
                .streams
                .lock()
                .await
                .contains_key(&slow_stream_id)
        );
        assert!(sent.lock().await.iter().any(|frame| {
            frame.stream_id == slow_stream_id
                && frame.is_rst()
                && frame::parse_rst_payload(&frame.payload)
                    .map(|(code, msg)| {
                        code == error::code::CANCELLED && msg == "response stream queue full"
                    })
                    .unwrap_or(false)
        }));
    }
}
