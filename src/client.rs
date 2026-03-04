use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};

#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;

use prost::Message;
use tokio::sync::{Mutex, mpsc, oneshot};

#[cfg(not(target_arch = "wasm32"))]
use crate::error;
use crate::error::Error;
use crate::frame::{self, Frame, flags};

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
    streams: Mutex<HashMap<u32, mpsc::UnboundedSender<Frame>>>,
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
                streams: Mutex::new(HashMap::new()),
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
                streams: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Route an incoming frame to the correct stream receiver.
    ///
    /// Called by the background reader task for each received WebSocket message.
    pub async fn route_frame(&self, frame: Frame) {
        let tx = {
            let streams = self.inner.streams.lock().await;
            streams.get(&frame.stream_id).cloned()
        };
        if let Some(tx) = tx {
            let _ = tx.send(frame);
        } else {
            tracing::debug!(
                stream_id = frame.stream_id,
                "no receiver for frame, dropping"
            );
        }
    }

    /// Decode a raw WebSocket message and route it to the correct stream.
    ///
    /// Convenience wrapper around [`Frame::decode`] + [`route_frame`](Self::route_frame).
    /// Designed for push-based transports such as the Cloudflare Workers
    /// `websocket_message` hibernation callback.
    pub async fn receive(&self, data: &[u8]) -> Result<(), Error> {
        let frame = Frame::decode(data)?;
        self.route_frame(frame).await;
        Ok(())
    }

    /// Allocate a new client-initiated stream ID.
    fn alloc_stream_id(&self) -> u32 {
        self.inner.next_stream_id.fetch_add(2, Ordering::Relaxed)
    }

    /// Return true when `stream_id` was initiated by this channel.
    ///
    /// The channel parity is invariant because IDs always increment by 2.
    #[cfg(feature = "server")]
    pub(crate) fn is_local_stream_id(&self, stream_id: u32) -> bool {
        let parity = self.inner.next_stream_id.load(Ordering::Relaxed) & 1;
        (stream_id & 1) == parity
    }

    /// Register a stream and return the receiver.
    async fn register_stream(&self, stream_id: u32) -> mpsc::UnboundedReceiver<Frame> {
        let (tx, rx) = mpsc::unbounded_channel();
        let mut streams = self.inner.streams.lock().await;
        debug_assert!(
            !streams.contains_key(&stream_id),
            "stream ID {stream_id} collision"
        );
        streams.insert(stream_id, tx);
        rx
    }

    /// Remove a stream from the dispatch table.
    async fn deregister_stream(&self, stream_id: u32) {
        self.inner.streams.lock().await.remove(&stream_id);
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

    /// Drop all active stream senders so waiting receivers wake with `Error::Closed`.
    #[cfg(any(feature = "native-client", feature = "wasm-client", test))]
    pub(crate) async fn close_all_streams(&self) {
        self.inner.streams.lock().await.clear();
    }

    /// Perform a unary RPC call.
    pub async fn unary<Req: Message, Resp: Message + Default>(
        &self,
        method: u8,
        req: &Req,
    ) -> Result<Resp, Error> {
        let stream_id = self.alloc_stream_id();
        let mut rx = self.register_stream(stream_id).await;

        // Send OPEN|END with method index + encoded request.
        let payload = frame::build_open_payload(method, &req.encode_to_vec());
        let frame = Frame {
            stream_id,
            flags: flags::OPEN | flags::END,
            payload,
        };
        if let Err(e) = self.send_raw(frame.encode()).await {
            self.deregister_stream(stream_id).await;
            return Err(e);
        }

        // Wait for the response frame(s).
        let resp = recv_unary_response::<Resp>(&mut rx).await;
        self.deregister_stream(stream_id).await;
        resp
    }

    /// Initiate a server-streaming RPC call.
    ///
    /// Returns a [`Streaming`] that yields decoded response messages.
    pub async fn server_streaming<Req: Message>(
        &self,
        method: u8,
        req: &Req,
    ) -> Result<Streaming, Error> {
        let stream_id = self.alloc_stream_id();
        let rx = self.register_stream(stream_id).await;

        // Send OPEN|END with method index + encoded request.
        let payload = frame::build_open_payload(method, &req.encode_to_vec());
        let frame = Frame {
            stream_id,
            flags: flags::OPEN | flags::END,
            payload,
        };
        if let Err(e) = self.send_raw(frame.encode()).await {
            self.deregister_stream(stream_id).await;
            return Err(e);
        }

        Ok(Streaming {
            channel: self.clone(),
            stream_id,
            rx,
        })
    }

    /// Initiate a client-streaming RPC call.
    ///
    /// Returns a [`StreamingSender`] for sending request messages and a
    /// [`ResponseFuture`] for awaiting the single response.
    pub async fn client_streaming(
        &self,
        method: u8,
    ) -> Result<(StreamingSender, ResponseFuture), Error> {
        let stream_id = self.alloc_stream_id();
        let rx = self.register_stream(stream_id).await;

        // Send OPEN (without END) with method index and empty payload.
        let payload = frame::build_open_payload(method, &[]);
        let frame = Frame {
            stream_id,
            flags: flags::OPEN,
            payload,
        };
        if let Err(e) = self.send_raw(frame.encode()).await {
            self.deregister_stream(stream_id).await;
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
}

/// Receive a unary response: collect DATA frames until END or RST.
async fn recv_unary_response<Resp: Message + Default>(
    rx: &mut mpsc::UnboundedReceiver<Frame>,
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

        if frame.is_data() || frame.is_open() {
            buf.extend_from_slice(&frame.payload);
        }

        if frame.is_end() {
            return Resp::decode(buf.as_slice()).map_err(Error::from);
        }
    }
}

/// A stream of server-sent response messages for a server-streaming RPC.
pub struct Streaming {
    channel: MuxChannel,
    stream_id: u32,
    rx: mpsc::UnboundedReceiver<Frame>,
}

impl Streaming {
    /// Receive the next message from the stream.
    ///
    /// Returns `Ok(None)` when the stream ends normally.
    pub async fn message<T: Message + Default>(&mut self) -> Result<Option<T>, Error> {
        loop {
            let frame = match self.rx.recv().await {
                Some(f) => f,
                None => return Ok(None),
            };

            if frame.is_rst() {
                let (code, msg) = frame::parse_rst_payload(&frame.payload)?;
                return Err(Error::Status {
                    code,
                    message: msg.to_owned(),
                });
            }

            // DATA|END with empty payload = end-of-stream marker
            if frame.is_end() && frame.payload.is_empty() {
                return Ok(None);
            }

            if frame.is_data() || frame.is_open() {
                if frame.payload.is_empty() {
                    continue;
                }
                let msg = T::decode(frame.payload.as_slice())?;
                return Ok(Some(msg));
            }
        }
    }
}

impl Drop for Streaming {
    fn drop(&mut self) {
        let channel = self.channel.clone();
        let stream_id = self.stream_id;
        #[cfg(not(target_arch = "wasm32"))]
        {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    // Notify the server we're no longer interested.
                    let rst = Frame {
                        stream_id,
                        flags: flags::RST,
                        payload: frame::build_rst_payload(error::code::CANCELLED, "cancelled"),
                    };
                    let _ = channel.send_raw(rst.encode()).await;
                    channel.deregister_stream(stream_id).await;
                });
            } else if let Ok(mut streams) = channel.inner.streams.try_lock() {
                streams.remove(&stream_id);
            }
        }
        #[cfg(target_arch = "wasm32")]
        {
            // Single-threaded: try_lock always succeeds outside an await point.
            // No RST on wasm32 — send_raw requires async which we can't
            // reliably do from Drop in all wasm contexts (CF Workers hibernation).
            if let Ok(mut streams) = channel.inner.streams.try_lock() {
                streams.remove(&stream_id);
            }
        }
    }
}

/// Sender for a client-streaming RPC.
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
        }
    }
}

/// Future that resolves to the single response of a client-streaming RPC.
pub struct ResponseFuture {
    channel: MuxChannel,
    stream_id: u32,
    rx: mpsc::UnboundedReceiver<Frame>,
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
        self.channel.deregister_stream(self.stream_id).await;
        result
    }
}

impl Drop for ResponseFuture {
    fn drop(&mut self) {
        let channel = self.channel.clone();
        let stream_id = self.stream_id;
        #[cfg(not(target_arch = "wasm32"))]
        {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    channel.deregister_stream(stream_id).await;
                });
            } else if let Ok(mut streams) = channel.inner.streams.try_lock() {
                streams.remove(&stream_id);
            }
        }
        #[cfg(target_arch = "wasm32")]
        {
            if let Ok(mut streams) = channel.inner.streams.try_lock() {
                streams.remove(&stream_id);
            }
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use prost::Message;

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
        channel.close_all_streams().await;

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
}
