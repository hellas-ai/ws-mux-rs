#![cfg(all(feature = "client", feature = "server"))]

use std::sync::Arc;

use prost::Message;
use tokio::sync::mpsc;
use ws_mux::client::{MuxChannel, SendFn};
use ws_mux::error::{self, Error};
use ws_mux::frame::{self, Frame, flags};
use ws_mux::server::{
    ServerSink, ServiceDispatch, StreamState, WsSink, handle_bidi_frame, handle_frame,
};

// ---------------------------------------------------------------------------
// Test protobuf messages — hand-rolled minimal prost Message impls
// ---------------------------------------------------------------------------

#[derive(Clone, Default, Debug, PartialEq)]
struct EchoRequest {
    value: u32,
}

impl Message for EchoRequest {
    fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut) {
        prost::encoding::uint32::encode(1, &self.value, buf);
    }
    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut impl prost::bytes::Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError> {
        if tag == 1 {
            prost::encoding::uint32::merge(wire_type, &mut self.value, buf, ctx)
        } else {
            prost::encoding::skip_field(wire_type, tag, buf, ctx)
        }
    }
    fn encoded_len(&self) -> usize {
        prost::encoding::uint32::encoded_len(1, &self.value)
    }
    fn clear(&mut self) {
        self.value = 0;
    }
}

#[derive(Clone, Default, Debug, PartialEq)]
struct EchoResponse {
    value: u32,
}

impl Message for EchoResponse {
    fn encode_raw(&self, buf: &mut impl prost::bytes::BufMut) {
        prost::encoding::uint32::encode(1, &self.value, buf);
    }
    fn merge_field(
        &mut self,
        tag: u32,
        wire_type: prost::encoding::WireType,
        buf: &mut impl prost::bytes::Buf,
        ctx: prost::encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError> {
        if tag == 1 {
            prost::encoding::uint32::merge(wire_type, &mut self.value, buf, ctx)
        } else {
            prost::encoding::skip_field(wire_type, tag, buf, ctx)
        }
    }
    fn encoded_len(&self) -> usize {
        prost::encoding::uint32::encoded_len(1, &self.value)
    }
    fn clear(&mut self) {
        self.value = 0;
    }
}

// ---------------------------------------------------------------------------
// In-process WebSocket pair using mpsc channels
// ---------------------------------------------------------------------------

fn ws_pair() -> (
    TestSink,
    mpsc::UnboundedReceiver<Vec<u8>>,
    TestSink,
    mpsc::UnboundedReceiver<Vec<u8>>,
) {
    let (a_tx, a_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let (b_tx, b_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    (TestSink(a_tx), a_rx, TestSink(b_tx), b_rx)
}

#[derive(Clone)]
struct TestSink(mpsc::UnboundedSender<Vec<u8>>);

impl WsSink for TestSink {
    async fn send(&self, data: Vec<u8>) -> Result<(), Error> {
        self.0.send(data).map_err(|_| Error::Closed)
    }
}

fn make_send_fn(sink: TestSink) -> SendFn {
    Arc::new(move |data: Vec<u8>| {
        let sink = sink.clone();
        Box::pin(async move { WsSink::send(&sink, data).await })
    })
}

// ---------------------------------------------------------------------------
// Test service: method 0 = echo (unary), method 1 = count (server-streaming),
//               method 2 = sum (client-streaming)
// ---------------------------------------------------------------------------

const METHOD_ECHO: u8 = 0;
const METHOD_COUNT: u8 = 1;
const METHOD_SUM: u8 = 2;
const METHOD_COUNT_ITEMS: u8 = 3;

#[derive(Clone)]
struct TestService;

impl ServiceDispatch for TestService {
    async fn dispatch(
        &self,
        stream_id: u32,
        method_index: u8,
        payload: &[u8],
        sink: &dyn ServerSink,
    ) -> Result<(), Error> {
        match method_index {
            METHOD_ECHO => {
                let req = EchoRequest::decode(payload)?;
                let resp = EchoResponse { value: req.value };
                let frame = Frame {
                    stream_id,
                    flags: flags::DATA | flags::END,
                    payload: resp.encode_to_vec(),
                };
                sink.send_frame(frame).await?;
            }
            METHOD_COUNT => {
                // Server-streaming: respond with `value` messages counting 1..=req.value
                let req = EchoRequest::decode(payload)?;
                for i in 1..=req.value {
                    let resp = EchoResponse { value: i };
                    let frame = Frame {
                        stream_id,
                        flags: flags::DATA,
                        payload: resp.encode_to_vec(),
                    };
                    sink.send_frame(frame).await?;
                }
                // End-of-stream marker
                let frame = Frame {
                    stream_id,
                    flags: flags::DATA | flags::END,
                    payload: vec![],
                };
                sink.send_frame(frame).await?;
            }
            METHOD_SUM => {
                // Client-streaming: payload is accumulated encoded EchoRequests.
                // Sum all values and return.
                let mut total = 0u32;
                let mut cursor = payload;
                while !cursor.is_empty() {
                    let req = EchoRequest::decode_length_delimited(&mut cursor)?;
                    total += req.value;
                }
                let resp = EchoResponse { value: total };
                let frame = Frame {
                    stream_id,
                    flags: flags::DATA | flags::END,
                    payload: resp.encode_to_vec(),
                };
                sink.send_frame(frame).await?;
            }
            METHOD_COUNT_ITEMS => {
                // Client-streaming: count how many messages were sent.
                let mut count = 0u32;
                let mut cursor = payload;
                while !cursor.is_empty() {
                    let _ = EchoRequest::decode_length_delimited(&mut cursor)?;
                    count += 1;
                }
                let frame = Frame {
                    stream_id,
                    flags: flags::DATA | flags::END,
                    payload: EchoResponse { value: count }.encode_to_vec(),
                };
                sink.send_frame(frame).await?;
            }
            _ => {
                let frame = Frame {
                    stream_id,
                    flags: flags::RST,
                    payload: frame::build_rst_payload(error::code::UNIMPLEMENTED, "unknown method"),
                };
                sink.send_frame(frame).await?;
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Run the server side: read frames from client_rx, dispatch, send responses to client via server_sink.
async fn run_server(
    service: TestService,
    server_sink: TestSink,
    mut client_rx: mpsc::UnboundedReceiver<Vec<u8>>,
) {
    let mut streams = StreamState::default();
    while let Some(data) = client_rx.recv().await {
        let service = service.clone();
        let sink = server_sink.clone();

        let frame = match Frame::decode(&data) {
            Ok(f) => f,
            Err(_) => continue,
        };

        if frame.is_open() && frame.is_end() {
            // Unary / server-streaming — dispatch in a separate task.
            let (method_index, payload) = match frame::parse_open_payload(&frame.payload) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let payload = payload.to_vec();
            tokio::spawn(async move {
                let _ = service
                    .dispatch(frame.stream_id, method_index, &payload, &sink)
                    .await;
            });
        } else {
            // Client-streaming: use handle_frame for state management.
            let _ = handle_frame(&service, &data, &sink, &mut streams).await;
        }
    }
}

#[tokio::test]
async fn unary_echo() {
    let (client_sink, client_rx, server_sink, server_rx) = ws_pair();

    let channel = MuxChannel::new(make_send_fn(client_sink));

    // Spawn background reader for client.
    let channel_clone = channel.clone();
    tokio::spawn(async move {
        let mut rx = server_rx;
        while let Some(data) = rx.recv().await {
            if let Ok(frame) = Frame::decode(&data) {
                channel_clone.route_frame(frame).await;
            }
        }
    });

    // Spawn server.
    tokio::spawn(run_server(TestService, server_sink, client_rx));

    // Send unary echo.
    let resp: EchoResponse = channel
        .unary(METHOD_ECHO, &EchoRequest { value: 42 })
        .await
        .unwrap();
    assert_eq!(resp.value, 42);
}

#[tokio::test]
async fn server_streaming_count() {
    let (client_sink, client_rx, server_sink, server_rx) = ws_pair();

    let channel = MuxChannel::new(make_send_fn(client_sink));

    let channel_clone = channel.clone();
    tokio::spawn(async move {
        let mut rx = server_rx;
        while let Some(data) = rx.recv().await {
            if let Ok(frame) = Frame::decode(&data) {
                channel_clone.route_frame(frame).await;
            }
        }
    });

    tokio::spawn(run_server(TestService, server_sink, client_rx));

    // Request count(5) — should get 1, 2, 3, 4, 5 then None.
    let mut stream = channel
        .server_streaming(METHOD_COUNT, &EchoRequest { value: 5 })
        .await
        .unwrap();

    for expected in 1..=5 {
        let msg: EchoResponse = stream.message().await.unwrap().unwrap();
        assert_eq!(msg.value, expected);
    }

    let end: Option<EchoResponse> = stream.message().await.unwrap();
    assert!(end.is_none());
}

#[tokio::test]
async fn unimplemented_method() {
    let (client_sink, client_rx, server_sink, server_rx) = ws_pair();

    let channel = MuxChannel::new(make_send_fn(client_sink));

    let channel_clone = channel.clone();
    tokio::spawn(async move {
        let mut rx = server_rx;
        while let Some(data) = rx.recv().await {
            if let Ok(frame) = Frame::decode(&data) {
                channel_clone.route_frame(frame).await;
            }
        }
    });

    tokio::spawn(run_server(TestService, server_sink, client_rx));

    let result: Result<EchoResponse, _> = channel.unary(99, &EchoRequest { value: 1 }).await;
    match result {
        Err(Error::Status { code, .. }) => assert_eq!(code, error::code::UNIMPLEMENTED),
        other => panic!("expected Status error, got: {other:?}"),
    }
}

#[tokio::test]
async fn client_streaming_sum() {
    let (client_sink, client_rx, server_sink, server_rx) = ws_pair();

    let channel = MuxChannel::new(make_send_fn(client_sink));

    let channel_clone = channel.clone();
    tokio::spawn(async move {
        let mut rx = server_rx;
        while let Some(data) = rx.recv().await {
            if let Ok(frame) = Frame::decode(&data) {
                channel_clone.route_frame(frame).await;
            }
        }
    });

    tokio::spawn(run_server(TestService, server_sink, client_rx));

    // Open a client-streaming RPC (method 2 = sum).
    let (sender, resp_future) = channel.client_streaming(METHOD_SUM).await.unwrap();

    sender.send(&EchoRequest { value: 10 }).await.unwrap();
    sender.send(&EchoRequest { value: 20 }).await.unwrap();
    sender.send(&EchoRequest { value: 30 }).await.unwrap();
    sender.close().await.unwrap();

    let resp: EchoResponse = resp_future.response().await.unwrap();
    assert_eq!(resp.value, 60);
}

#[tokio::test]
async fn client_streaming_count_items_no_phantom() {
    let (client_sink, client_rx, server_sink, server_rx) = ws_pair();

    let channel = MuxChannel::new(make_send_fn(client_sink));

    let channel_clone = channel.clone();
    tokio::spawn(async move {
        let mut rx = server_rx;
        while let Some(data) = rx.recv().await {
            if let Ok(frame) = Frame::decode(&data) {
                channel_clone.route_frame(frame).await;
            }
        }
    });

    tokio::spawn(run_server(TestService, server_sink, client_rx));

    let (sender, resp_future) = channel.client_streaming(METHOD_COUNT_ITEMS).await.unwrap();
    sender.send(&EchoRequest { value: 10 }).await.unwrap();
    sender.send(&EchoRequest { value: 20 }).await.unwrap();
    sender.send(&EchoRequest { value: 30 }).await.unwrap();
    sender.close().await.unwrap();

    let resp: EchoResponse = resp_future.response().await.unwrap();
    assert_eq!(resp.value, 3);
}

#[tokio::test]
async fn concurrent_unary() {
    let (client_sink, client_rx, server_sink, server_rx) = ws_pair();

    let channel = MuxChannel::new(make_send_fn(client_sink));

    let channel_clone = channel.clone();
    tokio::spawn(async move {
        let mut rx = server_rx;
        while let Some(data) = rx.recv().await {
            if let Ok(frame) = Frame::decode(&data) {
                channel_clone.route_frame(frame).await;
            }
        }
    });

    tokio::spawn(run_server(TestService, server_sink, client_rx));

    // Fire off 10 concurrent unary RPCs.
    let mut handles = vec![];
    for i in 0..10u32 {
        let ch = channel.clone();
        handles.push(tokio::spawn(async move {
            let resp: EchoResponse = ch
                .unary(METHOD_ECHO, &EchoRequest { value: i })
                .await
                .unwrap();
            assert_eq!(resp.value, i);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

#[tokio::test]
async fn bidi_routes_remote_client_streaming_frames() {
    // Side A: bidi endpoint (server + outbound client channel using even stream IDs).
    // Side B: remote peer sending a client-streaming RPC on odd stream ID 1.
    let (a_sink, mut a_rx, b_sink, mut b_rx) = ws_pair();
    let channel_a = MuxChannel::new_even(make_send_fn(a_sink.clone()));

    let service = TestService;
    let sink = a_sink.clone();
    let channel = channel_a.clone();
    tokio::spawn(async move {
        let mut streams = StreamState::default();
        while let Some(data) = b_rx.recv().await {
            let _ = handle_bidi_frame(&data, &service, &channel, &sink, &mut streams).await;
        }
    });

    let open = Frame {
        stream_id: 1,
        flags: flags::OPEN,
        payload: frame::build_open_payload(METHOD_COUNT_ITEMS, &[]),
    };
    WsSink::send(&b_sink, open.encode()).await.unwrap();

    let data1 = Frame {
        stream_id: 1,
        flags: flags::DATA,
        payload: EchoRequest { value: 10 }.encode_to_vec(),
    };
    WsSink::send(&b_sink, data1.encode()).await.unwrap();

    let data2 = Frame {
        stream_id: 1,
        flags: flags::DATA,
        payload: EchoRequest { value: 20 }.encode_to_vec(),
    };
    WsSink::send(&b_sink, data2.encode()).await.unwrap();

    let end = Frame {
        stream_id: 1,
        flags: flags::DATA | flags::END,
        payload: vec![],
    };
    WsSink::send(&b_sink, end.encode()).await.unwrap();

    let response = tokio::time::timeout(std::time::Duration::from_secs(1), a_rx.recv())
        .await
        .expect("timed out waiting for bidi response")
        .expect("connection closed before bidi response");
    let frame = Frame::decode(&response).expect("valid response frame");

    assert_eq!(frame.stream_id, 1);
    assert!(frame.is_data() && frame.is_end());

    let resp = EchoResponse::decode(frame.payload.as_slice()).expect("valid response payload");
    assert_eq!(resp.value, 2);
}

#[tokio::test]
async fn transport_closed_wakes_pending_unary_for_custom_transports() {
    let (sent_tx, sent_rx) = tokio::sync::oneshot::channel::<()>();
    let sent_tx = Arc::new(tokio::sync::Mutex::new(Some(sent_tx)));
    let send_fn: SendFn = {
        let sent_tx = sent_tx.clone();
        Arc::new(move |_data: Vec<u8>| {
            let sent_tx = sent_tx.clone();
            Box::pin(async move {
                if let Some(tx) = sent_tx.lock().await.take() {
                    let _ = tx.send(());
                }
                Ok(())
            })
        })
    };

    let channel = MuxChannel::new(send_fn);
    let ch = channel.clone();

    let call = tokio::spawn(async move {
        let result: Result<EchoResponse, Error> =
            ch.unary(METHOD_ECHO, &EchoRequest { value: 42 }).await;
        result
    });

    sent_rx.await.expect("request send should be observed");
    channel.transport_closed().await;

    let result = call.await.expect("join unary task");
    assert!(matches!(result, Err(Error::Closed)));
}
