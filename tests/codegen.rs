#![cfg(feature = "compile")]

/// Integration test that uses ws-mux codegen output for a test service.
use std::sync::Arc;

use tokio::sync::mpsc;
use ws_mux::client::{MuxChannel, SendFn};
use ws_mux::error::Error;
use ws_mux::frame::Frame;
use ws_mux::server::{StreamState, WsSink};

// Include the generated code.
#[allow(dead_code)]
mod pb {
    include!(concat!(env!("OUT_DIR"), "/test.rs"));
}

use pb::test_service_mux;

// ---------------------------------------------------------------------------
// In-process transport
// ---------------------------------------------------------------------------

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
// Service implementation
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct TestServiceImpl;

impl test_service_mux::TestService for TestServiceImpl {
    async fn echo(&self, req: pb::EchoRequest) -> Result<pb::EchoResponse, Error> {
        Ok(pb::EchoResponse { value: req.value })
    }

    type CountUpStream =
        futures_util::stream::Iter<std::vec::IntoIter<Result<pb::CountResponse, Error>>>;

    async fn count_up(&self, req: pb::CountRequest) -> Result<Self::CountUpStream, Error> {
        let items: Vec<Result<pb::CountResponse, Error>> = (1..=req.count)
            .map(|i| Ok(pb::CountResponse { value: i }))
            .collect();
        Ok(futures_util::stream::iter(items))
    }

    async fn sum(&self, messages: Vec<pb::SumItem>) -> Result<pb::SumResponse, Error> {
        let total: u32 = messages.iter().map(|m| m.value).sum();
        Ok(pb::SumResponse { total })
    }
}

// ---------------------------------------------------------------------------
// Server loop (reused from round_trip.rs pattern)
// ---------------------------------------------------------------------------

async fn run_server(server_sink: TestSink, mut client_rx: mpsc::UnboundedReceiver<Vec<u8>>) {
    let dispatcher = test_service_mux::TestServiceDispatcher::new(TestServiceImpl);
    let mut streams = StreamState::default();
    while let Some(data) = client_rx.recv().await {
        let frame = match Frame::decode(&data) {
            Ok(f) => f,
            Err(_) => continue,
        };

        if frame.is_open() && frame.is_end() {
            // Unary / server-streaming — dispatch concurrently.
            let dispatcher = dispatcher.clone();
            let sink = server_sink.clone();
            let data = data.clone();
            let mut streams = StreamState::default();
            tokio::spawn(async move {
                let _ = ws_mux::server::handle_frame(&dispatcher, &data, &sink, &mut streams).await;
            });
        } else {
            // Client-streaming: use handle_frame for state management.
            let _ =
                ws_mux::server::handle_frame(&dispatcher, &data, &server_sink, &mut streams).await;
        }
    }
}

fn ws_pair() -> (
    TestSink,
    mpsc::UnboundedReceiver<Vec<u8>>,
    TestSink,
    mpsc::UnboundedReceiver<Vec<u8>>,
) {
    let (a_tx, a_rx) = mpsc::unbounded_channel();
    let (b_tx, b_rx) = mpsc::unbounded_channel();
    (TestSink(a_tx), a_rx, TestSink(b_tx), b_rx)
}

fn setup() -> (
    test_service_mux::TestServiceClient,
    tokio::task::JoinHandle<()>,
    tokio::task::JoinHandle<()>,
) {
    let (client_sink, client_rx, server_sink, server_rx) = ws_pair();
    let channel = MuxChannel::new(make_send_fn(client_sink));

    let channel_clone = channel.clone();
    let reader = tokio::spawn(async move {
        let mut rx = server_rx;
        while let Some(data) = rx.recv().await {
            if let Ok(frame) = Frame::decode(&data) {
                channel_clone.route_frame(frame).await;
            }
        }
    });

    let server = tokio::spawn(run_server(server_sink, client_rx));

    let client = test_service_mux::TestServiceClient::new(channel);
    (client, reader, server)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn codegen_unary_echo() {
    let (client, _reader, _server) = setup();

    let resp = client.echo(pb::EchoRequest { value: 42 }).await.unwrap();
    assert_eq!(resp.value, 42);
}

#[tokio::test]
async fn codegen_server_streaming() {
    let (client, _reader, _server) = setup();

    let mut stream = client
        .count_up(pb::CountRequest { count: 3 })
        .await
        .unwrap();

    let msg: pb::CountResponse = stream.message().await.unwrap().unwrap();
    assert_eq!(msg.value, 1);
    let msg: pb::CountResponse = stream.message().await.unwrap().unwrap();
    assert_eq!(msg.value, 2);
    let msg: pb::CountResponse = stream.message().await.unwrap().unwrap();
    assert_eq!(msg.value, 3);
    let end: Option<pb::CountResponse> = stream.message().await.unwrap();
    assert!(end.is_none());
}

#[tokio::test]
async fn codegen_client_streaming() {
    let (client, _reader, _server) = setup();

    let (sender, resp_future) = client.sum().await.unwrap();

    sender.send(&pb::SumItem { value: 10 }).await.unwrap();
    sender.send(&pb::SumItem { value: 20 }).await.unwrap();
    sender.send(&pb::SumItem { value: 30 }).await.unwrap();
    sender.close().await.unwrap();

    let resp: pb::SumResponse = resp_future.response().await.unwrap();
    assert_eq!(resp.total, 60);
}
