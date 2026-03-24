#![cfg(all(target_arch = "wasm32", feature = "client", feature = "wasm-client"))]

use std::cell::RefCell;
use std::rc::Rc;

use futures_channel::oneshot;
use prost::Message;
use wasm_bindgen::JsValue;
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::wasm_bindgen_test;
use ws_mux::error;
use ws_mux::frame;
use ws_mux::{Error, Frame, MuxChannel, SendFn, flags};

#[derive(Clone, PartialEq, Message)]
struct TestMsg {
    #[prost(uint32, tag = "1")]
    value: u32,
}

fn recording_channel() -> (MuxChannel, Rc<RefCell<Vec<Frame>>>) {
    let sent = Rc::new(RefCell::new(Vec::new()));
    let send_fn: SendFn = {
        let sent = sent.clone();
        Rc::new(move |data: Vec<u8>| {
            let sent = sent.clone();
            Box::pin(async move {
                sent.borrow_mut().push(Frame::decode(&data)?);
                Ok(())
            })
        })
    };
    (MuxChannel::new(send_fn), sent)
}

async fn flush_microtasks() {
    let _ = JsFuture::from(js_sys::Promise::resolve(&JsValue::NULL))
        .await
        .expect("flush microtasks");
}

async fn wait_for_sent_frames(sent: &Rc<RefCell<Vec<Frame>>>, min_len: usize) {
    for _ in 0..8 {
        if sent.borrow().len() >= min_len {
            return;
        }
        flush_microtasks().await;
    }
    panic!("timed out waiting for {min_len} sent frames");
}

#[wasm_bindgen_test]
async fn receive_sync_routes_pending_unary_response() {
    let (channel, sent) = recording_channel();
    let request = TestMsg { value: 7 };
    let (tx, rx) = oneshot::channel();
    let ch = channel.clone();

    wasm_bindgen_futures::spawn_local(async move {
        let result: Result<TestMsg, Error> = ch.unary(3, &request).await;
        let _ = tx.send(result);
    });

    wait_for_sent_frames(&sent, 1).await;

    let request_frame = sent.borrow()[0].clone();
    assert_eq!(request_frame.stream_id, 1);
    assert!(request_frame.is_open());
    assert!(request_frame.is_end());

    let response = Frame {
        stream_id: request_frame.stream_id,
        flags: flags::DATA | flags::END,
        payload: TestMsg { value: 11 }.encode_to_vec(),
    };
    channel
        .receive_sync(&response.encode())
        .expect("route response");

    let result = rx.await.expect("response task");
    let msg = result.expect("unary result");
    assert_eq!(msg.value, 11);
}

#[wasm_bindgen_test]
async fn receive_sync_overflow_sends_cancellation_rst() {
    let (channel, sent) = recording_channel();
    let request = TestMsg { value: 1 };
    let _stream = channel
        .server_streaming(9, &request)
        .await
        .expect("open server stream");

    wait_for_sent_frames(&sent, 1).await;
    let stream_id = sent.borrow()[0].stream_id;

    for value in 0..128 {
        let frame = Frame {
            stream_id,
            flags: flags::DATA,
            payload: TestMsg { value }.encode_to_vec(),
        };
        channel
            .receive_sync(&frame.encode())
            .expect("route sync frame");
    }

    wait_for_sent_frames(&sent, 2).await;

    assert!(sent.borrow().iter().any(|frame| {
        frame.stream_id == stream_id
            && frame.is_rst()
            && frame::parse_rst_payload(&frame.payload)
                .map(|(code, msg)| {
                    code == error::code::CANCELLED && msg == "response stream queue full"
                })
                .unwrap_or(false)
    }));
}
