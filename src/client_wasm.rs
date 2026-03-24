use std::cell::RefCell;
use std::rc::Rc;

use js_sys::Uint8Array;
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;

use crate::client::{MuxChannel, SendFn};
use crate::error::Error;

impl MuxChannel {
    /// Connect to a WebSocket URL from wasm32 and set up the multiplexing channel.
    ///
    /// Uses the browser's WebSocket API. The `onmessage` callback routes
    /// incoming frames to the correct stream.
    pub async fn connect(url: &str) -> Result<Self, Error> {
        let ws = web_sys::WebSocket::new(url).map_err(|e| Error::Connect(format!("{e:?}")))?;
        ws.set_binary_type(web_sys::BinaryType::Arraybuffer);

        // Wait for the socket to reach OPEN state.
        let (open_tx, open_rx) = tokio::sync::oneshot::channel::<Result<(), String>>();
        let open_tx = Rc::new(RefCell::new(Some(open_tx)));

        let on_open = {
            let tx = open_tx.clone();
            Closure::<dyn FnMut()>::new(move || {
                if let Some(tx) = tx.borrow_mut().take() {
                    let _ = tx.send(Ok(()));
                }
            })
        };
        let on_error_connect = {
            let tx = open_tx.clone();
            Closure::<dyn FnMut(web_sys::Event)>::new(move |_: web_sys::Event| {
                if let Some(tx) = tx.borrow_mut().take() {
                    let _ = tx.send(Err("WebSocket connection failed".into()));
                }
            })
        };

        ws.set_onopen(Some(on_open.as_ref().unchecked_ref()));
        ws.set_onerror(Some(on_error_connect.as_ref().unchecked_ref()));

        // Guard: clean up handlers if future is cancelled.
        struct Guard<'a>(&'a web_sys::WebSocket);
        impl Drop for Guard<'_> {
            fn drop(&mut self) {
                self.0.set_onopen(None);
                self.0.set_onerror(None);
            }
        }
        let guard = Guard(&ws);

        let result = open_rx
            .await
            .map_err(|_| Error::Connect("cancelled".into()))?;
        result.map_err(Error::Connect)?;

        // Explicitly drop the guard to clear onopen/onerror — they've already fired.
        drop(guard);

        Self::from_ws(ws)
    }

    /// Build a channel from an already-OPEN [`web_sys::WebSocket`].
    ///
    /// Installs `onmessage` and `onclose` handlers for frame routing.
    pub fn from_ws(ws: web_sys::WebSocket) -> Result<Self, Error> {
        ws.set_binary_type(web_sys::BinaryType::Arraybuffer);
        if ws.ready_state() != web_sys::WebSocket::OPEN {
            return Err(Error::Connect(format!(
                "from_ws requires an OPEN websocket, got ready_state={}",
                ws.ready_state()
            )));
        }

        let ws_clone = ws.clone();
        let send_fn: SendFn = Rc::new(move |data: Vec<u8>| {
            let ws = ws_clone.clone();
            Box::pin(async move {
                let arr = Uint8Array::from(data.as_slice());
                ws.send_with_array_buffer(&arr.buffer())
                    .map_err(|e| Error::Send(format!("ws send: {e:?}")))?;
                Ok(())
            })
        });

        let channel = MuxChannel::new(send_fn);

        // Install onmessage to use the synchronous callback fast-path.
        let channel_clone = channel.clone();
        let on_message = Closure::<dyn FnMut(web_sys::MessageEvent)>::new(
            move |event: web_sys::MessageEvent| {
                let Ok(buf) = event.data().dyn_into::<js_sys::ArrayBuffer>() else {
                    return;
                };
                let data = Uint8Array::new(&buf).to_vec();
                if let Err(e) = channel_clone.receive_sync(&data) {
                    tracing::warn!(error = %e, "bad frame from server (wasm)");
                }
            },
        );
        ws.set_onmessage(Some(on_message.as_ref().unchecked_ref()));
        // Intentional leak: the closure must live as long as the WebSocket connection.
        // The browser will GC both the WebSocket and the closure together when the
        // connection closes. In CF Workers hibernation mode, connect()/from_ws() are
        // not used — callers use from_sink() + receive() instead.
        on_message.forget();

        // Install onclose/onerror for cleanup.
        let on_close_channel = channel.clone();
        let on_close =
            Closure::<dyn FnMut(web_sys::CloseEvent)>::new(move |_: web_sys::CloseEvent| {
                let ch = on_close_channel.clone();
                wasm_bindgen_futures::spawn_local(async move {
                    ch.transport_closed().await;
                });
                tracing::debug!("ws-mux wasm connection closed");
            });
        ws.set_onclose(Some(on_close.as_ref().unchecked_ref()));
        // Intentional leak — same rationale as on_message above.
        on_close.forget();

        let on_error_channel = channel.clone();
        let on_error = Closure::<dyn FnMut(web_sys::Event)>::new(move |_: web_sys::Event| {
            let ch = on_error_channel.clone();
            wasm_bindgen_futures::spawn_local(async move {
                ch.transport_closed().await;
            });
            tracing::debug!("ws-mux wasm connection error");
        });
        ws.set_onerror(Some(on_error.as_ref().unchecked_ref()));
        on_error.forget();

        Ok(channel)
    }
}
