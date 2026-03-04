use std::sync::Arc;

use futures_util::SinkExt;
use futures_util::stream::StreamExt;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite;

use crate::client::{MuxChannel, SendFn};
use crate::error::Error;
use crate::frame::Frame;

impl MuxChannel {
    /// Connect to a WebSocket URL and set up the multiplexing channel.
    ///
    /// Spawns a background reader task that routes incoming frames.
    pub async fn connect(url: &str) -> Result<Self, Error> {
        let (ws_stream, _) = tokio_tungstenite::connect_async(url)
            .await
            .map_err(|e| Error::Connect(e.to_string()))?;

        let (write, read) = ws_stream.split();
        let write = Arc::new(Mutex::new(write));

        // Build the send function.
        let write_clone = write.clone();
        let send_fn: SendFn = Arc::new(move |data: Vec<u8>| {
            let write = write_clone.clone();
            Box::pin(async move {
                let msg = tungstenite::Message::Binary(data.into());
                write
                    .lock()
                    .await
                    .send(msg)
                    .await
                    .map_err(|e| Error::Connect(e.to_string()))
            })
        });

        let channel = MuxChannel::new(send_fn);

        // Spawn background reader.
        let channel_clone = channel.clone();
        tokio::spawn(async move {
            let mut read = read;
            while let Some(msg) = read.next().await {
                match msg {
                    Ok(tungstenite::Message::Binary(data)) => match Frame::decode(&data) {
                        Ok(frame) => channel_clone.route_frame(frame).await,
                        Err(e) => {
                            tracing::warn!(error = %e, "bad frame from server");
                        }
                    },
                    Ok(tungstenite::Message::Close(_)) => break,
                    Ok(_) => {} // ignore text, ping, pong
                    Err(e) => {
                        tracing::debug!(error = %e, "ws read error");
                        break;
                    }
                }
            }
            channel_clone.close_all_streams().await;
            tracing::debug!("ws reader exited");
        });

        tracing::info!(url, "ws-mux channel connected");
        Ok(channel)
    }
}
