use std::sync::Arc;
use std::time::Duration;

use futures_util::SinkExt;
use futures_util::stream::StreamExt;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite;

use crate::client::{MuxChannel, SendFn};
use crate::error::Error;
use crate::frame::Frame;

const DEFAULT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

async fn send_message<S>(write: &Arc<Mutex<S>>, message: tungstenite::Message) -> Result<(), Error>
where
    S: futures_util::Sink<tungstenite::Message, Error = tungstenite::Error> + Unpin,
{
    write
        .lock()
        .await
        .send(message)
        .await
        .map_err(|e| Error::Send(e.to_string()))
}

impl MuxChannel {
    /// Connect to a WebSocket URL and set up the multiplexing channel.
    ///
    /// Spawns a background reader task that routes incoming frames and
    /// enables periodic WebSocket ping keepalives.
    pub async fn connect(url: &str) -> Result<Self, Error> {
        Self::connect_inner(url, DEFAULT_KEEPALIVE_INTERVAL).await
    }

    /// Like [`connect`](Self::connect), but overrides the keepalive ping interval.
    pub async fn connect_with_keepalive(url: &str, ping_interval: Duration) -> Result<Self, Error> {
        Self::connect_inner(url, ping_interval).await
    }

    async fn connect_inner(url: &str, keepalive_interval: Duration) -> Result<Self, Error> {
        if keepalive_interval.is_zero() {
            return Err(Error::Connect(
                "keepalive interval must be greater than zero".into(),
            ));
        }

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
                send_message(&write, tungstenite::Message::Binary(data.into())).await
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
                    Ok(tungstenite::Message::Ping(payload)) => {
                        tracing::trace!(bytes = payload.len(), "received ws ping");
                    }
                    Ok(tungstenite::Message::Pong(payload)) => {
                        tracing::trace!(bytes = payload.len(), "received ws pong");
                    }
                    Ok(_) => {}
                    Err(e) => {
                        tracing::debug!(error = %e, "ws read error");
                        break;
                    }
                }
            }
            channel_clone.transport_closed().await;
            tracing::debug!("ws reader exited");
        });

        let write = write.clone();
        tokio::spawn(async move {
            let mut ticks = tokio::time::interval_at(
                tokio::time::Instant::now() + keepalive_interval,
                keepalive_interval,
            );
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                ticks.tick().await;
                if let Err(e) =
                    send_message(&write, tungstenite::Message::Ping(Vec::new().into())).await
                {
                    tracing::debug!(error = %e, "ws client keepalive ping failed");
                    break;
                }
            }
        });

        tracing::info!(url, "ws-mux channel connected");
        Ok(channel)
    }
}
