pub mod error;
pub mod frame;

#[cfg(feature = "codegen")]
pub mod codegen {
    include!("codegen_impl.rs");
}

#[cfg(feature = "client")]
pub mod client;

#[cfg(all(feature = "native-client", not(target_arch = "wasm32")))]
mod client_native;

#[cfg(all(feature = "wasm-client", target_arch = "wasm32"))]
mod client_wasm;

#[cfg(feature = "server")]
pub mod server;

pub use error::Error;
pub use frame::{Frame, flags};

#[cfg(feature = "client")]
pub use client::{MuxChannel, ResponseFuture, SendFn, Streaming, StreamingSender};

#[cfg(feature = "server")]
pub use server::{ServerSink, ServiceDispatch, StreamState, WsSink, handle_frame};

#[cfg(all(feature = "client", feature = "server"))]
pub use server::handle_bidi_frame;

#[cfg(all(feature = "server", not(target_arch = "wasm32")))]
pub use server::{WsRecv, serve};

#[cfg(all(
    feature = "native-client",
    feature = "server",
    not(target_arch = "wasm32")
))]
pub use server::{NativeWsRecv, NativeWsSink};
