/// gRPC-compatible status codes.
pub mod code {
    pub const OK: u8 = 0;
    pub const CANCELLED: u8 = 1;
    pub const UNKNOWN: u8 = 2;
    pub const INVALID_ARGUMENT: u8 = 3;
    pub const NOT_FOUND: u8 = 5;
    pub const UNIMPLEMENTED: u8 = 12;
    pub const INTERNAL: u8 = 13;
    pub const UNAVAILABLE: u8 = 14;
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("frame too short: {0} bytes")]
    FrameTooShort(usize),

    #[error("frame payload too large: {len} bytes exceeds max {max}")]
    FrameTooLarge { len: usize, max: usize },

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("protobuf decode: {0}")]
    Decode(#[from] prost::DecodeError),

    #[error("protobuf encode: {0}")]
    Encode(#[from] prost::EncodeError),

    #[error("status {code}: {message}")]
    Status { code: u8, message: String },

    #[error("connection closed")]
    Closed,

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("connect: {0}")]
    Connect(String),

    #[error("send: {0}")]
    Send(String),
}
