use crate::error::Error;

/// Frame flag constants (OR-able).
pub mod flags {
    /// First frame on a stream. Payload: `[method_index: u8][protobuf...]`.
    pub const OPEN: u8 = 0x01;
    /// Continuation data. Payload is raw protobuf.
    pub const DATA: u8 = 0x02;
    /// Last frame from this side.
    pub const END: u8 = 0x04;
    /// Cancel/error. Payload: `[status_code: u8][UTF-8 message]`.
    pub const RST: u8 = 0x08;
}

/// A single multiplexed frame.
///
/// Wire format: `[stream_id: u32 LE][flags: u8][payload...]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub stream_id: u32,
    pub flags: u8,
    pub payload: Vec<u8>,
}

impl Frame {
    /// Size of the fixed header (stream_id + flags).
    pub const HEADER_LEN: usize = 5;

    /// Encode the frame into a byte vector suitable for a WebSocket binary message.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(Self::HEADER_LEN + self.payload.len());
        buf.extend_from_slice(&self.stream_id.to_le_bytes());
        buf.push(self.flags);
        buf.extend_from_slice(&self.payload);
        buf
    }

    /// Decode a frame from raw bytes (one WebSocket binary message).
    pub fn decode(data: &[u8]) -> Result<Self, Error> {
        if data.len() < Self::HEADER_LEN {
            return Err(Error::FrameTooShort(data.len()));
        }
        let stream_id = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let flags = data[4];
        let payload = data[Self::HEADER_LEN..].to_vec();
        Ok(Self {
            stream_id,
            flags,
            payload,
        })
    }

    pub fn is_open(&self) -> bool {
        self.flags & flags::OPEN != 0
    }

    pub fn is_data(&self) -> bool {
        self.flags & flags::DATA != 0
    }

    pub fn is_end(&self) -> bool {
        self.flags & flags::END != 0
    }

    pub fn is_rst(&self) -> bool {
        self.flags & flags::RST != 0
    }
}

/// Parse an OPEN frame payload into `(method_index, proto_bytes)`.
pub fn parse_open_payload(payload: &[u8]) -> Result<(u8, &[u8]), Error> {
    if payload.is_empty() {
        return Err(Error::Protocol("OPEN payload is empty".into()));
    }
    Ok((payload[0], &payload[1..]))
}

/// Build an OPEN frame payload: `[method_index][proto_bytes...]`.
pub fn build_open_payload(method_index: u8, proto_bytes: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + proto_bytes.len());
    buf.push(method_index);
    buf.extend_from_slice(proto_bytes);
    buf
}

/// Build an RST frame payload: `[status_code][UTF-8 message]`.
pub fn build_rst_payload(status_code: u8, message: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + message.len());
    buf.push(status_code);
    buf.extend_from_slice(message.as_bytes());
    buf
}

/// Parse an RST frame payload into `(status_code, message)`.
pub fn parse_rst_payload(payload: &[u8]) -> Result<(u8, &str), Error> {
    if payload.is_empty() {
        return Err(Error::Protocol("RST payload is empty".into()));
    }
    let code = payload[0];
    let message = std::str::from_utf8(&payload[1..])
        .map_err(|e| Error::Protocol(format!("RST message is not UTF-8: {e}")))?;
    Ok((code, message))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_round_trip() {
        let frame = Frame {
            stream_id: 42,
            flags: flags::OPEN | flags::END,
            payload: vec![1, 2, 3, 4],
        };
        let encoded = frame.encode();
        assert_eq!(encoded.len(), Frame::HEADER_LEN + 4);
        let decoded = Frame::decode(&encoded).unwrap();
        assert_eq!(frame, decoded);
    }

    #[test]
    fn frame_empty_payload() {
        let frame = Frame {
            stream_id: 0,
            flags: flags::END,
            payload: vec![],
        };
        let encoded = frame.encode();
        assert_eq!(encoded.len(), Frame::HEADER_LEN);
        let decoded = Frame::decode(&encoded).unwrap();
        assert_eq!(frame, decoded);
    }

    #[test]
    fn frame_too_short() {
        assert!(Frame::decode(&[0, 1, 2, 3]).is_err());
        assert!(Frame::decode(&[]).is_err());
    }

    #[test]
    fn frame_flags() {
        let frame = Frame {
            stream_id: 1,
            flags: flags::OPEN | flags::END,
            payload: vec![],
        };
        assert!(frame.is_open());
        assert!(!frame.is_data());
        assert!(frame.is_end());
        assert!(!frame.is_rst());
    }

    #[test]
    fn open_payload_round_trip() {
        let proto = b"hello proto";
        let payload = build_open_payload(7, proto);
        let (idx, bytes) = parse_open_payload(&payload).unwrap();
        assert_eq!(idx, 7);
        assert_eq!(bytes, proto);
    }

    #[test]
    fn open_payload_empty_proto() {
        let payload = build_open_payload(0, &[]);
        let (idx, bytes) = parse_open_payload(&payload).unwrap();
        assert_eq!(idx, 0);
        assert!(bytes.is_empty());
    }

    #[test]
    fn open_payload_empty_errors() {
        assert!(parse_open_payload(&[]).is_err());
    }

    #[test]
    fn rst_payload_round_trip() {
        let payload = build_rst_payload(13, "internal error");
        let (code, msg) = parse_rst_payload(&payload).unwrap();
        assert_eq!(code, 13);
        assert_eq!(msg, "internal error");
    }

    #[test]
    fn rst_payload_empty_message() {
        let payload = build_rst_payload(0, "");
        let (code, msg) = parse_rst_payload(&payload).unwrap();
        assert_eq!(code, 0);
        assert_eq!(msg, "");
    }

    #[test]
    fn rst_payload_empty_errors() {
        assert!(parse_rst_payload(&[]).is_err());
    }

    #[test]
    fn stream_id_endianness() {
        let frame = Frame {
            stream_id: 0x01020304,
            flags: flags::DATA,
            payload: vec![],
        };
        let encoded = frame.encode();
        // Little-endian: least significant byte first
        assert_eq!(encoded[0], 0x04);
        assert_eq!(encoded[1], 0x03);
        assert_eq!(encoded[2], 0x02);
        assert_eq!(encoded[3], 0x01);
    }

    #[test]
    fn large_stream_id() {
        let frame = Frame {
            stream_id: u32::MAX,
            flags: flags::DATA | flags::END,
            payload: vec![0xFF; 1024],
        };
        let decoded = Frame::decode(&frame.encode()).unwrap();
        assert_eq!(decoded.stream_id, u32::MAX);
        assert_eq!(decoded.payload.len(), 1024);
    }
}
