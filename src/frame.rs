use crate::error::Error;

/// Binary W3C trace context embedded in OPEN frames.
///
/// Carries the caller's trace identity for distributed tracing.
/// When the `otel` feature is enabled, this is populated from the current
/// OpenTelemetry context. All-zero `trace_id` means no active trace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TraceContext {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub trace_flags: u8,
}

impl TraceContext {
    /// Size of the binary trace context in bytes.
    pub const SIZE: usize = 25;

    /// Returns true if this contains valid (non-zero) trace and span IDs.
    pub fn is_valid(&self) -> bool {
        self.trace_id != [0u8; 16] && self.span_id != [0u8; 8]
    }

    /// Append the binary encoding to `buf`.
    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.trace_id);
        buf.extend_from_slice(&self.span_id);
        buf.push(self.trace_flags);
    }

    /// Decode from the start of `data`. Returns the context and remaining bytes.
    pub fn decode(data: &[u8]) -> Result<(Self, &[u8]), Error> {
        if data.len() < Self::SIZE {
            return Err(Error::Protocol("trace context too short in OPEN payload".into()));
        }
        let mut trace_id = [0u8; 16];
        trace_id.copy_from_slice(&data[..16]);
        let mut span_id = [0u8; 8];
        span_id.copy_from_slice(&data[16..24]);
        let trace_flags = data[24];
        Ok((
            Self {
                trace_id,
                span_id,
                trace_flags,
            },
            &data[Self::SIZE..],
        ))
    }

    /// Capture the current trace context.
    ///
    /// With the `otel` feature enabled, extracts from the active OpenTelemetry
    /// span. Without it, returns an empty (all-zero) context.
    pub fn current() -> Self {
        #[cfg(feature = "otel")]
        {
            crate::otel::current_trace_context()
        }

        #[cfg(not(feature = "otel"))]
        {
            Self::default()
        }
    }

    /// Set this trace context as the parent of the given tracing span.
    ///
    /// No-op when the `otel` feature is disabled or the IDs are invalid.
    pub fn set_span_parent(&self, _span: &tracing::Span) {
        #[cfg(feature = "otel")]
        if self.is_valid() {
            crate::otel::set_span_parent(self, _span);
        }
    }
}

impl Default for TraceContext {
    fn default() -> Self {
        Self {
            trace_id: [0; 16],
            span_id: [0; 8],
            trace_flags: 0,
        }
    }
}

/// Frame flag constants (OR-able).
pub mod flags {
    /// First frame on a stream. Payload: `[method_index: u8][trace_context: 25 bytes][protobuf...]`.
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
    /// Maximum payload size for a single frame.
    pub const MAX_PAYLOAD_LEN: usize = 4 * 1024 * 1024;

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
        let payload_len = data.len() - Self::HEADER_LEN;
        if payload_len > Self::MAX_PAYLOAD_LEN {
            return Err(Error::FrameTooLarge {
                len: payload_len,
                max: Self::MAX_PAYLOAD_LEN,
            });
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

/// Parse an OPEN frame payload into `(method_index, trace_context, proto_bytes)`.
pub fn parse_open_payload(payload: &[u8]) -> Result<(u8, TraceContext, &[u8]), Error> {
    if payload.len() < 1 + TraceContext::SIZE {
        return Err(Error::Protocol(format!(
            "OPEN payload too short: {} bytes, need at least {}",
            payload.len(),
            1 + TraceContext::SIZE,
        )));
    }
    let method_index = payload[0];
    let (trace_ctx, proto_bytes) = TraceContext::decode(&payload[1..])?;
    Ok((method_index, trace_ctx, proto_bytes))
}

/// Build an OPEN frame payload: `[method_index][trace_context (25 bytes)][proto_bytes...]`.
pub fn build_open_payload(
    method_index: u8,
    trace_ctx: &TraceContext,
    proto_bytes: &[u8],
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(1 + TraceContext::SIZE + proto_bytes.len());
    buf.push(method_index);
    trace_ctx.encode(&mut buf);
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
    fn frame_too_large() {
        let mut data = vec![0; Frame::HEADER_LEN + Frame::MAX_PAYLOAD_LEN + 1];
        data[4] = flags::DATA;
        let err = Frame::decode(&data).expect_err("oversized frame should be rejected");
        assert!(matches!(
            err,
            Error::FrameTooLarge {
                len,
                max: Frame::MAX_PAYLOAD_LEN,
            } if len == Frame::MAX_PAYLOAD_LEN + 1
        ));
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
        let tc = TraceContext::default();
        let payload = build_open_payload(7, &tc, proto);
        let (idx, tc_out, bytes) = parse_open_payload(&payload).unwrap();
        assert_eq!(idx, 7);
        assert_eq!(tc_out, tc);
        assert_eq!(bytes, proto);
    }

    #[test]
    fn open_payload_with_trace_context() {
        let tc = TraceContext {
            trace_id: [1; 16],
            span_id: [2; 8],
            trace_flags: 0x01,
        };
        let proto = b"request";
        let payload = build_open_payload(3, &tc, proto);
        let (idx, tc_out, bytes) = parse_open_payload(&payload).unwrap();
        assert_eq!(idx, 3);
        assert_eq!(tc_out, tc);
        assert!(tc_out.is_valid());
        assert_eq!(bytes, proto);
    }

    #[test]
    fn open_payload_empty_proto() {
        let tc = TraceContext::default();
        let payload = build_open_payload(0, &tc, &[]);
        let (idx, tc_out, bytes) = parse_open_payload(&payload).unwrap();
        assert_eq!(idx, 0);
        assert_eq!(tc_out, tc);
        assert!(!tc_out.is_valid());
        assert!(bytes.is_empty());
    }

    #[test]
    fn open_payload_too_short() {
        assert!(parse_open_payload(&[]).is_err());
        assert!(parse_open_payload(&[0; 10]).is_err());
        // Exactly 1 + 25 = 26 bytes is the minimum valid OPEN payload.
        assert!(parse_open_payload(&[0; 25]).is_err());
        assert!(parse_open_payload(&[0; 26]).is_ok());
    }

    #[test]
    fn trace_context_default_is_invalid() {
        let tc = TraceContext::default();
        assert!(!tc.is_valid());
        assert_eq!(tc.trace_id, [0; 16]);
        assert_eq!(tc.span_id, [0; 8]);
        assert_eq!(tc.trace_flags, 0);
    }

    #[test]
    fn trace_context_zero_span_id_is_invalid() {
        let tc = TraceContext {
            trace_id: [1; 16],
            span_id: [0; 8],
            trace_flags: 0x01,
        };
        assert!(!tc.is_valid());
    }

    #[test]
    fn trace_context_encode_decode_round_trip() {
        let tc = TraceContext {
            trace_id: [0xAB; 16],
            span_id: [0xCD; 8],
            trace_flags: 0xFF,
        };
        let mut buf = Vec::new();
        tc.encode(&mut buf);
        assert_eq!(buf.len(), TraceContext::SIZE);
        let (decoded, rest) = TraceContext::decode(&buf).unwrap();
        assert_eq!(decoded, tc);
        assert!(rest.is_empty());
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
