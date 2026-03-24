//! OpenTelemetry trace context integration for ws-mux.
//!
//! Provides injection of the current trace context into OPEN frames (client side)
//! and extraction + span parenting from received OPEN frames (server side).

use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceFlags, TraceId, TraceState};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::frame::TraceContext;

/// Extract the current OpenTelemetry span context into a [`TraceContext`]
/// for embedding in an OPEN frame.
pub(crate) fn current_trace_context() -> TraceContext {
    let cx = tracing::Span::current().context();
    let span_ref = cx.span();
    let sc = span_ref.span_context();

    if !sc.is_valid() {
        return TraceContext::default();
    }

    TraceContext {
        trace_id: sc.trace_id().to_bytes(),
        span_id: sc.span_id().to_bytes(),
        trace_flags: sc.trace_flags().to_u8(),
    }
}

/// Set the given trace context as the parent of a tracing span.
pub(crate) fn set_span_parent(tc: &TraceContext, span: &tracing::Span) {
    let sc = SpanContext::new(
        TraceId::from_bytes(tc.trace_id),
        SpanId::from_bytes(tc.span_id),
        TraceFlags::new(tc.trace_flags),
        true, // remote
        TraceState::default(),
    );

    let parent_cx = opentelemetry::Context::current().with_remote_span_context(sc);
    let _ = span.set_parent(parent_cx);
}
