from opentelemetry import trace

from taproot_worker.propagation import (
    KafkaHeadersGetter,
    KafkaHeadersSetter,
    context_from,
    headers_with_context,
)

from .conftest import PARENT_ID, TRACE_ID, TRACEPARENT, checkout_headers


# 1. The getter reads Kafka's (str, bytes) headers as text, ignoring case.
def test_getter_reads_headers():
    g = KafkaHeadersGetter()
    h = checkout_headers()
    assert g.get(h, "traceparent") == [TRACEPARENT]
    assert g.get(h, "TraceParent") == [TRACEPARENT]
    assert g.get(h, "tracestate") is None
    assert sorted(g.keys(h)) == ["baggage", "traceparent"]
    assert g.get(None, "traceparent") is None
    assert g.keys(None) == []


# 2. A valid traceparent becomes the remote parent; a missing or broken one gives no parent.
def test_extract():
    sc = trace.get_current_span(context_from(checkout_headers())).get_span_context()
    assert sc.is_valid and sc.is_remote
    assert format(sc.trace_id, "032x") == TRACE_ID
    assert format(sc.span_id, "016x") == PARENT_ID

    for bad in (None, [], [("traceparent", b"00-zz-yy-01")],
                [("traceparent", f"00-{TRACE_ID[:-1]}-{PARENT_ID}-01".encode())],
                [("traceparent", b"\xff\xfe")]):
        sc = trace.get_current_span(context_from(bad)).get_span_context()
        assert not sc.is_valid, bad


# 9. Injecting over headers that already carry a traceparent replaces it.
def test_setter_replaces():
    h = checkout_headers()
    KafkaHeadersSetter().set(h, "traceparent", "00-new")
    assert [v for k, v in h if k == "traceparent"] == [b"00-new"]
    assert len(h) == 2


def test_inject_without_span_adds_nothing():
    assert headers_with_context() == []
