import json

from opentelemetry.trace import SpanKind, StatusCode

from taproot_worker.worker import AUDITED_TOPIC, Ledger, audit, record

from .conftest import BAGGAGE, PARENT_ID, TRACE_ID, FakeMsg, FakeProducer, checkout_headers, order


def by_name(exporter):
    return {s.name: s for s in exporter.get_finished_spans()}


def hexid(n, width):
    return format(n, f"0{width}x")


def header(headers, key):
    vals = [v.decode() for k, v in headers if k == key]
    return vals[0] if vals else None


# 3. The auditor's work joins checkout's trace, as a child of checkout's publish span.
def test_audit_joins_sender_trace(spans):
    tracer, exp = spans
    assert audit(FakeMsg(order(), headers=checkout_headers(), partition=1, offset=42), FakeProducer(), tracer)
    s = by_name(exp)
    proc = s["process orders"]
    assert proc.kind == SpanKind.CONSUMER
    assert hexid(proc.context.trace_id, 32) == TRACE_ID
    assert hexid(proc.parent.span_id, 16) == PARENT_ID and proc.parent.is_remote
    a = proc.attributes
    assert a["messaging.system"] == "kafka"
    assert a["messaging.operation.type"] == "process"
    assert a["messaging.destination.name"] == "orders"
    assert a["messaging.consumer.group.name"] == "taproot-order-auditor"
    assert a["messaging.destination.partition.id"] == "1"
    assert a["messaging.kafka.offset"] == 42
    assert s["audit order"].parent.span_id == proc.context.span_id


# 4. The message the auditor sends on carries the same trace, with its producer span as parent,
# and passes the baggage along.
def test_audit_injects_context(spans):
    tracer, exp = spans
    prod = FakeProducer()
    audit(FakeMsg(order(), headers=checkout_headers()), prod, tracer)
    assert len(prod.sent) == 1 and prod.sent[0].topic() == AUDITED_TOPIC
    pub = by_name(exp)["publish orders.audited"]
    assert pub.kind == SpanKind.PRODUCER
    assert pub.parent.span_id == by_name(exp)["process orders"].context.span_id
    tp = header(prod.sent[0].headers(), "traceparent")
    assert tp == f"00-{TRACE_ID}-{hexid(pub.context.span_id, 16)}-01"
    assert header(prod.sent[0].headers(), "baggage") == BAGGAGE


# 5. Two hops over Kafka are still one trace: the ledger is a child of the auditor's publish span.
def test_two_hops_one_trace(spans):
    tracer, exp = spans
    prod = FakeProducer()
    audit(FakeMsg(order(), headers=checkout_headers()), prod, tracer)
    ledger = Ledger()
    assert record(prod.sent[0], tracer, ledger)
    s = by_name(exp)
    assert s["process orders.audited"].kind == SpanKind.CONSUMER
    assert s["process orders.audited"].parent.span_id == s["publish orders.audited"].context.span_id
    assert s["record order"].parent.span_id == s["process orders.audited"].context.span_id
    assert {hexid(sp.context.trace_id, 32) for sp in s.values()} == {TRACE_ID}
    assert ledger.count == 1


# 6. A message without a slip still gets processed, as the start of a new trace.
def test_no_context_starts_new_trace(spans):
    tracer, exp = spans
    prod = FakeProducer()
    assert audit(FakeMsg(order(), headers=None), prod, tracer)
    proc = by_name(exp)["process orders"]
    assert proc.parent is None
    assert proc.context.is_valid and hexid(proc.context.trace_id, 32) != TRACE_ID
    assert len(prod.sent) == 1


# 7. A value that isn't an order: the span says so, nothing is sent on, nothing escapes.
def test_bad_value(spans):
    tracer, exp = spans
    prod = FakeProducer()
    assert not audit(FakeMsg(b"\xff\x00not protobuf", headers=checkout_headers()), prod, tracer)
    proc = by_name(exp)["process orders"]
    assert proc.status.status_code == StatusCode.ERROR
    assert any(e.name == "exception" for e in proc.events)
    assert prod.sent == []

    exp.clear()
    assert not record(FakeMsg(b"not json", topic=AUDITED_TOPIC), tracer, Ledger())
    assert by_name(exp)["process orders.audited"].status.status_code == StatusCode.ERROR


# 8. The audit reads the order: item count and total (unit price x quantity, plus shipping).
def test_order_content(spans):
    tracer, exp = spans
    prod = FakeProducer()
    # 2 x 10.50 + 1 x 3.00 + 5.00 shipping = 29.00
    audit(FakeMsg(order("order-9"), headers=checkout_headers()), prod, tracer)
    a = by_name(exp)["audit order"].attributes
    assert a["order.id"] == "order-9"
    assert a["order.items"] == 2
    assert abs(a["order.total"] - 29.0) < 1e-9
    sent = json.loads(prod.sent[0].value())
    assert sent["order_id"] == "order-9" and abs(sent["total"] - 29.0) < 1e-9
    assert prod.sent[0].key() == "order-9"


# An order with no items is an audit finding: recorded as an event, still passed on.
def test_empty_order_flagged(spans):
    tracer, exp = spans
    prod = FakeProducer()
    assert audit(FakeMsg(order(items=()), headers=checkout_headers()), prod, tracer)
    assert any(e.name == "audit.finding" for e in by_name(exp)["audit order"].events)
    assert len(prod.sent) == 1
