"""The two order workers, without any broker code so tests can drive them.

order-auditor reads checkout's orders from the demo topic `orders`, checks each one, and sends
a summary on to `orders.audited`. order-ledger reads those summaries and keeps running totals.
Each message's work is a consumer span whose parent is the span that sent the message, so
checkout → auditor → ledger is one trace (see propagation.py).
"""

import json
import logging
from contextlib import contextmanager

from google.protobuf.message import DecodeError
from opentelemetry import context as otel_context
from opentelemetry.trace import SpanKind, Status, StatusCode, Tracer

from . import orders_pb2
from .propagation import context_from, headers_with_context

AUDITED_TOPIC = "orders.audited"
AUDITOR_GROUP = "taproot-order-auditor"
LEDGER_GROUP = "taproot-order-ledger"

log = logging.getLogger(__name__)


@contextmanager
def _sent_context(msg):
    """Runs the block inside the context the sender taped to msg (extract, then attach).

    Attaching matters: passing the extracted context only as a span's `context=` makes that
    span a child of the sender, but the context current inside it is built from whatever was
    current before, so the rest of the slip (baggage such as session.id) would be dropped
    and not passed on to the next hop."""
    token = otel_context.attach(context_from(msg.headers()))
    try:
        yield
    finally:
        otel_context.detach(token)


def _consume_attrs(msg, group: str) -> dict:
    """OpenTelemetry messaging attributes for processing one Kafka message."""
    attrs = {
        "messaging.system": "kafka",
        "messaging.operation.type": "process",
        "messaging.operation.name": "process",
        "messaging.destination.name": msg.topic(),
        "messaging.consumer.group.name": group,
        "messaging.destination.partition.id": str(msg.partition()),
        "messaging.kafka.offset": msg.offset(),
    }
    if msg.value() is not None:
        attrs["messaging.message.body.size"] = len(msg.value())
    key = msg.key()
    if key:
        attrs["messaging.kafka.message.key"] = key.decode("utf-8", "replace") if isinstance(key, bytes) else str(key)
    return attrs


def _fail(span, err: Exception, what: str) -> bool:
    span.record_exception(err)
    span.set_status(Status(StatusCode.ERROR, what))
    log.warning("%s: %s", what, err)
    return False


def _money(m) -> float:
    return m.units + m.nanos / 1e9


def audit(msg, producer, tracer: Tracer, group: str = AUDITOR_GROUP) -> bool:
    """Checks one order and sends its summary on. Returns False if the message wasn't an order
    (the span records why); the caller moves past it either way."""
    # Extract: the parent is checkout's "publish orders" span, from the message headers.
    with _sent_context(msg), tracer.start_as_current_span(
        f"process {msg.topic()}", kind=SpanKind.CONSUMER,
        attributes=_consume_attrs(msg, group),
        record_exception=False, set_status_on_exception=False,
    ) as span:
        with tracer.start_as_current_span("audit order") as step:
            try:
                order = orders_pb2.OrderResult()
                order.ParseFromString(msg.value() or b"")
            except DecodeError as err:
                step.set_status(Status(StatusCode.ERROR, "not an OrderResult"))
                return _fail(span, err, "not an OrderResult")
            total = _money(order.shipping_cost) + sum(
                it.item.quantity * _money(it.cost) for it in order.items)
            step.set_attributes({"order.id": order.order_id, "order.items": len(order.items),
                                 "order.total": total})
            if not order.items:
                step.add_event("audit.finding", {"finding": "order has no items"})

        summary = {"order_id": order.order_id, "items": len(order.items), "total": total,
                   "currency": order.shipping_cost.currency_code}
        with tracer.start_as_current_span(
            f"publish {AUDITED_TOPIC}", kind=SpanKind.PRODUCER,
            attributes={"messaging.system": "kafka", "messaging.operation.type": "send",
                        "messaging.operation.name": "publish",
                        "messaging.destination.name": AUDITED_TOPIC},
        ):
            # Inject, inside the producer span: the ledger's parent will be this span.
            producer.produce(AUDITED_TOPIC, value=json.dumps(summary).encode(),
                             key=order.order_id, headers=headers_with_context())
        return True


class Ledger:
    """Running totals of audited orders, logged every `every` orders."""

    def __init__(self, every: int = 10):
        self.count, self.total, self.every = 0, 0.0, every

    def add(self, total: float) -> None:
        self.count += 1
        self.total += total
        if self.count % self.every == 0:
            log.info("ledger: %d orders, %.2f total", self.count, self.total)


def record(msg, tracer: Tracer, ledger: Ledger, group: str = LEDGER_GROUP) -> bool:
    """Adds one audited order to the ledger. Returns False if the message wasn't a summary."""
    # Extract: the parent is the auditor's "publish orders.audited" span.
    with _sent_context(msg), tracer.start_as_current_span(
        f"process {msg.topic()}", kind=SpanKind.CONSUMER,
        attributes=_consume_attrs(msg, group),
        record_exception=False, set_status_on_exception=False,
    ) as span:
        try:
            summary = json.loads(msg.value())
            order_id, total = summary["order_id"], float(summary["total"])
        except (ValueError, TypeError, KeyError) as err:
            return _fail(span, err, "not an audited order")
        with tracer.start_as_current_span("record order") as step:
            ledger.add(total)
            step.set_attributes({"order.id": order_id, "ledger.orders": ledger.count})
        return True
