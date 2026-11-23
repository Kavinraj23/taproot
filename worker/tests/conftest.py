"""Fakes with confluent-kafka's method names, so the workers run without a broker."""

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from taproot_worker import orders_pb2

# What checkout puts on an orders message (seen on the demo's Kafka, 2026-11-23).
TRACE_ID = "52a6194bbb459c70829c695e462ee4a7"
PARENT_ID = "562ce708078bc841"
TRACEPARENT = f"00-{TRACE_ID}-{PARENT_ID}-01"
BAGGAGE = "session.id=2fd288c9-e3aa-4a7a-8822-db8e271caf08"


class FakeMsg:
    def __init__(self, value, headers=None, topic="orders", partition=0, offset=7, key=None):
        self._value, self._headers = value, headers
        self._topic, self._partition, self._offset, self._key = topic, partition, offset, key

    def value(self):
        return self._value

    def headers(self):
        return self._headers

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def key(self):
        return self._key

    def error(self):
        return None


class FakeProducer:
    def __init__(self):
        self.sent = []

    def produce(self, topic, value=None, key=None, headers=None):
        self.sent.append(FakeMsg(value, headers=headers, topic=topic, key=key))

    def poll(self, timeout=0):
        return 0

    def flush(self, timeout=None):
        return 0


def order(order_id="order-1", items=((2, 10, 500_000_000), (1, 3, 0)), shipping=(5, 0)):
    """An OrderResult; items are (quantity, units, nanos) of each item's unit price."""
    o = orders_pb2.OrderResult(order_id=order_id)
    o.shipping_cost.currency_code = "USD"
    o.shipping_cost.units, o.shipping_cost.nanos = shipping
    for i, (qty, units, nanos) in enumerate(items):
        it = o.items.add()
        it.item.product_id, it.item.quantity = f"P{i}", qty
        it.cost.currency_code, it.cost.units, it.cost.nanos = "USD", units, nanos
    return o.SerializeToString()


def checkout_headers():
    return [("traceparent", TRACEPARENT.encode()), ("baggage", BAGGAGE.encode())]


@pytest.fixture
def spans():
    """A tracer that records finished spans in memory: (tracer, exporter)."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider.get_tracer("test"), exporter
