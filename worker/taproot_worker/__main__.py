"""Runs one order worker against the demo's Kafka:  ROLE=auditor|ledger python -m taproot_worker

Environment: KAFKA_ADDR (default kafka:9092), OTEL_SERVICE_NAME, and the standard
OTEL_EXPORTER_OTLP_ENDPOINT for where spans go (the demo's collector).
"""

import logging
import os
import signal
import sys

from confluent_kafka import Consumer, KafkaError, Producer
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from .worker import AUDITED_TOPIC, AUDITOR_GROUP, LEDGER_GROUP, Ledger, audit, record

# role: (topic, group, default service name, where a new group starts reading)
# The auditor starts at new orders rather than replaying the demo topic's history. The ledger
# reads our own small topic from its start: orders.audited is created by the auditor's first
# message, and starting at "latest" could skip summaries sent before the ledger joined.
ROLES = {
    "auditor": ("orders", AUDITOR_GROUP, "order-auditor", "latest"),
    "ledger": (AUDITED_TOPIC, LEDGER_GROUP, "order-ledger", "earliest"),
}

log = logging.getLogger("taproot_worker")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    role = os.environ.get("ROLE", "")
    if role not in ROLES:
        log.error("ROLE must be one of %s, got %r", sorted(ROLES), role)
        return 2
    topic, group, default_name, reset = ROLES[role]
    brokers = os.environ.get("KAFKA_ADDR", "kafka:9092")

    provider = TracerProvider(resource=Resource.create(
        {"service.name": os.environ.get("OTEL_SERVICE_NAME", default_name)}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
    tracer = trace.get_tracer("taproot_worker")

    consumer = Consumer({
        "bootstrap.servers": brokers,
        "group.id": group,
        "auto.offset.reset": reset,
        # Offsets advance only for messages already handled (store_offsets below), and are
        # committed in the background: a crash re-reads at most the unfinished ones.
        "enable.auto.offset.store": False,
    })
    consumer.subscribe([topic])
    producer = Producer({"bootstrap.servers": brokers}) if role == "auditor" else None
    ledger = Ledger()

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    log.info("%s reading %s at %s as group %s", role, topic, brokers, group)

    try:
        while running:
            msg = consumer.poll(1.0)
            if producer is not None:
                producer.poll(0)  # delivery reports
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.warning("kafka: %s", msg.error())
                continue
            if role == "auditor":
                audit(msg, producer, tracer, group)
            else:
                record(msg, tracer, ledger, group)
            consumer.store_offsets(msg)  # handled (or skipped as unreadable): move past it
    finally:
        log.info("stopping")
        consumer.close()  # commits stored offsets and leaves the group
        if producer is not None:
            producer.flush(10)
        provider.shutdown()  # sends the spans still batched
    return 0


if __name__ == "__main__":
    sys.exit(main())
