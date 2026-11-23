"""Carrying a trace across Kafka: read the slip taped to a message, and tape one on.

Over HTTP the trace context travels in request headers, and instrumentation libraries copy it
for you. A Kafka message waits in a topic until someone reads it, so the context has to ride
in the message's own headers (W3C `traceparent`, plus `baggage`), and both ends have to handle
it on purpose: the producer *injects* it, the consumer *extracts* it.

OpenTelemetry's propagators already know the header formats. What they don't know is where the
headers live: they talk to a "carrier" through a getter and a setter. confluent-kafka keeps
headers as a list of (str, bytes) pairs, so these two classes translate.

Hand-written piece 1 (learning target 1), written by Claude at the user's request on
2026-11-23 (docs/decisions/0008, task 6).
"""

from typing import List, Optional, Tuple

from opentelemetry import propagate
from opentelemetry.context import Context
from opentelemetry.propagators.textmap import Getter, Setter

Headers = List[Tuple[str, bytes]]


class KafkaHeadersGetter(Getter[Optional[Headers]]):
    """Reads Kafka headers for a propagator. Header names are matched without case, as HTTP
    does, so `TraceParent` from some other client still counts. A value that isn't valid
    UTF-8 is treated as absent: a broken slip means "no slip", never a crash."""

    def get(self, carrier: Optional[Headers], key: str) -> Optional[List[str]]:
        if not carrier:
            return None
        key = key.lower()
        values = []
        for k, v in carrier:
            if k.lower() != key or v is None:
                continue
            try:
                values.append(v.decode("utf-8") if isinstance(v, bytes) else str(v))
            except UnicodeDecodeError:
                continue
        return values or None

    def keys(self, carrier: Optional[Headers]) -> List[str]:
        return [k for k, _ in carrier] if carrier else []


class KafkaHeadersSetter(Setter[Headers]):
    """Writes a header for a propagator, replacing any header of the same name: a message
    must carry one traceparent, and it must be ours (the span that sends it)."""

    def set(self, carrier: Headers, key: str, value: str) -> None:
        carrier[:] = [(k, v) for k, v in carrier if k.lower() != key.lower()]
        carrier.append((key, value.encode("utf-8")))


GETTER = KafkaHeadersGetter()
SETTER = KafkaHeadersSetter()


def context_from(headers: Optional[Headers]) -> Context:
    """Extract: the context the sender taped to this message. Start the consumer span with it
    as `context=` and the span becomes a child of the sender's span, in the sender's trace.
    No slip, or an unreadable one, gives an empty context, and the span starts a new trace."""
    return propagate.extract(headers or [], getter=GETTER)


def headers_with_context(context: Optional[Context] = None) -> Headers:
    """Inject: headers carrying the current context (or `context`), to send with a message.
    Call it inside the producer span, so the next reader's parent is that span."""
    headers: Headers = []
    propagate.inject(headers, context=context, setter=SETTER)
    return headers
