# Order workers

A small hand-instrumented Python service watched by Taproot (M4 task 6, `docs/decisions/0008`).
It runs as two containers built from this one image:

- **order-auditor** (`ROLE=auditor`) reads checkout's orders from the demo's Kafka topic
  `orders`, checks each one, and sends a summary to `orders.audited`.
- **order-ledger** (`ROLE=ledger`) reads `orders.audited` and keeps running totals.

Both carry the trace across Kafka by hand with the OpenTelemetry SDK
(`taproot_worker/propagation.py`): extract the sender's context from the message headers,
make the consumer span its child, and inject the producer span's context into the next
message. So checkout → order-auditor → order-ledger is one trace.

They start with `bash scripts/up.sh` (`deploy/otel-demo/compose.taproot.yaml`).

## Tests

```sh
cd worker
python -m venv .venv
.venv/Scripts/pip install -e ".[dev]"   # .venv/bin/ on Linux/macOS
.venv/Scripts/python -m pytest -q
```

## Regenerating `orders_pb2.py`

After changing `proto/orders.proto` (Taproot's minimal copy of the demo's order messages):

```sh
.venv/Scripts/python -m grpc_tools.protoc -Iproto --python_out=taproot_worker proto/orders.proto
```

The generated code needs a protobuf runtime at least as new as the generator's (see the
`Protobuf Python Version` line in the file), so keep `protobuf` in `pyproject.toml` in step.
