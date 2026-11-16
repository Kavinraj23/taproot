.PHONY: build test lint up down check loss

build:
	go build ./...

test:
	go test ./...
	cd worker && python -m pytest

lint:
	go vet ./...

# Taproot stack plus the OpenTelemetry demo. The scripts also run without make (bash scripts/up.sh).
up:
	bash scripts/up.sh

down:
	bash scripts/down.sh

check:
	bash scripts/check-pipeline.sh
	bash scripts/check-grafana.sh
	bash scripts/check-demo.sh
	bash scripts/check-sensors.sh

# Lost or duplicated points over the last 10 minutes (M3). Not in check until M3 makes it pass.
# Outage scenarios: bash scripts/check-loss.sh --outage processor|clickhouse|redpanda|redpanda-agent
loss:
	bash scripts/check-loss.sh
