.PHONY: build test lint up down check loss loss-proof

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
	bash scripts/check-loss.sh -window 5m

# Lost or duplicated points over the last 10 minutes (M3).
loss:
	bash scripts/check-loss.sh

# M3 proof: break each pipeline part in turn (about 17 minutes), then check nothing was lost or
# stored twice. One scenario: bash scripts/check-loss.sh --outage <name>
loss-proof:
	bash scripts/check-loss.sh --all
