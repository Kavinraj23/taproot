.PHONY: build test lint up down check

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
