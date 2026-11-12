# Memory check: OTel demo + Taproot stack (M1, task 1)

Checks the Deployment decision in `docs/design.md` section 5: full OTel demo plus Taproot's stack on a 16 GB laptop with local Docker Compose.

## Setup
- Host: Windows 11, 15.4 GB RAM, Docker Desktop (WSL2), 22 CPUs.
- Docker VM cap: 7.99 GB (default, no `.wslconfig`).
- Demo: `open-telemetry/opentelemetry-demo` at IMAGE_VERSION 3.1.0, cloned to `projects/otel-demo` (outside this repo). Full mode: `compose.yaml` + `compose.full.yaml` + `compose.observability.yaml` + `compose.extras.yaml` (what `make start` runs). 28 containers.
- Taproot stack: `deploy/docker-compose.yml` (Redpanda, ClickHouse, Postgres, Grafana), started alongside.
- Measured with `docker stats --no-stream` about 4 minutes after start, demo load generator running, other apps open on the host.

## Results

| Measure | Result |
|---|---|
| Host free RAM, Docker Desktop idle | 1.7 GB |
| Demo alone (containers) | 3.9 GiB (design estimated ~6 GB) |
| Demo + Taproot stack (containers) | 4.9 GiB |
| Docker VM footprint (`vmmem`) | 5.1 GB |
| Host free RAM with both stacks up | 0.5 GB |

Largest demo containers: opensearch 911 MiB, kafka 531 MiB, load-generator 449 MiB, grafana 385 MiB, ad 240 MiB, fraud-detection 239 MiB.
Taproot stack: clickhouse 441 MiB, redpanda 253 MiB, grafana 144 MiB, postgres 81 MiB (about 0.9 GiB, under the 2–3 GB estimate).

## Findings
- Containers fit inside the 8 GB Docker cap with room to spare. The pressure is on Windows, which was left with ~0.5 GB free; the design's ~4 GB for OS and apps was not available.
- `checkout` and `product-catalog` stayed unhealthy (logs: exporter `DeadlineExceeded` to the collector), probably from memory pressure. Not confirmed. `checkout` matters for the Kafka overload scenario.
- Taproot services: Redpanda works with the compose flags (`rpk cluster info` returns a broker on 9092), ClickHouse answers on 8123, Postgres accepts connections on 5433. Grafana was still running first-start migrations very slowly after ~3 minutes and never answered on 3001, so it is unverified.
- No port clashes with the demo (Postgres on 5433, Grafana on 3001).

## Decision
Lowering the Docker cap does not free RAM that the VM isn't using, but it stops the VM from growing into Windows' share through file cache. Not a deviation from design.md.
- Set `memory=6GB` and `autoMemoryReclaim=gradual` in `~/.wslconfig`.
- Re-measure with the Kafka overload flag on to see peak usage. Keep 6 GB if the peak stays under about 5.5 GB; go back to 7 GB if containers are OOM-killed.

## Rerun under the 6 GB cap (2026-11-05)
- `~/.wslconfig` set to `memory=6GB`, `autoMemoryReclaim=gradual`; Docker reports a 6.2 GB cap.
- Demo + Taproot stack: 29 containers, 3.84 GiB total; host free RAM 1.1 GB (up from 0.5 GB). Memory fits under the lower cap.
- `product-catalog` (and `checkout`) never became healthy, so the store shows no products (`/api/products` returns 500). Same symptom at the 8 GB cap, so it is not caused by the cap.
- Pinning the images with `DEMO_VERSION=3.1.0` (`.env.override` in the demo clone) did not fix it. `recommendation` then went unhealthy and compose aborted before `frontend-proxy` started.
- `product-catalog` runs at 65–376% CPU with no stdout (it logs through OpenTelemetry, not stdout), at ~17 of its 20 MiB, no OOM kills.
- The demo clone was `main` (Nov 4) while 3.1.0 was released 2026-09-18, so I checked out the `3.1.0` tag and pinned `DEMO_VERSION=3.1.0` (appended to the tracked `.env.override`). Source, compose files and images now match. This did not fix the hang, but is still the right way to pin the demo.

## Root cause and fix
- `product-catalog` and `checkout` are Go services limited to 20 MiB with `GOMEMLIMIT=16MiB`. On this setup their heap sits at the Go limit, so the garbage collector runs continuously (CPU pegged) and the service never starts serving; health checks time out.
- Fix tested: `compose.local.yaml` in the demo clone raises both to a 64 MiB limit and `GOMEMLIMIT=48MiB`. Both became healthy within seconds at ~0.03% CPU using ~17 MiB, and `/api/products` returns the catalog.
- Cost: ~88 MiB more allowed, negligible. Taproot's run-one-command setup must include this override (or an equivalent) when starting the demo.
- Not verified why the upstream limits work for others (possibly OTel SDK overhead on this setup).

## Final measurements (demo healthy, 6 GB cap, `3.1.0` + `compose.local.yaml`)

| State | All containers | Docker VM | Host free RAM |
|---|---|---|---|
| Healthy, flag off | 4.02 GiB (demo 3.67, Taproot stack 0.36) | 4.1 GB | 0.2 GB |
| `kafkaQueueProblems` on, 3 min | 3.93 GiB | n/a | 0.3 GB |

- Taproot's Grafana (port 3001) answers 200; Redpanda, ClickHouse and Postgres were verified earlier.
- Under the Kafka overload flag, memory did not grow: kafka 297 MiB of 620, checkout 35 MiB of 64, fraud-detection 173 MiB of 300, accounting 111 MiB of 160. No container unhealthy, OOM-killed or restarted.
- Largest: opensearch ~1.0 GiB, load-generator ~0.6 GiB.
- Could not read consumer lag with `kafka-consumer-groups.sh` from inside the broker container (the broker's advertised listener isn't reachable via localhost there). Lag is something Taproot's Kafka sensor will measure.

## Conclusion
The deployment decision holds: the full demo plus Taproot's stack runs on the 16 GB laptop with Docker Compose, using ~4 GiB inside a 6 GB Docker cap, and survives the Kafka overload scenario. Host free RAM stays at 0.2–0.3 GB with my usual apps open, so close heavy apps during demos. Taproot's own Go services and the Python worker aren't running yet and will add a little.

Requirements for the one-command setup: pin the demo to a release tag (3.1.0), carry the `compose.local.yaml` memory override for `product-catalog` and `checkout`, and set the Docker cap in `.wslconfig`.

## Sustained run (2026-11-12)
- **Full demo (with its observability stack) + Taproot, a few hours under the 6 GB cap:** the VM had used 2013 of 2048 MB swap with 74 MB free.
  - VM CPU sat at 50–90% with no added load.
  - ClickHouse stopped answering queries, and the agent missed ticks.
  - Taproot's pipeline lost nothing (Redpanda and ClickHouse counts matched).
  - The container total measured above (~4 GiB) misses the VM's kernel, file cache and swap, so it under-reported the pressure.
- **Fix:** run the demo without its observability stack (OpenSearch, Jaeger, Prometheus, Grafana). See `docs/decisions/0004-demo-without-observability-stack.md`.
- **After the fix:** 29 containers, ~3.6–3.7 GiB inside containers. Start-up dropped from ~4 min to 1m19s. 30-minute soak, laptop awake:

| Time | VM available | VM swap | Rows / 5 min | VM CPU |
|---|---|---|---|---|
| 11:31 | 2420 MB | 754 MB | 284 | 11.3% |
| 11:36 | 2455 MB | 925 MB | 283 | 10.6% |
| 11:41 | 2390 MB | 979 MB | 286 | 10.7% |
| 11:46 | 2345 MB | 1027 MB | 283 | 10.3% |
| 11:51 | 2365 MB | 1023 MB | 281 | 10.4% |
| 11:56 | 2294 MB | 1049 MB | 283 | 10.7% |

- **Swap:** it levels off at about 1 GB, with pages moving in and out at up to ~45 MB per 10s. Memory pressure (PSI, `full avg60`) is only 0.2–0.4%, so tasks are barely stalled. This is the kernel trading idle pages for file cache, not thrashing, so `check-demo.sh` B5 checks pressure rather than swap.
- **Windows:** the host is the tight side, with 0.5 GB free while Docker's VM held 4.5 GB and Chrome, Spotify and others were open. Claude Code's background-task memory guard fired once. Close heavy apps during demos.
- **Open item (M2):** the agent records about 283 points per 5 minutes instead of 300 (~6% short). Redpanda and ClickHouse match, so the pipeline loses nothing; the agent ticks less often than once per real second. Suspected cause: the VM is paused in small slices under host memory pressure, and its clock drifts (seen before, 40s off and correcting in jumps). Not confirmed. Check when M2 adds 1-second sensors.
- **Load test** through `scripts/up.sh`: VM CPU went from 11.4% to 51.1% under 8 busy loops (expected ≈ 11 + 8/22 × 100 ≈ 47%), then back down.
