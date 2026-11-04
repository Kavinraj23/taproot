# Taproot: Design

Source of truth for Taproot's goals, scope, and design decisions (exported from the proposal workbook). Changes to anything here are recorded in `docs/decisions/`.

## How to use this workbook

You fill every section; Claude only asks questions and pokes holes. Nothing here is decided yet, including whether the project ends up looking like a mini-Instana at all.

- Work top to bottom. Research (sections 2–3) comes before scope and design, because design choices you make before you understand the domain are guesses.
- Write in your own words. If you paste from a source, add one line on why it matters to you.
- Mark anything you're unsure of with **?** so review time goes where it's needed.
- When a section is done, comment "ready for review" on its heading. Expect questions back, not answers.
- The proposal is done when you can defend every decision in section 10 without notes.

## 1. Why this project

Answer these before any research, then revisit them at the end to see what changed.

1. What do you want this project to prove to an interviewer? Pick one or two things, not five.
2. Which three skills or concepts do you want to genuinely understand by the end, well enough to teach them?
3. Who is the audience: IBM Instana specifically, observability companies broadly, or infra and data roles in general? How does that change what you build?
4. How will you know it's done? Write one sentence that a stranger could check.
5. What did DriftGate teach you about how you work, and what will you do differently here?

**Your answers:**

- **Q1. What it proves:** that I have end-to-end understanding of how an observability system works, can rebuild its core myself, and can build my own features on top as POCs, the kind of thing I'd prototype on an infra team for later adoption. Evidence: a working demo and being able to explain the design tradeoffs.

* **Q2. Learning targets** (self-rated 2/5 on "could I explain this without looking it up"):
  1. Trace propagation: how a request's identity follows it across services
  2. Reliable data movement: queues, backpressure, and at-least-once vs exactly-once delivery
  3. Graph-based root cause: modeling service dependencies as a graph and walking it to find where a failure started

- **Q3. Audience:** infra and data roles in general. Instana is the inspiration for what I build, but the features should impress any infra team, company-agnostic.

* **Q4. Definition of done:** a stranger runs one command, and the system discovers the running services with no configuration, collects their data without losing any when a pipeline component fails, and identifies the root cause of several injected failure types quickly, showing the evidence behind its answer. **?** Exact number of failure types and detection time to be set once I know what's realistic.
  - Stretch goal: an agent layer that explains the root-cause reasoning in words.

- **Q5. Lessons from DriftGate:** technically, I mainly learned tool calls and which models to use for which use cases. The reason I didn't learn more: Claude Code wrote the deep parts and I mostly directed it.
  - What I'll do differently: no delegating huge plans. If I use Claude Code or subagents, it's on smaller pieces, so I always know what I'm working on.

## 2. Domain research

Answer each question in two or three sentences of your own, and note where you found it. The sources below are starting points, not the only places to look.

**Observability fundamentals**

- [ ] What's the difference between metrics, logs, and traces? When is each the right tool?
- [ ] What is a span, a trace, and context propagation? How does a trace ID get from one service to the next?
- [ ] What is sampling (head vs tail), and why do most systems sample? What does it cost to *not* sample?
- [ ] What are the RED and USE methods, and what questions does each answer?
- [ ] What is cardinality, and why does it break metrics systems?

**How Instana does it**

- [ ] What does the host agent do, and what is a sensor? How does the agent decide which sensors to run?
- [ ] What is AutoTrace, and how is it different from manual instrumentation with an SDK?
- [ ] What is the Dynamic Graph, and what does Instana use it for?
- [ ] How does Instana handle OpenTelemetry data?
- [ ] Pick one competitor (Datadog, Dynatrace, Honeycomb, or Grafana). What does it do differently from Instana?

**The machinery underneath**

- [ ] How can a program find out what processes are running on a Linux host, and what each one is? What lives in `/proc`?
- [ ] Why do observability backends use a message queue between collection and storage?
- [ ] Why are columnar databases popular for telemetry? What makes them fast for this workload and bad for others?
- [ ] What is eBPF, and why are observability tools moving toward it? (Read only; don't plan to build with it yet.)

**Starting sources**

- [Dapper paper (Google, 2010)](https://research.google/pubs/dapper-a-large-scale-distributed-systems-tracing-infrastructure/): the paper most distributed tracing systems descend from
- [OpenTelemetry concepts](https://opentelemetry.io/docs/concepts/)
- [Google SRE book: Monitoring Distributed Systems](https://sre.google/sre-book/monitoring-distributed-systems/)
- [Brendan Gregg: the USE method](https://www.brendangregg.com/usemethod.html)
- [About Instana (IBM Docs)](https://www.ibm.com/docs/en/SSE1JP5_current/src/pages/about/overview.html) and [Tracing in Instana](https://www.ibm.com/docs/en/SSE1JP5/src/pages/tracing/index.html)
- [`proc(5)` man page](https://man7.org/linux/man-pages/man5/proc.5.html)

**Your notes:**

- **Agent and sensors:** sensors are like adapters for different technologies, so the agent can pick up their metrics. The agent figures out which sensors to run by checking the processes running on the machine. **If a process is renamed, the agent can use other clues, from cheapest to most reliable: the process name, the port it listens on (e.g., 5432 suggests Postgres), the executable path the OS records under /proc/\<pid>/, and finally talking to the service directly and seeing whether it responds like the technology it's supposed to be.**
- **Dynamic Graph:** a dependency graph of how components relate. Without it, you only see that several things went wrong at the same time, with no way to tell how they're connected. With it, you can trace a cascading failure: e.g., a batch job floods Postgres with writes, Postgres slows, and that trickles up through payment to checkout. Symptoms show up at the top; you walk down the graph to find the cause, then confirm with evidence.

* **AutoTrace vs SDK:** with an SDK, a developer adds the tracing code, which gives more customizability (e.g., tracing custom business logic). With AutoTrace, no developer touches the code; it works out of the box for a set of supported technologies. **How it works: the agent dynamically injects bytecode into the running application without modifying its source code.**
* **Why ingest OpenTelemetry:** OTel standardizes the data format. Without OTel support, a company that already instrumented with OTel would have to rip it out and redo everything with Instana's SDK, and do it again if they ever switched vendors. With OTel, they instrument once and can send the data anywhere, avoiding vendor lock-in.

- **Competitor (Datadog):** Instana collects metrics every 1 second; Datadog collects every 15–60 seconds. Instana's selling point is hands-off automation: drop it into a cluster and it does its work. Datadog offers more customizability, isn't just an APM tool, and offers a lot more out of the box.
  - Tradeoff: 1-second data helps root-cause analysis, but it's 15x the data of 15-second collection, so network, processing, and storage costs skyrocket at scale.

* **Context propagation:** when a request enters the system, it gets a unique trace ID. Every outgoing call carries that ID in an HTTP header (in Instana, `X-INSTANA-T`), and each service tags its own work with the same ID. The ID has to identify the request, not the process or service: two users clicking "buy" through the same checkout process are two different requests, so they get two different trace IDs. The backend later stitches every span with the same ID into one trace.

- **Metrics vs logs vs traces:** metrics are stats/numerical data, logs are timestamped discrete events, and traces track the end-to-end path of a single request across services. In an incident: metrics tell you *that* something is wrong and how bad, traces tell you *where* in the chain the time is going, and logs tell you *why* that component failed.

* **Sampling:** most systems sample traces (e.g., keep 1 in 100) because it's expensive to collect and store all of this data. Instana's selling point is keeping every trace.
  - Head sampling decides at the start of a request. It's cheap but blind: if you keep 1 in 100 randomly, there's only about a 1% chance of keeping the one failing request you actually need.
  - Tail sampling decides at the end. The system has to hold all the spans until the trace is done, then dispose of them if the request didn't fail. Knowing when a trace is complete involves tradeoffs. One approach: a buffer, e.g., call the trace done if no new spans arrive for N seconds. A shorter N is faster and uses less memory but risks incomplete traces; a longer N is more complete but holds more in memory. (Feeds the trace assembly row in section 5.)

- **RED vs USE:** RED (rate, errors, duration) is for services, which deal primarily with requests. USE (utilization, saturation, errors) is for resources, things that are bottlenecked by capacity. In the checkout scenario, RED on checkout and payment shows the symptom, and USE on Postgres's CPU shows the cause.

* **Cardinality:** each unique combination of label values on a metric is stored as its own time series. 10 services × 5 status codes = 50 series, which is fine. Add a `user_id` label with 1 million users and it becomes 50 million series, a cardinality explosion. Per-user details belong in traces (or logs), which are records of individual requests, so adding a field doesn't multiply anything.

- **/proc:** helps us learn about processes on Linux, both to discover them and to measure them. The kernel generates its files on the spot when we read them (e.g., `/proc/<pid>/exe` for the executable, `/proc/<pid>/stat` for CPU, `/proc/<pid>/status` for memory). `top` reads this data in a loop, and my agent can collect host metrics the same way.

* **Message queues and backpressure:** without a queue between the agents and the database, data is lost whenever the database is down. A queue stores it so the database can be repopulated when it comes back. If data arrives faster than the database can write it, the queue keeps growing and can eventually run out of space, leading to data loss. Options when the queue is nearly full, each trading away something different:
  1. Grow the queue: buys time, costs storage.
  2. Scale the consumers (more readers/writers): doesn't help if the database itself is the bottleneck.
  3. Push back on the agents: they buffer locally and slow down (this is backpressure); their buffers are finite too.
  4. Downsample or prioritize: e.g., send 15-second averages instead of 1-second points, or keep failed traces and drop healthy ones. Still captures some data, but not all of it.

- **Columnar databases:** a row store like Postgres keeps each row's values together (like one index card per row), so a query that needs only one column (e.g., average CPU over the last hour) still reads every full row, about 20x more data than needed in a 20-column table. A columnar database like ClickHouse stores each column as its own strip, so the same query reads only the CPU and timestamp strips, and those compress well. The flip side: rebuilding one full row means pulling a value from all 20 columns. Row stores are good for working with individual rows (apps); column stores are good for scanning a few columns across billions of rows (telemetry).

* **eBPF:** lets you run small, safe programs inside the kernel and attach them to events (like system calls or network packets), so tools can observe apps without modifying them or caring about their language. Tradeoff: the kernel isn't language-specific while bytecode injection is, but bytecode injection has richer context (e.g., which endpoint was called, not just which port data went to).

## 3. Problem statement

Write this only after section 2. A good problem statement names a person, a situation, and a pain, and says nothing about the solution.

- **Who:** the user of your system (an on-call engineer? a developer? a platform team?)
- **Situation:** what are they doing when the problem hits?
- **Pain:** what's slow, hard, or impossible for them today? Why don't existing tools fully solve it?
- **One-sentence statement:** fill in *"When \_\_\_, a \_\_\_ struggles to \_\_\_ because \_\_\_."*

Think back to your NMC2 internship: were there moments you or your team couldn't tell why something broke? That's real evidence, and stronger than anything you read.

**Your draft:**

- **Who:** an on-call engineer responsible for running services.
- **Situation:** a running service breaks or slows down.
- **Pain:** logs alone aren't enough to understand the failure, the evidence is scattered across sources (app logs, Kubernetes logs, and more), and correlating it by hand is slow.
- **Why existing setups fall short:** the data is often incomplete or disconnected. Even at NMC2, the observability team was still building out collection of this data.
- **Statement:** When a running service breaks or slows down, an on-call engineer struggles to find the root cause quickly because logs alone aren't enough, the evidence is scattered across disconnected sources, and correlating it by hand is slow.

## 4. Scope

The out-of-scope list matters as much as the in-scope one. Every item you cut is time you get back for depth.

|  | Your list |
| --- | --- |
| In scope for v1 (core, built first) | **System to monitor:** the OpenTelemetry demo, plus one small hand-instrumented service (a worker reading from the demo's Kafka queue). **Sensors:** Postgres, Valkey, Kafka (one database, one cache, one queue). **Failure scenarios** (via the demo's feature flags): ad service high CPU (easy: cause and symptom in the same place, a baseline check), Kafka overload (hard by distance: symptoms in checkout and fraud detection, cause in Kafka), recommendation memory leak (hard by time: gradual, so the challenge is noticing when it started). |
| In scope for v1 (added after the core works) | **Multi-machine:** one agent per machine from day one; prove scale-out on cloud VMs (see Deployment in section 5), so I can show the system scales to multiple machines. **Logs:** ingest and store the demo's OTel logs and link them by trace ID, so root-cause findings can show matching logs as evidence; completes the logs/metrics/traces stack. No full log search UI. **Alerting:** send a notification (e.g., Slack webhook) once a root cause is found, to show actionable insights. Built last. |
| Explicitly out of scope | Polished web UI (a basic dashboard or command-line output is enough). Users, logins, and multiple teams. Sensors for the demo's other infrastructure (Envoy, nginx, flagd). |
| Maybe later | Agent layer that explains the root-cause reasoning in words. Vector DB of past incident records for RAG, feeding that agent layer. |

**MVP definition.** What is the smallest version that still demonstrates the core idea from section 3? Describe one demo, step by step, that would convince a skeptical engineer it works.

1. Run one command. It starts the OpenTelemetry demo and my system (agent, pipeline, backend).
2. With no configuration, the agent discovers Postgres, Valkey, and Kafka, and they show up with live metrics. Traces from the demo start flowing in.
3. Kill one component of my own pipeline for a minute, then restart it. Show that no telemetry was lost.
4. Flip the Kafka overload feature flag in the demo's feature UI.
5. Checkout and fraud detection show symptoms. The engine walks the dependency graph and names Kafka as the root cause, showing the evidence behind its answer.
6. Turn the flag off and show the system recovering.

This is the core MVP only. Multi-machine, logs, and alerting come after it works.

**Check yourself:** could you cut your MVP in half and still have something worth showing? If yes, cut it.

## 5. Open design questions

For each question, find at least two real options before picking one. If you can only think of one, that's a sign to research more. Skip rows that don't apply to your scope, and add rows for questions you discover.

| Area | Question | Options you found | Tradeoffs | Your pick |
| --- | --- | --- | --- | --- |
| Discovery | How does the system learn what's running on a host without being told? | (A) Scan /proc for processes and identify them by name, port, executable path, or a protocol probe. (B) Query the Docker daemon's API for running containers and their images. | /proc sees every process, including ones not in a container, with more detail, but container info is buried. Docker gives container and image names directly (easy mapping to services), but misses anything outside a container. | /proc as the base (it sees everything), with Docker on top to add container and image names. |
| Collection | How often do you collect, and what does each choice cost in CPU, network, and storage? | 1 second; 1 second plus rollups; 5–10 seconds. | 5–10 seconds is cheaper, but the engine can't tell which of two events came first if they happen a few seconds apart (e.g., Kafka lag before checkout slows). 1 second gives that precision; at v1 scale (\~200 metrics, about 17 million points a day) it's small once compressed in a columnar database. Rollups keep storage in check if it scales. | 1 second plus rollups: keep 1-second data for recent hours, roll older data up to 1-minute averages. |
| Instrumentation | How do request-level traces get created inside an app? | Demo services are already instrumented with OpenTelemetry. For my Kafka worker: (A) OTel auto-instrumentation; (B) OTel SDK, manually; (C) no library, building trace headers and spans by hand. | A writes zero tracing code, which defeats the purpose of the worker. C is the most hands-on, but the extra work is on ID formats and exporting, not propagation. B still has me write the propagation logic (reading context from Kafka message headers, starting spans, passing context on) and is what engineers use in practice. | B: OpenTelemetry SDK with manual instrumentation for the Kafka worker. |
| Transport | How does data get from a host to the backend? What happens when the backend is down? | (A) Agent sends straight to the database. (B) Agent sends to a message queue and the backend consumes from it; the agent buffers locally when the queue pushes back. For B: share the demo's Kafka, or run a separate queue. | A loses data whenever the database is down or slow. B survives outages and spikes. Sharing the demo's Kafka is less setup, but my Kafka overload scenario would overload my own pipeline too, so monitoring would go partially blind exactly when the incident happens. Monitoring shouldn't share failure modes with what it monitors. | B, with a separate queue just for my pipeline, plus local buffering in the agent for backpressure. |
| Processing | What has to happen to raw data before it's useful, and where does that run? | Transformations: raw counters → rates (e.g., CPU usage = change in CPU time since the last reading); spans → complete traces; spans → RED metrics per service; spans → dependency graph edges (who calls whom). Each can run in the agent (per host) or the backend (after the queue). | Jobs that only need one host's data (counters → rates, which just needs the previous reading) can run in the agent. Jobs that need data from many hosts can't: one trace's spans come from different hosts, and an agent only sees its own side of a call between hosts (checkout on host A calling payment on host B). | Agent: counters → rates. Backend: trace assembly, RED metrics per service, and the dependency graph. |
| Trace assembly | Spans for one trace arrive from different hosts at different times. When is a trace "complete"? | (A) Hold spans in memory until no new spans arrive for a trace for N seconds, then write the complete trace. (B) Assemble on read: write every span immediately, tagged with its trace ID, and assemble the trace at query time. | A costs memory and risks losing late spans that arrive after the trace is closed. B loses nothing (a late span just shows up the next time someone queries that trace ID) and holds nothing in memory, since I keep every trace and never need to decide what to throw away. But the root-cause engine could analyze a trace before its slowest span arrives, and during an incident the slowest span is often the culprit, so it might blame the wrong service. | B, assemble on read. The engine waits until no new spans have arrived for a trace for N seconds before analyzing it. Starting with N = 10 seconds as a balance between speed and completeness; adjust after measuring real span timing in the demo. |
| Storage | Where does each kind of data live, and for how long? | Row store (Postgres) vs column store (ClickHouse) per data type. For spans in ClickHouse: (A) sort by trace ID; (B) sort by service and time; (C) sort by service and time, plus a small table mapping each trace ID to its time range. | Metrics, spans, and logs are high-volume and scanned across a few columns (e.g., RED metrics computed from span durations), which suits a column store. The dependency graph is small and its edges are looked up and updated individually, which suits a row store. A ClickHouse table is sorted one way: A makes RED queries scan more, B makes trace lookups search widely, C gives both but adds another table to write to and keep in sync. | ClickHouse: metrics (1-second data for recent hours, then rollups), spans (option C), logs. Postgres: dependency graph, plus an incident record per root-cause finding (cause, time window, trace IDs used, key metric values), kept permanently. Raw spans and logs kept about two days, so I can look back at yesterday's test run. Vector DB for RAG over incident records goes under "maybe later" with the agent layer. |
| Relationships | How does the system know which services depend on which? | Service → service: (A) span-based edges, where a span whose parent belongs to a different service becomes an edge (parent service → child service); (B) connection-based edges, from open network connections listed in /proc matched to discovered processes. Service → infrastructure: /proc plus Docker from discovery (which process runs in which container on which host). | Spans carry rich context: which operation, how long it took, whether it failed, how often. But components that don't produce spans never show up. Connections catch those, but only tell you two things are connected, not latency or errors (same tradeoff as eBPF vs in-app instrumentation). | Both. Spans are the primary source, since the root-cause engine needs their latency and error data; connections fill the gaps for anything without spans. Infrastructure edges come from discovery. |
| Analysis | When something goes wrong, how does the system point toward a cause? | Noticing: (A) fixed thresholds; (B) a baseline per service that keeps learning; (C) a frozen baseline learned during a known-good period; plus an optional trend check for steady growth. Finding the cause: use the dependency graph (structure) and 1-second timing (order). | Fixed thresholds can't fit services whose normal values differ a lot (5ms vs 200ms). A baseline that keeps learning drifts: it slowly absorbs a gradual memory leak and never flags it. A frozen baseline avoids drift but can miss slow leaks on its own; a leak only goes up, so a trend check catches it. For the cause: a flagged component that several flagged services depend on explains all the symptoms, and a cause must turn abnormal before its effects. When structure and timing agree, the engine is confident; when they disagree, it should say it's less sure. | Notice: frozen baseline per service plus a trend check. Find: walk the dependency graph from the flagged services to a shared flagged dependency, and confirm it turned abnormal first. Show: graph view with the broken path highlighted, a timeline of when each component turned abnormal, metric charts against the frozen baseline, and one example trace (plus linked logs once logs are built). All saved in the incident record. |
| Deployment | Where does each component run for your demo? | (A) Local Docker Compose on my laptop. (B) Local multi-node kind cluster. (C) Cloud VM. | My laptop has 16 GB of RAM. The demo's low-memory minimal mode drops Kafka, which my Kafka overload scenario needs, so I need the full demo (\~6 GB) plus my stack (\~2–3 GB) plus OS and apps (\~4 GB): tight but workable locally. A multi-node kind cluster adds Kubernetes overhead on top, likely too much for 16 GB. A cloud VM has room but costs money and must be stopped when not in use. | Core phase: local Docker Compose with the full demo. Scale-out phase: a cloud VM, used only for that phase. First build session: start the full demo alone and check memory use to confirm the plan. |

**Stress-test your picks.** For each one, answer: what happens when it fails, when load goes up 10x, and when data arrives late or twice?

*Stress-test fixes (suggested by Claude, reviewed and accepted):*

**Already covered by my design**

- Backend down → the queue holds data; agents buffer when the queue pushes back (Transport).
- Demo's Kafka overloaded → my pipeline uses a separate queue, so monitoring doesn't go blind (Transport).
- Late spans → written whenever they arrive and show up in the next query (Trace assembly).
- 10x metric volume → rollups keep storage in check (Collection).

**Data arriving twice**

- Duplicate metric points (agent resends after a lost acknowledgment) → give every point a unique key (metric + labels + timestamp) and keep one copy per key, so resends are harmless. This makes at-least-once delivery behave like exactly-once.
- Duplicate spans → every span already has a unique span ID; keep one copy per span ID.
- Backend consumer crashes mid-batch → only mark a batch as done in the queue after it's safely written to storage. On restart it re-reads the batch, and the duplicate rules above absorb the repeats.

**Something fails**

- Agent restarts → it loses its previous counter readings, so skip the first reading instead of emitting a rate. Also, if a counter goes down (the process restarted), treat it as a reset, not a negative rate.
- Agent's local buffer → keep it on disk, not in memory, so an agent restart doesn't lose buffered data. Cap its size.
- Agent's buffer fills up (long outage or 10x load) → degrade gracefully: send coarser metrics and keep failed or slow traces over healthy ones.
- Root-cause engine crashes mid-incident → keep it stateless: everything it needs is in storage, so on restart it re-runs over the recent window. Give each incident a unique ID so a re-run updates the same incident record instead of creating a duplicate.
- Postgres down → the engine keeps using the last known dependency graph; graph updates retry until Postgres returns.
- A new container starts between discovery scans → subscribe to Docker's event stream so the agent hears about new containers immediately, with the /proc scan as a backstop.

**Timing across machines (scale-out phase)**

- The engine's "which turned abnormal first" check compares timestamps from different hosts. If their clocks disagree by even a second, it could get the order wrong. On one laptop there's a single clock, so this isn't an issue for the core. For the cloud VMs, keep clocks synced, and have the engine treat very small time gaps across hosts as "too close to call" and lower its confidence.

## 6. Tech choices

Fill this in only after section 5. Tools follow from design, not the other way around. "I already know it" is a valid reason, but say so honestly, and also say what you'd use if you were starting fresh.

| Component | Language / tool | Why this one | What you rejected and why |
| --- | --- | --- | --- |
| Agent | Go | Compiles to one standalone file with no runtime to install, uses little memory, and handles many tasks at once easily (several sensors in parallel). Docker's official client library is Go-native. Already know it from my internship. | Python: needs an interpreter and packages on every host, heavier memory. Java: the JVM uses a lot of memory just to start. Rust: efficient, but a steep learning curve would take time from the observability concepts. |
| Metrics, spans, logs storage | ClickHouse | Column store suits high-volume data scanned across a few columns (section 5, Storage). | Postgres: row store reads every full row for single-column scans. |
| Dependency graph and incident records | Postgres | Small data, looked up and updated row by row (section 5, Storage). | ClickHouse: built for scans, not individual row updates. |
| Kafka worker instrumentation | OpenTelemetry SDK (manual) | I write the propagation logic myself, which is the point of this worker (section 5, Instrumentation). | Auto-instrumentation: writes the tracing for me, defeating the purpose. |
| Local deployment | Docker Compose | Simplest way to run the full demo plus my stack on one laptop (section 5, Deployment). | Local kind cluster: too much overhead for 16 GB. |
| Message queue | Redpanda | Durable and replayable (re-read a batch after a consumer crash), and lighter than a second Kafka next to the demo's on a 16 GB laptop. Speaks Kafka's protocol, so the code and concepts (topics, partitions, offsets, consumer groups) are the same as Kafka, and I can switch to real Kafka with a config change. | Kafka: first pick for resume value, but a second instance is heavy for my memory budget. NATS JetStream: light, but less standard for telemetry pipelines. RabbitMQ: consumed messages are typically gone, so replay is awkward. |
| Backend processor | Go | High-throughput streaming job (Redpanda → ClickHouse), which Go handles well. Same language as the agent and engine: one toolchain, shared code (data types, ClickHouse queries). | Java: would double as practice for a JVM-heavy backend world, but adds a second language and toolchain. |
| Root-cause engine | Go | Same language as the rest of the system and shares its code. The statistics needed are simple (averages, standard deviations for the baseline, a slope for the trend check), so I'll write them myself, which fits my goal of understanding how it works. | Python: rich statistics libraries and fits my statistics minor, but adds a second language; speed wasn't the issue since the engine runs occasionally. |
| Kafka worker language | Python | My most comfortable language, so I can focus purely on the tracing concepts (propagating context through Kafka) while using the OpenTelemetry SDK. | Go: would keep the whole system in one language. |
| Showing results | Grafana connected to ClickHouse | Free and standard; metric charts with almost no code. **?** Verify in the first build session that its node-graph panel and trace view work with ClickHouse data for the dependency graph and example trace. Fallback: a tiny custom page just for the graph. | CLI only: can't show charts or a graph. Custom web page for everything: polished UI is out of scope. |

**Ask yourself:** is there a choice here that would make you learn something you'd otherwise avoid? Is that worth the extra time?

- My answer: I considered adding Java (e.g., for the backend processor) as practice for a JVM-heavy backend world, but I want this to stay a doable project. Java would add a second backend language and toolchain, plus JVM memory on an already tight laptop, for a narrow upside. A project I finish and deeply understand matters more; the learning here lives in the design, not the languages. I can practice Java separately if needed.

## 7. Risks and unknowns

List what could sink the project or eat your time. Include technical risks (something you've never done) and personal ones (exam weeks, recruiting).

| Risk | How likely | What you'd do if it happens |
| --- | --- | --- |
| Getting busy or inconsistent with the project | High | Fixed daily time slot, even 15 minutes. End each session by writing down the very next step so the next one starts immediately. |
| Running into blocks that stop serious progress | Medium | If the same block survives two sessions, ask for a hint (not the code) or switch to another task in the milestone and come back later. |

**Biggest unknown:** which part are you least sure you can build? Plan a small spike to test it before committing to the full design.

- Putting everything together and running the demo (integration).
- Spike, a "walking skeleton": build the thinnest path through every layer first, then thicken each piece. One metric makes the whole trip: the Go agent reads CPU usage from /proc (change in CPU time since the last reading) → sends it to Redpanda → the Go backend processor consumes it and writes it to ClickHouse → it shows up in a Grafana chart.

## 8. Milestones and time budget

Start with a realistic weekly hour count alongside classes, research, and recruiting. Then size milestones to fit it, not the other way around.

**Hours per week you can actually give this:** at least 15 minutes a day (\~2 hours/week) as the floor. **?** Realistic average, including longer weekend sessions.

*Milestones (drafted by Claude, reviewed and accepted). Hour estimates are rough guesses; replace them with real numbers after M1.*

| Milestone | What works at the end of it | How you'll prove it works | Estimated hours |
| --- | --- | --- | --- |
| **M1. Walking skeleton** | One metric (host CPU usage) travels agent → Redpanda → backend processor → ClickHouse → Grafana. Full demo running alongside. | The CPU chart in Grafana moves when I load the machine. Memory check: demo + my stack fit in 16 GB. | 6–10 |
| **M2. Discovery and sensors** | Agent finds processes via /proc, enriched with Docker; Postgres, Valkey, and Kafka sensors collect every 1 second; rollups to 1-minute. | Start the demo with no config; all three show up with live metrics. Restart a container and the agent re-discovers it. | 8–12 |
| **M3. Lossless pipeline** | Disk-backed agent buffer, unique keys for dedup, batches marked done only after they're written. | Kill the backend processor (or Redpanda) for a minute, restart it: no gaps and no duplicates in ClickHouse. (MVP demo step 3.) | 6–10 |
| **M4. Traces** | OTel spans from the demo stored in ClickHouse (sorted by service and time, plus the trace-ID lookup table); RED metrics per service; my Python Kafka worker hand-instrumented. | Look up one trace ID and see its full path, including a hop through my worker across Kafka. | 10–15 |
| **M5. Dependency graph** | Edges from spans (primary) and /proc connections (gaps) stored in Postgres; graph shown in Grafana or a tiny fallback page. | The graph shows checkout → Kafka → fraud detection and my worker, with no edges entered by hand. | 6–10 |
| **M6. Root-cause engine (core MVP)** | Frozen baselines, trend check, graph walk, timing order, incident record. | All 3 injected failures (ad service CPU, Kafka overload, memory leak) identified correctly; full MVP demo runs end to end. | 12–18 |
| **M7. Multi-machine** | One agent per VM on a cloud VM setup; backend combines them; clock-sync safeguard in the engine. | Same Kafka scenario diagnosed correctly with services spread across machines. | 6–10 |
| **M8. Logs** | Demo's OTel logs stored in ClickHouse and linked by trace ID. | An incident finding shows the matching logs as evidence. | 4–6 |
| **M9. Alerting** | Slack notification when a root cause is found. | Flip the Kafka flag, get a Slack message naming Kafka with a link to the evidence. | 2–4 |

Each milestone should be a stopping point: if you stopped there, would you have something you could show and explain?

## 9. Build plan: what you write vs. delegate

My approach: Claude Code does most of the implementation, and I own the plan, the tests, and the understanding. Goals: understand what it's doing, and get better at orchestration, writing tests, and writing detailed plans instead of broad ones. One piece per learning target I write by hand.

| Piece | You write it | Delegate | Why |
| --- | --- | --- | --- |
| Context propagation in the Python Kafka worker | ✓ |  | Learning target 1 (trace propagation), hand-written. |
| Dedup keys and commit-after-write in the backend processor | ✓ |  | Learning target 2 (reliable data movement), hand-written. |
| Root-cause graph walk (shared dependency + timing order) | ✓ |  | Learning target 3 (graph-based root cause), hand-written. |
| Everything else: /proc reader, discovery, sensors, disk buffer, ClickHouse schemas, graph edges, baselines and trend check, rollups |  | ✓ | I write the detailed plan and test cases; Claude Code implements; I explain it back before merging. |
| Plumbing: Docker Compose, Grafana setup, CI, test scaffolding |  | ✓ | Not interview material; delegate fully, still reviewed. |

**Working agreement for each session:**

- [ ] One task = one piece, never a whole milestone. If I can't write a detailed plan for it, it's too big or I don't understand it yet: split it.
- [ ] Before Claude Code writes anything: I write the plan (inputs, outputs, edge cases, which section 5 decision it implements) and the list of test cases with expected results.
- [ ] After it writes: I explain what the code does and why without rereading it (the whiteboard test). If I can't, Claude Code walks me through it before I move on.
- [ ] I add one test I think might break it.
- [ ] End each session by writing down the very next step, and add a line to the decision log for any decision made.