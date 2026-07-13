# S5a transport-observability evidence

`manifest.json` is the complete, measurement-only public-CLI matrix: four
120x40 topologies (`1x4`, `4x1`, `4x4`, `8x1`), 10,000 history rows per pane,
nine workloads, and seven transports (local, SSH stdio, UDP baseline, UDP 25 ms,
UDP 100 ms, and UDP with 0% or 1% loss). It contains 252 scenario combinations;
there are no omitted combinations. A genuinely unavailable combination must
remain listed with `inapplicable_reason` and no launched roles.

Build and run the harness with at least a 10-second warmup, 30-second measured
interval, and 10 independent repetitions:

```sh
go build -o /tmp/vev-perf ./
rm -rf testdata/perf/results
go run ./cmd/vev-perf-harness --vev-bin /tmp/vev-perf \
  --manifest testdata/perf/manifest.json --out testdata/perf/results \
  --warmup 10s --duration 30s --repetitions 10
```

Each run writes `manifest.json`, `raw-harness.jsonl`, one JSONL trace per
launched process, `runs.json`, and `summary.json`. The process manifest records
process ID, role, scenario/run, clock domain, exact exclusive trace path, and
identity. Every launched role receives `VEV_PERF_TRACE`, `VEV_PERF_PROCESS_ID`,
`VEV_PERF_SCENARIO`, and `VEV_PERF_RUN`; the first is exclusive to that process
and the remaining values correlate it to the manifest. The harness owns only the end-to-end boundaries: input injection to
terminal bytes successfully flushed. It never subtracts ticks from daemon,
client, or adapter processes.

A process trace is JSONL with exactly these keys and types:

```json
{"schema":1,"process_id":"p","component":"daemon","scenario":"s","run":1,"sequence":1,"request_id":1,"epoch":1,"kind":"diff_start","tick":0,"bytes":0,"fragments":0,"retransmits":0,"pending":0,"ack_rtt_nanos":0,"valid":true}
```

`tick` is owned and stamped by that process's injected monotonic clock. Producers
must leave it zero; one observer/clock is shared by components in a process.
Ticks from different `process_id` or clock domains are never ordered or
subtracted. Correlation uses scenario, run, sequence, request ID, and epoch.

Named process-local spans are: capture, compose, diff (`diff_start/end`), queue
wait (`queue_enqueued/dequeued`), ACK-blocked interval, emit, adapter send,
and adapter receive. Adapter spans are reported by component/transport; bytes,
fragments, retransmits, pending, and ACK RTT remain diagnostics. Reports retain
raw records and counts plus p50/p95/p99 (and max where applicable). End-to-end
reports are harness-clock distributions only.

The harness injects at most one measured event per second and requires at least
10 complete in-interval event pairs. Both the input-injection and successful
terminal-flush boundaries must be within the measured interval; warmup and
straddling pairs are excluded from percentile samples (but retained as raw
evidence). Reject a result for missing or unmatched boundaries, missing flush,
invalid correlation, nonmonotonic same-process sequence, negative same-domain
duration, fewer than 10 complete in-interval pairs, invalid denominators,
cross-process tick math, less than 30 seconds measured, or fewer than 10
repetitions. Warmup is excluded from measured samples. Raw traces are evidence, not a transport-policy or
optimization gate; wire bytes, protocol version, rendering policy, pacing,
compression, and loss behavior are unchanged.

## Bounded manifest and harness checks

Before a measurement, validate the manifest shape and run the harness tests without launching the full matrix:

```sh
jq -e '(.topologies|length)==4 and (.workloads|length)==9 and (.transports|length)==7 and (.scenarios|length)==252' testdata/perf/manifest.json
go test ./cmd/vev-perf-harness
```

The full command above is the measurement procedure; its 10-second warmup, 30-second measured interval, and 10 repetitions are the minimum accepted settings. For a bounded local evidence run, add `--scenario 1x4-idle-local`; the harness validates the complete canonical manifest before it applies that selection, so a single-scenario run cannot hide missing matrix coverage.
