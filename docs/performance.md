# Performance measurement

## S5a transport observability

S5a is measurement-only. It records process-local runtime marks and harness-owned
end-to-end boundaries without changing rendering, pacing, transport policy, wire
bytes, or `ProtocolVersion`. The canonical matrix and workload/topology/
transport coverage are in [`testdata/perf/manifest.json`](../testdata/perf/manifest.json):
`1x4`, `4x1`, `4x4`, and `8x1`, all at `120x40` with 10,000 rows per pane;
idle, active/all/inactive output, interactive flood, copy search, resize sweep,
snapshot/output/resize, and attach/restore/tab switch; local, SSH stdio, UDP
baseline, UDP 25/100 ms, and UDP 0%/1% loss. It has 252 scenarios (4×9×7),
with inapplicable cases required to be explicit. The seven transport entries are
`local`, `ssh_stdio`, `udp_baseline`, `udp_loss_0pct`, `udp_25ms`, `udp_100ms`,
and `udp_loss_1pct`.

### Trace schema and clock domains

Every process JSONL record has exactly these fields:
`schema` (uint), `process_id` (string), `component` (string), `scenario`
(string), `run`, `sequence`, `request_id`, `epoch` (uint), `kind` (string),
`tick` (int64), `bytes`, `fragments`, `retransmits`, `pending` (uint),
`ack_rtt_nanos` (int64), and `valid` (bool). Schema 1 is represented by:

```json
{"schema":1,"process_id":"p","component":"daemon","scenario":"s","run":1,"sequence":1,"request_id":1,"epoch":1,"kind":"diff_start","tick":0,"bytes":0,"fragments":0,"retransmits":0,"pending":0,"ack_rtt_nanos":0,"valid":true}
```

`tick` is stamped only by the concrete JSONL observer using its process-local
injected monotonic clock; producers supply no timestamp. The harness sets `VEV_PERF_TRACE`,
`VEV_PERF_PROCESS_ID`, `VEV_PERF_SCENARIO`, and `VEV_PERF_RUN` for every
launched role, identifying its exclusive trace path and process/scenario/run
mapping. Components in one OS
process share that observer. The harness clock is a separate monotonic domain
and is the sole clock for input-injection → terminal-bytes-successfully-flushed
end-to-end samples. Cross-process ticks are never ordered or subtracted;
records correlate only by scenario/run/sequence/request/epoch IDs.

The named process-local spans are capture, compose, diff, queue wait,
ACK-blocked interval, emit, adapter send, and adapter receive. Their start/end
marks are paired within one process and summarized by component/adapter with
raw records, counts, p50/p95/p99, and max. Diagnostic fields (bytes, fragments,
retransmits, pending, ACK RTT) remain separate from duration calculations.

### Proxy ANSI baseline

The current proxy path transports ANSI and applies it through the local proxy
VT parser. Build the fixed baseline binary and harness, then verify the harness
and parser fixtures:

```sh
go build -o /tmp/vev-proxy-baseline .
go build -o /tmp/vev-perf-harness ./cmd/vev-perf-harness
go test ./cmd/vev-perf-harness -count=1
go test ./internal/usecase/daemon -run '^TestProxyANSIBenchmarkFixtures$' -count=1
go test ./internal/usecase/daemon -run '^$' \
  -bench '^BenchmarkProxyANSIApply$' -benchmem -count=10
```

The pre-change parser results below are medians of ten repetitions on
Linux/amd64 with an AMD Ryzen 9 7900X3D. The runtime source was commit
`919bcd18c428`; the benchmark command above ran with Go 1.26.5.

| ANSI workload | ns/op | B/op | allocs/op | ANSI bytes/op |
| --- | ---: | ---: | ---: | ---: |
| absolute-position one-cell | 91.06 | 16 | 1 | 9 |
| 120-column full line | 4,659.5 | 129 | 1 | 126 |
| fragmented truecolor styled line | 5,861 | 450 | 1 | 419 |
| full-width 40-row scroll | 233,271.5 | 5,472 | 1 | 4,880 |

Keep the ordinary local ANSI renderer as a separate guardrail:

```sh
go test ./pkg/renderer -run '^$' -bench '^BenchmarkRenderer' -benchmem -count=10
```

### Public-CLI transport runs

Use a 10-second warmup, a 30-second measured interval, and ten independent
repetitions. This representative matrix covers one-cell active output,
120-column full-line output, fragmented truecolor-styled output, sustained
interactive flood, resize sweep, snapshot/output/resize, and attach/tab-switch
across local, SSH stdio, UDP baseline, UDP 25 ms, and UDP 1% loss (35
scenarios):

```sh
rm -rf /tmp/vev-proxy-selected
mkdir -m 700 /tmp/vev-proxy-selected
for workload in \
  active_output all_output inactive_output interactive_flood resize_sweep \
  snapshot_output_resize attach_restore_tab_switch
do
  for transport in local ssh_stdio udp_baseline udp_25ms udp_loss_1pct
  do
    scenario="1x4-${workload}-${transport}"
    /tmp/vev-perf-harness --vev-bin /tmp/vev-proxy-baseline \
      --manifest testdata/perf/manifest.json --scenario "$scenario" \
      --out "/tmp/vev-proxy-selected/$scenario" \
      --warmup 10s --duration 30s --repetitions 10
  done
done
```

`all_output` and `inactive_output` provide full-line and styled payload shapes;
these runs do not claim to activate every pane in their manifest topologies.
Run the complete canonical matrix only when full-matrix evidence is required:

```sh
rm -rf /tmp/vev-proxy-matrix
/tmp/vev-perf-harness --vev-bin /tmp/vev-proxy-baseline \
  --manifest testdata/perf/manifest.json --out /tmp/vev-proxy-matrix \
  --warmup 10s --duration 30s --repetitions 10
```

Warmup is excluded. During the measured interval the harness injects at most
one event per second. At least 10 complete event pairs are required: both input
injection and successful terminal flush must fall inside the interval.
Straddling or failed events remain raw diagnostics, not samples. Each run must
contain `manifest.json`, `raw-harness.jsonl`, per-process JSONL, `runs.json`, and
`summary.json`.

`summary.json` records the source `git_sha` and measurement parameters;
`runs.json`, run manifests, and JSONL identify scenario, run, process, component,
and correlation IDs. Preserve those files with the benchmark output and record
`go version -m /tmp/vev-proxy-baseline`; do not commit result directories. CPU
and allocation evidence comes from Go microbenchmarks and process-local span
durations. End-to-end latency, terminal flush, and byte evidence comes from the
public-CLI harness. Reset, snapshot, and span counters for structured output are
candidate-only until that output exists.

Reject results with missing input/flush boundaries, unmatched IDs, bad
correlation, nonmonotonic same-process sequence, negative same-domain spans,
fewer than 10 complete in-interval event pairs, invalid denominators, any
cross-process tick arithmetic, or unmet duration/repetition minima. Raw JSONL
and run manifests are evidence; no result alone authorizes an optimization.

## Existing in-process smoke checks

These commands measure local daemon work only and are not end-to-end transport
results:

```sh
go test ./internal/usecase/daemon -run '^$' -bench '^BenchmarkDaemonHistory' -benchtime=1x -benchmem
go test ./pkg/renderer ./pkg/vt ./internal/adapters/ipc -run '^$' -bench=. -benchmem
go test ./internal/usecase/daemon -run '^$' -bench '^BenchmarkComposeCapturedFloatingFrameCached$' -benchmem
```
