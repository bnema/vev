# Performance measurement

## S5a transport observability

S5a is measurement-only. It records process-local runtime marks and harness-owned
end-to-end boundaries without changing rendering, pacing, transport policy, wire
bytes, or `ProtocolVersion`. The canonical matrix and workload/topology/
transport coverage are in [`testdata/perf/manifest.json`](../testdata/perf/manifest.json):
`1x4`, `4x1`, `4x4`, and `8x1`, all at `120x40` with 10,000 rows per pane;
idle, active/all/inactive output, interactive flood, copy search, resize sweep,
snapshot/output/resize, and attach/restore/tab switch; local, SSH stdio, UDP
baseline, UDP 25/100 ms, and UDP 1% loss. It has 216 scenarios (4×9×6), with
inapplicable cases required to be explicit.

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
injected monotonic clock; producers supply no timestamp. Components in one OS
process share that observer. The harness clock is a separate monotonic domain
and is the sole clock for input-injection → terminal-bytes-successfully-flushed
end-to-end samples. Cross-process ticks are never ordered or subtracted;
records correlate only by scenario/run/sequence/request/epoch IDs.

The named process-local spans are capture, compose, diff, queue wait,
ACK-blocked interval, emit, adapter send, and adapter receive. Their start/end
marks are paired within one process and summarized by component/adapter with
raw records, counts, p50/p95/p99, and max. Diagnostic fields (bytes, fragments,
retransmits, pending, ACK RTT) remain separate from duration calculations.

### Baseline procedure and validity

Use the public CLI harness (not internal packages):

```sh
go build -o /tmp/vev-perf ./
rm -rf testdata/perf/results
go run ./cmd/vev-perf-harness --vev-bin /tmp/vev-perf \
  --manifest testdata/perf/manifest.json --out testdata/perf/results \
  --warmup 10s --duration 30s --repetitions 10
```

The minimums are 30 seconds measured and 10 independent repetitions; warmup is
excluded. Each run must contain `manifest.json`, `raw-harness.jsonl`, per-process
JSONL, `runs.json`, and `summary.json`. Reject results with missing input/flush
boundaries, unmatched IDs, bad correlation, nonmonotonic same-process sequence,
negative same-domain spans, insufficient measured events, invalid denominators,
any cross-process tick arithmetic, or unmet duration/repetition minima. Raw
JSONL and run manifests are the evidence files; no result authorizes a policy
or optimization change.

## Existing in-process smoke checks

These commands measure local daemon work only and are not end-to-end transport
results:

```sh
go test ./internal/usecase/daemon -run '^$' -bench '^BenchmarkDaemonHistory' -benchtime=1x -benchmem
go test ./pkg/renderer ./pkg/vt ./internal/adapters/ipc -run '^$' -bench=. -benchmem
go test ./internal/usecase/daemon -run '^$' -bench '^BenchmarkComposeFloatingFrameCached$' -benchmem
```
