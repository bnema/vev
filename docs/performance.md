# Performance measurement

vev measures the current session and attachment stack with process-local runtime
marks and harness-owned end-to-end boundaries. Measurement does not alter
rendering, pacing, transport policy, wire bytes, or `ProtocolVersion`.

## Canonical matrix

The canonical matrix is [`testdata/perf/manifest.json`](../testdata/perf/manifest.json):

- topologies `1x4`, `4x1`, `4x4`, and `8x1`;
- `120x40` terminal content with 10,000 history rows per pane;
- idle, active output, all/inactive output, interactive flood, copy search,
  resize sweep, snapshot/output/resize, and attach/restore/tab switch;
- local, SSH stdio, direct UDP, UDP with 25/100 ms latency, and UDP with 0%/1%
  loss.

The matrix contains 252 scenarios (4 topologies × 9 workloads × 7
transports). Inapplicable scenarios stay present with an explicit reason.
Multiple-attachment scenarios measure shared session mutations together with
independent attachment views and output streams.

A connection handshake is bounded by 15 seconds. A command request is bounded
by 10 seconds. These limits apply during measurement as well as normal use.

## Trace schema and clocks

Every process JSONL record has exactly these fields:
`schema`, `process_id`, `component`, `scenario`, `run`, `sequence`, `request_id`,
`epoch`, `kind`, `tick`, `bytes`, `fragments`, `retransmits`, `pending`,
`ack_rtt_nanos`, and `valid`.

Schema 1 is represented by:

```json
{"schema":1,"process_id":"p","component":"daemon","scenario":"s","run":1,"sequence":1,"request_id":1,"epoch":1,"kind":"diff_start","tick":0,"bytes":0,"fragments":0,"retransmits":0,"pending":0,"ack_rtt_nanos":0,"valid":true}
```

The concrete JSONL observer stamps `tick` with its process-local injected
monotonic clock; producers supply no timestamp. Components in one OS process
share that observer. The harness owns a separate monotonic clock for the
input-injection to successfully-flushed-terminal-bytes boundary. Ticks from
different processes or clock domains are never ordered or subtracted. Records
correlate through scenario, run, sequence, request, and epoch IDs.

Process-local spans cover capture, compose, diff, queue wait, acknowledgement
blocking, emit, adapter send, and adapter receive. Bytes, fragments,
retransmits, pending work, and acknowledgement RTT remain diagnostics rather
than duration inputs. Reports retain raw records, counts, p50/p95/p99, and max
where applicable.

## Running the matrix

Use a clean build and at least a 10-second warmup, a 30-second measured
interval, and ten independent repetitions:

```sh
go build -o /tmp/vev ./
rm -rf testdata/perf/results
go run ./cmd/vev-perf-harness \
  --vev-bin /tmp/vev \
  --manifest testdata/perf/manifest.json \
  --out testdata/perf/results \
  --warmup 10s --duration 30s --repetitions 10
```

Each run writes `manifest.json`, `raw-harness.jsonl`, one trace per launched
process, `runs.json`, and `summary.json`. The process manifest records process
identity, role, scenario/run, clock domain, and its exclusive trace path.

The harness injects at most one measured event per second. A sample requires
both input injection and successful terminal flush inside the measured
interval; warmup, straddling, and failed events remain raw diagnostics. Reject
results with missing or unmatched boundaries, invalid correlation,
nonmonotonic same-process sequence, negative same-domain duration, fewer than
10 complete in-interval pairs, invalid denominators, cross-process tick math,
less than 30 seconds measured, or fewer than 10 repetitions.

Before a full run, validate the manifest and harness:

```sh
jq -e '(.topologies|length)==4 and (.workloads|length)==9 and (.transports|length)==7 and (.scenarios|length)==252' testdata/perf/manifest.json
go test ./cmd/vev-perf-harness
```

For a bounded local check, add `--scenario 1x4-idle-local`. The harness still
validates the complete manifest before selecting that scenario. Do not commit
result directories; raw traces and run manifests are the measurement evidence.

## In-process checks

These commands measure local daemon or VT work only, not end-to-end transport
latency:

```sh
go test ./internal/usecase/daemon -run '^$' -bench '^BenchmarkDaemonHistory' -benchtime=1x -benchmem
go test ./pkg/renderer ./pkg/vt ./internal/adapters/ipc -run '^$' -bench=. -benchmem
go test ./internal/usecase/daemon -run '^$' -bench '^BenchmarkComposeCapturedFloatingFrameCached$' -benchmem
```
