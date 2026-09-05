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

## Output snapshot encoding

Full state-bearing output snapshots larger than 1 KiB are encoded with
standard-library zlib only when the compressed representation is smaller.
Incremental output, small snapshots, and incompressible snapshots remain
uncompressed. The wire carries a closed compression kind and exact decoded
length; decoding rejects unknown kinds, truncation, trailing bytes, integrity
failures, and output beyond the frame bound.

Measure the representative snapshot encoder with:

```sh
go test ./internal/ports -run '^$' -bench '^BenchmarkMarshalOutput$' -benchmem
```

The benchmark reports source throughput, encoded bytes, allocations, and time
for empty and styled `120×40` snapshots plus a styled `200×60` snapshot. It is
an encoder benchmark, not a mobile-link latency measurement.

## In-process checks

These commands measure local daemon or VT work only, not end-to-end transport
latency:

```sh
go test ./internal/usecase/daemon -run '^$' -bench '^BenchmarkDaemonHistory' -benchtime=1x -benchmem
go test github.com/bnema/vev-vt/ansi github.com/bnema/vev-vt ./internal/adapters/ipc -run '^$' -bench=. -benchmem
go test ./internal/usecase/daemon -run '^$' -bench '^BenchmarkComposeCapturedFloatingFrameCached$' -benchmem
```

## Mouse scroll rendering

Wheel input moves the immutable copy viewport, rather than walking the keyboard
cursor to an edge before scrolling. Ordinary copy paints preserve the output
epoch. For unchanged full-width viewports, composition rotates retained compact
rows and paints only exposed rows and cursor highlights. The ANSI renderer checks
the scroll against its own committed shadow before using a scroll region in
either direction. Selection, search, narrow panes, floating panes, notices, and
geometry/theme changes keep their conservative composition paths.

The unadorned live-pane cache stays separate from the displayed copy viewport.
Neither committed frame is mutated while preparing a candidate; failed output
cannot publish its viewport metadata. Copy exit refreshes live content.

Mouse animation is tested separately with injected clocks: first response,
16 ms pacing, burst accumulation, deceleration, reversal, cancellation, and the
120 ms tail deadline. The rendering benchmark deliberately bypasses those timers
to measure work rather than sleep time:

```sh
go test ./internal/usecase/daemon -run '^$' \
  -bench '^BenchmarkDaemonHistoryCopyScroll$' -benchmem -count=3
go test ./internal/usecase/daemon ./internal/usecase/copy -race \
  -run 'TestCopyScrollAnimation|TestCopyWheel|TestScrollRows|TestRenderRowsRange'
```

### Local comparison (2026-09-05)

AMD Ryzen 5 7535HS, Linux amd64, Go 1.27.1; three repetitions, median time.
Baseline: vev `cb489d5b` with vev-vt v0.5.0. Each operation is two wheel movements
with 10,000 retained rows; setup and copy entry are outside the timed loop.

| Viewport | Before, ms/pair | After, ms/pair | Last ANSI bytes/wheel, before → after |
| --- | ---: | ---: | ---: |
| 120×40 | 1.306 | 0.654 | 4,998 → 713 |
| 182×53 | 2.531 | 1.159 | 9,873 → 1,023 |
| 240×70 | 4.329 | 1.850 | 17,088 → 1,313 |

Important limits of this comparison:

- The old alternating-wheel loop mostly moved a cursor within a stationary
  viewport after its first scroll. The new loop actually moves the viewport on
  every wheel event. These are the same input events, not equivalent old/new
  visual behavior.
- Allocated bytes remain roughly unchanged: about 0.66/1.16/1.95 MB per pair.
  Transactional compact-frame copies still dominate allocations; the speedup
  is not evidence of eliminating GC pressure.
- This repetitive fixture compresses snapshots very well. Encoded output payloads
  **increase** from 574/751/896 to 762/1,072/1,362 bytes per wheel because ordinary
  incremental output is uncompressed. Fewer ANSI bytes means less terminal work,
  not automatically fewer network bytes. Animation also adds intermediate frames.
- These are in-process measurements, not terminal presentation latency or a
  substitute for the canonical remote matrix. Subjective motion and remote
  latency still need real-terminal validation.

Ghostty was inspected as a design reference at local commit `c81f0b268`, especially
`src/renderer/generic.zig` and `src/renderer/Thread.zig`: retain viewport state,
track dirty work, and separate terminal capture from rendering work. vev keeps
its server-side ANSI architecture; no GPU renderer or Ghostty code is imported.
