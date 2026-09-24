# Performance measurement

For contributors. How to measure vev's latency end to end, and the budgets it should meet.

**Quick start:**

```sh
go build -o /tmp/vev ./
go run ./cmd/vev-perf-harness --vev-bin /tmp/vev \
  --manifest testdata/perf/manifest.json --out testdata/perf/results \
  --scenario 1x4-idle-local
```

Measurement never changes rendering, pacing, transports, or wire bytes.

## Canonical matrix

The canonical matrix is [`testdata/perf/manifest.json`](../testdata/perf/manifest.json):

- topologies `1x4`, `4x1`, `4x4`, and `8x1`;
- `120x40` terminal content with 10,000 history rows per pane;
- idle, active output, all/inactive output, interactive flood, copy search,
  resize sweep, snapshot/output/resize, and attach/restore/tab switch;
- local Unix IPC and SSH stdio.

The matrix contains 72 scenarios (4 topologies × 9 workloads × 2 transports).
QUIC clean/adverse transport evidence is owned by the real-QUIC integration
suite described below rather than simulated by the process launcher.
Inapplicable scenarios stay present with an explicit reason.
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
jq -e '(.topologies|length)==4 and (.workloads|length)==9 and (.transports|length)==2 and (.scenarios|length)==72' testdata/perf/manifest.json
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

Measure the complete production Output path with:

```sh
go test ./internal/adapters/sessionwire -run '^$' \
  -bench '^BenchmarkOutputProductionRoundTrip$' -benchmem -count=5
```

It includes semantic conversion, compression policy, Protobuf marshal, strict
wire scanning, unmarshal, decompression, validation, and semantic conversion
back. Its incremental, compressible snapshot, and deterministic random
incompressible snapshot fixtures are valid `protocol.Output` state chains.
It is a codec benchmark, not a mobile-link latency measurement.

## Protobuf and QUIC budgets

The production Output benchmark covers incremental, compressible snapshot, and
incompressible snapshot traffic. The zlib writer pool is part of the production
path because it materially reduces compression time and allocation volume. The
harness boundary is successful writing of bytes read from the client PTY; it
deliberately does not call `file.Sync`, which measures filesystem durability
rather than terminal delivery.

The release budgets are:

- local p95 input-to-terminal-write ratio ≤1.10× on calm and active-output work;
- steady-state CPU/work ratio ≤1.15× when captured on the same host;
- no unbounded heap, goroutine, or queue growth under slow-reader/adverse tests;
- real QUIC must preserve ordered, duplicate-free typed traffic through the
  clean path and the train profile (25 ms base latency, 15 ms jitter, 4% loss,
  6% duplication, 10% reordering, 500 ms blackout), and through NAT rebinding.

The real-QUIC integration tests are:

```sh
go test ./internal/adapters/quicnettest -race \
  -run 'TestRealQUICTypedConversationOverTrainProfile|TestRealQUICNATRebindKeepsTypedConversation' \
  -count=10
```

They exercise the production QUIC and sessionwire adapters. Latency under the
train profile reflects configured impairment and QUIC recovery, so it is not
compared to the local IPC budget.

The representative QUIC workload is `Input` → validated incremental `Output` →
`Ack`, with 512-byte Input and Output bodies. The evidence test logs latency
percentiles, process CPU, allocations, GC, retained heap, goroutines, wire
amplification, blackout recovery, NAT-rebind recovery, and proxy accounting.
Reproduce the bounded evidence and isolated benchmarks with:

```sh
go test ./internal/adapters/quicnettest -race \
  -run TestRealQUICPerformanceEvidence -count=3 -v
go test ./internal/adapters/quicnettest -run '^$' \
  -bench 'BenchmarkRealQUICExchange|BenchmarkRealQUICBlackoutRecovery|BenchmarkRealQUICNATRebindRecovery' \
  -benchtime=50x -benchmem
```

Process CPU is collected with `getrusage(RUSAGE_SELF)`. It provides an absolute
CPU/work signal for QUIC. Track it together with bounded retained heap and zero
queue overflow; treat the train profile as a resilience workload rather than a
local-latency gate.

Application recovery after irreversible QUIC loss is covered separately by
`TestRealQUICApplicationReconnectResumesTypedSession`. It runs the real client
use case over two distinct production QUIC connections and two sessionwire
preambles. Closing the first accepted connection must trigger a second dial
whose `Hello` uses `IntentResume`, preserves the client ID and previous resume
token, receives a rotated token, accepts a fresh full Output, presents it, and
returns the matching ACK without requesting an Output reset. Run it with:

```sh
go test ./internal/adapters/quicnettest -race \
  -run TestRealQUICApplicationReconnectResumesTypedSession -count=10
```

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
