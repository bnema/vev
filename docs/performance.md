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
process share that observer. A spawned daemon is deliberately stripped of
these variables so it cannot append to a role's trace. The harness clock is a
separate monotonic domain and is the sole clock for input-injection → terminal-
bytes-successfully-flushed end-to-end samples. Cross-process ticks are never
ordered or subtracted; records correlate only by scenario/run/sequence/request/
epoch IDs. A `component=observer`, `kind=transport_diagnostic`, `valid=false`
record explicitly reports bounded-observer loss; spans crossing that gap are
excluded, while an unmatched end without such a gap remains an error.

The named process-local spans are capture, compose, diff, queue wait,
ACK-blocked interval, emit, adapter send, and adapter receive. Their start/end
marks are paired within one process and summarized by component/adapter with
raw records, counts, p50/p95/p99, and max. Diagnostic fields (bytes, fragments,
retransmits, pending, ACK RTT) remain separate from duration calculations.

### Historical proxy ANSI baseline

Before the structured path, proxied state-bearing screen updates transported ANSI
and were applied through the local proxy VT parser. In the candidate, proxied
state-bearing frames are `MsgScreenUpdate`; this parser benchmark remains a
historical diagnostic. Ordinary local output remains the ANSI `MsgOutput`
guardrail.

The binaries were built from clean, exact commits in temporary clones so their
provenance is embedded and checkable:

```sh
GIT_CONFIG_GLOBAL=/dev/null git clone --no-local /home/brice/dev/projects/vev /tmp/vev-perf-baseline-1f2c802-clone
GIT_CONFIG_GLOBAL=/dev/null git -C /tmp/vev-perf-baseline-1f2c802-clone checkout --detach 1f2c802d0707785cab979134db6a95645250a82c
GIT_CONFIG_GLOBAL=/dev/null git clone --no-local /home/brice/dev/projects/vev/.worktrees/perf/proxy-screen-baseline /tmp/vev-perf-candidate-2b48abf9-clone
GIT_CONFIG_GLOBAL=/dev/null git -C /tmp/vev-perf-candidate-2b48abf9-clone checkout --detach 2b48abf9b481c3dbdc371ba13c15cf06051d9df0
(cd /tmp/vev-perf-baseline-1f2c802-clone && go build -o /tmp/vev-proxy-baseline-1f2c802 .)
(cd /tmp/vev-perf-candidate-2b48abf9-clone && go build -o /tmp/vev-proxy-candidate-2b48abf9 .)
GIT_CONFIG_GLOBAL=/dev/null git clone --no-local /home/brice/dev/projects/vev/.worktrees/perf/proxy-screen-baseline /tmp/vev-perf-harness-d479fe9-clone
GIT_CONFIG_GLOBAL=/dev/null git -C /tmp/vev-perf-harness-d479fe9-clone checkout --detach d479fe99581cf1988efba7906914c9ddd8653d7d
(cd /tmp/vev-perf-harness-d479fe9-clone && go build -o /tmp/vev-perf-harness-d479fe9 ./cmd/vev-perf-harness)
go version -m /tmp/vev-proxy-baseline-1f2c802
go version -m /tmp/vev-proxy-candidate-2b48abf9
go version -m /tmp/vev-perf-harness-d479fe9
```

The baseline benchmark below is a measured median of ten repetitions on
Linux/amd64 with an AMD Ryzen 9 7900X3D, using Go 1.26.5 and commit
`1f2c802d0707785cab979134db6a95645250a82c`:

```sh
cd /tmp/vev-perf-baseline-clone
go test ./internal/usecase/daemon -run '^$' \
  -bench '^BenchmarkProxyANSIApply$' -benchmem -count=10 \
  | tee /tmp/vev-perf-results/baseline-1f2c802-proxy-ansi.txt
```

| ANSI workload | ns/op | B/op | allocs/op | ANSI bytes/op |
| --- | ---: | ---: | ---: | ---: |
| absolute-position one-cell | 93.31 | 16 | 1 | 9 |
| 120-column full line | 4,777 | 130 | 1 | 126 |
| fragmented truecolor styled line | 6,119.5 | 450 | 1 | 419 |
| full-width 40-row scroll | 238,709 | 5,477.5 | 1 | 4,880 |

Keep the ordinary local ANSI renderer as a separate guardrail:

```sh
cd /tmp/vev-perf-candidate-2b48abf9-clone
go test ./pkg/renderer -run '^$' -bench '^BenchmarkRenderer' -benchmem -count=10 \
  | tee /tmp/vev-perf-results/candidate-2b48abf9-renderer.txt
```

The candidate guardrail completed with Go 1.26.5; its ten-repetition output is
retained at `/tmp/vev-perf-results/candidate-2b48abf9-renderer.txt`.

### Comparable proxy pipeline benchmark

`BenchmarkProxyPipeline` is a local comparison harness, not a network or
public-CLI result. Its four fixtures—one-cell, full-line, styled-line, and
full-width scroll—use semantically paired initial and mutated `renderer.Frame`
values. Snapshot/full-paint setup is completed before the timed steady-state
loop; the loop applies the same mutation and consecutive state transitions on
both sides.

The `ansi-pipeline` slice measures `Renderer.Prepare`/bytes,
`MarshalOutput`, a real benchmark sender that performs the simulated transport,
`UnmarshalOutput`, `proxyANSIApplyState.apply`, and `Commit`. The
`structured-pipeline` slice measures `structuredOutputStream.prepare` (including
`MarshalScreenUpdate`), a real benchmark sender that performs the simulated
transport, `UnmarshalScreenUpdate`, `proxyScreenState.Apply`, damage
acknowledgement, and `prepared.send` commit. Both slices are local and exclude
network effects. `wirebytes/op` is the complete message payload size excluding
connection framing; unlike the historical table above, it includes the
`MsgOutput` or `MsgScreenUpdate` payload header. `B/op` is the benchmark
allocator metric; explicitly, `B/op != wirebytes/op`. `spans/op` is reported
separately for structured updates. Run it with:

```sh
cd /tmp/vev-perf-candidate-2b48abf9-clone
go test ./internal/usecase/daemon -run '^$' \
  -bench '^BenchmarkProxyPipeline$' -benchmem -count=10 \
  | tee /tmp/vev-perf-results/candidate-2b48abf9-proxy-pipeline.txt
```

Measured candidate medians from that command (Go 1.26.5, Linux/amd64):

| fixture | ANSI ns/op | structured ns/op | ANSI B/op | structured B/op | ANSI wirebytes/op | structured wirebytes/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| one-cell | 530.55 | 1,016.5 | 192 | 888 | 37 | 71 |
| full-line | 8,637.5 | 22,187.5 | 674 | 19,928 | 154 | 190 |
| styled-line | 10,565.5 | 25,682 | 1,652 | 24,952 | 388 | 432 |
| full-width-scroll | 55,713.5 | 73,858 | 760 | 19,944 | 177 | 196 |

The component diagnostics completed ten repetitions with separate
commands; their raw output is retained under `/tmp/vev-perf-results/`:

```sh
cd /tmp/vev-perf-candidate-2b48abf9-clone
go test ./internal/usecase/daemon -run '^$' -bench '^BenchmarkProxyANSIApply$' -benchmem -count=10 \
  | tee /tmp/vev-perf-results/candidate-2b48abf9-proxy-ansi.txt
go test ./internal/usecase/daemon -run '^$' -bench '^BenchmarkStructuredOutputPrepare$' -benchmem -count=10 \
  | tee /tmp/vev-perf-results/candidate-2b48abf9-structured-prepare.txt
go test ./internal/usecase/daemon -run '^$' -bench '^BenchmarkProxyScreenStateApply$' -benchmem -count=10 \
  | tee /tmp/vev-perf-results/candidate-2b48abf9-screen-apply.txt
```

Their medians are respectively 107.45/5,107/6,561.5/251,126.5 ns/op for
ANSI one-cell/full-line/styled-line/scroll rows as reported in the raw file;
structured prepare medians are 673/16,928/23,220.5/61,424.5 ns/op and decoded
apply medians are 53.29/1,225/1,234/3,659.5 ns/op. These remain component
diagnostics, not head-to-head pipeline comparisons.

The 35-scenario public-CLI harness A/B matrix remains the adoption gate. This
local pipeline benchmark does not replace that gate and this document records
no unmeasured result or conclusion.

### Public-CLI transport runs

The adoption gate requires a 10-second warmup, a 30-second measured interval,
and ten independent repetitions for each of seven workloads × five transports
(35 scenarios), for both exact baseline and candidate binaries. This exact
command template was executed for every scenario (the two 35-scenario sets
were run separately to avoid oversubscribing the host):

```sh
for bin in baseline candidate; do
  case "$bin" in
    baseline) vev_bin=/tmp/vev-proxy-baseline-1f2c802 ;;
    candidate) vev_bin=/tmp/vev-proxy-candidate-2b48abf9 ;;
  esac
  for workload in active_output all_output inactive_output interactive_flood resize_sweep \
    snapshot_output_resize attach_restore_tab_switch; do
    for transport in local ssh_stdio udp_baseline udp_25ms udp_loss_1pct; do
      scenario="1x4-${workload}-${transport}"
      /tmp/vev-perf-harness-d479fe9 \
        --vev-bin "$vev_bin" \
        --manifest /tmp/vev-perf-harness-d479fe9-clone/testdata/perf/manifest.json \
        --scenario "$scenario" \
        --out "/tmp/vev-perf-results/public-final-${bin}-matrix/$scenario" \
        --warmup 10s --duration 30s --repetitions 10
    done
  done
done
```

The measured outputs are retained only under `/tmp/vev-perf-results/`. Each
binary completed 13/35 scenarios with ten runs; 22 scenarios stopped before a
summary. The common completed results below are harness end-to-end medians
from `summary.json` (p50/p95, ns):

| scenario | baseline samples | candidate samples | baseline p50/p95 | candidate p50/p95 |
| --- | ---: | ---: | ---: | ---: |
| 1x4-active_output-local | 300 | 300 | 16,913,252 / 17,407,463 | 16,922,803 / 17,444,022 |
| 1x4-active_output-ssh_stdio | 300 | 300 | 17,204,192 / 17,621,643 | 17,209,503 / 17,521,432 |
| 1x4-active_output-udp_baseline | 300 | 300 | 17,065,232 / 17,715,522 | 17,032,283 / 17,729,992 |
| 1x4-active_output-udp_25ms | 290 | 290 | 43,067,851 / 69,676,961 | 43,086,281 / 69,753,290 |
| 1x4-all_output-local | 300 | 300 | 16,977,662 / 17,352,912 | 16,958,053 / 17,326,013 |
| 1x4-all_output-ssh_stdio | 300 | 300 | 17,250,583 / 17,541,982 | 17,284,843 / 17,643,873 |
| 1x4-all_output-udp_baseline | 300 | 300 | 19,826,321 / 20,920,741 | 20,017,971 / 20,717,372 |
| 1x4-all_output-udp_25ms | 290 | 290 | 48,814,889 / 70,035,730 | 48,740,189 / 70,268,490 |
| 1x4-inactive_output-local | 300 | 300 | 17,278,873 / 17,705,613 | 17,279,933 / 17,690,672 |
| 1x4-inactive_output-ssh_stdio | 300 | 300 | 17,140,253 / 17,731,882 | 17,142,882 / 17,781,913 |
| 1x4-inactive_output-udp_baseline | 300 | 300 | 21,030,321 / 23,221,780 | 20,795,161 / 23,274,900 |
| 1x4-inactive_output-udp_25ms | 280 | 280 | 89,857,641 / 104,327,126 | 89,843,102 / 102,844,306 |
| 1x4-attach_restore_tab_switch-local | 300 | 300 | 7,812,797 / 8,501,746 | 7,725,516 / 8,593,246 |

`1x4-active_output-ssh_stdio` now completes for both binaries, so the former
trace-correlation blocker is fixed. The remaining failed scenarios are
retained as stderr evidence: all five `interactive_flood`, all five
`resize_sweep`, all five `snapshot_output_resize`, the four non-local
`attach_restore_tab_switch` transports, and the three `udp_loss_1pct`
output scenarios. They report terminal-start/readiness failures or missing
receive span pairs; they are not converted into samples.

The balanced gate is **blocked, not passed**: only 13/35 scenarios have valid
A/B summaries, so the 5% gate and adoption cannot be declared. The completed
common p50 values happened to stay within ±1.12%, but that is not a gate
result and does not substitute for the 22 missing scenarios.

`summary.json` provenance was checked for every completed result:
`git_sha=d479fe99581cf1988efba7906914c9ddd8653d7d`, with
`vev_git_sha=1f2c802d0707785cab979134db6a95645250a82c` for baseline and
`vev_git_sha=2b48abf9b481c3dbdc371ba13c15cf06051d9df0` for candidate. The
complete 252-scenario canonical matrix was not run because the representative
gate was incomplete. Its exact command remains:

```sh
rm -rf /tmp/vev-perf-results/public-full-baseline
/tmp/vev-perf-harness-d479fe9 --vev-bin /tmp/vev-proxy-baseline-1f2c802 \
  --manifest /tmp/vev-perf-harness-d479fe9-clone/testdata/perf/manifest.json \
  --out /tmp/vev-perf-results/public-full-baseline \
  --warmup 10s --duration 30s --repetitions 10
```

Warmup is excluded. During the measured interval the harness injects at most
one event per second. At least 10 complete event pairs are required: both input
injection and successful terminal flush must fall inside the interval.
Straddling or failed events remain raw diagnostics, not samples. Each run must
contain `manifest.json`, `raw-harness.jsonl`, per-process JSONL, `runs.json`, and
`summary.json`.

`summary.json` records the harness source as `git_sha`, the measured vev binary
source as `vev_git_sha`, and the measurement parameters. `runs.json`, run
manifests, and JSONL identify scenario, run, process, component, and correlation
IDs. Preserve those files with the benchmark output and record
`go version -m /tmp/vev-proxy-baseline-1f2c802`,
`go version -m /tmp/vev-proxy-candidate-2b48abf9`, and
`go version -m /tmp/vev-perf-harness-d479fe9`; do not commit result directories.
CPU and allocation evidence comes from Go microbenchmarks and process-local
span durations. End-to-end latency, terminal flush, and byte evidence comes
from the public-CLI harness. Reset, snapshot, and span counters for structured
output are candidate-only until that output exists.

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
