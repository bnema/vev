# Public transport-observability harness

`manifest.json` is the complete S5a measurement matrix: 1x4, 4x1, 4x4, and
8x1 layouts at 120x40 with 10,000 rows per pane; every canonical workload; and
local, SSH stdio, UDP baseline, 25/100ms RTT, and 1% loss transport fixtures.
Each listed scenario is applicable. A future unavailable combination must remain
in the manifest with `inapplicable_reason` and no launched roles; omission is
invalid.

Run only after building the public binary:

```sh
go build -o /tmp/vev-perf ./
go run ./cmd/vev-perf-harness --vev-bin /tmp/vev-perf \
  --manifest testdata/perf/manifest.json --out /tmp/vev-perf-results \
  --warmup 10s --duration 30s --repetitions 10
```

The harness creates one `O_EXCL` JSONL path per launched role before launch and
records the assigned process ID, clock domain, role, and path in that run's
manifest. `raw-harness.jsonl` contains only harness-clock input/terminal-flush
pairs. Process traces are correlated by IDs, never by timestamps across process
clock domains. Results with missing pairs, bad order, insufficient samples, or
cross-process tick arithmetic are invalid.
