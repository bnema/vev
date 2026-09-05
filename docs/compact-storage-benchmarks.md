# Compact storage: measured budgets

Measured September 5, 2026 (CEST; September 4 UTC) on Linux amd64, Ryzen 5 7535HS, Go 1.27.1,
`GOMAXPROCS=2`. The existing manual-test binary was not replaced or restarted.

## References and method

- Consumer baseline: clean vev `9bf07496` (before this integration).
- Measured compact consumer: the integration captured in vev `b59725e3`, using vev-vt `f912b86` (before the final review fixes).
- Current integration dependency: vev-vt `f2960699a487`. The figures below were not remeasured at this revision; they describe the earlier `f912b86` dependency state.
- Retained-history baseline: vev-vt foundation `a0f7c5a`, before compact pages.
- Consumer workloads: 120 × 40 terminals, 10,000 history rows per pane.
- Main consumer comparisons: three 100 ms samples, reported as medians.
- Additional four/eight-tab cases: one 100 ms sample; directional evidence only.
- Retained heap: three one-iteration samples with explicit GC; not process RSS.

These are local short-run measurements, not a statistical latency guarantee.

## Consumer results

| Operation | Dense ms/op | Compact ms/op | Dense bytes/op | Compact bytes/op | Dense allocs/op | Compact allocs/op |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Live paint, 1 tab / 1 pane | 0.197 | 0.951 | 356,090 | 86,507 | 36 | 40 |
| Live paint, 1 tab / 4 panes | 0.129 | 0.444 | 355,884 | 86,041 | 36 | 40 |
| Copy entry, 1 tab / 1 pane | 0.708 | 2.180 | 2,100,051 | 449,512 | 64 | 85 |
| Copy entry, 1 tab / 4 panes | 0.629 | 1.389 | 1,542,092 | 296,382 | 82 | 114 |
| Copy entry, 4 tabs / 1 pane* | 1.165 | 2.144 | 2,096,593 | 433,169 | 68 | 88 |
| Copy entry, 4 tabs / 4 panes* | 0.974 | 1.383 | 1,541,684 | 296,647 | 86 | 118 |
| Copy entry, 8 tabs / 1 pane* | 1.113 | 2.169 | 2,098,087 | 434,861 | 76 | 96 |

`*` Single-sample comparison.

Main workloads emitted the same output bytes and one output frame per operation:
57/59 bytes for live paint and 524/971 bytes for copy entry.

**The trade-off is real:** allocated bytes fall approximately 4–5×, but the
main workloads take approximately 2.2–4.8× longer. Do not describe this change as
a general throughput improvement. The integration's cell-by-cell grid copies
and page-local style bookkeeping remain optimization candidates. No unsafe
copying, pooling, or new allocator was introduced to make allocation counts pass.

## Retained history

Plain ASCII, 10,000 × 120 cells:

| Representation | Median retained bytes |
| --- | ---: |
| Dense foundation | 95,283,872 |
| Compact, uncompressed | 22,769,712 |
| Compact after two idle compression passes | 4,209,776 |

Uncompressed storage meets issue #9's **4× retained-memory reduction** target
(approximately 4.18×). Cold storage retains approximately 22.6× less than dense
storage, with 38 cold pages and 173,190 compressed bytes.

Building this synthetic history allocates **148.5 MB cumulatively**, versus
95.8 MB for the dense reference. Lower retained memory does not imply lower
allocation traffic during construction. The mutable semantic tail contributes
to this cost. Retained-memory sample times include measurement GCs and must not
be treated as isolated append throughput.

## Regression gates

The previous 38/66–90 allocation-count thresholds came from vev's dense-frame
benchmarks, not from the vev-vt issues. The floating composition threshold of
two allocations explicitly assumed two dense slices.

The recalibrated tests retain count checks and add independent byte checks:

- Live paint: at most **44 allocations** and **96 KiB/op**.
- Copy entry: approximately 10% count headroom over measured compact values;
  at most **512 KiB/op** for single-pane tabs and **384 KiB/op** for four-pane tabs.
- Cached floating composition: the measured cost of one compact clone plus one
  allocation for initial style growth, and at most **40 KiB/op**.
- Warm title/bar composition still requires **zero allocations**.
- Retained-history and ownership/safety checks remain separate and unchanged.

Byte checks use Go benchmark allocation accounting, excluding fixture setup.
Instrumented race runs exercise behavior but do not enforce byte budgets.
Wall-clock timings are reported rather than asserted as machine-dependent CI
thresholds. The CPU regression remains visible above; these gates do not claim
that it has been removed.

## Reproduce

Run the same consumer command in clean vev `9bf07496` and the integration tree.
The current integration measures the final dependency, not the exact historical
sample above. For the historical comparison, use a separate checkout of vev
`b59725e3`, which pins the compatible `f912b86` dependency and pre-review consumer
API. Do not downgrade the dependency alone in the current integration.

```sh
GOMAXPROCS=2 go test ./internal/usecase/daemon -run '^$' \
  -bench '^BenchmarkDaemonHistory(LivePaint|CopyEnter)$/^1tab-(1pane-control|4panes)$' \
  -benchmem -benchtime=100ms -count=3
```

Run in the dense foundation and compact vev-vt trees:

```sh
GOMAXPROCS=2 go test -run '^$' \
  -bench '^BenchmarkHistoryRetained10Kx120$/plain-ascii$' \
  -benchtime=1x -count=3 -benchmem
```

The compact tree also provides `BenchmarkColdHistoryRetained10Kx120`.
