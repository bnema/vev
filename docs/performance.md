# Performance methodology (M5)

This baseline measures in-process daemon work only. It deliberately does **not**
measure coordinator behavior, chunk encoding, compression, GC tuning, or real
network latency. Output and snapshot metrics count encoded output bytes and
frames produced by the daemon; they are useful proxies for local rendering and
IPC payload work, not measurements of a Unix socket, SSH, or UDP transport.
Real Unix-socket, SSH, and UDP impairment/latency benchmarks are deferred.

## Large-history daemon baseline

Use a single iteration only as a smoke check; it is not suitable for drawing
performance conclusions:

```sh
go test ./internal/usecase/daemon -run '^$' -bench '^BenchmarkDaemonHistory' -benchtime=1x -benchmem
```

For before/after comparison, collect repeated samples for the **full matrix**
(all workloads and topologies), pinning CPU/governor and recording the host.
The fixed duration and count make the output suitable for `benchstat`:

```sh
go test ./internal/usecase/daemon -run '^$' -bench '^BenchmarkDaemonHistory' -benchtime=1s -count=5 -benchmem > /tmp/vev-history-after.txt
benchstat /tmp/vev-history-before.txt /tmp/vev-history-after.txt
```

The matrix exercises live paint, snapshot capture, copy-mode entry, copy search,
and resize at `120x40` client geometry. Each pane is populated with 10,000
full-width deterministic history rows containing a stable `needle-<tab>-<row>`
prefix and patterned text. The layouts are:

- 1 tab x 1 pane (control)
- 1 tab x 4 panes
- 4 tabs x 1 pane
- 4 tabs x 4 panes
- 8 tabs x 1 pane

The fixture starts no PTY readers, schedulers, clocks, or transports. It primes
the renderer shadow before timing; live paint then alternates fixed writes. The
reported `outputbytes/op`, `framepayloadbytes/op`, and `outputframes/op` values
are encoded in-process output proxies. `snapshotbytes/op` is the serialized
in-process snapshot size. They must not be presented as network throughput or
latency.

Capture a pre-change result outside the repository, including Go and kernel
context:

```sh
{
  date -Is
  go version
  uname -a
  git rev-parse HEAD
  # Smoke only; do not use this result for comparisons.
  go test ./internal/usecase/daemon -run '^$' -bench '^BenchmarkDaemonHistory' -benchtime=1x -benchmem
  # Repeated full matrix for every workload/topology used in conclusions.
  go test ./internal/usecase/daemon -run '^$' -bench '^BenchmarkDaemonHistory' -benchtime=1s -count=5 -benchmem
} > /tmp/vev-history-before.txt 2>&1
```

## Lower-level smoke baseline

Use this command to check renderer, VT, IPC, and daemon microbenchmarks without
claiming an end-to-end transport result:

```sh
go test ./pkg/renderer ./pkg/vt ./internal/adapters/ipc ./internal/usecase/daemon -run '^$' -bench=. -benchmem
```

Enable daemon pprof explicitly when collecting a daemon profile:

```sh
VEV_PPROF_ADDR=127.0.0.1:6060 vev --daemon
```
