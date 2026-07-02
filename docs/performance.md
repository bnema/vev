# Performance methodology (M5)

Enable daemon pprof explicitly with `VEV_PPROF_ADDR=127.0.0.1:6060 vev --daemon` (or by exporting the variable before launching clients). When unset, no debug HTTP server is started.

Run microbenchmarks with:

```sh
go test ./pkg/renderer ./pkg/vt ./internal/adapters/ipc -bench=. -benchmem
```

Demo flood workloads are scripted in `scripts/bench-workloads.sh` for `yes`, `seq`, and `cat` styles for raw/tmux runs. vev does not currently support `vev new <name> -- command`, so automated vev workload command overrides are intentionally not documented. Capture bytes/frame and syscalls/frame externally (for example with `strace -c` around the client/daemon) and allocations with `-benchmem`/pprof.

## Local baseline status

Full end-to-end vev-vs-tmux workload baselines are deferred/waived for this non-interactive branch because they require an interactive terminal/daemon pair, external tracing tools around both processes, and (for automated vev flood workloads) future command override support. Until that exists, only start the daemon/client normally and do not claim a scripted vev flood command. tmux/raw comparison commands remain useful for shaping workloads:

```sh
VEV_PPROF_ADDR=127.0.0.1:6060 strace -ff -c -o vev-daemon.strace vev --daemon
strace -ff -c -o vev-client.strace vev new perf-manual
strace -ff -c -o tmux-yes.strace tmux new-session -d -s perf-yes 'timeout 30s yes'
strace -ff -c -o tmux-seq.strace tmux new-session -d -s perf-seq 'timeout 30s sh -c "while :; do seq 1 10000; done"'
strace -ff -c -o tmux-cat.strace tmux new-session -d -s perf-cat 'timeout 30s sh -c "while :; do cat /path/to/sample.txt; done"'
```

Record host CPU, OS/kernel, terminal emulator, vev commit, tmux version, terminal size, workload duration, raw command lines, syscall summaries, bytes written per rendered frame, and heap profiles or allocation counts before comparing.

Microbenchmarks executed locally for this review using `go test ./internal/adapters/ipc -run '^$' -bench=. -benchmem`:

| benchmark | methodology |
| --- | --- |
| BenchmarkTransportSend | writes encoded frames to an in-memory discard `net.Conn`; allocations reported here are Send-side frame assembly costs. |
| BenchmarkTransportRecvReuse | reads a pre-encoded frame from a looping in-memory reader; no `Send` call, goroutine, channel, or frame production work runs inside the measured loop, so allocations reported here are Recv-side costs only. |

| workload | vev baseline | tmux baseline | notes |
| --- | --- | --- | --- |
| yes | not run in this non-interactive review environment | not run in this non-interactive review environment | use same terminal size and duration |
| seq | not run in this non-interactive review environment | not run in this non-interactive review environment | use same terminal size and duration |
| cat | not run in this non-interactive review environment | not run in this non-interactive review environment | use same input file, terminal size, and duration |
