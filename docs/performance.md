# Performance methodology (M5)

Enable daemon pprof explicitly with `VEV_PPROF_ADDR=127.0.0.1:6060 vev --daemon` (or by exporting the variable before launching clients). When unset, no debug HTTP server is started.

Run microbenchmarks with:

```sh
go test ./pkg/renderer ./pkg/vt ./internal/adapters/ipc -bench=. -benchmem
```

Demo flood workloads are scripted in `scripts/bench-workloads.sh` for `yes`, `seq`, and `cat` styles. Capture bytes/frame and syscalls/frame externally (for example with `strace -c` around the client/daemon) and allocations with `-benchmem`/pprof.

## Local baseline status

Full end-to-end vev-vs-tmux workload baselines were not run for this change because they require an interactive terminal/daemon pair and external tracing tools around both processes. The reproducible methodology is:

1. Record host CPU, OS/kernel, terminal emulator, vev commit, tmux version, terminal size, and workload duration.
2. Start vev daemon with pprof enabled only when collecting profiles.
3. Run each workload (`yes`, `seq`, `cat`) for the same fixed duration and terminal size under vev and tmux.
4. Capture client/daemon syscall summaries with `strace -c` (or platform equivalent), bytes written per rendered frame, and heap profiles or `-benchmem` allocation counts.
5. Store raw command lines and outputs beside this document before comparing.

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
