# Performance methodology (M5)

Enable daemon pprof explicitly with `VEV_PPROF_ADDR=127.0.0.1:6060 vev --daemon` (or by exporting the variable before launching clients). When unset, no debug HTTP server is started.

Run microbenchmarks with:

```sh
go test ./pkg/renderer ./pkg/vt ./internal/adapters/ipc -bench=. -benchmem
```

Demo flood workloads are scripted in `scripts/bench-workloads.sh` for `yes`, `seq`, and `cat` styles. Capture bytes/frame and syscalls/frame externally (for example with `strace -c` around the client/daemon) and allocations with `-benchmem`/pprof.

Comparison table placeholders (record local machine details before filling):

| workload | vev baseline | tmux baseline | notes |
| --- | --- | --- | --- |
| yes | TODO local run | TODO local run | same terminal size |
| seq | TODO local run | TODO local run | same duration |
| cat | TODO local run | TODO local run | same input file |
