# Demo and sandbox scripts

- `run.sh`, `demo.tape`, `smoke.tape`: VHS demo recording.
- `sandbox-run.sh`: automated acceptance. Client and two remotes all run in
  containers on an internal network.
- `test-remotes.sh`: two containerised remotes for manual testing from your own
  terminal.

All of them build `build/vev` and bake it into the `scripts/demo/Dockerfile`
image, so local and remote vev are the same binary. Docker must be rootless.

## Manual remote testing

```sh
scripts/demo/test-remotes.sh up       # build, start remote-a/remote-b, register both hosts
scripts/demo/test-remotes.sh shell    # print the client command
scripts/demo/test-remotes.sh status   # containers, ports, vev host list, vev ls --all
scripts/demo/test-remotes.sh broker-restart  # stop only the test broker; the next vev call respawns it
scripts/demo/test-remotes.sh down     # stop the test broker/daemon, remove containers and keys
```

Run the printed command from a real terminal, or `scripts/demo/test-remotes.sh
shell --exec [vev args]`. It sets:

- `VEV_ENV=test` and `VEV_ENV_ROOT=<checkout>/.dev`: all vev state lives in
  `.dev/test/`, apart from your installed vev.
- `PATH=.dev/test/bin:$PATH`: vev runs `ssh` from `PATH` and has no ssh config
  option, so `.dev/test/bin/ssh` wraps the real client with
  `-F .dev/test/ssh/config`. The key, `known_hosts` and config are generated in
  `.dev/test/ssh/`. `~/.ssh` and your agent are never used.
- `VEV_REMOTE_TRANSPORT=stdio`: hosts are registered with the SSH-only carriage.
  QUIC cannot work here. The remote proxy listens on a random UDP port, and
  the client dials that port on the SSH target name (`remote-a`), which does
  not resolve to the loopback port mapping.
- `-u VEV`: a client started from a pane of your real vev is not treated as
  nested.

sshd is published on `127.0.0.1:2222` (remote-a) and `127.0.0.1:2223`
(remote-b). Override the ports with `VEV_TEST_SSH_PORT_A` and
`VEV_TEST_SSH_PORT_B`, and the environment name with `VEV_TEST_ENV`. Plain
shell access: `.dev/test/bin/ssh remote-a`.

`up` is idempotent: it recreates the containers, reinstalls the key and
skips hosts that are already registered. A `VEV_ENV=test` broker started by an
older build keeps running until it is stopped, and `up` warns when it finds
one. Run `down` then `up` after a rebuild. `down` stops **every**
`VEV_ENV=test` daemon of this checkout, including sessions you opened
there by hand.

Remote sessions live in the containers and disappear on `down`.

### Checklist

Open a few sessions first, with at least two tabs in one of them, local and on
each remote: `vev new work`, `vev new ra remote-a`, `vev new rb remote-b`
(each run with the environment above).

- [ ] Picker over a session (`Alt+Space`, `SSP`): opens over the live
      session; `Esc` returns to the same session, tab and pane.
- [ ] Move pane (`MFP`) and move tab (`MAT`): the destination picker lists
      local and remote sessions and tabs; the move lands where you chose.
- [ ] `x` in the picker kills the selected session (local and remote); the
      row disappears and `vev ls --all` agrees.
- [ ] Tab rows: expand a session and see one row per tab, with the right names
      and active marker. Selecting a tab row lands on that tab.
- [ ] MRU order: switch A → B → C, reopen the picker, and check the order is
      C, B, A.
- [ ] Bell icons: in a background tab or session run `sleep 2; printf '\a'`,
      switch away, and check that the bell icon shows on that session and tab
      row. It clears once you visit it. Try it on a remote session too.
- [ ] Live preview: moving the cursor over a local session, then over a remote
      one, shows its current screen. Run `ticker` there to check that the
      preview updates live.
- [ ] Remote attach and switch: `vev attach demo@remote-a:<session>` from
      the shell (this also adds the learned target `demo@remote-a`), then
      switch between local, remote-a and remote-b sessions from the picker
      without dropping input or output.
- [ ] Broker restart: `scripts/demo/test-remotes.sh broker-restart` stops only
      the test broker. Reopen the picker from a running client and check that
      hosts and sessions come back.
