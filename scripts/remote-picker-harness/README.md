# Remote navigation acceptance

Run `make remote-acceptance` with Docker, Python 3 and `ssh-keygen` installed.

The runner builds `scripts/demo/Dockerfile` from the current worktree and starts
two disposable Arch containers on a private network. Per-run client and host
keys authenticate SSH with host-key pinning. No host credentials, state or
sockets are mounted. Containers, image, network and temporary keys are removed
on success, failure or interruption.

The real UI driver exercises local A → local B → remote → local A. It asserts
qualified palette labels, exactly one imported local destination, the original
session lifecycle at return, and a successfully committed navigation action.
A failed assertion fails the command. This is a targeted regression, not an
exhaustive transport or geometry matrix.

`navigation_repro.py CLIENT_CONTAINER REMOTE_CONTAINER` also runs against
already prepared disposable demo containers. The client needs a pinned `remote`
SSH alias and `vev host add remote`. The script creates uniquely named sessions
and detaches its clients; the container owner handles session cleanup.
