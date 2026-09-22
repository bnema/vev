#!/usr/bin/env python3
"""Linux PTY regression: python3 internal/app/testdata/broker_terminal_repro.py /absolute/vev

Build the real binary first. Keeps the PTY open (no UI-driver/EOF shortcut),
uses an implicit long VEV_ENV_ROOT, and cleans up only its isolated processes.
"""
import fcntl
import hashlib
import os
import pathlib
import pty
import select
import signal
import struct
import sys
import tempfile
import termios
import time

binary = os.path.abspath(sys.argv[1])
with tempfile.TemporaryDirectory(prefix="vev-pty-") as temporary:
    cwd = pathlib.Path(temporary) / ("long-worktree-" * 8)
    cwd.mkdir()
    root = cwd / ".dev"
    runtime = root / "test/runtime/vev"
    short = pathlib.Path("/tmp/vev-%d-%s" % (
        os.getuid(), hashlib.sha256(str(runtime).encode()).hexdigest()))
    pid = None
    fd = None

    def read_until(predicate, timeout):
        output = b""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if select.select([fd], [], [], 0.1)[0]:
                try:
                    output += os.read(fd, 65536)
                except OSError:
                    break
            if predicate(output):
                return output
        raise AssertionError(repr(output[-2000:]))

    def start():
        global pid, fd
        pid, fd = pty.fork()
        if pid == 0:
            os.chdir(cwd)
            os.environ.pop("VEV", None)
            os.environ.pop("VEV_ENV_ROOT", None)
            os.environ["VEV_ENV"] = "test"
            os.environ["TERM"] = "xterm-256color"
            os.environ["SHELL"] = "/bin/sh"
            os.execv(binary, [binary])
        fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 100, 0, 0))

    try:
        start()
        # The shell command proves client -> broker -> daemon -> PTY, not
        # merely that the broker socket accepted a connection.
        read_until(lambda data: b"?25h" in data, 15)
        os.write(fd, b"printf 'VEV_PTY_%s_OK\\n' REPRO\n")
        output = read_until(lambda data: b"VEV_PTY_REPRO_OK" in data, 10)
        assert b"broker connection lost" not in output
        print("PASS: long implicit development root reaches a real shell")
        os.kill(pid, signal.SIGTERM)
        os.waitpid(pid, 0)
        pid = None
        os.close(fd)
        fd = None

        # An insecure configuration fails before any service is adopted.
        # Raw-mode Ctrl-C must still end the process with stdin held open.
        (root / "test/config/vev").chmod(0o755)
        start()
        read_until(lambda data: b"broker connection lost" in data, 10)
        os.write(fd, b"\x03")
        deadline = time.monotonic() + 5
        while os.waitpid(pid, os.WNOHANG)[0] == 0:
            if time.monotonic() >= deadline:
                raise AssertionError("Ctrl-C failed with stdin held open")
            time.sleep(0.01)
        pid = None
        print("PASS: Ctrl-C exits a failed connection without terminal EOF")
    finally:
        if pid is not None:
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
        if fd is not None:
            os.close(fd)
        # Detached roles inherit the resolved root; never kill by binary name.
        for process in pathlib.Path("/proc").iterdir():
            if not process.name.isdigit():
                continue
            try:
                environment = (process / "environ").read_bytes().split(b"\0")
                if ("VEV_ENV_ROOT=" + str(root)).encode() in environment:
                    os.kill(int(process.name), signal.SIGKILL)
            except (OSError, ProcessLookupError):
                pass
        import shutil
        shutil.rmtree(short, ignore_errors=True)
