#!/usr/bin/env python3
"""Fixture-only SSH wrapper: pin SSH and rewrite only QUIC readiness port."""
import json
import os
import socket
import subprocess
import sys


def control(request):
    with socket.socket(socket.AF_UNIX) as conn:
        conn.settimeout(2)
        conn.connect(os.environ.get("RESILIENCE_CONTROL", "/home/demo/relay/control.sock"))
        conn.sendall(json.dumps(request).encode() + b"\n")
        reply = json.loads(conn.makefile("rb").readline(4096))
        if "error" in reply:
            raise RuntimeError(reply["error"])
        return reply["result"]


def main():
    args = sys.argv[1:]
    command = ["/usr/bin/ssh-real", "-p", "2222", "-o", "HostKeyAlias=remote"] + args
    if not any("_broker-mux-quic-bootstrap" in arg for arg in args):
        os.execv(command[0], command)
    process = subprocess.Popen(command, stdin=sys.stdin.buffer, stdout=subprocess.PIPE)
    try:
        line = process.stdout.readline(4097)
        if len(line) > 4096 or not line.endswith(b"\n"):
            raise RuntimeError("invalid bounded bootstrap readiness")
        readiness = json.loads(line)
        port = readiness.get("port")
        if not isinstance(port, int) or not 0 < port <= 65535:
            raise RuntimeError("invalid bootstrap port")
        readiness["port"] = control({"id": 1, "op": "udp", "port": port})
        sys.stdout.buffer.write(json.dumps(readiness, separators=(",", ":")).encode() + b"\n")
        sys.stdout.buffer.flush()
        # No readiness or encrypted payload is recorded in artifacts.
        while chunk := process.stdout.read1(32768):
            sys.stdout.buffer.write(chunk)
            sys.stdout.buffer.flush()
        return process.wait()
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, RuntimeError):
        print("resilience SSH fixture failed", file=sys.stderr)
        sys.exit(1)
