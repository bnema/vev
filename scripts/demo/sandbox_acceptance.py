#!/usr/bin/env python3
"""Bounded local + two-SSH-host acceptance using the real vev UI driver."""
import json
import select
import subprocess
import sys
import time
import uuid

REQUEST_TIMEOUT = 20


def run(*args, timeout=30, check=True):
    result = subprocess.run(args, text=True, capture_output=True, timeout=timeout)
    if check and result.returncode:
        raise RuntimeError(f"command failed ({result.returncode}): {' '.join(args)}\n{result.stdout}{result.stderr}")
    return result


class Driver:
    def __init__(self, container, args):
        self.p = subprocess.Popen(
            ["docker", "exec", "-i", "-e", "VEV_LOG=debug", container,
             "vev", "--ui-driver", "--cols", "100", "--rows", "30", *args],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True,
        )
        self.ident = 0
        ready = self.read("ready")
        self.attachment = ready["attachment"]
        self.generation = ready.get("generation", 0)
        deadline = time.monotonic() + REQUEST_TIMEOUT
        while ready.get("status") != "attached":
            if time.monotonic() >= deadline:
                raise RuntimeError(f"UI driver did not attach: {ready}")
            ready = self.call("capture").get("context", {})
        self.generation = ready["generation"]

    def read(self, label):
        readable, _, _ = select.select([self.p.stdout], [], [], REQUEST_TIMEOUT)
        if not readable:
            self.close()
            raise RuntimeError(f"UI driver timed out during {label}")
        line = self.p.stdout.readline()
        if not line:
            err = self.p.stderr.read()
            raise RuntimeError(f"UI driver EOF during {label}: {err}")
        envelope = json.loads(line)
        if "error" in envelope:
            raise RuntimeError(f"UI driver error during {label}: {envelope['error']}")
        return envelope["result"]

    def call(self, op, **fields):
        self.ident += 1
        request = {"version": 1, "id": self.ident, "op": op,
                   "attachment": self.attachment, **fields}
        if op in ("text", "keys"):
            request.setdefault("generation", self.generation)
        self.p.stdin.write(json.dumps(request) + "\n")
        self.p.stdin.flush()
        result = self.read(op)
        self.generation = result.get("context", {}).get("generation", self.generation)
        return result

    def close(self):
        if self.p.poll() is None:
            self.p.stdin.close()
            try:
                self.p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.p.kill()
                self.p.wait(timeout=5)


def scenario(container, endpoint=None):
    name = ("local" if endpoint is None else endpoint.replace("-", "")) + uuid.uuid4().hex[:7]
    args = ["--session", name]
    if endpoint:
        args = ["--remote", endpoint, *args]
    driver = Driver(container, args)
    try:
        context = driver.call("capture")["context"]
        assert context["status"] == "attached", context
        assert context["session"]["session_name"] == name, context
        marker = "OK_" + name
        accepted = driver.call("text", text=f"printf '{marker}'")
        assert accepted["status"] == "processed", accepted
        driver.call("keys", keys=["Enter"])
        observed = driver.call("wait", expect={"text_contains": marker}, timeout_ms=15000)
        assert observed["context"]["session"] == context["session"], observed
        print(f"PASS ui endpoint={endpoint or 'local'} session={name} lifecycle={context['session']['lifecycle_id']}")
    finally:
        driver.close()


def main():
    if len(sys.argv) != 2:
        raise SystemExit("usage: sandbox_acceptance.py CLIENT_CONTAINER")
    client = sys.argv[1]
    hosts = run("docker", "exec", client, "vev", "host", "list").stdout
    for endpoint in ("remote-a", "remote-b"):
        if endpoint not in hosts:
            raise RuntimeError(f"CLI host list omitted {endpoint}: {hosts}")
        catalog = run("docker", "exec", client, "vev", "ls", endpoint, timeout=30)
        print(f"PASS cli endpoint={endpoint} catalog_lines={len(catalog.stdout.splitlines())}")
    scenario(client)
    scenario(client, "remote-a")
    scenario(client, "remote-b")


if __name__ == "__main__":
    main()
