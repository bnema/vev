#!/usr/bin/env python3
"""UI-driver regression against two disposable demo-image containers.

Pass client and remote container names. The client must have a pinned SSH
host named remote reaching the remote container. No host session is touched.
"""

import json
import subprocess
import sys
import uuid


class Driver:
    def __init__(self, container, session):
        self.process = subprocess.Popen(
            ["docker", "exec", "-i", container, "vev", "--ui-driver",
             "--session", session, "--cols", "100", "--rows", "30"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True,
        )
        ready = json.loads(self.process.stdout.readline())["result"]
        self.attachment = ready["attachment"]
        self.generation = ready["generation"]
        self.request_id = 0

    def call(self, operation, **fields):
        self.request_id += 1
        request = dict(version=1, id=self.request_id, op=operation,
                       attachment=self.attachment, **fields)
        if operation in ("keys", "text"):
            request["generation"] = self.generation
        self.process.stdin.write(json.dumps(request) + "\n")
        self.process.stdin.flush()
        response = json.loads(self.process.stdout.readline())
        assert "error" not in response, response
        result = response["result"]
        self.generation = result.get("context", {}).get("generation", self.generation)
        return result

    def palette(self, query, label):
        self.call("keys", keys=["Alt+Space"])
        self.call("text", text=query)
        self.call("wait", expect={"text_contains": label}, timeout_ms=10000)
        return self.call("capture")["text"]

    def close(self):
        self.process.stdin.close()
        self.process.wait(timeout=10)


def run(client_container, remote_container):
    suffix = uuid.uuid4().hex[:8]
    first, second, remote = (name + suffix for name in ("sample", "second", "other"))
    fixture = Driver(remote_container, remote)
    fixture.close()
    driver = Driver(client_container, first)
    try:
        initial = driver.call("capture")["context"]["session"]
        fixture = Driver(client_container, second)
        fixture.close()
        driver.palette(second, "Switch to session " + second + "@local")
        driver.call("keys", keys=["Enter"])
        driver.palette(remote, "Switch to session " + remote + "@remote")
        entered = driver.call("keys", keys=["Enter"])
        assert entered["context"]["session"]["session_name"] == remote
        label = "Switch to session " + first + "@local"
        screen = driver.palette(first, label)
        assert screen.count(label) == 1, screen
        assert "Switch to session " + first + " " not in screen, screen
        selected = driver.call("keys", keys=["Enter"])
        assert selected["context"]["session"] == initial, selected
        driver.call("wait", expect={"session": initial}, timeout_ms=10000)
        print("PASS: exact local return, one client-qualified row, committed UI action")
    finally:
        driver.close()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("usage: navigation_repro.py CLIENT_CONTAINER REMOTE_CONTAINER")
    run(*sys.argv[1:])
