#!/usr/bin/env python3
"""Disposable dual-transport fixture and physical Wayland input acceptance."""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]


def run(*args, **kwargs):
    return subprocess.run(args, check=True, timeout=kwargs.pop("timeout", 60), **kwargs)


def docker(*args, **kwargs):
    return run("docker", *args, **kwargs)


def main():
    name = "vev-resilience-" + uuid.uuid4().hex[:10]
    artifacts = Path(tempfile.mkdtemp(prefix="vev-resilience-artifacts-"))
    processes = []
    containers = []
    network = False
    def interrupt(signum, frame):
        raise KeyboardInterrupt("fixture interrupted")
    signal.signal(signal.SIGTERM, interrupt)
    with tempfile.TemporaryDirectory(prefix="vev-resilience-private-") as directory:
        private = Path(directory)
        runtime = private / "runtime"
        runtime.mkdir(mode=0o700)
        try:
            for tool in ("docker", "foot", "ssh-keygen", "go"):
                if shutil.which(tool) is None:
                    raise RuntimeError("missing prerequisite: " + tool)
            run("go", "build", "-o", str(ROOT / "build/vev"), ".", cwd=ROOT, timeout=600)
            for helper in ("relay", "pixels"):
                run("go", "build", "-o", str(ROOT / f"build/resilience-{helper}"), f"./scripts/remote-resilience-harness/{helper}", cwd=ROOT, timeout=600)
            nefer = os.environ.get("NEFERWL_BIN")
            if not nefer:
                nefer = str(private / "neferwl")
                common = run("git", "rev-parse", "--path-format=absolute", "--git-common-dir", cwd=ROOT, capture_output=True, text=True).stdout.strip()
                source = Path(os.environ.get("NEFERWL_SOURCE", str(Path(common).parent.parent / "neferwl")))
                run("go", "build", "-o", nefer, "./cmd/neferwl", cwd=source, env={**os.environ, "CGO_ENABLED": "0"}, timeout=600)
            docker("build", "-f", str(ROOT / "scripts/demo/Dockerfile"), "-t", name, str(ROOT), timeout=600)
            docker("network", "create", name)
            network = True
            for role in ("client", "remote"):
                container = name + "-" + role
                docker("run", "-d", "--name", container, "--network", name, "--network-alias", "upstream" if role == "remote" else "remote", "--entrypoint", "sleep", name, "infinity")
                containers.append(container)
            client, remote = containers
            for key in ("client", "host"):
                run("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", "fixture", "-f", str(private / key))
            docker("cp", str(private / "host"), remote + ":/etc/ssh/ssh_host_ed25519_key")
            docker("cp", str(private / "host.pub"), remote + ":/etc/ssh/ssh_host_ed25519_key.pub")
            docker("cp", str(private / "client.pub"), remote + ":/home/demo/.ssh/authorized_keys")
            docker("exec", "--user", "root", remote, "sh", "-c", "rm -f /home/demo/.ssh/id_ed25519*; chmod 600 /etc/ssh/ssh_host_ed25519_key /home/demo/.ssh/authorized_keys; chown demo:demo /home/demo/.ssh/authorized_keys; mkdir -p /run/sshd; /usr/sbin/sshd")
            docker("exec", "--user", "root", client, "sh", "-c", "rm -f /home/demo/.ssh/id_ed25519*")
            docker("cp", str(private / "client"), client + ":/home/demo/.ssh/id_ed25519")
            public = (private / "host.pub").read_text().split()
            (private / "known_hosts").write_text("remote " + " ".join(public[:2]) + "\n")
            (private / "config").write_text("Host remote\n HostName remote\n Port 2222\n User demo\n IdentityFile ~/.ssh/id_ed25519\n IdentitiesOnly yes\n StrictHostKeyChecking yes\n HostKeyAlias remote\n UserKnownHostsFile ~/.ssh/known_hosts\n")
            for file in ("known_hosts", "config"):
                docker("cp", str(private / file), client + ":/home/demo/.ssh/" + file)
            docker("cp", str(ROOT / "build/resilience-relay"), client + ":/tmp/relay")
            docker("cp", str(ROOT / "scripts/remote-resilience-harness/ssh.py"), client + ":/tmp/ssh")
            docker("exec", "--user", "root", client, "sh", "-c", "chown demo:demo /home/demo/.ssh/*; chmod 600 /home/demo/.ssh/*; chmod 755 /tmp/ssh; install -d -m 700 -o demo -g demo /home/demo/relay; mkdir -p /tmp/bin; ln -s /tmp/ssh /tmp/bin/ssh; mv /usr/bin/ssh /usr/bin/ssh-real; ln -s /tmp/ssh /usr/bin/ssh")
            transport = os.environ.get("VEV_RESILIENCE_TRANSPORT", "quic")
            if transport not in ("quic", "ssh"):
                raise ValueError("VEV_RESILIENCE_TRANSPORT must be quic or ssh")
            relay_args = ["-degraded"] if os.environ.get("VEV_RESILIENCE_DEGRADED") == "1" else []
            relay = subprocess.Popen(["docker", "exec", client, "/tmp/relay", "-remote", "upstream", "-control", "/home/demo/relay/control.sock", "-tcp", ":2222", "-listen", "0.0.0.0", *relay_args])
            processes.append(relay)
            control_id = 0
            def control(op, **fields):
                nonlocal control_id
                control_id += 1
                request = {"id": control_id, "op": op, **fields}
                code = "import socket,json; s=socket.socket(socket.AF_UNIX); s.settimeout(3); s.connect('/home/demo/relay/control.sock'); s.sendall(" + repr(json.dumps(request).encode() + b"\n") + "); print(s.makefile().readline())"
                result = docker("exec", client, "python3", "-c", code, capture_output=True, text=True)
                reply = json.loads(result.stdout)
                assert reply["id"] == control_id and "error" not in reply, reply
                return reply["result"]
            # Fixture image must explicitly supply Python rather than using host tools.
            docker("exec", "--user", "root", client, "sh", "-c", "command -v python3", capture_output=True)
            deadline = time.monotonic() + 5
            while True:
                ready = subprocess.run(["docker", "exec", client, "test", "-S", "/home/demo/relay/control.sock"], capture_output=True, timeout=3)
                if ready.returncode == 0:
                    break
                if time.monotonic() > deadline:
                    raise RuntimeError("relay control socket did not become ready")
                time.sleep(0.05)
            probe = subprocess.run(["docker", "exec", client, "/usr/bin/ssh", "-v", "remote", "true"], capture_output=True, text=True, timeout=10)
            if probe.returncode:
                (artifacts / "ssh-probe.log").write_text(probe.stderr)
                raise RuntimeError("fixture SSH authentication failed")
            env_args = ["-e", "PATH=/tmp/bin:/usr/local/bin:/usr/bin", "-e", "VEV_LOG=debug", "-e", "SSH_AUTH_SOCK="]
            spec = importlib.util.spec_from_file_location("acceptance", ROOT / "scripts/remote-picker-harness/acceptance.py")
            acceptance = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(acceptance)
            fixture = acceptance.Driver(remote, ["--session", "resilience"])
            fixture.close()
            docker("exec", *env_args, client, "vev", "host", "add", "--transport", transport, "remote")
            # Wait for the broker publication before resolving direct attach.
            deadline = time.monotonic() + 30
            while True:
                inventory = docker("exec", *env_args, client, "vev", "ls", "remote", capture_output=True, text=True)
                if "resilience" in inventory.stdout:
                    break
                (artifacts / "inventory.txt").write_text(inventory.stdout + inventory.stderr)
                if time.monotonic() > deadline:
                    raise RuntimeError("remote fixture was not published")
                time.sleep(0.2)
            shots = artifacts / "shots"
            shots.mkdir()
            docker_endpoint = run("docker", "context", "inspect", "--format", "{{.Endpoints.docker.Host}}", capture_output=True, text=True).stdout.strip()
            env = {**os.environ, "DOCKER_HOST": docker_endpoint, "XDG_RUNTIME_DIR": str(runtime), "XDG_CONFIG_HOME": str(private / "config-home"), "XDG_STATE_HOME": str(private / "state"), "HOME": str(private)}
            env["TERMINAL"] = "foot --config=/dev/null --override=cursor.blink=no --log-level=info --hold -- docker --host=" + docker_endpoint + " exec -it " + " ".join(env_args) + " " + client + " vev attach demo@remote:resilience --ui-observe --ui-socket /home/demo/visual/ui.sock"
            log = (artifacts / "compositor.log").open("w")
            compositor = subprocess.Popen([nefer, "--backend=headless", "--size=1280x720", "--input=-", "--screenshot", str(shots), "--timeout", "180s"], stdin=subprocess.PIPE, stdout=log, stderr=log, text=True, env=env)
            processes.append(compositor)
            deadline = time.monotonic() + 15
            while not list(runtime.glob("wayland-*")):
                if compositor.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError("headless compositor did not create socket")
                time.sleep(0.05)
            sockets = [p for p in runtime.glob("wayland-*") if not p.name.endswith(".lock")]
            env["WAYLAND_DISPLAY"] = str(sockets[0])
            env.pop("WAYLAND_SOCKET", None)
            env.pop("DISPLAY", None)
            def capture():
                bridge = subprocess.Popen(["docker", "exec", "-i", client, "vev", "--ui-driver", "--socket", "/home/demo/visual/ui.sock"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                try:
                    import select
                    if not select.select([bridge.stdout], [], [], 1)[0]:
                        raise RuntimeError("observation discovery timed out")
                    discovery = json.loads(bridge.stdout.readline())
                    request = {"version": 1, "id": 1, "op": "capture", "attachment": discovery["result"]["attachment"]}
                    output, error = bridge.communicate(json.dumps(request) + "\n", timeout=1)
                    envelopes = [json.loads(line) for line in output.splitlines()]
                    if not envelopes or "error" in envelopes[-1]:
                        raise RuntimeError("observation capture failed: " + str(envelopes) + error)
                    return envelopes[-1]["result"]
                finally:
                    if bridge.poll() is None:
                        bridge.kill()
                        bridge.communicate()
            compositor.stdin.write("sleep 1s\nkey Return\n")
            compositor.stdin.flush()
            deadline = time.monotonic() + 30
            while True:
                try:
                    state = capture()
                    if state.get("context", {}).get("status") == "attached" and state.get("context", {}).get("session", {}).get("session_name") == "resilience":
                        break
                except (subprocess.SubprocessError, ValueError, RuntimeError) as error:
                    (artifacts / "capture-error.txt").write_text(str(error) + "\n" + str(getattr(error, "stderr", "")))
                if time.monotonic() > deadline:
                    diagnosis = subprocess.run(["docker", "exec", client, "sh", "-c", "ps -ef; ls -l /home/demo/visual/ui.sock"], capture_output=True, text=True, timeout=5)
                    (artifacts / "client-processes.txt").write_text(diagnosis.stdout + diagnosis.stderr)
                    (artifacts / "relay-stats.json").write_text(json.dumps(control("stats")))
                    (artifacts / "last-capture.json").write_text(json.dumps(locals().get("state")))
                    raise RuntimeError("visual client did not attach")
                time.sleep(0.1)
            (artifacts / "initial.json").write_text(json.dumps(state))
            def key(command):
                compositor.stdin.write(command + "\n")
                compositor.stdin.flush()
            def responsive(label):
                latest = shots / "latest.png"
                before = artifacts / (label + "-before.png")
                shutil.copyfile(latest, before)
                start = time.monotonic()
                stamp = latest.stat().st_mtime_ns
                key("key Alt+space")
                key("type SSP")
                key("key Return")
                while time.monotonic() - start < 2:
                    if latest.stat().st_mtime_ns != stamp:
                        after = artifacts / (label + "-after.png")
                        shutil.copyfile(latest, after)
                        changed = subprocess.run([str(ROOT / "build/resilience-pixels"), str(before), str(after)], capture_output=True, text=True)
                        observed = capture()
                        (artifacts / (label + "-observed.json")).write_text(json.dumps(observed))
                        if changed.returncode == 0 and "Sessions" in observed.get("text", "") and "Esc close" in observed.get("text", ""):
                            key("key Escape")
                            return {"elapsed": time.monotonic()-start, "pixels": json.loads(changed.stdout), "context": observed["context"]}
                    time.sleep(0.05)
                raise RuntimeError(label + ": local picker failed 2s response gate")
            evidence = {"healthy": responsive("healthy")}
            time.sleep(0.5)
            key("key Alt+space")
            key("type SSP")
            key("key Return")
            deadline = time.monotonic() + 5
            while "Sessions" not in capture().get("text", ""):
                if time.monotonic() > deadline:
                    raise RuntimeError("picker did not open before blackout")
                time.sleep(0.1)
            before = artifacts / "blackout-before.png"
            shutil.copyfile(shots / "latest.png", before)
            assert "Sessions" in capture().get("text", ""), "picker was not open before blackout"
            impairment = os.environ.get("VEV_RESILIENCE_LINK", "both")
            if impairment not in ("both", "udp", "tcp"):
                raise ValueError("VEV_RESILIENCE_LINK must be both, udp or tcp")
            control("blackout", udp=impairment != "tcp", tcp=impairment != "udp", active=True)
            start = time.monotonic()
            key("type /resilience")
            deadline = start + 2
            while time.monotonic() < deadline:
                after = artifacts / "blackout-after.png"
                shutil.copyfile(shots / "latest.png", after)
                changed = subprocess.run([str(ROOT / "build/resilience-pixels"), str(before), str(after)], capture_output=True, text=True)
                observed = capture()
                if changed.returncode == 0 and "1 matches" in observed.get("text", ""):
                    evidence["blackout"] = {"elapsed": time.monotonic()-start, "pixels": json.loads(changed.stdout)}
                    break
                time.sleep(0.05)
            else:
                raise RuntimeError("open picker search froze during blackout")
            time.sleep(3)
            control("blackout", udp=True, tcp=True, active=False)
            key("key Escape")
            time.sleep(0.2)
            key("key Escape")
            deadline = time.monotonic() + 5
            while "Sessions" in capture().get("text", ""):
                if time.monotonic() > deadline:
                    raise RuntimeError("picker did not close after restoration")
                time.sleep(0.1)
            key("key Ctrl+c")
            time.sleep(1)
            initial_session = state["context"]["session"]
            key("type printf 'RESILIENCE_RECOVERED\\n'")
            key("key Return")
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                recovered = capture()
                if "\nRESILIENCE_RECOVERED" in recovered.get("text", "") and recovered["context"]["session"] == initial_session:
                    evidence["recovered"] = recovered["context"]
                    break
                time.sleep(0.1)
            else:
                raise RuntimeError("session did not recover after blackout")
            # Negative control: compositor can still render while client is
            # stopped. No client observation is allowed to satisfy the gate.
            pid_result = docker("exec", client, "pgrep", "-f", "^vev attach", capture_output=True, text=True)
            pid = pid_result.stdout.strip().splitlines()[0]
            docker("exec", client, "kill", "-STOP", pid)
            frozen = artifacts / "frozen.png"
            shutil.copyfile(shots / "latest.png", frozen)
            try:
                frozen_at = (shots / "latest.png").stat().st_mtime_ns
                key("key Alt+space")
                # The compositor must keep capturing, or an unchanged frame
                # proves nothing about the stopped client.
                deadline = time.monotonic() + 3
                while (shots / "latest.png").stat().st_mtime_ns == frozen_at:
                    if time.monotonic() > deadline:
                        raise RuntimeError("compositor stopped capturing during the negative control")
                    time.sleep(0.05)
                time.sleep(0.3)
                check = subprocess.run([str(ROOT / "build/resilience-pixels"), str(frozen), str(shots / "latest.png")], capture_output=True, text=True)
                assert check.returncode == 1, "visual gate accepted a stopped client"
                evidence["negative_control"] = "stopped client rejected"
            finally:
                docker("exec", client, "kill", "-CONT", pid)
            if os.environ.get("VEV_RESILIENCE_LONG") == "1":
                # The negative control's Alt+space was buffered while the
                # client was stopped and opens the palette once it resumes.
                # Close it on observed state only: a blind extra Escape would
                # reach the shell as a lone ESC, which readline keeps as a
                # Meta prefix and uses to eat the first replayed key.
                deadline = time.monotonic() + 5
                while "Commands" not in capture().get("text", ""):
                    if time.monotonic() > deadline:
                        raise RuntimeError("palette did not open after the stopped client resumed")
                    time.sleep(0.1)
                key("key Escape")
                deadline = time.monotonic() + 5
                while "Commands" in capture().get("text", ""):
                    if time.monotonic() > deadline:
                        raise RuntimeError("palette did not close before the long outage")
                    time.sleep(0.1)
                time.sleep(0.5)
                control("blackout", udp=True, tcp=True, active=True)
                if transport == "ssh":
                    control("disconnect")
                deadline = time.monotonic() + 90
                while time.monotonic() < deadline:
                    observed = capture()
                    if observed.get("context", {}).get("status") == "connecting":
                        break
                    time.sleep(0.2)
                else:
                    raise RuntimeError("long outage did not enter resume")
                if os.environ.get("VEV_RESILIENCE_RESUME") == "1":
                    key("type echo HELD_ONE; echo HELD_TWO")
                    key("key Return")
                    # Keep the route down until the compositor has delivered
                    # the complete physical input script to the resume owner.
                    time.sleep(2)
                    control("blackout", udp=True, tcp=True, active=False)
                    deadline = time.monotonic() + 30
                    while time.monotonic() < deadline:
                        resumed = capture()
                        text = resumed.get("text", "")
                        if "\nHELD_ONE" in text and "\nHELD_TWO" in text and resumed.get("context", {}).get("session") == initial_session:
                            assert text.index("\nHELD_ONE") < text.index("\nHELD_TWO"), "held input reordered"
                            # The echoed command line proves the first held
                            # byte was not swallowed before the shell saw it.
                            assert "$ echo HELD_ONE; echo HELD_TWO" in text, "held input replayed incompletely"
                            evidence["resumed"] = resumed["context"]
                            break
                        time.sleep(0.1)
                    else:
                        raise RuntimeError("long outage did not replay held input")
                else:
                    start = time.monotonic()
                    cancel_key = os.environ.get("VEV_RESILIENCE_CANCEL", "Ctrl+c")
                    if cancel_key not in ("Ctrl+c", "Escape"):
                        raise ValueError("cancel must be Ctrl+c or Escape")
                    key("key " + cancel_key)
                    deadline = start + 2
                    while time.monotonic() < deadline:
                        observed = capture()
                        if observed.get("context", {}).get("status") == "picker":
                            evidence["resume_cancel"] = {"elapsed": time.monotonic()-start}
                            break
                        time.sleep(0.05)
                    else:
                        raise RuntimeError(cancel_key + " did not cancel resume")
                    control("blackout", udp=True, tcp=True, active=False)
            evidence["stats"] = control("stats")
            if transport == "quic":
                assert evidence["stats"]["udp"], "QUIC bypassed impairment relay"
            assert evidence["stats"]["tcp_bytes"] > 0, "SSH bypassed impairment relay"
            (artifacts / "evidence.json").write_text(json.dumps(evidence, indent=2))
            control("stop")
        finally:
            for process in reversed(processes):
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
            for container in containers:
                role = container.rsplit("-", 1)[-1]
                for filename in ("vev-client.log", "vev-daemon.log", "broker/log/vev-broker.log"):
                    subprocess.run(["docker", "cp", container + ":/home/demo/.local/state/vev/" + filename, str(artifacts / (role + "-" + filename.replace("/", "-")))], timeout=30, capture_output=True)
                subprocess.run(["docker", "rm", "-f", container], timeout=30, capture_output=True)
            if network:
                subprocess.run(["docker", "network", "rm", name], timeout=30, capture_output=True)
            subprocess.run(["docker", "image", "rm", name], timeout=30, capture_output=True)
            print("artifacts:", artifacts)


if __name__ == "__main__":
    main()
