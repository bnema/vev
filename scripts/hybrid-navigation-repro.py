#!/usr/bin/env python3
"""Exercise real hybrid navigation in private VEV_ENV=dev daemons.

Requires Linux, Python 3, tmux, ssh and an sshd runnable as the current user.
No installed vev binary, normal daemon, SSH config or session is modified.
"""

import argparse
import os
from pathlib import Path
import pwd
import shlex
import shutil
import socket
import subprocess
import tempfile
import time


def run(binary, mode, artifacts):
    with tempfile.TemporaryDirectory(prefix="vev-hybrid-") as temporary:
        root = Path(temporary)
        bindir = root / "bin"
        bindir.mkdir(mode=0o700)
        (bindir / "vev").symlink_to(binary)
        tmux = ["tmux", "-L", root.name]
        env = dict(os.environ, VEV_ENV="dev", VEV_ENV_ROOT=str(root / "local"),
                   VEV_LOG="debug", VEV_REMOTE_TRANSPORT=mode, SHELL="/bin/bash",
                   PATH=f"{bindir}:/usr/bin:/bin")
        env.pop("VEV", None)
        user = pwd.getpwuid(os.getuid()).pw_name
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]

        def command(argv, **kwargs):
            return subprocess.run(argv, cwd=root, env=env, text=True,
                                  capture_output=True, timeout=15, check=True, **kwargs).stdout

        def script(path, text):
            path.write_text(text)
            path.chmod(0o700)

        for name in ("host-key", "client-key"):
            command(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(root / name)])
        script(root / "remote-shell", f'''#!/bin/bash
export VEV_ENV=dev VEV_ENV_ROOT={root}/remote VEV_LOG=debug SHELL=/bin/bash
export PATH={bindir}:/usr/bin:/bin
unset VEV
cd {root}
exec /bin/bash -c "$SSH_ORIGINAL_COMMAND"
''')
        (root / "sshd.conf").write_text(f'''ListenAddress 127.0.0.1
Port {port}
HostKey {root}/host-key
PidFile {root}/sshd.pid
AuthorizedKeysFile {root}/client-key.pub
StrictModes no
PasswordAuthentication no
KbdInteractiveAuthentication no
UsePAM no
AllowUsers {user}
ForceCommand {root}/remote-shell
''')
        (root / "ssh.conf").write_text(f'''Host localhost
    HostName 127.0.0.1
    Port {port}
    User {user}
    IdentityFile {root}/client-key
    IdentitiesOnly yes
    UserKnownHostsFile {root}/known_hosts
    StrictHostKeyChecking accept-new
    BatchMode yes
    ConnectTimeout 2
''')
        script(bindir / "ssh", f'#!/bin/bash\nexec /usr/bin/ssh -F {root}/ssh.conf "$@"\n')
        script(root / "client", "#!/bin/bash\n" + "\n".join(
            f"export {key}={shlex.quote(env[key])}" for key in
            ("VEV_ENV", "VEV_ENV_ROOT", "VEV_LOG", "VEV_REMOTE_TRANSPORT", "SHELL", "PATH")
        ) + "\nunset VEV\nexec vev attach local-one\n")
        sshd_path = shutil.which("sshd") or shutil.which("sshd", path="/usr/sbin:/sbin")
        if sshd_path is None:
            raise RuntimeError("sshd not found; install openssh-server or add it to PATH")
        sshd = subprocess.Popen([sshd_path, "-D", "-f", str(root / "sshd.conf"),
                                 "-E", str(root / "sshd.log")], cwd=root)

        def screen():
            return command(tmux + ["capture-pane", "-p", "-t", "repro"])

        def wait(label, predicate):
            deadline = time.monotonic() + 10
            while True:
                text = screen()
                if predicate(text):
                    (artifacts / f"{mode}-{label}.txt").write_text(text)
                    return text
                if time.monotonic() >= deadline:
                    (artifacts / f"{mode}-{label}-FAILED.txt").write_text(text)
                    raise AssertionError(f"{mode}: {label}\n{text}")
                time.sleep(0.1)

        def send(*keys):
            command(tmux + ["send-keys", "-t", "repro", *keys])
            # Separate a lone Escape from the next Alt-Space sequence.
            time.sleep(0.2)

        def palette(query):
            send("Escape", "Space")
            wait("palette-open", lambda text: " Commands " in text)
            send("-l", query)

        def picker():
            palette("SSP")
            send("Enter")
            wait("picker-open", lambda text: " Sessions " in text and "[up]" in text)

        def select(query):
            send("/")
            send("-l", query)
            wait("picker-search", lambda text: f"{query}_" in text)
            send("Enter")

        def active(text, name):
            return text.splitlines()[-1].strip().split("  ")[0] == name

        try:
            deadline = time.monotonic() + 10
            while True:
                try:
                    command([str(bindir / "ssh"), "localhost", "true"])
                    break
                except subprocess.CalledProcessError as err:
                    if sshd.poll() is not None or time.monotonic() >= deadline:
                        log = root / "sshd.log"
                        detail = log.read_text() if log.exists() else "sshd produced no log"
                        raise RuntimeError(detail) from err
                    time.sleep(0.1)
            command([str(binary), "host", "add", "localhost"])
            for name in ("local-one", "local-two"):
                command(["env", "VEV=bootstrap", str(binary), "new", name])
            command([str(binary), "cmd", "-s", "local-two", "new-tab"])
            command([str(binary), "cmd", "-s", "local-two", "rename-tab", "chosen-tab"])
            for name in ("remote-one", "remote-two"):
                command([str(bindir / "ssh"), "localhost", f"VEV=bootstrap vev new {name}"])
            command(tmux + ["-f", "/dev/null", "new-session", "-d", "-s", "repro",
                            "-x", "120", "-y", "36", str(root / "client")])
            wait("initial", lambda text: active(text, "local-one"))
            picker()
            select("remote-one")
            wait("remote", lambda text: active(text, "remote-one@localhost"))
            picker()
            wait("remote-picker-context", lambda text: active(text, "remote-one@localhost"))
            # Exercise Back before committing a local selection.
            send("Escape")
            wait("back", lambda text: active(text, "remote-one@localhost") and " Sessions " not in text)
            picker()
            select("chosen-tab")
            wait("local-selected", lambda text: active(text, "local-two") and "chosen-tab" in text.splitlines()[0]
                 and "remote-one@localhost" in text.splitlines()[-1])
            picker()
            select("remote-one")
            wait("remote-again", lambda text: active(text, "remote-one@localhost"))
            palette("local")
            wait("local-history-in-remote-palette", lambda text: "Switch to session local-one" in text
                 and "Switch to session local-two" in text)
            send("Escape")
            palette("local-two")
            send("Enter")
            wait("local-palette-return", lambda text: active(text, "local-two") and "chosen-tab" in text.splitlines()[0])
            print(f"{mode}: PASS — picker context, Back, explicit tab, history, remote palette, return")
        finally:
            for argv in ([str(binary), "kill", "--all"],
                         [str(bindir / "ssh"), "localhost", "vev kill --all"],
                         tmux + ["kill-server"]):
                try:
                    command(argv)
                except (subprocess.SubprocessError, OSError):
                    pass
            sshd.terminate()
            sshd.wait(timeout=10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--artifacts", required=True, type=Path)
    parser.add_argument("--transport", choices=("udp", "stdio", "both"), default="both")
    args = parser.parse_args()
    args.artifacts.mkdir(parents=True, exist_ok=True, mode=0o700)
    for mode in (("udp", "stdio") if args.transport == "both" else (args.transport,)):
        run(args.binary.resolve(), mode, args.artifacts.resolve())


if __name__ == "__main__":
    main()
