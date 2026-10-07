"""Fixture readiness relay tests; no network or production credentials."""
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch, MagicMock

spec = importlib.util.spec_from_file_location("fixture_ssh", Path(__file__).with_name("ssh.py"))
wrapper = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wrapper)


class ControlTests(unittest.TestCase):
    def test_control_failure_is_not_a_port(self):
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.makefile.return_value.readline.return_value = b'{"id":1,"error":"listener limit"}\n'
        with patch.object(wrapper.socket, "socket", return_value=conn):
            with self.assertRaises(RuntimeError):
                wrapper.control({"id": 1, "op": "udp", "port": 1234})

    def test_control_returns_fixture_port(self):
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.makefile.return_value.readline.return_value = b'{"id":1,"result":4321}\n'
        with patch.object(wrapper.socket, "socket", return_value=conn):
            self.assertEqual(wrapper.control({"id": 1, "op": "udp", "port": 1234}), 4321)


class MainTests(unittest.TestCase):
    BOOTSTRAP = ["-T", "--", "remote", "'vev' '_broker-mux-quic-bootstrap'"]

    def run_main(self, readiness, rewritten=4321):
        process = MagicMock()
        process.stdout.readline.return_value = readiness
        process.stdout.read1.return_value = b""
        process.wait.return_value = 0
        process.poll.return_value = 0
        out = MagicMock()
        with patch.object(wrapper.sys, "argv", ["ssh"] + self.BOOTSTRAP), \
                patch.object(wrapper.subprocess, "Popen", return_value=process), \
                patch.object(wrapper, "control", return_value=rewritten) as control, \
                patch.object(wrapper.sys, "stdout", MagicMock(buffer=out)):
            code = wrapper.main()
        return code, control, out

    def test_rewrites_only_the_port(self):
        code, control, out = self.run_main(b'{"port":1234,"fingerprint":"f"}\n')
        self.assertEqual(code, 0)
        control.assert_called_once_with({"id": 1, "op": "udp", "port": 1234})
        self.assertEqual(out.write.call_args_list[0].args[0], b'{"port":4321,"fingerprint":"f"}\n')

    def test_rejects_invalid_readiness(self):
        for line in (b'{"port":1}', b"x" * 4097 + b"\n", b'{"port":0}\n', b'{"port":65536}\n', b'{"port":"22"}\n'):
            with self.subTest(line=line[:20]):
                with self.assertRaises(RuntimeError):
                    self.run_main(line)

    def test_other_commands_exec_ssh_unchanged(self):
        with patch.object(wrapper.sys, "argv", ["ssh", "remote", "true"]), \
                patch.object(wrapper.os, "execv", side_effect=SystemExit(0)) as execv:
            with self.assertRaises(SystemExit):
                wrapper.main()
        execv.assert_called_once_with("/usr/bin/ssh-real", ["/usr/bin/ssh-real", "-p", "2222", "-o", "HostKeyAlias=remote", "remote", "true"])


if __name__ == "__main__":
    unittest.main()
