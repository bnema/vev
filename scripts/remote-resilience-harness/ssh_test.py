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


if __name__ == "__main__":
    unittest.main()
