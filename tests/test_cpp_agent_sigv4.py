import http.client
import os
import pathlib
import shutil
import socket
import subprocess
import tempfile
import time

import pytest


ROOT = pathlib.Path(__file__).resolve().parents[1]
AGENT = ROOT / "agent-cpp" / ("agent.exe" if os.name == "nt" else "agent")
BUCKET = "sigv4-test"
ACCESS_KEY = "FSPTESTACCESSKEY0001"
SECRET_KEY = "sigv4-test-secret"

botocore = pytest.importorskip("botocore")
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def _sign(
    port: int,
    method: str,
    path: str,
    *,
    range_value: str | None = None,
    access_key: str = ACCESS_KEY,
    secret_key: str = SECRET_KEY,
) -> dict[str, str]:
    url = f"http://127.0.0.1:{port}{path}"
    headers = {"host": f"127.0.0.1:{port}"}
    if range_value is not None:
        headers["range"] = range_value
    request = AWSRequest(method=method, url=url, headers=headers)
    S3SigV4Auth(
        Credentials(access_key, secret_key), "s3", "us-east-1"
    ).add_auth(request)
    return dict(request.headers)


class TestCppAgentSigV4:
    @classmethod
    def setup_class(cls):
        if not AGENT.exists():
            pytest.skip("build the C++ Agent first")
        cls.store = pathlib.Path(tempfile.mkdtemp(prefix="cpp-agent-sigv4-"))
        (cls.store / "allowed").mkdir()
        (cls.store / "allowed" / "safe.txt").write_text("safe", encoding="utf-8")
        (cls.store / "allowed" / "space name.txt").write_text("space", encoding="utf-8")
        (cls.store / "allowed" / "nested").mkdir()
        (cls.store / "allowed" / "nested" / "item.txt").write_text("nested", encoding="utf-8")
        (cls.store / "private").mkdir()
        (cls.store / "private" / "secret.txt").write_text("private", encoding="utf-8")
        cls.port = _free_port()
        env = os.environ.copy()
        env.update(
            {
                "PORT": str(cls.port),
                "STORE_DIR": str(cls.store),
                "S3_BUCKET": BUCKET,
                "S3_AUTH_MODE": "sigv4",
                "S3_ACCESS_KEY_ID": ACCESS_KEY,
                "S3_SECRET_ACCESS_KEY": SECRET_KEY,
                "S3_ALLOWED_PREFIXES": "allowed",
            }
        )
        cls.process = subprocess.Popen(
            [str(AGENT)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env
        )
        for _ in range(50):
            try:
                connection = http.client.HTTPConnection("127.0.0.1", cls.port, timeout=1)
                connection.request("GET", "/healthz")
                response = connection.getresponse()
                response.read()
                connection.close()
                if response.status == 200:
                    return
            except OSError:
                pass
            time.sleep(0.1)
        cls.process.terminate()
        cls.process.wait(timeout=5)
        shutil.rmtree(cls.store, ignore_errors=True)
        raise RuntimeError("C++ Agent failed to start in SigV4 mode")

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, "process"):
            cls.process.terminate()
            cls.process.wait(timeout=5)
        if hasattr(cls, "store"):
            shutil.rmtree(cls.store, ignore_errors=True)

    def request(
        self,
        path: str,
        headers: dict[str, str] | None = None,
        *,
        method: str = "GET",
    ):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        connection.request(method, path, headers=headers or {})
        response = connection.getresponse()
        result = response.status, response.read()
        connection.close()
        return result

    def raw_request(self, raw: bytes) -> bytes:
        with socket.create_connection(("127.0.0.1", self.port), timeout=5) as connection:
            connection.sendall(raw)
            response = bytearray()
            while True:
                chunk = connection.recv(4096)
                if not chunk:
                    break
                response.extend(chunk)
        return bytes(response)

    def test_sigv4_allows_get_and_rejects_unsigned_and_tampered_requests(self):
        path = f"/{BUCKET}/allowed/safe.txt"
        headers = _sign(self.port, "GET", path)
        status, body = self.request(path, headers)
        assert status == 200
        assert body == b"safe"

        status, body = self.request(path)
        assert status == 403
        assert b"AccessDenied" in body

        unknown_key = _sign(
            self.port,
            "GET",
            path,
            access_key="FSPUNKNOWNACCESSKEY0001",
            secret_key="unknown-key-secret",
        )
        status, body = self.request(path, unknown_key)
        assert status == 403
        assert b"InvalidAccessKeyId" in body

        tampered = _sign(self.port, "GET", path, range_value="bytes=0-1")
        tampered["range"] = "bytes=1-2"
        status, body = self.request(path, tampered)
        assert status == 403
        assert b"SignatureDoesNotMatch" in body

    def test_prefix_acl_and_idempotent_retries(self):
        denied_path = f"/{BUCKET}/private/secret.txt"
        status, body = self.request(denied_path, _sign(self.port, "GET", denied_path))
        assert status == 403
        assert b"AccessDenied" in body

        path = f"/{BUCKET}/allowed/safe.txt"
        headers = _sign(self.port, "GET", path)
        assert self.request(path, headers)[0] == 200
        retry_status, retry_body = self.request(path, headers)
        assert retry_status == 200
        assert retry_body == b"safe"

    def test_signed_head_range_and_scoped_list(self):
        path = f"/{BUCKET}/allowed/safe.txt"
        assert self.request(path, _sign(self.port, "HEAD", path), method="HEAD")[0] == 200

        headers = _sign(self.port, "GET", path, range_value="bytes=0-1")
        status, body = self.request(path, headers)
        assert status == 206
        assert body == b"sa"

        list_path = f"/{BUCKET}/?list-type=2&prefix=allowed%2F"
        status, body = self.request(list_path, _sign(self.port, "GET", list_path))
        assert status == 200
        assert b"allowed/safe.txt" in body
        assert b"private/secret.txt" not in body

        unrestricted_list = f"/{BUCKET}/?list-type=2"
        status, body = self.request(
            unrestricted_list, _sign(self.port, "GET", unrestricted_list)
        )
        assert status == 403
        assert b"AccessDenied" in body

    def test_encoded_path_and_repeated_valid_request(self):
        for path, expected in (
            (f"/{BUCKET}/allowed/space%20name.txt", b"space"),
            (f"/{BUCKET}/allowed%2Fnested%2Fitem.txt", b"nested"),
        ):
            headers = _sign(self.port, "GET", path)
            status, body = self.request(path, headers)
            assert status == 200
            assert body == expected
            assert self.request(path, headers) == (200, expected)

    def test_parser_rejects_duplicate_folded_and_body_headers(self):
        for headers in (
            b"Range: bytes=0-1\r\nRange: bytes=1-2\r\n",
            b"X-Test: one\r\n two\r\n",
            b"Content-Length: 1\r\n",
            b"X-Test: bad%2path\r\n",
        ):
            request_path = (
                b"/" + BUCKET.encode() + b"/bad%2path"
                if headers.startswith(b"X-Test: bad")
                else b"/" + BUCKET.encode() + b"/allowed/safe.txt"
            )
            response = self.raw_request(
                b"GET " + request_path + b" HTTP/1.1\r\n"
                b"Host: localhost\r\n" + headers + b"\r\n"
            )
            assert b"400 Bad Request" in response

    def test_sigv4_mode_fails_closed_without_credentials(self):
        env = os.environ.copy()
        env["S3_AUTH_MODE"] = "sigv4"
        env.pop("S3_ACCESS_KEY_ID", None)
        env.pop("S3_SECRET_ACCESS_KEY", None)
        result = subprocess.run(
            [str(AGENT)], env=env, capture_output=True, text=True, timeout=5, check=False
        )
        assert result.returncode == 2
        assert "S3_ACCESS_KEY_ID" in result.stderr

    def test_health_endpoint_does_not_require_sigv4(self):
        status, body = self.request("/healthz")
        assert status == 200
        assert b'"status":"ok"' in body

    def test_trusted_upstream_mode_keeps_unsigned_local_reads(self):
        port = _free_port()
        env = os.environ.copy()
        env.update(
            {
                "PORT": str(port),
                "STORE_DIR": str(self.store),
                "S3_BUCKET": BUCKET,
                "S3_AUTH_MODE": "trusted-upstream",
            }
        )
        process = subprocess.Popen(
            [str(AGENT)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env
        )
        try:
            for _ in range(50):
                try:
                    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=1)
                    connection.request("GET", f"/{BUCKET}/allowed/safe.txt")
                    response = connection.getresponse()
                    body = response.read()
                    connection.close()
                    assert (response.status, body) == (200, b"safe")
                    return
                except OSError:
                    time.sleep(0.1)
            pytest.fail("trusted-upstream Agent failed to start")
        finally:
            process.terminate()
            process.wait(timeout=5)
