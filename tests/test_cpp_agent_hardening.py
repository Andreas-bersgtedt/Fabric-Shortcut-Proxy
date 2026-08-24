import http.client
import os
import pathlib
import shutil
import subprocess
import tempfile
import time
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
AGENT = ROOT / "agent-cpp" / "agent"
BUCKET = "test-bucket"


def read_hwm_kib(pid):
    status = pathlib.Path(f"/proc/{pid}/status")
    for line in status.read_text().splitlines():
        if line.startswith("VmHWM:"):
            return int(line.split()[1])
    return 0


class CppAgentHardeningTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if os.name == "nt":
            raise unittest.SkipTest("Linux socket smoke test")
        if not AGENT.exists():
            raise unittest.SkipTest("build agent-cpp/agent first")
        cls.store = pathlib.Path(tempfile.mkdtemp(prefix="cpp-agent-") )
        (cls.store / "safe.txt").write_text("safe", encoding="utf-8")
        (cls.store / "nested").mkdir()
        (cls.store / "nested" / "item.txt").write_text("nested", encoding="utf-8")
        for index in range(1001):
            (cls.store / "many").mkdir(exist_ok=True)
            (cls.store / "many" / f"item-{index:04d}.txt").write_text("x", encoding="utf-8")
        with (cls.store / "large.bin").open("wb") as stream:
            stream.write(b"x" * (16 * 1024 * 1024))

        cls.port = 19400 + (os.getpid() % 500)
        env = os.environ.copy()
        env.update({"PORT": str(cls.port), "STORE_DIR": str(cls.store), "S3_BUCKET": BUCKET})
        cls.process = subprocess.Popen(
            [str(AGENT)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env
        )
        for _ in range(50):
            try:
                connection = http.client.HTTPConnection("127.0.0.1", cls.port, timeout=1)
                connection.request("GET", "/readyz")
                if connection.getresponse().status == 200:
                    connection.close()
                    return
                connection.close()
            except OSError:
                time.sleep(0.1)
        raise RuntimeError("agent did not become ready")

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, "process"):
            cls.process.terminate()
            cls.process.wait(timeout=5)
        if hasattr(cls, "store"):
            shutil.rmtree(cls.store, ignore_errors=True)

    def request(self, path, headers=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        connection.request("GET", path, headers=headers or {})
        response = connection.getresponse()
        body = response.read()
        connection.close()
        return response.status, body

    def test_bucket_mismatch_is_rejected(self):
        status, body = self.request("/wrong-bucket/safe.txt")
        self.assertEqual(status, 404)
        self.assertIn(b"NoSuchBucket", body)

    def test_path_traversal_and_absolute_paths_are_rejected(self):
        for path in (
            f"/{BUCKET}/../safe.txt",
            f"/{BUCKET}/%2e%2e/safe.txt",
            f"/{BUCKET}/%2Fetc%2Fpasswd",
            f"/{BUCKET}/C:%5CWindows%5Cwin.ini",
        ):
            with self.subTest(path=path):
                status, _ = self.request(path)
                self.assertEqual(status, 400)

    def test_malformed_and_overflowing_ranges_are_rejected(self):
        for value in ("bytes=bad", "bytes=0-9223372036854775808", "bytes=-0", "bytes=4-2"):
            with self.subTest(value=value):
                status, _ = self.request(f"/{BUCKET}/safe.txt", {"Range": value})
                self.assertEqual(status, 416)

    def test_oversized_headers_are_rejected(self):
        status, _ = self.request(f"/{BUCKET}/safe.txt", {"X-Large": "x" * (70 * 1024)})
        self.assertEqual(status, 413)

    def test_list_is_bucket_scoped_and_capped(self):
        status, body = self.request(f"/{BUCKET}/?list-type=2&prefix=many/")
        self.assertEqual(status, 200)
        self.assertIn(b"<KeyCount>1000</KeyCount>", body)
        self.assertIn(b"<IsTruncated>true</IsTruncated>", body)
        self.assertNotIn(b"item-1000.txt", body)

        status, body = self.request(
            f"/{BUCKET}/?list-type=2&prefix=many/&continuation-token=many%2Fitem-0999.txt"
        )
        self.assertEqual(status, 200)
        self.assertIn(b"<KeyCount>1</KeyCount>", body)
        self.assertIn(b"many/item-1000.txt", body)
        self.assertIn(b"<IsTruncated>false</IsTruncated>", body)

    def test_list_delimiter_and_invalid_max_keys(self):
        status, body = self.request(f"/{BUCKET}/?list-type=2&prefix=&delimiter=/&max-keys=3")
        self.assertEqual(status, 200)
        self.assertIn(b"<CommonPrefixes><Prefix>many/</Prefix></CommonPrefixes>", body)
        self.assertIn(b"<CommonPrefixes><Prefix>nested/</Prefix></CommonPrefixes>", body)
        self.assertIn(b"<KeyCount>3</KeyCount>", body)

        for value in ("bad", "-1", "1001"):
            with self.subTest(value=value):
                status, body = self.request(f"/{BUCKET}/?list-type=2&max-keys={value}")
                self.assertEqual(status, 400)
                self.assertIn(b"InvalidArgument", body)

    def test_large_object_is_streamed(self):
        before = read_hwm_kib(self.process.pid)
        status, body = self.request(f"/{BUCKET}/large.bin")
        after = read_hwm_kib(self.process.pid)
        self.assertEqual(status, 200)
        self.assertEqual(len(body), 16 * 1024 * 1024)
        self.assertLess(after - before, 4096)


if __name__ == "__main__":
    unittest.main()
