from contextlib import closing
from datetime import datetime, timedelta, timezone
import ipaddress
import io
import json
import os
from pathlib import Path
import socket
import sqlite3
import ssl
import subprocess
import sys
import threading
import time

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID
from fastapi import FastAPI
import httpx
import pyarrow.parquet as pq
import uvicorn

from enterprise.control.edge_http import create_edge_router
from enterprise.control.edge_receiver import EdgeBinding, EdgeReplayReceiver
from enterprise.control.federation import GlobalCatalog
from enterprise.control.placement import PlacementConfig
from fabric_shortcut_proxy.runtime.artifact_store import LocalDirStore
from fabric_shortcut_proxy.security.agent_auth import AgentAuthMiddleware
from tests.enterprise.test_edge_receiver import _generation
from tests.enterprise.test_federated_publication_runtime import _runtime


def _wait_for(predicate, timeout=30):
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= deadline:
            raise TimeoutError("edge process condition was not reached")
        time.sleep(0.1)


def _certificate(tmp_path):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "127.0.0.1")])
    now = datetime.now(timezone.utc)
    cert = (
        x509.CertificateBuilder().subject_name(name).issuer_name(name)
        .public_key(key.public_key()).serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    cert_path = tmp_path / "test-cert.pem"
    key_path = tmp_path / "test-key.pem"
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ))
    return cert_path, key_path


def test_edge_process_restarts_offline_and_replays_over_https(tmp_path, monkeypatch):
    source = tmp_path / "source.db"
    with closing(sqlite3.connect(source)) as db, db:
        db.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY, name TEXT)")
        db.execute("INSERT INTO sales VALUES (1, 'offline')")
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "config.system.json").write_text(json.dumps({"warehouse_prefix": "warehouse"}))
    (config_dir / "config.connection.json").write_text(json.dumps({
        "connections": [{"id": "source", "db_url": f"sqlite+aiosqlite:///{source.as_posix()}"}],
    }))
    (config_dir / "config.tables.json").write_text(json.dumps({"tables": [{
        "name": "sales", "source_table": "sales", "connection_id": "source",
        "num_splits": 1, "key_column": "id", "split_strategy": "modulo",
        "schema": [
            {"field_id": 1, "name": "id", "type": "long", "nullable": False},
            {"field_id": 2, "name": "name", "type": "string", "nullable": True},
        ],
    }]}))
    cert_path, key_path = _certificate(tmp_path)
    token = "test-edge-" + "x" * 32
    monkeypatch.setenv("AGENT_AUTH_MODE", "required")
    monkeypatch.setenv("AGENT_TOKEN", token)
    monkeypatch.setenv("AGENT_IDENTITY_TOKENS", json.dumps({"site-1": token}))
    catalog, _, stores, primary, _ = _runtime()
    app = FastAPI()
    app.add_middleware(AgentAuthMiddleware)
    app.include_router(create_edge_router(EdgeReplayReceiver(
        primary, {"site-1": EdgeBinding("source::sales", "sales", "warehouse/sales")}
    )))
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    journal_path = tmp_path / "edge.db"
    output = tmp_path / "edge.log"
    env = os.environ.copy()
    root = Path(__file__).resolve().parents[2]
    env.update({
        "FSP_CONFIG_DIR": str(config_dir),
        "PYTHONPATH": os.pathsep.join((str(root / "src"), str(root))),
        "PYTHONDONTWRITEBYTECODE": "1",
        "SSL_CERT_FILE": str(cert_path),
        "NO_PROXY": "127.0.0.1,localhost",
        "OBJECT_PATH_LAYOUT": "legacy",
        "PIN_MATERIALIZED_SPLITS": "0",
        "PARQUET_DISK_CACHE": "0",
        "ARTIFACT_STORE_SERVING": "0",
        "TABLE_FORMAT": "iceberg",
    })
    command = [
        sys.executable, "-m", "enterprise.control.edge_service",
        "--table", "sales", "--journal", str(journal_path),
        "--store-dir", str(tmp_path / "store"),
        "--manager-url", f"https://127.0.0.1:{port}", "--identity", "site-1",
        "--refresh-seconds", "0.5", "--retry-seconds", "0.2",
        "--max-pending", "3", "--max-store-bytes", "10000000",
    ]

    def pending():
        if not journal_path.exists():
            return 0
        try:
            with closing(sqlite3.connect(journal_path)) as db:
                return db.execute(
                    "SELECT COUNT(*) FROM generations WHERE acknowledged = 0"
                ).fetchone()[0]
        except sqlite3.OperationalError:
            return 0

    def acknowledged_originals():
        with closing(sqlite3.connect(journal_path)) as db:
            return db.execute(
                "SELECT COUNT(*) FROM generations WHERE sequence <= 3 AND acknowledged = 1"
            ).fetchone()[0]

    process = None
    server = None
    server_thread = None
    with output.open("wb") as log:
        try:
            process = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
            for number in (1, 2):
                _wait_for(lambda: pending() >= number or process.poll() is not None)
                assert process.poll() is None
                with closing(sqlite3.connect(source)) as db, db:
                    db.execute("UPDATE sales SET name = ?", (f"offline-{number + 1}",))
            _wait_for(lambda: pending() == 3 or process.poll() is not None)
            assert process.poll() is None
            with closing(sqlite3.connect(journal_path)) as db:
                expected = [row[0] for row in db.execute(
                    "SELECT generation_id FROM generations ORDER BY sequence"
                ).fetchall()]
            assert len(expected) == 3
            assert catalog.get("source::sales")["replicas"] == {}
            process.terminate()
            process.wait(timeout=10)
            process = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
            listener.listen(128)
            server = uvicorn.Server(uvicorn.Config(
                app, ssl_certfile=str(cert_path), ssl_keyfile=str(key_path),
                log_level="error", lifespan="off",
            ))
            server_thread = threading.Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
            server_thread.start()
            _wait_for(lambda: server.started)
            _wait_for(lambda: catalog.get("source::sales").get("edge_replay", {}).get("sequence", 0) >= 3)
            _wait_for(lambda: acknowledged_originals() == 3)
            process.terminate()
            process.wait(timeout=10)
            assert acknowledged_originals() == 3
            record = catalog.get("source::sales")
            published = [
                item["generation_id"] for _, item in sorted(
                    record["generations"].items(), key=lambda item: int(item[0])
                )
            ]
            assert published[:3] == expected
            generations = sorted(record["generations"].items(), key=lambda item: int(item[0]))
            names = []
            for _, item in generations[:3]:
                split = next(ref for ref in item["objects"] if ref["key"].endswith(".parquet"))
                table = pq.read_table(io.BytesIO(stores["published"].get(split["key"])))
                names.append(table.to_pylist()[0]["name"])
            assert names == ["offline", "offline-2", "offline-3"]
        finally:
            if process is not None and process.poll() is None:
                process.terminate()
                process.wait(timeout=10)
            if server is not None:
                server.should_exit = True
            if server_thread is not None:
                server_thread.join(timeout=10)
            listener.close()


def test_real_regional_manager_process_accepts_edge_replay(tmp_path):
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "config.tables.json").write_text(json.dumps({"tables": [{
        "name": "sales", "source_table": "sales", "connection_id": "source",
        "num_splits": 1, "key_column": "id", "split_strategy": "modulo",
        "schema": [{"field_id": 1, "name": "id", "type": "long", "nullable": False}],
    }]}))
    (config_dir / "config.connection.json").write_text(json.dumps({
        "connections": [{"id": "source", "db_url": f"sqlite+aiosqlite:///{(tmp_path / 'source.db').as_posix()}"}],
    }))
    with closing(sqlite3.connect(tmp_path / "source.db")) as db, db:
        db.execute("CREATE TABLE sales (id INTEGER PRIMARY KEY)")
    cert_path, key_path = _certificate(tmp_path)
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    location = "azure:swedencentral"
    placement_data = {
        "pools": [{
            "pool_id": "local", "identities": ["site-1"], "location": location,
            "storage_profile": "published", "allowed_connection_ids": ["source"],
        }],
        "stores": {"published": {"location": location, "provider": "local"}},
        "serving_endpoints": {"primary": {
            "location": location, "storage_profile": "published", "url": "https://example.test",
        }},
        "connections": {"source": {
            "required_pool": "local", "required_storage_profile": "published",
            "staging_storage_profile": "published", "residency_locations": [location],
            "serving_endpoints": ["primary"],
        }},
    }
    store = LocalDirStore(str(tmp_path / "regional"))
    catalog = GlobalCatalog(store, PlacementConfig.from_dict(placement_data))
    catalog.create(
        "source::sales", owner_id="manager-local", owner_location=location,
        lease_ms=100_000, freshness_target_ms=60_000,
    )
    token = "test-edge-" + "x" * 32
    root = Path(__file__).resolve().parents[2]
    env = os.environ.copy()
    env.update({
        "FSP_CONFIG_DIR": str(config_dir),
        "PYTHONPATH": os.pathsep.join((str(root / "src"), str(root))),
        "PYTHONDONTWRITEBYTECODE": "1",
        "CONTROL_HOST": "127.0.0.1", "CONTROL_PORT": str(port),
        "TLS_CERT_FILE": str(cert_path), "TLS_KEY_FILE": str(key_path),
        "MANAGER_HA": "0", "MANAGER_SUPERVISION_MODE": "external",
        "MANAGER_AUTH_ENABLED": "1", "MANAGER_AUTH_PASSWORD": "manager-test-" + "y" * 32,
        "AGENT_AUTH_MODE": "required", "AGENT_TOKEN": "shared-test-" + "z" * 32,
        "AGENT_IDENTITY_TOKENS": json.dumps({"site-1": token}),
        "AGENT_PLACEMENT_CONFIG": json.dumps(placement_data),
        "ARTIFACT_STORE_BACKEND": "local", "ARTIFACT_STORE_DIR": str(tmp_path / "regional"),
        "FSP_ARTIFACT_STORE_PROFILE": "published", "FSP_FEDERATION_ENABLED": "1",
        "FSP_REGIONAL_MANAGER_ID": "manager-local", "FSP_REGIONAL_MANAGER_LOCATION": location,
        "FSP_EDGE_BINDINGS": json.dumps({
            "site-1": {"dataset_id": "source::sales", "table": "sales", "key_prefix": "warehouse/sales"},
        }),
        "FSP_CATALOG_OWNER_LEASE_MS": "100000", "FSP_CATALOG_RENEW_INTERVAL_SECONDS": "20",
        "FSP_FEDERATION_REPLICA_STORES": "{}",
        "PIN_MATERIALIZED_SPLITS": "0", "ARTIFACT_STORE_SERVING": "0", "REQUIRE_SIGV4": "0",
    })
    log_path = tmp_path / "manager.log"
    with log_path.open("wb") as log:
        process = subprocess.Popen(
            [sys.executable, "-m", "enterprise.manager"], env=env,
            stdout=log, stderr=subprocess.STDOUT,
        )
        try:
            with httpx.Client(
                base_url=f"https://127.0.0.1:{port}",
                verify=ssl.create_default_context(cafile=str(cert_path)),
                trust_env=False, timeout=1,
            ) as client:
                def ready():
                    if process.poll() is not None:
                        return True
                    try:
                        return client.get("/healthz").status_code == 200
                    except httpx.RequestError:
                        return False

                _wait_for(ready)
                assert process.poll() is None, log_path.read_text(errors="replace")
                response = client.post(
                    "/control/edge/replay",
                    headers={"X-FSP-Agent-ID": "site-1", "X-FSP-Agent-Token": token},
                    json=_generation(tmp_path, 1),
                )
                assert response.status_code == 200, response.text
                assert catalog.get("source::sales")["edge_replay"]["sequence"] == 1
        finally:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=10)
