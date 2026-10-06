#!/usr/bin/env python3
"""Temporarily block an Impala endpoint from an AKS pod and verify recovery."""

from __future__ import annotations

import base64
import os
import subprocess
import time
import uuid


def required_environment(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return value


def run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=check,
        capture_output=True,
        text=True,
        timeout=120,
    )


def tcp_from_pod(
    namespace: str,
    pod: str,
    host: str,
    port: int,
    timeout_seconds: int = 3,
) -> bool:
    source = (
        "import socket\n"
        "try:\n"
        f" c=socket.create_connection(({host!r},{port}),{timeout_seconds})\n"
        "except OSError:\n"
        " raise SystemExit(1)\n"
        "else:\n"
        " c.close()\n"
    )
    encoded = base64.b64encode(source.encode()).decode()
    result = run(
        [
            "kubectl",
            "-n",
            namespace,
            "exec",
            pod,
            "--",
            "python",
            "-c",
            f"import base64;exec(base64.b64decode({encoded!r}))",
        ],
        check=False,
    )
    return result.returncode == 0


def wait_for_tcp_state(
    expected: bool,
    deadline_seconds: int,
    *,
    namespace: str,
    pod: str,
    host: str,
    port: int,
) -> None:
    deadline = time.monotonic() + deadline_seconds
    while time.monotonic() < deadline:
        if tcp_from_pod(namespace, pod, host, port) is expected:
            return
        time.sleep(5)
    raise TimeoutError(
        f"TCP state did not become {expected} within {deadline_seconds} seconds"
    )


def main() -> int:
    resource_group = required_environment("IMPALA_FAULT_RESOURCE_GROUP")
    nsg = required_environment("IMPALA_FAULT_NSG")
    source = required_environment("IMPALA_FAULT_SOURCE_PREFIX")
    destination = required_environment("IMPALA_FAULT_DESTINATION_PREFIX")
    namespace = required_environment("IMPALA_FAULT_POD_NAMESPACE")
    pod = required_environment("IMPALA_FAULT_POD")
    host = required_environment("IMPALA_HOST")
    port = int(required_environment("IMPALA_PORT"))
    priority = required_environment("IMPALA_FAULT_PRIORITY")
    rule = f"fsp-impala-fault-{uuid.uuid4().hex[:12]}"

    probe = {
        "namespace": namespace,
        "pod": pod,
        "host": host,
        "port": port,
    }
    if not tcp_from_pod(**probe):
        raise RuntimeError("the baseline HS2 connection is not reachable")

    created = False
    try:
        run(
            [
                "az",
                "network",
                "nsg",
                "rule",
                "create",
                "--resource-group",
                resource_group,
                "--nsg-name",
                nsg,
                "--name",
                rule,
                "--priority",
                priority,
                "--direction",
                "Inbound",
                "--access",
                "Deny",
                "--protocol",
                "Tcp",
                "--source-address-prefixes",
                source,
                "--destination-address-prefixes",
                destination,
                "--destination-port-ranges",
                str(port),
                "--output",
                "none",
            ]
        )
        created = True
        wait_for_tcp_state(False, 180, **probe)
    finally:
        if created:
            run(
                [
                    "az",
                    "network",
                    "nsg",
                    "rule",
                    "delete",
                    "--resource-group",
                    resource_group,
                    "--nsg-name",
                    nsg,
                    "--name",
                    rule,
                    "--output",
                    "none",
                ]
            )

    wait_for_tcp_state(True, 300, **probe)
    print("Impala network interruption and recovery gate passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
