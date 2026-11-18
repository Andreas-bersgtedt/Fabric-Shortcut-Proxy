"""HTTPS client for ordered edge replay."""

from __future__ import annotations

import asyncio
import base64
import json
import os
from urllib.parse import urlsplit

import httpx

from enterprise.control.edge_journal import EdgeJournal, EdgeJournalEntry, encode_edge_record
from enterprise.control.edge_receiver import MAX_EDGE_REQUEST_BYTES
from fabric_shortcut_proxy.runtime.artifact_store import LocalDirStore
from fabric_shortcut_proxy.security.agent_auth import AGENT_ID_HEADER, AGENT_TOKEN_HEADER


def build_edge_request(entry: EdgeJournalEntry, store: LocalDirStore) -> bytes:
    refs = [
        (split.object_key, split.size_bytes, split.content_hash)
        for split in entry.manifest.splits
    ] + [(item.key, item.size_bytes, item.sha256) for item in entry.metadata]
    if sum((size + 2) // 3 * 4 for _, size, _ in refs) > MAX_EDGE_REQUEST_BYTES:
        raise ValueError("edge generation exceeds the request limit")
    objects = {}
    for key, size, digest in refs:
        if not store.verify(key, size=size, content_hash=digest):
            raise ValueError(f"edge object failed verification: {key}")
        data = store.get(key)
        if len(data) != size:
            raise ValueError(f"edge object size changed: {key}")
        objects[key] = base64.b64encode(data).decode("ascii")
    payload = {
        "sequence": entry.sequence,
        "manifest_sha256": entry.manifest_sha256,
        "record": json.loads(encode_edge_record(entry.manifest, entry.metadata)),
        "objects": objects,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    if len(encoded) > MAX_EDGE_REQUEST_BYTES:
        raise ValueError("edge generation exceeds the request limit")
    return encoded


class EdgeReplayClient:
    def __init__(
        self, manager_url: str, identity: str, *, transport: httpx.AsyncBaseTransport | None = None
    ) -> None:
        parsed = urlsplit(manager_url)
        if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError("edge replay requires an HTTPS Manager URL without credentials")
        if not identity:
            raise ValueError("edge replay requires an identity")
        self.identity = identity
        self._credential = None
        mode = os.environ.get("AGENT_AUTH_MODE", "").strip().lower()
        if mode == "entra":
            from fabric_shortcut_proxy.security.entra_agent_auth import AgentEntraCredential

            self._credential = AgentEntraCredential()
        elif mode == "required":
            token = os.environ.get("AGENT_TOKEN", "")
            if len(token) < 32 or not token.isascii() or any(not 0x21 <= ord(c) <= 0x7e for c in token):
                raise ValueError("edge replay requires an identity-specific Agent token")
        else:
            raise ValueError("edge replay requires Agent authentication mode required or entra")
        self._client = httpx.AsyncClient(
            base_url=manager_url.rstrip("/"), timeout=30, transport=transport
        )

    async def replay_once(self, journal: EdgeJournal, store: LocalDirStore) -> bool:
        entry = await asyncio.to_thread(journal.next_pending)
        if entry is None:
            return False
        payload = await asyncio.to_thread(build_edge_request, entry, store)
        headers = {AGENT_ID_HEADER: self.identity, "content-type": "application/json"}
        if self._credential is not None:
            headers["authorization"] = f"Bearer {await self._credential.token()}"
        else:
            headers[AGENT_TOKEN_HEADER] = os.environ["AGENT_TOKEN"]
        response = await self._client.post("/control/edge/replay", content=payload, headers=headers)
        response.raise_for_status()
        ack = response.json()
        if ack != {
            "sequence": entry.sequence,
            "generation_id": entry.manifest.generation_id,
            "manifest_sha256": entry.manifest_sha256,
        }:
            raise ValueError("regional acknowledgement does not match the journal entry")
        await asyncio.to_thread(
            journal.acknowledge, entry.sequence, entry.manifest.generation_id, entry.manifest_sha256
        )
        return True

    async def aclose(self) -> None:
        try:
            await self._client.aclose()
        finally:
            if self._credential is not None:
                await self._credential.close()
