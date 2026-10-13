"""Durable measured cross-boundary byte ledger and monthly reports."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from fabric_shortcut_proxy.runtime.artifact_store import ArtifactStore


_PREFIX = "_control/egress/v1"
_MONTH = re.compile(r"\d{4}-(0[1-9]|1[0-2])")
_GIB = Decimal(1024**3)


@dataclass(frozen=True)
class EgressEvent:
    event_id: str
    occurred_at_ms: int
    tenant_id: str
    dataset_id: str
    pool_id: str
    source_region: str
    destination_region: str
    direction: str
    bytes: int

    @property
    def month(self) -> str:
        return datetime.fromtimestamp(
            self.occurred_at_ms / 1000,
            timezone.utc,
        ).strftime("%Y-%m")

    def validate(self) -> None:
        for name in (
            "event_id",
            "dataset_id",
            "pool_id",
            "source_region",
            "destination_region",
            "direction",
        ):
            if not str(getattr(self, name)).strip():
                raise ValueError(f"egress event {name} must be non-empty")
        if self.occurred_at_ms < 1:
            raise ValueError(
                "egress event occurred_at_ms must be positive"
            )
        if (
            isinstance(self.bytes, bool)
            or not isinstance(self.bytes, int)
            or self.bytes < 0
        ):
            raise ValueError(
                "egress event bytes must be a non-negative integer"
            )
        if self.source_region == self.destination_region:
            raise ValueError(
                "egress event must cross a region or site boundary"
            )

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return {
            "version": 1,
            "month": self.month,
            **asdict(self),
        }

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "EgressEvent":
        if (
            not isinstance(value, dict)
            or value.get("version") != 1
        ):
            raise ValueError("invalid egress event")
        event = cls(
            event_id=str(value["event_id"]),
            occurred_at_ms=int(value["occurred_at_ms"]),
            tenant_id=str(value.get("tenant_id", "")),
            dataset_id=str(value["dataset_id"]),
            pool_id=str(value["pool_id"]),
            source_region=str(value["source_region"]),
            destination_region=str(value["destination_region"]),
            direction=str(value["direction"]),
            bytes=int(value["bytes"]),
        )
        event.validate()
        if value.get("month") != event.month:
            raise ValueError("egress event month does not match timestamp")
        return event


def parse_pricing(raw: str) -> dict[str, Any]:
    if not raw.strip():
        return {"currency": "", "rates": {}}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("FSP_EGRESS_PRICING must be valid JSON") from exc
    if value in ({}, None):
        return {"currency": "", "rates": {}}
    if (
        not isinstance(value, dict)
        or set(value) != {"currency", "rates"}
        or not isinstance(value["currency"], str)
        or not value["currency"].strip()
        or not isinstance(value["rates"], dict)
    ):
        raise ValueError(
            "FSP_EGRESS_PRICING requires currency and rates"
        )
    rates = {}
    for boundary, raw_rate in value["rates"].items():
        if (
            not isinstance(boundary, str)
            or "->" not in boundary
            or isinstance(raw_rate, bool)
        ):
            raise ValueError(
                "egress pricing rates must map source->destination to a number"
            )
        try:
            rate = Decimal(str(raw_rate))
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(
                "egress pricing rates must be decimal numbers"
            ) from exc
        if not rate.is_finite() or rate < 0:
            raise ValueError(
                "egress pricing rates must be finite and non-negative"
            )
        rates[boundary] = rate
    return {
        "currency": value["currency"].strip(),
        "rates": rates,
    }


class EgressLedger:
    def __init__(self, store: ArtifactStore):
        self.store = store

    @staticmethod
    def _event_key(event: EgressEvent) -> str:
        digest = hashlib.sha256(event.event_id.encode()).hexdigest()
        return f"{_PREFIX}/{event.month}/{digest}.json"

    def record(self, event: EgressEvent) -> None:
        raw = json.dumps(
            event.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        self.store.put_stream_if_absent(
            self._event_key(event),
            [raw],
            length=len(raw),
        )

    def events(self, month: str) -> list[EgressEvent]:
        if not _MONTH.fullmatch(month):
            raise ValueError("egress report month must use YYYY-MM")
        events = []
        for item in self.store.list(f"{_PREFIX}/{month}/"):
            try:
                value = json.loads(self.store.get(item.key))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ValueError(
                    f"invalid egress ledger record {item.key}"
                ) from exc
            event = EgressEvent.from_dict(value)
            if event.month != month:
                raise ValueError(
                    f"egress ledger record is under the wrong month: {item.key}"
                )
            events.append(event)
        return sorted(
            events,
            key=lambda item: (
                item.tenant_id,
                item.dataset_id,
                item.pool_id,
                item.source_region,
                item.destination_region,
                item.direction,
                item.event_id,
            ),
        )

    def report(
        self,
        month: str,
        *,
        pricing: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        price = pricing or {"currency": "", "rates": {}}
        currency = str(price.get("currency", ""))
        rates = price.get("rates") or {}
        rows: dict[tuple[str, ...], dict[str, Any]] = {}
        for event in self.events(month):
            key = (
                event.tenant_id,
                event.dataset_id,
                event.pool_id,
                event.source_region,
                event.destination_region,
                event.direction,
            )
            row = rows.setdefault(
                key,
                {
                    "tenant_id": event.tenant_id,
                    "dataset_id": event.dataset_id,
                    "pool_id": event.pool_id,
                    "source_region": event.source_region,
                    "destination_region": event.destination_region,
                    "direction": event.direction,
                    "bytes": 0,
                    "transfers": 0,
                },
            )
            row["bytes"] += event.bytes
            row["transfers"] += 1
        total_bytes = 0
        priced_bytes = 0
        unpriced_bytes = 0
        total_cost = Decimal(0)
        priced_rows = []
        for key in sorted(rows):
            row = rows[key]
            total_bytes += int(row["bytes"])
            boundary = (
                f"{row['source_region']}->{row['destination_region']}"
            )
            rate = rates.get(boundary)
            cost = (
                Decimal(int(row["bytes"])) / _GIB * rate
                if rate is not None
                else None
            )
            row["price_per_gib"] = (
                str(rate) if rate is not None else None
            )
            row["estimated_cost"] = (
                str(cost.quantize(Decimal("0.000001")))
                if cost is not None
                else None
            )
            if cost is not None:
                total_cost += cost
                priced_bytes += int(row["bytes"])
            else:
                unpriced_bytes += int(row["bytes"])
            priced_rows.append(row)
        return {
            "month": month,
            "currency": currency or None,
            "total_bytes": total_bytes,
            "priced_bytes": priced_bytes,
            "unpriced_bytes": unpriced_bytes,
            "estimated_cost": (
                str(total_cost.quantize(Decimal("0.000001")))
                if currency
                else None
            ),
            "rows": priced_rows,
        }
