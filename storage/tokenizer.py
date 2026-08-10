"""
Proxy-side column tokenizer for object-store mounts (issue #12).

This is the in-process, Arrow-native analogue of the SQL pushdown
``Dialect.render_projection`` in ``planner/dialects.py``: it transforms column
*values* for Delta/Iceberg object-store sources, which have no compute engine to
push a token expression down to. It lives in the storage subsystem and is used
only by the transforming mount path — the relational SQL->Iceberg/Delta engine is
never involved.

Containment rule: this module imports only the shared, source-agnostic policy
model (``config.ColumnTransform`` / ``config.ColumnDef`` — resolved lazily) plus
pyarrow. It must NOT import ``planner``/``db``/``iceberg``/``delta``/``runtime``;
``tests/test_objectstore_tokenizer.py`` enforces that so the engine stays
uncoupled.

Token construction matches the pushdown dialects for best-effort cross-engine
equality: ``SHA-256(key | domain | normalize(value))`` rendered as uppercase hex.
Cross-engine equality is best-effort only — string encoding and numeric/date
formatting differ per source, exactly as documented for multi-dialect pushdown.
"""
from __future__ import annotations

import hashlib
import uuid
from typing import TYPE_CHECKING, Protocol

import pyarrow as pa

if TYPE_CHECKING:  # avoid importing config (and the world) at module load
    from config import ColumnDef, ColumnTransform


class TokenizerError(Exception):
    """Raised for an unknown transform kind or a mistargeted transform."""


def _to_text(value) -> str | None:
    """Best-effort ``CAST(value AS text)`` equivalent; ``None`` stays ``None``."""
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value)


def _normalize(text: str, mode: str) -> str:
    """Apply the column's normalization, matching the dialect logic (trim then lower)."""
    if mode in ("trim", "trim_lower"):
        text = text.strip()
    if mode == "trim_lower":
        text = text.lower()
    return text


class Tokenizer(Protocol):
    kind: str

    def apply(self, values: "pa.Array", *, transform: "ColumnTransform",
              column: "ColumnDef") -> "pa.Array": ...


class DeterministicHashTokenizer:
    """Stable keyed SHA-256 token: equal normalized inputs produce equal tokens."""

    kind = "deterministic_hash"

    def apply(self, values, *, transform, column):
        import config  # lazy: keeps this module import-clean (see containment rule)

        key = config.resolve_tokenization_key(transform.key_ref)
        domain = transform.domain or column.name
        prefix = f"{key}|{domain}|"
        out: list[str | None] = []
        for value in values.to_pylist():
            text = _to_text(value)
            if text is None:
                out.append(None)
                continue
            normalized = _normalize(text, transform.normalization)
            digest = hashlib.sha256((prefix + normalized).encode("utf-8")).hexdigest()
            out.append(digest.upper())
        return pa.array(out, type=pa.string())


class RandomTokenTokenizer:
    """Fresh UUID per non-null value; equality relationships are intentionally lost."""

    kind = "random_token"

    def apply(self, values, *, transform, column):
        out = [None if value is None else str(uuid.uuid4()) for value in values.to_pylist()]
        return pa.array(out, type=pa.string())


_REGISTRY: dict[str, Tokenizer] = {}


def register_tokenizer(tokenizer: Tokenizer) -> None:
    """Register a tokenizer implementation under its ``kind`` (extension point)."""
    _REGISTRY[tokenizer.kind] = tokenizer


def get_tokenizer(kind: str) -> Tokenizer:
    """Return the tokenizer for ``kind`` or fail closed on an unknown kind."""
    try:
        return _REGISTRY[kind]
    except KeyError:
        raise TokenizerError(
            f"No tokenizer registered for transform kind {kind!r}; "
            f"known kinds: {sorted(_REGISTRY)}"
        ) from None


def supported_kinds() -> tuple[str, ...]:
    return tuple(sorted(_REGISTRY))


register_tokenizer(DeterministicHashTokenizer())
register_tokenizer(RandomTokenTokenizer())


def tokenize_batch(batch: "pa.RecordBatch", columns: "list[ColumnDef]") -> "pa.RecordBatch":
    """Project one source ``RecordBatch`` into the output schema.

    For each output ``ColumnDef``: apply its tokenizer when a transform is set,
    else pass the source column through (renamed to the output name). Columns not
    present in ``columns`` are dropped — the same omission rule as pushdown, so a
    removed source column never reaches Parquet. Output field names are the output
    column names, so ``RecordBatch.to_pylist()`` feeds ``rows_to_parquet`` directly.
    """
    names = set(batch.schema.names)
    arrays: list[pa.Array] = []
    out_names: list[str] = []
    for column in columns:
        source = column.source_name
        if source not in names:
            raise TokenizerError(
                f"source column {source!r} for output {column.name!r} is not in the "
                f"source table (columns: {batch.schema.names})"
            )
        values = batch.column(batch.schema.get_field_index(source))
        if column.transform:
            values = get_tokenizer(column.transform.kind).apply(
                values, transform=column.transform, column=column
            )
        arrays.append(values)
        out_names.append(column.name)
    return pa.RecordBatch.from_arrays(arrays, names=out_names)


def output_arrow_schema(source_schema: "pa.Schema", columns: "list[ColumnDef]") -> "pa.Schema":
    """Arrow schema of the tokenized output: transformed columns are ``string``,
    passthrough columns keep their source type. Used to write an empty tokenized
    table when the source has no rows, without invoking a tokenizer (and its key)."""
    fields = []
    for column in columns:
        if column.transform:
            fields.append(pa.field(column.name, pa.string(), nullable=column.nullable))
            continue
        index = source_schema.get_field_index(column.source_name)
        if index < 0:
            raise TokenizerError(
                f"source column {column.source_name!r} for output {column.name!r} "
                f"is not in the source table (columns: {source_schema.names})"
            )
        fields.append(pa.field(column.name, source_schema.field(index).type, nullable=column.nullable))
    return pa.schema(fields)
