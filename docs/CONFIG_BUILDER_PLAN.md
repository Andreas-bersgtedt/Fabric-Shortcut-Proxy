# Sidequest Plan: Config Builder SPA

A small web tool that connects to a **SQL Server** or **PostgreSQL** database with
just host / user / password, lets you pick one or more tables, and **downloads a
ready-to-use `config.json`** for the proxy.

Status: **✅ BUILT (M1–M4)**: enable with `ENABLE_CONFIG_BUILDER=1`, open
`http://localhost:9000/_config/`. Backend in [configbuilder/router.py](../configbuilder/router.py)
+ [db/reflect.py](../db/reflect.py); SPA in [configbuilder/index.html](../configbuilder/index.html);
tests in `tests/test_config_builder.py`. Decisions taken: `asyncpg` is a
`pip install` (not bundled); password included in `db_url` by default with an
omit toggle; SPA served inline from a Python route.

---

## 1. Goal & user flow

1. Open the builder in a browser (served by the proxy at `/_config/`).
2. Pick a dialect, enter **host, database, username, password** → *Test connection*.
3. See the list of tables/views → **select one or more** (search, select-all).
4. (Optional) tweak per-table key column & split count, and global settings.
5. Live JSON preview → **Download `config.json`** (or copy to clipboard).

The downloaded file drops straight next to `main.py` (or via `CONFIG_FILE`) and the
proxy serves those tables, schema auto-reflected, no hand-written columns.

---

## 2. Key architectural insight

A browser **cannot** open a TCP connection to MSSQL/PostgreSQL or load their
drivers. So the builder is **SPA + thin backend**:

- **Backend** (Python, in this repo): opens the DB connection, lists tables,
  reflects columns/keys. **Reuses the reflection code we already have** in
  [db/executor.py](../db/executor.py): `reflect_columns`, `reflect_primary_key`,
  `sqlalchemy_type_to_iceberg`, `derive_table_schema`, `_split_qualified`.
- **Frontend**: a single static HTML page (vanilla JS, no build step) that calls
  the backend and assembles/downloads the JSON.

The builder is an **optional, flag-gated admin surface**: off by default,
intended for local/admin use (it accepts DB credentials).

---

## 3. Backend design

### 3.1 New: reflect against an *arbitrary* connection

Current helpers use the global engine (`config.DB_URL`). The builder connects to a
**user-supplied** database, so add a temporary-engine variant:

```python
# db/reflect.py (new) or extend db/executor.py
class SchemaReflector:
    """Create a throwaway async engine for a given URL; reflect; dispose."""
    async def list_tables(schema=None) -> list[dict]   # {schema, name, kind: table|view}
    async def list_schemas() -> list[str]
    async def columns(source_table) -> list[dict]      # name, iceberg_type, nullable
    async def primary_key(source_table) -> list[str]
    async def approx_row_count(source_table) -> int | None  # fast estimate (optional)
```

- Builds the URL with **`sqlalchemy.engine.URL.create(drivername, username,
  password, host, port, database, query)`** so special chars are encoded correctly.
- **Drivername comes from an allowlist keyed by dialect** (never accept a raw URL
  from the SPA): `postgresql`→`postgresql+asyncpg`, `mssql`→`mssql+aioodbc`
  (+ `sqlite+aiosqlite` for tests only).
- `approx_row_count` uses fast catalog estimates (Postgres `pg_class.reltuples`,
  SQL Server `sys.dm_db_partition_stats`), never a full `COUNT(*)` on big tables.

### 3.2 New: `/_config` API router (mounted only when enabled)

`configbuilder/router.py` (an `APIRouter`), included in [main.py](../main.py) only when
`config.ENABLE_CONFIG_BUILDER` is true, and added to the SigV4 `_AUTH_EXEMPT_PREFIXES`.

| Route | Body | Returns |
|---|---|---|
| `GET /_config/` | — | the SPA HTML |
| `GET /_config/api/settings` | — | full settings catalog (key/env/type/default/category/help/secret) |
| `GET /_config/api/current` | — | effective running settings with source (`env`/`file`/`default`), secrets redacted |
| `GET /_config/api/bootstrap` | — | builder bootstrap snapshot (non-secret defaults + active table mappings) |
| `POST /_config/api/save` | `{settings:{key:value,...}}` | validate + persist updates into `config.json` |
| `POST /_config/api/connect` | `{dialect, host, port?, database, username, password, driver?, trust_cert?, schema?}` | `{ok, server_version?, schemas[], tables:[{schema,name,kind}]}` or `{ok:false, error}` |
| `POST /_config/api/inspect` | `{connection…, tables:[{schema,name}]}` | per table: `{name, source_table, detected_key, integer_keys[], columns:[{name,type,nullable}], approx_rows?}` |

- **Stateless**: the SPA re-sends connection params on each call (creds live only in
  the browser for the session). No server-side secret storage.
- Live-mode saves write to `config.json`; environment variables still take
  precedence at runtime (`env > file > default`).

### 3.3 Config flags ([config.py](../config.py))

```
ENABLE_CONFIG_BUILDER      (bool, default 0)   # mounts the /_config surface
```

---

## 4. Frontend design (single `configbuilder/index.html`)

Vanilla HTML/CSS/JS, no framework, no build. Three-step wizard on one page:

1. **Connect**: dialect dropdown (PostgreSQL / SQL Server), host, port
   (auto-defaults 5432 / 1433), database, username, password; *Advanced* (ODBC
   driver name, `TrustServerCertificate`, schema filter). *Test connection* →
   shows server version + table count.
2. **Pick tables**: schema-grouped list with checkboxes, live **search**,
   select-all/none. Each selected row shows the **detected key column** in a
   dropdown limited to integer columns (override allowed) and an optional
   `num_splits`. Rows with no integer key are flagged (proxy requires an int key).
3. **Review & download**: global settings (bucket, default `num_splits`,
   `require_sigv4`, "include password in db_url" toggle, advanced flags), a **live
   JSON preview**, and **Download config.json** (client-side `Blob`) + **Copy**.

Niceties: remember non-secret form values in `localStorage`; never persist the
password; inline validation; friendly errors from the API.

---

## 5. Generated output (already the proxy's format)

```json
{
  "db_url": "postgresql+asyncpg://user:pass@host:5432/salesdb",
  "num_splits": 8,
  "require_sigv4": false,
  "tables": [
    { "name": "orders",    "source_table": "public.orders",    "key_column": "order_id" },
    { "name": "customers", "source_table": "public.customers", "key_column": "customer_id", "num_splits": 4 }
  ]
}
```

Matches [config.example.json](../config.example.json) exactly, so the file works
with the existing loader with zero changes.

---

## 6. Security (must-haves)

- **Off by default** (`ENABLE_CONFIG_BUILDER=0`); it accepts DB credentials.
- **Local/admin only**: document "do not expose publicly"; optionally refuse
  non-loopback binds unless `CONFIG_BUILDER_ALLOW_REMOTE=1`.
- **Never log credentials** (reuse `redact_db_url`); creds are transient.
- **No raw URLs/SQL from the client**: build the URL from structured fields via
  `URL.create` with an **allowlisted drivername**; only reflect **selected tables
  validated against the reflected list** (inspector API only, no string interpolation).
- The generated `config.json` contains the password by design (it's a connection
  string), it's already **gitignored**; the SPA warns and offers the
  password-omit toggle.
- `/_config` routes are exempt from SigV4 (separate admin surface) and only exist
  when the flag is on.

---

## 7. File & test plan

**New files**
- `configbuilder/index.html`: the SPA.
- `configbuilder/router.py`: the `/_config` API + HTML route.
- `db/reflect.py` (or extend `db/executor.py`), `SchemaReflector`.

**Edited**
- `config.py`: `ENABLE_CONFIG_BUILDER`.
- `main.py`: conditionally include the router; extend SigV4 exempt prefixes.
- README / CONFIGURATION.md, a short "Config Builder" section.

**Tests** (`tests/test_config_builder.py`)
- URL builder encodes special chars (`URL.create`), allowlist rejects unknown dialects.
- `/api/connect` against a temp **SQLite** DB → lists tables.
- `/api/inspect` → detects key + maps column types.
- `/api/save` + `/api/current` + `/api/bootstrap` → valid persisted+loaded config flow.
- `GET /_config/` → 200 HTML smoke. (Optional later: Playwright click-through.)

SQLite is used purely to exercise the endpoints in CI without a live PG/MSSQL.

---

## 8. Milestones

| # | Scope | Outcome |
|---|---|---|
| **M1** | `SchemaReflector` + `/_config/api/connect` & `/inspect` + tests | Backend can list/reflect any DB |
| **M2** | SPA (connect → pick → preview → download) | End-to-end usable tool |
| **M3** | Key-column dropdowns, `num_splits` suggestions, password toggle, localStorage | Polished UX |
| **M4** | Security hardening + docs + smoke test | Shippable |

**Effort:** ~M (a couple of focused sessions). **Risk:** low, isolated,
flag-gated, reuses existing reflection; the only new surface area is a static page
and two thin endpoints.

---

## 9. Open questions

1. **Postgres driver** (`asyncpg`) isn't bundled, require `pip install asyncpg`
   for the builder, or add it to deps?
2. **num_splits suggestion**: ship the fast row-estimate heuristic in M1, or defer
   to M3?
3. **Password in download**: default to **included** (simplest) with a prominent
   warning + omit toggle, confirm that's the desired default.
4. Serve the SPA **inline** from a Python route (no static-files dependency) or via
   `fastapi.staticfiles`? Inline keeps it single-file and dependency-free.
