"""
Manager-owned, encrypted credential store.

Persists source-database connection URLs (which carry passwords) **encrypted at
rest** so they survive a Manager/Agent restart without ever being written into
the gitignored JSON config in plaintext. The Manager hydrates these into the
process environment (``DB_URL`` / ``DB_URL_<ID>``) at startup, so the Agents it
spawns connect with working credentials on every restart.

Encryption backends (auto-selected, no plaintext ever):

* **DPAPI** (Windows) — via ``crypt32`` through ``ctypes``. Zero extra deps;
  the ciphertext is bound to the current Windows user account + machine.
* **Fernet** (any OS) — used when ``cryptography`` is installed, or when an
  explicit ``FSP_CRED_KEY`` (a urlsafe-base64 Fernet key) is provided. A random
  per-store key is generated in ``<store_dir>/.credkey`` (0600) when needed.

If neither backend is available the store is *unavailable*: reads return nothing
and writes raise — it never falls back to storing secrets in the clear.

The on-disk file (default ``<repo>/secrets/credentials.json``, gitignored) holds
only base64 ciphertext plus non-secret metadata (connection id, timestamp).
"""
from __future__ import annotations

import base64
import json
import os
import re
import sys
import tempfile
from datetime import datetime, timezone
from typing import Protocol


# ---------------------------------------------------------------------------
# Env-var naming (kept in sync with connection_config._env_url_for, but WITHOUT
# importing config — hydration must run before config is imported).
# ---------------------------------------------------------------------------

def env_var_for(connection_id: str | None) -> str:
    """The environment variable an Agent reads for this connection's URL.

    ``default`` (or empty) -> ``DB_URL``; a named id -> ``DB_URL_<ID>`` with the
    id upper-cased and non-alphanumerics collapsed to ``_``.
    """
    cid = (connection_id or "").strip()
    if not cid or cid.lower() == "default":
        return "DB_URL"
    return "DB_URL_" + re.sub(r"[^A-Za-z0-9]+", "_", cid).upper()


def looks_masked(url: str) -> bool:
    """True if the URL's password was redacted to ``***`` (i.e. not a real secret).

    ``redact_db_url`` renders ``user:password@host`` as ``user:***@host``; such a
    value must never be stored or hydrated as a credential.
    """
    return bool(url) and (":***@" in url or ":%2A%2A%2A@" in url)


def _repo_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _default_store_path() -> str:
    override = (os.environ.get("CREDENTIAL_STORE_PATH")
                or os.environ.get("FSP_CREDENTIAL_STORE") or "").strip()
    if override:
        return override
    return os.path.join(_repo_root(), "secrets", "credentials.json")


# ---------------------------------------------------------------------------
# Cipher backends
# ---------------------------------------------------------------------------

class _Cipher(Protocol):
    name: str
    def encrypt(self, plaintext: bytes) -> bytes: ...
    def decrypt(self, ciphertext: bytes) -> bytes: ...


class _DpapiCipher:
    """Windows DPAPI (crypt32) — user+machine bound, no key management."""

    name = "dpapi"

    @staticmethod
    def available() -> bool:
        return os.name == "nt"

    def _crypt(self, data: bytes, protect: bool) -> bytes:
        import ctypes
        from ctypes import wintypes

        class _BLOB(ctypes.Structure):
            _fields_ = [("cbData", wintypes.DWORD),
                        ("pbData", ctypes.POINTER(ctypes.c_char))]

        buf_in = ctypes.create_string_buffer(data, len(data))
        blob_in = _BLOB(len(data), ctypes.cast(buf_in, ctypes.POINTER(ctypes.c_char)))
        blob_out = _BLOB()
        fn = (ctypes.windll.crypt32.CryptProtectData if protect
              else ctypes.windll.crypt32.CryptUnprotectData)
        # (data_in, name, entropy, reserved, prompt, flags, data_out)
        if not fn(ctypes.byref(blob_in), None, None, None, None, 0, ctypes.byref(blob_out)):
            raise ctypes.WinError()  # type: ignore[attr-defined]
        try:
            return ctypes.string_at(blob_out.pbData, blob_out.cbData)
        finally:
            ctypes.windll.kernel32.LocalFree(blob_out.pbData)

    def encrypt(self, plaintext: bytes) -> bytes:
        return self._crypt(plaintext, protect=True)

    def decrypt(self, ciphertext: bytes) -> bytes:
        return self._crypt(ciphertext, protect=False)


class _FernetCipher:
    """cryptography.Fernet — portable AES-based symmetric encryption."""

    name = "fernet"

    def __init__(self, key: bytes) -> None:
        from cryptography.fernet import Fernet  # imported lazily (optional dep)
        self._f = Fernet(key)

    def encrypt(self, plaintext: bytes) -> bytes:
        return self._f.encrypt(plaintext)

    def decrypt(self, ciphertext: bytes) -> bytes:
        return self._f.decrypt(ciphertext)


def _fernet_from_env() -> _FernetCipher | None:
    key = (os.environ.get("FSP_CRED_KEY") or "").strip()
    if not key:
        return None
    try:
        return _FernetCipher(key.encode("ascii"))
    except Exception as exc:  # noqa: BLE001
        print(f"[credential_store] FSP_CRED_KEY is invalid: {exc}", file=sys.stderr)
        return None


def _fernet_from_keyfile(store_path: str) -> _FernetCipher | None:
    try:
        from cryptography.fernet import Fernet
    except Exception:
        return None
    keyfile = os.path.join(os.path.dirname(store_path) or ".", ".credkey")
    try:
        if os.path.exists(keyfile):
            key = open(keyfile, "rb").read().strip()
        else:
            key = Fernet.generate_key()
            os.makedirs(os.path.dirname(keyfile) or ".", exist_ok=True)
            with open(keyfile, "wb") as fh:
                fh.write(key)
            _restrict_perms(keyfile)
        return _FernetCipher(key)
    except Exception as exc:  # noqa: BLE001
        print(f"[credential_store] could not initialize key file: {exc}", file=sys.stderr)
        return None


def _restrict_perms(path: str) -> None:
    if os.name != "nt":
        try:
            os.chmod(path, 0o600)
        except OSError:
            pass


def _select_cipher(store_path: str) -> _Cipher | None:
    # An explicit key wins (portable across machines / for POSIX deployments).
    cipher = _fernet_from_env()
    if cipher is not None:
        return cipher
    if _DpapiCipher.available():
        return _DpapiCipher()
    # POSIX without an explicit key: use a local key file if cryptography exists.
    return _fernet_from_keyfile(store_path)


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------

_VERSION = 1


class CredentialStore:
    """Encrypted, file-backed store of per-connection database URLs."""

    def __init__(self, path: str | None = None, *, cipher: _Cipher | None = None) -> None:
        self.path = path or _default_store_path()
        # ``cipher`` injection is for tests only; production auto-selects.
        self._cipher = cipher if cipher is not None else _select_cipher(self.path)
        # Optional read-through source (issue #16): fn(kind, key) -> str | None,
        # consulted only on a local miss; the value is written through to the
        # encrypted cache so subsequent reads are local (cache-first).
        self.read_through = None
        # Optional write-back sink (issue #16, Phase 4): fn(kind, key, value) called
        # after a successful local write to also persist to Key Vault (fail-soft).
        self.write_through = None
        # Optional delete-back sink (issue #16, Phase 4): fn(kind, key) called after a
        # successful local delete to also remove the secret from Key Vault (fail-soft).
        self.delete_through = None

    # -- capability -------------------------------------------------------
    @property
    def available(self) -> bool:
        return self._cipher is not None

    @property
    def backend_name(self) -> str:
        return self._cipher.name if self._cipher is not None else "unavailable"

    def encrypt_blob(self, payload: bytes) -> bytes:
        """Encrypt an opaque application blob with the store's existing cipher."""
        if self._cipher is None:
            raise RuntimeError("credential store encryption is unavailable")
        return self._cipher.encrypt(payload)

    def decrypt_blob(self, payload: bytes) -> bytes:
        """Decrypt an opaque application blob with the store's existing cipher."""
        if self._cipher is None:
            raise RuntimeError("credential store encryption is unavailable")
        return self._cipher.decrypt(payload)

    # -- file io ----------------------------------------------------------
    def _load(self) -> dict:
        try:
            with open(self.path, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            if isinstance(data, dict) and isinstance(data.get("connections"), dict):
                return data
        except FileNotFoundError:
            pass
        except (OSError, ValueError) as exc:
            print(f"[credential_store] could not read {self.path}: {exc}", file=sys.stderr)
        return {"version": _VERSION, "backend": self.backend_name, "connections": {}}

    def _save(self, data: dict) -> None:
        data["version"] = _VERSION
        data["backend"] = self.backend_name
        d = os.path.dirname(self.path) or "."
        os.makedirs(d, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=".cred-", dir=d)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(data, fh, indent=2)
            _restrict_perms(tmp)
            os.replace(tmp, self.path)
            _restrict_perms(self.path)
        finally:
            if os.path.exists(tmp):
                try:
                    os.remove(tmp)
                except OSError:
                    pass

    def _backend_matches(self, data: dict) -> bool:
        stored = str(data.get("backend") or self.backend_name)
        return stored == self.backend_name

    def _resolve_via_source(self, kind: str, key: str):
        """On a local miss, pull from the read-through source and write through.

        ``kind`` is ``"url"`` (returns a str) or ``"secret"`` (returns a dict).
        Best-effort: any source error is swallowed so a lookup never raises — an
        unreachable Key Vault must never fail the caller (owner directive).
        """
        fn = getattr(self, "read_through", None)
        if fn is None or not self.available:
            return None
        try:
            raw = fn(kind, key)
        except Exception as exc:  # noqa: BLE001 - read-through is best-effort
            print(f"[credential_store] read-through failed for {kind} {key!r}: {exc}", file=sys.stderr)
            return None
        if raw is None:
            return None
        if kind == "url":
            if isinstance(raw, str) and raw.strip() and not looks_masked(raw):
                self.set_url(key, raw)
                return raw
            return None
        obj = raw
        if isinstance(raw, str):
            try:
                obj = json.loads(raw)
            except ValueError:
                return None
        if isinstance(obj, dict):
            self.set_secret(key, obj)
            return obj
        return None

    def _emit_write_through(self, kind: str, key: str, value) -> None:
        """After a local write, also persist to the write-back sink (Key Vault).

        Fail-soft: a write-back failure is logged and swallowed so the operator's
        save always succeeds locally (owner directive; local is the source of cache).
        """
        fn = getattr(self, "write_through", None)
        if fn is None:
            return
        try:
            fn(kind, key, value)
        except Exception as exc:  # noqa: BLE001 - write-back is best-effort
            print(f"[credential_store] write-back failed for {kind} {key!r}: {exc}", file=sys.stderr)

    def _emit_delete_through(self, kind: str, key: str) -> None:
        """After a local delete, also remove the credential from the write-back sink.

        Fail-soft: a delete-back failure is logged and swallowed so removing a
        credential locally always succeeds (owner directive; local is authoritative).
        """
        fn = getattr(self, "delete_through", None)
        if fn is None:
            return
        try:
            fn(kind, key)
        except Exception as exc:  # noqa: BLE001 - delete-back is best-effort
            print(f"[credential_store] delete write-back failed for {kind} {key!r}: {exc}", file=sys.stderr)

    # -- public api -------------------------------------------------------
    def set_url(self, connection_id: str, db_url: str) -> None:
        """Encrypt and persist a connection's full database URL."""
        if not self.available:
            raise RuntimeError(
                "credential store unavailable (no encryption backend). On non-Windows "
                "hosts install 'cryptography' or set FSP_CRED_KEY.")
        cid = (connection_id or "").strip() or "default"
        if not db_url or not db_url.strip():
            raise ValueError("db_url must be a non-empty connection string")
        if looks_masked(db_url):
            raise ValueError(
                "db_url is masked (contains '***'); supply the real password "
                "(test the connection) before saving the credential")
        token = base64.b64encode(self._cipher.encrypt(db_url.encode("utf-8"))).decode("ascii")
        data = self._load()
        if not self._backend_matches(data) and data.get("connections"):
            # Backend changed since the file was written; existing entries can't
            # be decrypted, so start a fresh set under the current backend.
            print(f"[credential_store] backend changed to {self.backend_name!r}; "
                  "previous entries are no longer readable and will be replaced.",
                  file=sys.stderr)
            data["connections"] = {}
        data["connections"][cid] = {
            "enc": token,
            "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        self._save(data)
        self._emit_write_through("url", cid, db_url)

    def get_url(self, connection_id: str) -> str | None:
        """Decrypt and return a connection's URL, or ``None`` if absent/unreadable.

        On a local miss an optional read-through source (issue #16) is consulted
        and its value written through to the encrypted cache.
        """
        cid = (connection_id or "").strip() or "default"
        if not self.available:
            return None
        data = self._load()
        if not self._backend_matches(data):
            return None
        entry = data.get("connections", {}).get(cid)
        if isinstance(entry, dict) and entry.get("enc"):
            try:
                return self._cipher.decrypt(base64.b64decode(entry["enc"])).decode("utf-8")
            except Exception as exc:  # noqa: BLE001
                print(f"[credential_store] could not decrypt {connection_id!r}: {exc}", file=sys.stderr)
                return None
        return self._resolve_via_source("url", cid)

    def delete(self, connection_id: str) -> bool:
        cid = (connection_id or "").strip() or "default"
        data = self._load()
        if cid in data.get("connections", {}):
            del data["connections"][cid]
            self._save(data)
            self._emit_delete_through("url", cid)
            return True
        return False

    def list_ids(self) -> list[str]:
        return sorted(self._load().get("connections", {}).keys())

    # -- secrets (encrypted JSON blobs, e.g. upstream S3 auth) -------------
    def set_secret(self, secret_id: str, obj: dict) -> None:
        """Encrypt and persist an arbitrary JSON secret (e.g. an S3 auth blob)."""
        if not self.available:
            raise RuntimeError(
                "credential store unavailable (no encryption backend). On non-Windows "
                "hosts install 'cryptography' or set FSP_CRED_KEY.")
        sid = (secret_id or "").strip()
        if not sid:
            raise ValueError("secret_id must be non-empty")
        payload = json.dumps(obj, separators=(",", ":")).encode("utf-8")
        token = base64.b64encode(self._cipher.encrypt(payload)).decode("ascii")
        data = self._load()
        if not self._backend_matches(data) and data.get("secrets"):
            # Backend changed: old ciphertext is unreadable, so start fresh.
            data["secrets"] = {}
        data.setdefault("secrets", {})[sid] = {
            "enc": token,
            "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        self._save(data)
        self._emit_write_through("secret", sid, obj)

    def get_secret(self, secret_id: str) -> dict | None:
        """Decrypt and return a JSON secret, or ``None`` if absent/unreadable.

        On a local miss an optional read-through source (issue #16) is consulted
        and its value written through to the encrypted cache.
        """
        sid = (secret_id or "").strip()
        if not self.available:
            return None
        data = self._load()
        if not self._backend_matches(data):
            return None
        entry = data.get("secrets", {}).get(sid)
        if isinstance(entry, dict) and entry.get("enc"):
            try:
                raw = self._cipher.decrypt(base64.b64decode(entry["enc"])).decode("utf-8")
                return json.loads(raw)
            except Exception as exc:  # noqa: BLE001
                print(f"[credential_store] could not decrypt secret {secret_id!r}: {exc}", file=sys.stderr)
                return None
        return self._resolve_via_source("secret", sid)

    def delete_secret(self, secret_id: str) -> bool:
        sid = (secret_id or "").strip()
        data = self._load()
        if sid in data.get("secrets", {}):
            del data["secrets"][sid]
            self._save(data)
            self._emit_delete_through("secret", sid)
            return True
        return False

    def list_secret_ids(self) -> list[str]:
        return sorted(self._load().get("secrets", {}).keys())

    # -- access keys (encrypted proxy access-key + ACL records) -----------
    def set_access_key(self, key_id: str, obj: dict) -> None:
        """Encrypt and persist a proxy access-key record (secret + ACL scope)."""
        if not self.available:
            raise RuntimeError(
                "credential store unavailable (no encryption backend). On non-Windows "
                "hosts install 'cryptography' or set FSP_CRED_KEY.")
        kid = (key_id or "").strip()
        if not kid:
            raise ValueError("access key id must be non-empty")
        payload = json.dumps(obj, separators=(",", ":")).encode("utf-8")
        token = base64.b64encode(self._cipher.encrypt(payload)).decode("ascii")
        data = self._load()
        if not self._backend_matches(data) and data.get("access_keys"):
            data["access_keys"] = {}
        data.setdefault("access_keys", {})[kid] = {
            "enc": token,
            "updated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        self._save(data)
        self._emit_write_through("access_key", kid, obj)

    def get_access_key(self, key_id: str) -> dict | None:
        """Decrypt and return a proxy access-key record, or ``None`` if absent."""
        if not self.available:
            return None
        data = self._load()
        if not self._backend_matches(data):
            return None
        entry = data.get("access_keys", {}).get((key_id or "").strip())
        if not isinstance(entry, dict) or not entry.get("enc"):
            return None
        try:
            raw = self._cipher.decrypt(base64.b64decode(entry["enc"])).decode("utf-8")
            return json.loads(raw)
        except Exception as exc:  # noqa: BLE001
            print(f"[credential_store] could not decrypt access key {key_id!r}: {exc}", file=sys.stderr)
            return None

    def delete_access_key(self, key_id: str) -> bool:
        kid = (key_id or "").strip()
        data = self._load()
        if kid in data.get("access_keys", {}):
            del data["access_keys"][kid]
            self._save(data)
            self._emit_delete_through("access_key", kid)
            return True
        return False

    def list_access_key_ids(self) -> list[str]:
        return sorted(self._load().get("access_keys", {}).keys())

    def export_records(self) -> dict:
        """Return every locally stored secret in plaintext for an encrypted export."""
        if not self.available:
            raise RuntimeError("credential store encryption is unavailable")
        return {
            "connections": {
                key: value for key in self.list_ids()
                if (value := self.get_url(key)) is not None
            },
            "secrets": {
                key: value for key in self.list_secret_ids()
                if (value := self.get_secret(key)) is not None
            },
            "access_keys": {
                key: value for key in self.list_access_key_ids()
                if (value := self.get_access_key(key)) is not None
            },
        }

    @staticmethod
    def validate_records(records: dict) -> None:
        if not isinstance(records, dict):
            raise ValueError("credential records must be an object")
        for section in ("connections", "secrets", "access_keys"):
            values = records.get(section)
            if not isinstance(values, dict):
                raise ValueError(f"credential records are missing {section}")
            for key, value in values.items():
                if not isinstance(key, str) or not key.strip():
                    raise ValueError(f"{section} contains an invalid id")
                if section == "connections" and (not isinstance(value, str) or not value.strip()):
                    raise ValueError("connection records must contain non-empty URLs")
                if section != "connections" and not isinstance(value, dict):
                    raise ValueError(f"{section} records must contain objects")

    def replace_records(self, records: dict) -> None:
        """Atomically replace all local credential records using this store's cipher."""
        if self._cipher is None:
            raise RuntimeError("credential store encryption is unavailable")
        self.validate_records(records)
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")

        def encrypted(value) -> dict:
            raw = value if isinstance(value, str) else json.dumps(value, separators=(",", ":"))
            token = base64.b64encode(self._cipher.encrypt(raw.encode("utf-8"))).decode("ascii")
            return {"enc": token, "updated_at": now}

        data = {
            "version": _VERSION,
            "backend": self.backend_name,
            "connections": {key: encrypted(value) for key, value in records["connections"].items()},
            "secrets": {key: encrypted(value) for key, value in records["secrets"].items()},
            "access_keys": {key: encrypted(value) for key, value in records["access_keys"].items()},
        }
        self._save(data)

    def status(self) -> dict:
        """Non-secret summary for the UI (never returns plaintext URLs)."""
        data = self._load()
        readable = self._backend_matches(data)
        conns = []
        for cid, entry in sorted(data.get("connections", {}).items()):
            if isinstance(entry, dict):
                conns.append({
                    "id": cid,
                    "env_var": env_var_for(cid),
                    "updated_at": entry.get("updated_at"),
                })
        return {
            "available": self.available,
            "backend": self.backend_name,
            "path": self.path,
            "readable": readable,
            "connections": conns,
        }

    def env_overrides(self) -> dict[str, str]:
        """``{DB_URL / DB_URL_<ID>: url}`` for every stored connection."""
        out: dict[str, str] = {}
        if not self.available:
            return out
        data = self._load()
        if not self._backend_matches(data):
            return out
        for cid in data.get("connections", {}):
            url = self.get_url(cid)
            # Never hydrate a masked value left by an older buggy save — it would
            # make the Agent try to log in with the literal password '***'.
            if url and not looks_masked(url):
                out[env_var_for(cid)] = url
        return out


# ---------------------------------------------------------------------------
# Startup hydration
# ---------------------------------------------------------------------------

def _store_enabled() -> bool:
    val = os.environ.get("ENABLE_CREDENTIAL_STORE")
    if val is None:
        return True
    return val.strip().lower() in ("1", "true", "yes", "on")


def hydrate_environment(store: CredentialStore | None = None) -> list[str]:
    """Fill missing ``DB_URL`` / ``DB_URL_<ID>`` env vars from the store.

    Best-effort: an env var already set (e.g. via ``-DbUrl``) always wins, and
    any failure is swallowed so a broken/absent store never blocks startup.
    Returns the list of env-var names that were hydrated.
    """
    if not _store_enabled():
        return []
    hydrated: list[str] = []
    try:
        st = store if store is not None else CredentialStore()
        if not st.available:
            return []
        for name, url in st.env_overrides().items():
            if name not in os.environ and url:
                os.environ[name] = url
                hydrated.append(name)
    except Exception as exc:  # noqa: BLE001
        print(f"[credential_store] hydration skipped: {exc}", file=sys.stderr)
    return hydrated
