#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 9 ]; then
  echo "usage: install.sh <manager-url> <agent-api-app-id> <identity-client-id> <storage-account> <git-ref> <agent-id> <agent-location> <package-blob-name|-> <package-sha256|->" >&2
  exit 2
fi

manager_url="$1"
agent_api_app_id="$2"
identity_client_id="$3"
storage_account="$4"
git_ref="$5"
agent_id="$6"
agent_location="$7"
package_blob_name="$8"
package_sha256="$9"

fail() {
  echo "ERROR: $*" >&2
  exit 1
}

if [ "$(id -u)" -ne 0 ]; then
  fail "Run Command must execute this installer as root."
fi
if [[ ! "$manager_url" =~ ^https://[A-Za-z0-9.-]+(:[0-9]+)?(/[A-Za-z0-9._~/-]*)?$ ]]; then
  fail "Manager URL must be an HTTPS URL with a DNS hostname and no query, fragment, or credentials."
fi
if [[ ! "$agent_api_app_id" =~ ^[A-Fa-f0-9-]{36}$ ]]; then
  fail "Agent API application ID must be a GUID."
fi
if [[ ! "$identity_client_id" =~ ^[A-Fa-f0-9-]{36}$ ]]; then
  fail "Managed identity client ID must be a GUID."
fi
if [[ ! "$storage_account" =~ ^[a-z0-9]{3,24}$ ]]; then
  fail "Storage account name must contain 3-24 lowercase letters or digits."
fi
if [[ ! "$agent_id" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]]; then
  fail "Agent ID contains unsupported characters."
fi
if [[ ! "$agent_location" =~ ^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$ ]]; then
  fail "Agent location contains unsupported characters."
fi
if [ "$package_blob_name" = "-" ]; then
  package_blob_name=""
fi
if [ "$package_sha256" = "-" ]; then
  package_sha256=""
fi
if [ -n "$package_blob_name" ]; then
  if [[ ! "$package_blob_name" =~ ^packages/fsp-hybrid-site-[0-9a-f]{64}\.tar\.gz$ ]] ||
     [[ ! "$package_sha256" =~ ^[0-9a-f]{64}$ ]] ||
     [ "${package_blob_name##*-}" != "${package_sha256}.tar.gz" ]; then
    fail "Source package blob name and SHA-256 must identify the same content-addressed archive."
  fi
elif [ -n "$package_sha256" ]; then
  fail "A package SHA-256 was supplied without a package blob name."
elif [[ ! "$git_ref" =~ ^[A-Za-z0-9][A-Za-z0-9._/-]{0,127}$ ]] ||
     [[ "$git_ref" == *..* ]] || [[ "$git_ref" == */*/* ]]; then
  fail "Git ref is invalid; use a published branch or tag, or supply a packaged source archive."
fi

for prerequisite in \
  awk chmod chown curl date getent grep install journalctl mktemp mv python3 \
  rm runuser seq sha256sum sleep sort systemctl tar useradd; do
  command -v "$prerequisite" >/dev/null 2>&1 || fail "Required command is missing: $prerequisite"
done
if [ -z "$package_blob_name" ]; then
  command -v git >/dev/null 2>&1 || fail "git is required when no source package is supplied."
fi
python3 -m pip --version >/dev/null 2>&1 || fail "python3-pip is required."
python3 -c 'import sys; raise SystemExit(0 if sys.version_info >= (3, 11) else 1)' ||
  fail "Python 3.11 or newer is required."
systemctl --version >/dev/null 2>&1 || fail "systemd is required."

blob_host="${storage_account}.blob.core.windows.net"
blob_addresses="$(getent ahostsv4 "$blob_host" | awk '{print $1}' | sort -u)"
[ -n "$blob_addresses" ] ||
  fail "Blob DNS did not resolve. Verify the private DNS zone link and VNet peering."
while IFS= read -r address; do
  IFS=. read -r octet1 octet2 _ <<< "$address"
  if [ "$octet1" = "10" ] || {
    [ "$octet1" = "192" ] && [ "$octet2" = "168" ]
  }; then
    continue
  fi
  if [ "$octet1" = "172" ] && [ "$octet2" -ge 16 ] && [ "$octet2" -le 31 ]; then
    continue
  fi
  fail "Blob hostname resolved to a non-private IPv4 address ($address)."
done <<< "$blob_addresses"

blob_status="$(curl --silent --show-error --connect-timeout 5 --max-time 15 \
  --output /dev/null --write-out '%{http_code}' "https://${blob_host}/")" ||
  fail "Blob HTTPS connectivity failed. Verify the private endpoint route and TLS."
[[ "$blob_status" =~ ^[1-5][0-9][0-9]$ ]] ||
  fail "Blob HTTPS endpoint returned no HTTP status."

manager_base="${manager_url%/}"
manager_ready_url="${manager_base}/readyz"
manager_ready=false
for attempt in $(seq 1 12); do
  if curl --silent --show-error --fail --connect-timeout 5 --max-time 10 \
    "$manager_ready_url" --output /dev/null; then
    manager_ready=true
    break
  fi
  sleep 5
done
[ "$manager_ready" = true ] ||
  fail "Manager /readyz did not return HTTP 200. Verify private DNS, routing, TLS, and Manager readiness."

curl --silent --show-error --fail --noproxy '*' --connect-timeout 5 --max-time 10 \
  --header 'Metadata: true' \
  "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https%3A%2F%2Fstorage.azure.com%2F&client_id=${identity_client_id}" \
  --output /dev/null ||
  fail "The configured user-assigned managed identity is unavailable from this VM."

install -d -m 0755 /opt/fsp
work_dir="$(mktemp -d /opt/fsp/.hybrid-install.XXXXXX)"
cleanup() {
  rm -rf -- "$work_dir"
}
trap cleanup EXIT
repo_dir="${work_dir}/repo"

if [ -n "$package_blob_name" ]; then
  python3 -m venv /opt/fsp/bootstrap-venv
  /opt/fsp/bootstrap-venv/bin/python -m pip install --disable-pip-version-check \
    'azure-identity>=1.15' 'azure-storage-blob>=12.19'
  archive_path="${work_dir}/source.tar.gz"
  AZURE_CLIENT_ID="$identity_client_id" \
    SOURCE_STORAGE_ACCOUNT="$storage_account" \
    SOURCE_BLOB_NAME="$package_blob_name" \
    SOURCE_ARCHIVE_PATH="$archive_path" \
    /opt/fsp/bootstrap-venv/bin/python - <<'PY'
import os
from azure.identity import ManagedIdentityCredential
from azure.storage.blob import BlobClient

credential = ManagedIdentityCredential(client_id=os.environ["AZURE_CLIENT_ID"])
try:
    client = BlobClient(
        account_url=f"https://{os.environ['SOURCE_STORAGE_ACCOUNT']}.blob.core.windows.net",
        container_name="fsp-hybrid-staging",
        blob_name=os.environ["SOURCE_BLOB_NAME"],
        credential=credential,
    )
    with open(os.environ["SOURCE_ARCHIVE_PATH"], "wb") as archive:
        client.download_blob().readinto(archive)
finally:
    credential.close()
PY
  actual_sha256="$(sha256sum "$archive_path" | awk '{print $1}')"
  [ "$actual_sha256" = "$package_sha256" ] ||
    fail "Downloaded source package SHA-256 did not match the expected digest."
  python3 - "$archive_path" "$repo_dir" <<'PY'
import pathlib
import sys
import tarfile

archive_path = pathlib.Path(sys.argv[1])
destination = pathlib.Path(sys.argv[2]).resolve()
destination.mkdir(parents=True, exist_ok=True)
with tarfile.open(archive_path, "r:gz") as archive:
    members = archive.getmembers()
    for member in members:
        path = pathlib.PurePosixPath(member.name)
        if path.is_absolute() or any(part in ("", ".", "..") for part in path.parts):
            raise SystemExit(f"Unsafe path in source archive: {member.name}")
        if member.issym() or member.islnk() or member.isdev() or member.isfifo():
            raise SystemExit(f"Unsupported link or special file in source archive: {member.name}")
        target = (destination / pathlib.Path(*path.parts)).resolve()
        if target != destination and destination not in target.parents:
            raise SystemExit(f"Source archive path escapes extraction directory: {member.name}")
    archive.extractall(destination)
required = (
    destination / "main.py",
    destination / "pyproject.toml",
    destination / "enterprise" / "pyproject.toml",
    destination / "src" / "fabric_shortcut_proxy",
)
missing = [str(path.relative_to(destination)) for path in required if not path.exists()]
if missing:
    raise SystemExit(f"Source package is incomplete; missing: {', '.join(missing)}")
PY
else
  git clone --depth 1 --single-branch --branch "$git_ref" \
    https://github.com/Andreas-bersgtedt/Fabric-Shortcut-Proxy.git "$repo_dir"
  [ -f "$repo_dir/enterprise/pyproject.toml" ] ||
    fail "Published Git ref does not contain the enterprise package."
fi

if ! getent passwd fsp >/dev/null 2>&1; then
  useradd --system --user-group --home-dir /var/lib/fsp \
    --shell /usr/sbin/nologin fsp
fi
install -d -o fsp -g fsp -m 0750 /var/lib/fsp

if [ -d /opt/fsp/repo ]; then
  systemctl stop fsp-hybrid-agent.service || true
fi
mv "$repo_dir" /opt/fsp/repo.new
if [ -d /opt/fsp/repo ]; then
  old_repo="${work_dir}/repo.previous"
  mv /opt/fsp/repo "$old_repo"
fi
mv /opt/fsp/repo.new /opt/fsp/repo

python3 -m venv /opt/fsp/venv
/opt/fsp/venv/bin/python -m pip install --disable-pip-version-check --upgrade pip
/opt/fsp/venv/bin/python -m pip install --disable-pip-version-check \
  -e '/opt/fsp/repo[agent-entra,azureblob]'
/opt/fsp/venv/bin/python -m pip install --disable-pip-version-check \
  -e /opt/fsp/repo/enterprise

chown -R fsp:fsp /opt/fsp /var/lib/fsp

db_url="sqlite+aiosqlite:////var/lib/fsp/hybrid-site.db"
runuser -u fsp -- env DB_URL="$db_url" \
  /opt/fsp/venv/bin/python -c '
import asyncio, os
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
url = os.environ["DB_URL"]
if not url.startswith("sqlite+aiosqlite:///"):
    raise SystemExit("DB_URL must use the async aiosqlite driver.")
async def check():
    engine = create_async_engine(url)
    try:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))
    finally:
        await engine.dispose()
asyncio.run(check())
'

runuser -u fsp -- env AZURE_CLIENT_ID="$identity_client_id" \
  STORAGE_ACCOUNT="$storage_account" \
  /opt/fsp/venv/bin/python -c '
import os
from azure.identity import ManagedIdentityCredential
from azure.storage.blob import BlobServiceClient
credential = ManagedIdentityCredential(client_id=os.environ["AZURE_CLIENT_ID"])
try:
    service = BlobServiceClient(
        account_url=f"https://{os.environ['STORAGE_ACCOUNT']}.blob.core.windows.net",
        credential=credential,
    )
    service.get_container_client("fsp-hybrid-staging").get_container_properties()
finally:
    credential.close()
'

umask 077
cat > /etc/fsp-hybrid.env.new <<EOF
MANAGER_URL=${manager_base}
AGENT_ID=${agent_id}
AGENT_POOL_ID=hybrid-site
AGENT_LOCATION=${agent_location}
AGENT_AUTH_MODE=entra
AGENT_ENTRA_SCOPE=api://${agent_api_app_id}/.default
AZURE_CLIENT_ID=${identity_client_id}
ARTIFACT_STORE_BACKEND=azure
ARTIFACT_STORE_ACCOUNT_URL=https://${storage_account}.blob.core.windows.net
ARTIFACT_STORE_CONTAINER=fsp-hybrid-staging
ARTIFACT_STORE_AUTH_MODE=managed_identity
ARTIFACT_STORE_CLIENT_ID=${identity_client_id}
DB_URL=${db_url}
DB_SOURCE_TABLE=sales
TABLE_NAME=sales
HOST=127.0.0.1
PORT=9000
ENABLE_MONITOR=1
ENABLE_ADMIN_UI=0
ENABLE_CONFIG_BUILDER=0
EOF
chmod 0600 /etc/fsp-hybrid.env.new
mv /etc/fsp-hybrid.env.new /etc/fsp-hybrid.env

cat > /etc/systemd/system/fsp-hybrid-agent.service.new <<'EOF'
[Unit]
Description=Fabric Shortcut Proxy hybrid materializer
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=fsp
Group=fsp
WorkingDirectory=/opt/fsp/repo
EnvironmentFile=/etc/fsp-hybrid.env
ExecStart=/opt/fsp/venv/bin/python -m main
Restart=always
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true
ProtectHome=true
ProtectSystem=strict
ReadWritePaths=/opt/fsp /var/lib/fsp

[Install]
WantedBy=multi-user.target
EOF
chmod 0644 /etc/systemd/system/fsp-hybrid-agent.service.new
mv /etc/systemd/system/fsp-hybrid-agent.service.new \
  /etc/systemd/system/fsp-hybrid-agent.service

systemctl daemon-reload
systemctl enable fsp-hybrid-agent.service
started_at="$(date --iso-8601=seconds)"
systemctl restart fsp-hybrid-agent.service

registration_seen=false
for attempt in $(seq 1 60); do
  if systemctl is-active --quiet fsp-hybrid-agent.service &&
     journalctl --unit fsp-hybrid-agent.service --since "$started_at" \
       --no-pager --output cat 2>/dev/null |
       grep -F "agent_registered_ok" |
       grep -F "$agent_id" >/dev/null; then
    registration_seen=true
    break
  fi
  sleep 5
done

if [ "$registration_seen" != true ]; then
  systemctl --no-pager --full status fsp-hybrid-agent.service >&2 || true
  journalctl --unit fsp-hybrid-agent.service --since "$started_at" \
    --no-pager --output cat --lines 80 >&2 || true
  fail "The service did not register agent ${agent_id} with the ready Manager."
fi

printf 'FSP_HYBRID_INSTALL_READY agent_id=%s db_url=sqlite+aiosqlite storage=private-blob manager=ready\n' \
  "$agent_id"
