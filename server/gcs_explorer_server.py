#!/usr/bin/env python3
"""
GCS Parquet Explorer — Server version for remote access.
Browsing and querying SAP CDS Parquet data in GCS, Azure, and AWS.
Uses DuckDB for SQL, PyArrow for parquet, email-based login.

Prerequisites:
    pip3 install google-cloud-storage duckdb pyarrow psutil

Usage:
    python3 gcs_explorer_server.py
    Then open http://<hostname>:8765
"""

import base64
import hashlib
import http.server
import http.cookies
import io
import json
import multiprocessing
import os
import psutil
import queue
import secrets
import ssl
import sys
import time
import threading
import urllib.parse
import urllib.request
import uuid
from collections import OrderedDict
from datetime import datetime

# Ensure DuckDB/Azure can find SSL CA certificates
for _ca in ["/etc/ssl/ca-bundle.pem", "/etc/ssl/certs/ca-certificates.crt", "/etc/pki/tls/certs/ca-bundle.crt"]:
    if os.path.exists(_ca):
        os.environ.setdefault("CURL_CA_BUNDLE", _ca)
        os.environ.setdefault("SSL_CERT_FILE", _ca)
        break

try:
    import duckdb
except ImportError:
    print("ERROR: pip3 install duckdb"); sys.exit(1)
try:
    import pyarrow.parquet as pq
    import pyarrow
except ImportError:
    print("ERROR: pip3 install pyarrow"); sys.exit(1)
try:
    from google.cloud import storage as gcs_storage
except ImportError:
    print("ERROR: pip3 install google-cloud-storage"); sys.exit(1)

PORT = 443
BIND_ADDR = "0.0.0.0"  # Listen on all interfaces for remote access
FQDN = "sapidesecc8.fivetran-internal-sales.com"
BASE_PATH = "/datalake_reader"  # URL prefix — all routes live under this
SSL_CERT = "/usr/sap/gcs_explorer_cert.pem"
SSL_KEY = "/usr/sap/gcs_explorer_key.pem"
BUCKET_NAME = "sap_cds_dbt"
BASE_PREFIX = "sap_cds_views/"
STATE_FILE = os.path.expanduser("~/.gcs_explorer_state.json")

# Authentication
LOGIN_PASSWORD = os.environ.get("GCS_EXPLORER_PASSWORD", "changeme")
# Sessions: token -> {"email": "user@example.com", "created": datetime}
active_sessions = {}

def _hash_password(pw):
    return hashlib.sha256(pw.encode()).hexdigest()

PASSWORD_HASH = _hash_password(LOGIN_PASSWORD)

def create_session(email):
    token = secrets.token_hex(32)
    active_sessions[token] = {"email": email, "created": datetime.now()}
    print(f"  Login: {email}")
    return token

def get_session(cookie_header):
    if not cookie_header:
        return None
    cookies = http.cookies.SimpleCookie()
    try:
        cookies.load(cookie_header)
    except Exception:
        return None
    morsel = cookies.get("session")
    if not morsel:
        return None
    return active_sessions.get(morsel.value)

LOGIN_PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>GCS Parquet Explorer — Login</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; background: #f8f9fa; color: #1f2937; display: flex; justify-content: center; align-items: center; min-height: 100vh; }
.login-box { background: #ffffff; border: 1px solid #e5e7eb; border-radius: 12px; padding: 40px; width: 420px; box-shadow: 0 4px 24px rgba(0,0,0,0.08); }
.login-box h1 { font-size: 24px; color: #0073FF; margin-bottom: 8px; }
.login-box .sub { font-size: 14px; color: #6b7280; margin-bottom: 28px; }
.login-box label { display: block; font-size: 14px; color: #374151; margin-bottom: 6px; margin-top: 16px; }
.login-box input { width: 100%; background: #f9fafb; border: 1px solid #d1d5db; color: #1f2937; padding: 10px 12px; border-radius: 6px; font-size: 16px; font-family: inherit; }
.login-box input:focus { outline: none; border-color: #0073FF; box-shadow: 0 0 0 2px rgba(0,115,255,0.1); }
.login-box button { width: 100%; margin-top: 24px; padding: 12px; background: #0073FF; color: #fff; border: none; border-radius: 6px; font-size: 16px; font-weight: 600; cursor: pointer; }
.login-box button:hover { background: #005ecb; }
.error { color: #ef4444; font-size: 14px; margin-top: 12px; display: none; }
.info { font-size: 12px; color: #9ca3af; margin-top: 20px; text-align: center; }
</style>
</head>
<body>
<div class="login-box">
  <h1>GCS Parquet Explorer</h1>
  <div class="sub">Fivetran SAP Specialist Team</div>
  <form id="loginForm" onsubmit="return doLogin(event)">
    <label for="email">Email</label>
    <input type="email" id="email" name="email" placeholder="you@fivetran.com" required autofocus>
    <button type="submit">Sign In</button>
    <div class="error" id="errMsg"></div>
  </form>
  <div class="info">Your email will be used to authenticate with Google Cloud, Azure, and AWS.</div>
</div>
<script>
async function doLogin(e) {
  e.preventDefault();
  const email = document.getElementById('email').value;
  const errEl = document.getElementById('errMsg');
  errEl.style.display = 'none';
  try {
    const resp = await fetch('/datalake_reader/api/login', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({email})
    });
    const r = await resp.json();
    if (r.status === 'ok') {
      window.location.href = '/datalake_reader/';
    } else {
      errEl.textContent = r.message;
      errEl.style.display = 'block';
    }
  } catch(err) {
    errEl.textContent = 'Connection failed';
    errEl.style.display = 'block';
  }
  return false;
}
</script>
</body>
</html>"""

# Polaris / Iceberg catalog presets
CATALOG_PRESETS = {
    "gcs": {
        "label": "Google Cloud (GCS)",
        "default_alias": "gcs",
        "endpoint": "https://1k56c2c4xlti6-acc.us-east4.gcp.polaris.fivetran.com/api/catalog",
        "catalog": "obeisance_plaintive",
        "client_id": os.environ.get("POLARIS_GCS_CLIENT_ID", ""),
        "client_secret": os.environ.get("POLARIS_GCS_CLIENT_SECRET", ""),
    },
    "azure": {
        "label": "Azure Data Lake",
        "default_alias": "ts_adls_destination_demo",
        "endpoint": "https://1k56c2c4xlti6-acc.eastus2.azure.polaris.fivetran.com/api/catalog",
        "catalog": "log_pseudo",
        "client_id": os.environ.get("POLARIS_AZURE_CLIENT_ID", ""),
        "client_secret": os.environ.get("POLARIS_AZURE_CLIENT_SECRET", ""),
    },
    "aws": {
        "label": "AWS S3 Data Lake",
        "default_alias": "aws",
        "endpoint": "https://pack-dictate.us-west-2.aws.polaris.fivetran.com/api/catalog",
        "catalog": "surfacing_caramel",
        "client_id": os.environ.get("POLARIS_AWS_CLIENT_ID", ""),
        "client_secret": os.environ.get("POLARIS_AWS_CLIENT_SECRET", ""),
    },
}

# Global state
gcs_client = None
_server_start_time = time.time()


# ── DuckDB Subprocess Worker ──────────────────────────────────────────────────
# DuckDB's C extension holds the Python GIL during network I/O (ATTACH ICEBERG,
# iceberg_scan over S3). This freezes ALL Python threads — including the HTTP
# server. Moving DuckDB into a child process isolates its GIL from the server.
# The main process communicates via multiprocessing.Queue (command/response).

def _duckdb_worker(cmd_q, resp_q):
    """Subprocess entry point. Owns the DuckDB connection — no other process touches it."""
    import signal
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    import duckdb as _ddb
    import pyarrow as _pa
    import pyarrow.ipc as _ipc

    db = None
    arrow_tables = {}  # name -> pyarrow.Table (prevent GC so DuckDB references stay valid)

    while True:
        try:
            cmd = cmd_q.get(timeout=5)
        except Exception:
            continue
        cmd_id = cmd.get("id", "")
        cmd_name = cmd.get("cmd", "")
        args = cmd.get("args", {})
        try:
            if cmd_name == "init":
                if not os.environ.get("HOME"):
                    os.environ["HOME"] = "/root"
                db = _ddb.connect(":memory:")
                db.execute(f"SET home_directory='{os.environ['HOME']}';")
                db.execute("INSTALL httpfs; LOAD httpfs;")
                db.execute("INSTALL iceberg; LOAD iceberg;")
                db.execute("INSTALL azure; LOAD azure;")
                db.execute("SET azure_transport_option_type='curl';")
                resp_q.put({"id": cmd_id, "status": "ok", "result": {}})

            elif cmd_name == "exec_sql":
                q = args["query"]
                r = db.execute(q)
                cols = [desc[0] for desc in r.description] if r.description else []
                data = r.fetchall()
                # Convert to strings for safe serialization
                safe = []
                for row in data:
                    safe.append([str(v) if v is not None else "" for v in row])
                resp_q.put({"id": cmd_id, "status": "ok", "result": {"cols": cols, "data": safe}})

            elif cmd_name == "exec_multi":
                # Execute multiple SQL statements (no result needed)
                for stmt in args["statements"]:
                    db.execute(stmt)
                resp_q.put({"id": cmd_id, "status": "ok", "result": {}})

            elif cmd_name == "register_table":
                name = args["name"]
                ipc_bytes = args["ipc_bytes"]
                reader = _ipc.open_stream(ipc_bytes)
                table = reader.read_all()
                arrow_tables[name] = table
                db.register(name, table)
                resp_q.put({"id": cmd_id, "status": "ok", "result": {}})

            elif cmd_name == "unregister_table":
                name = args["name"]
                try:
                    db.unregister(name)
                except Exception:
                    pass
                arrow_tables.pop(name, None)
                resp_q.put({"id": cmd_id, "status": "ok", "result": {}})

            elif cmd_name == "interrupt":
                try:
                    db.interrupt()
                except Exception:
                    pass
                resp_q.put({"id": cmd_id, "status": "ok", "result": {}})

            elif cmd_name == "shutdown":
                resp_q.put({"id": cmd_id, "status": "ok", "result": {}})
                break

            else:
                resp_q.put({"id": cmd_id, "status": "error", "error": f"Unknown command: {cmd_name}"})

        except Exception as e:
            resp_q.put({"id": cmd_id, "status": "error", "error": str(e)})


def _serialize_arrow_table(table):
    """Serialize a PyArrow table to IPC bytes for cross-process transfer."""
    import pyarrow as _pa
    import pyarrow.ipc as _ipc
    sink = _pa.BufferOutputStream()
    writer = _ipc.new_stream(sink, table.schema)
    writer.write_table(table)
    writer.close()
    return sink.getvalue().to_pybytes()


class DuckDBWorker:
    """Main-process client for the DuckDB subprocess. Thread-safe — multiple HTTP
    handler threads can call send_command concurrently; commands are serialized
    in the subprocess (one at a time via Queue)."""

    def __init__(self):
        self._cmd_q = None
        self._resp_q = None
        self._proc = None
        self._pending = {}  # cmd_id -> {"event": Event, "result": dict}
        self._reader_thread = None

    def start(self):
        """Spawn (or respawn) the subprocess and initialize DuckDB."""
        self._cmd_q = multiprocessing.Queue()
        self._resp_q = multiprocessing.Queue()
        self._proc = multiprocessing.Process(
            target=_duckdb_worker,
            args=(self._cmd_q, self._resp_q),
            daemon=True
        )
        self._proc.start()
        # Background thread dispatches responses to waiters
        self._reader_thread = threading.Thread(target=self._response_reader, daemon=True)
        self._reader_thread.start()
        # Initialize DuckDB in the subprocess
        self.send_command("init", {}, timeout=30)
        print("  DuckDB extensions: httpfs, iceberg, azure loaded (azure transport: curl)")

    def send_command(self, cmd, args, timeout=60):
        """Send a command and block until response or timeout.
        On timeout, kills the subprocess and respawns."""
        cmd_id = str(uuid.uuid4())
        event = threading.Event()
        self._pending[cmd_id] = {"event": event, "result": None}
        self._cmd_q.put({"id": cmd_id, "cmd": cmd, "args": args})
        if not event.wait(timeout=timeout):
            # Subprocess is hung — kill and respawn
            self._kill_and_respawn()
            self._pending.pop(cmd_id, None)
            raise Exception(f"DuckDB operation timed out after {timeout}s — worker restarted.")
        result = self._pending.pop(cmd_id, {}).get("result", {})
        if result.get("status") == "error":
            raise Exception(result.get("error", "Unknown error"))
        return result.get("result")

    def _response_reader(self):
        """Background thread that reads responses and wakes waiting callers."""
        while True:
            try:
                resp = self._resp_q.get(timeout=2)
            except Exception:
                if self._proc and not self._proc.is_alive():
                    # Worker died unexpectedly — wake all waiters
                    for entry in self._pending.values():
                        entry["result"] = {"status": "error", "error": "DuckDB worker crashed"}
                        entry["event"].set()
                    self._pending.clear()
                continue
            pending = self._pending.get(resp.get("id"))
            if pending:
                pending["result"] = resp
                pending["event"].set()

    def _kill_and_respawn(self):
        """Kill a hung subprocess and start a fresh one."""
        print("  WORKER: DuckDB subprocess timed out — killing and respawning...")
        if self._proc and self._proc.is_alive():
            self._proc.kill()
            self._proc.join(timeout=5)
        # Wake all pending waiters with error
        for entry in self._pending.values():
            entry["result"] = {"status": "error", "error": "Worker restarted due to timeout"}
            entry["event"].set()
        self._pending.clear()
        # Clear main-process state that references dead subprocess
        loaded_tables.clear()
        polaris_catalogs.clear()
        print("  WORKER: All tables and catalog connections lost — respawning...")
        self.start()


duckdb_worker = None  # initialized in main()
current_bucket = BUCKET_NAME  # active bucket (can change at runtime)
polaris_catalogs = {}  # alias -> {"endpoint":..., "catalog":..., "connected": True}
# Cache loaded tables for SQL queries: name -> pyarrow table (ordered by load time for LRU eviction)
loaded_tables = OrderedDict()
MEMORY_THRESHOLD = 0.85  # evict oldest tables when system memory usage exceeds 85%


def _save_state(bucket, prefix):
    """Persist last browsed bucket/prefix to disk."""
    try:
        with open(STATE_FILE, "w") as f:
            json.dump({"bucket": bucket, "prefix": prefix}, f)
    except Exception:
        pass


def _load_state():
    """Load last browsed bucket/prefix from disk."""
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return None


def _maybe_evict():
    """Evict oldest loaded tables until system memory usage drops below threshold."""
    mem = psutil.virtual_memory()
    while mem.percent / 100.0 >= MEMORY_THRESHOLD and loaded_tables:
        name, meta = loaded_tables.popitem(last=False)  # oldest first
        try:
            duckdb_worker.send_command("unregister_table", {"name": name}, timeout=10)
        except Exception:
            pass
        mb = meta.get("nbytes", 0) / (1024 * 1024) if isinstance(meta, dict) else 0
        mem = psutil.virtual_memory()
        print(f"  Evicted table '{name}' (~{mb:.1f} MB) — memory now at {mem.percent:.0f}%")


def init_gcs():
    """Initialize GCS client using Application Default Credentials."""
    global gcs_client
    try:
        gcs_client = gcs_storage.Client()
        # Quick test
        bucket = gcs_client.bucket(BUCKET_NAME)
        bucket.reload()
        state = _load_state()
        return {
            "status": "ok",
            "message": f"Connected to gs://{BUCKET_NAME}",
            "last_bucket": state["bucket"] if state else BUCKET_NAME,
            "last_prefix": state["prefix"] if state else "",
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


def list_buckets():
    """List all GCS buckets accessible to the authenticated account."""
    try:
        buckets = []
        for b in gcs_client.list_buckets():
            buckets.append({"name": b.name})
        return {"status": "ok", "buckets": buckets}
    except Exception as e:
        return {"status": "error", "message": str(e)}


def init_duckdb():
    """Start the DuckDB subprocess worker."""
    global duckdb_worker
    duckdb_worker = DuckDBWorker()
    duckdb_worker.start()


def connect_polaris(alias, endpoint, catalog, client_id, client_secret):
    """Connect to a Polaris Iceberg catalog via DuckDB with a given alias."""
    if not alias or not endpoint or not catalog or not client_id or not client_secret:
        return {"status": "error", "message": "All fields are required"}
    safe_alias = alias.replace("-", "_").replace(" ", "_").lower()
    # Skip if already connected with same catalog
    existing = polaris_catalogs.get(safe_alias)
    if existing and existing.get("catalog") == catalog and existing.get("endpoint") == endpoint:
        print(f"  Catalog '{safe_alias}' already connected, skipping re-attach")
        return {"status": "ok", "alias": safe_alias, "message": f"Already connected: {safe_alias} ({catalog}). Query as: {safe_alias}.<namespace>.<table>"}
    secret_name = f"secret_{safe_alias}"
    oauth_uri = endpoint.rstrip("/") + "/v1/oauth/tokens"
    try:
        # 65s timeout — ATTACH TYPE ICEBERG makes network calls that can hang.
        # If it hangs, send_command kills and respawns the subprocess instead of
        # freezing the entire server (the old GIL-blocking problem).
        duckdb_worker.send_command("exec_multi", {"statements": [
            f"DETACH DATABASE IF EXISTS {safe_alias};",
            f"DROP SECRET IF EXISTS {secret_name};",
            f"""CREATE SECRET {secret_name} (
                TYPE iceberg,
                CLIENT_ID '{client_id}',
                CLIENT_SECRET '{client_secret}',
                OAUTH2_SCOPE 'PRINCIPAL_ROLE:ALL',
                OAUTH2_SERVER_URI '{oauth_uri}'
            );""",
            f"""ATTACH '{catalog}' AS {safe_alias} (
                TYPE ICEBERG,
                ENDPOINT '{endpoint}',
                SECRET {secret_name}
            );""",
        ]}, timeout=65)
        polaris_catalogs[safe_alias] = {"endpoint": endpoint, "catalog": catalog, "alias": safe_alias,
                                        "client_id": client_id, "client_secret": client_secret}
        print(f"  Catalog '{safe_alias}' ({catalog}) attached")
        return {"status": "ok", "alias": safe_alias, "message": f"Connected: {safe_alias} ({catalog}). Query as: {safe_alias}.<namespace>.<table>"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


def disconnect_polaris(alias):
    """Disconnect a Polaris catalog."""
    safe_alias = alias.replace("-", "_").replace(" ", "_").lower()
    try:
        duckdb_worker.send_command("exec_multi", {"statements": [
            f"DETACH DATABASE IF EXISTS {safe_alias};",
            f"DROP SECRET IF EXISTS secret_{safe_alias};",
        ]}, timeout=15)
        polaris_catalogs.pop(safe_alias, None)
        return {"status": "ok", "message": f"Disconnected: {safe_alias}"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


def _polaris_oauth_token(endpoint, client_id, client_secret):
    """Get an OAuth2 token from the Polaris REST API."""
    token_url = endpoint.rstrip("/") + "/v1/oauth/tokens"
    data = urllib.parse.urlencode({
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "scope": "PRINCIPAL_ROLE:ALL",
    }).encode()
    req = urllib.request.Request(token_url, data=data, method="POST",
                                 headers={"Content-Type": "application/x-www-form-urlencoded"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())["access_token"]


def _polaris_rest_get(endpoint, path, token, extra_headers=None):
    """GET a Polaris REST API path with bearer token."""
    url = endpoint.rstrip("/") + path
    headers = {"Authorization": f"Bearer {token}"}
    if extra_headers:
        headers.update(extra_headers)
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


# Cache vended S3 credentials per catalog
_s3_vended_cache = {}  # alias -> {"token": str, "expires": float, "s3_creds": dict}


def _get_aws_polaris_token_and_creds(alias):
    """Get or refresh OAuth token + S3 vended credentials for an AWS Polaris catalog."""
    cache = _s3_vended_cache.get(alias, {})
    if cache.get("expires", 0) > time.time() and cache.get("s3_creds"):
        return cache["token"], cache["s3_creds"]

    cat = polaris_catalogs.get(alias)
    if not cat:
        return None, None
    try:
        token = _polaris_oauth_token(cat["endpoint"], cat["client_id"], cat["client_secret"])
        catalog_name = cat["catalog"]
        # Get any table to fetch vended S3 credentials (they're catalog-wide)
        ns_data = _polaris_rest_get(cat["endpoint"], f"/v1/{catalog_name}/namespaces", token)
        namespaces = ns_data.get("namespaces", [])
        if not namespaces:
            return token, None
        ns_name = namespaces[0][0] if isinstance(namespaces[0], list) else namespaces[0]
        tbl_data = _polaris_rest_get(cat["endpoint"],
            f"/v1/{catalog_name}/namespaces/{ns_name}/tables", token)
        tables = tbl_data.get("identifiers", [])
        if not tables:
            return token, None
        tbl_name = tables[0].get("name", tables[0]) if isinstance(tables[0], dict) else tables[0]
        tbl_meta = _polaris_rest_get(cat["endpoint"],
            f"/v1/{catalog_name}/namespaces/{ns_name}/tables/{tbl_name}", token,
            extra_headers={"X-Iceberg-Access-Delegation": "vended-credentials"})
        s3_creds = tbl_meta.get("config", {})
        _s3_vended_cache[alias] = {"token": token, "expires": time.time() + 3000, "s3_creds": s3_creds}
        # Create/refresh S3 secret in DuckDB
        ak = s3_creds.get("s3.access-key-id", "")
        sk = s3_creds.get("s3.secret-access-key", "")
        st = s3_creds.get("s3.session-token", "")
        if ak and sk:
            duckdb_worker.send_command("exec_multi", {"statements": [
                f"DROP SECRET IF EXISTS s3_vended_{alias};",
                f"""CREATE SECRET s3_vended_{alias} (
                    TYPE S3,
                    KEY_ID '{ak}',
                    SECRET '{sk}',
                    SESSION_TOKEN '{st}',
                    REGION 'us-west-2'
                );""",
            ]}, timeout=15)
            print(f"  S3 vended credentials refreshed for catalog '{alias}'")
        return token, s3_creds
    except Exception as e:
        print(f"  WARNING: Failed to get vended S3 credentials for {alias}: {e}")
        return None, None


def _rewrite_aws_query(query):
    """Rewrite queries targeting AWS Polaris catalogs to use iceberg_scan().

    DuckDB's ATTACH TYPE ICEBERG hangs on S3-backed catalogs because it doesn't
    send the X-Iceberg-Access-Delegation header needed for vended credentials.
    Workaround: resolve table metadata-location via REST API, then use iceberg_scan().
    """
    import re
    q_lower = query.lower().strip()

    # Find which AWS catalog alias is referenced
    target_alias = None
    for alias, cat in polaris_catalogs.items():
        if alias in q_lower and (".aws." in cat.get("endpoint", "") or "aws" in alias):
            target_alias = alias
            break
    if not target_alias:
        return query  # not an AWS query, return unchanged

    cat = polaris_catalogs[target_alias]
    catalog_name = cat["catalog"]

    # Extract table references like aws.namespace.table
    # Pattern: alias.namespace.table (possibly quoted)
    pattern = re.compile(
        rf'\b{re.escape(target_alias)}\.([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)\b',
        re.IGNORECASE
    )
    matches = pattern.findall(query)
    if not matches:
        return query  # no table references found

    token, s3_creds = _get_aws_polaris_token_and_creds(target_alias)
    if not token:
        return query  # can't get token, let it fail normally

    rewritten = query
    for namespace, table in matches:
        try:
            tbl_meta = _polaris_rest_get(cat["endpoint"],
                f"/v1/{catalog_name}/namespaces/{namespace}/tables/{table}", token,
                extra_headers={"X-Iceberg-Access-Delegation": "vended-credentials"})
            meta_loc = tbl_meta.get("metadata-location", "")
            if not meta_loc:
                continue
            # Replace catalog.namespace.table with iceberg_scan('metadata-location')
            full_ref = f"{target_alias}.{namespace}.{table}"
            replacement = f"iceberg_scan('{meta_loc}')"
            # Case-insensitive replace of the table reference
            rewritten = re.sub(
                rf'\b{re.escape(target_alias)}\.{re.escape(namespace)}\.{re.escape(table)}\b',
                replacement, rewritten, flags=re.IGNORECASE
            )
            print(f"  AWS query rewrite: {full_ref} -> iceberg_scan(...)")

            # Also refresh S3 creds from this specific table's config
            cfg = tbl_meta.get("config", {})
            ak = cfg.get("s3.access-key-id", "")
            sk = cfg.get("s3.secret-access-key", "")
            st = cfg.get("s3.session-token", "")
            if ak and sk:
                duckdb_worker.send_command("exec_multi", {"statements": [
                    f"DROP SECRET IF EXISTS s3_vended_{target_alias};",
                    f"""CREATE SECRET s3_vended_{target_alias} (
                        TYPE S3,
                        KEY_ID '{ak}',
                        SECRET '{sk}',
                        SESSION_TOKEN '{st}',
                        REGION 'us-west-2'
                    );""",
                ]}, timeout=15)
        except Exception as e:
            print(f"  WARNING: Failed to resolve {target_alias}.{namespace}.{table}: {e}")

    return rewritten


def list_polaris_namespaces(alias):
    """List namespaces in a specific attached catalog via Polaris REST API."""
    safe_alias = alias.replace("-", "_").replace(" ", "_").lower()
    if safe_alias not in polaris_catalogs:
        return {"status": "error", "message": f"Catalog '{alias}' not connected"}
    cat = polaris_catalogs[safe_alias]
    try:
        token = _polaris_oauth_token(cat["endpoint"], cat["client_id"], cat["client_secret"])
        result = _polaris_rest_get(cat["endpoint"], f"/v1/{cat['catalog']}/namespaces", token)
        namespaces = sorted([ns[0] if isinstance(ns, list) else ns for ns in result.get("namespaces", [])])
        return {"status": "ok", "namespaces": namespaces, "alias": safe_alias}
    except Exception as e:
        return {"status": "error", "message": str(e)}


def list_polaris_tables(alias, namespace):
    """List tables in a namespace via Polaris REST API."""
    safe_alias = alias.replace("-", "_").replace(" ", "_").lower()
    if safe_alias not in polaris_catalogs:
        return {"status": "error", "message": f"Catalog '{alias}' not connected"}
    cat = polaris_catalogs[safe_alias]
    try:
        token = _polaris_oauth_token(cat["endpoint"], cat["client_id"], cat["client_secret"])
        ns_path = urllib.parse.quote(namespace, safe="")
        result = _polaris_rest_get(cat["endpoint"], f"/v1/{cat['catalog']}/namespaces/{ns_path}/tables", token)
        tables = sorted([t.get("name", t) if isinstance(t, dict) else t for t in result.get("identifiers", [])])
        return {"status": "ok", "namespace": namespace, "tables": tables, "alias": safe_alias}
    except Exception as e:
        return {"status": "error", "message": str(e)}


def list_all_polaris_tables():
    """List all tables across all connected catalogs via Polaris REST API."""
    if not polaris_catalogs:
        return {"status": "ok", "tables": []}
    tables = []
    for alias, cat in list(polaris_catalogs.items()):
        try:
            token = _polaris_oauth_token(cat["endpoint"], cat["client_id"], cat["client_secret"])
            ns_result = _polaris_rest_get(cat["endpoint"], f"/v1/{cat['catalog']}/namespaces", token)
            for ns in ns_result.get("namespaces", []):
                ns_name = ns[0] if isinstance(ns, list) else ns
                ns_path = urllib.parse.quote(ns_name, safe="")
                tbl_result = _polaris_rest_get(cat["endpoint"], f"/v1/{cat['catalog']}/namespaces/{ns_path}/tables", token)
                for t in tbl_result.get("identifiers", []):
                    tbl_name = t.get("name", t) if isinstance(t, dict) else t
                    tables.append({"namespace": ns_name, "name": tbl_name, "fqn": f"{alias}.{ns_name}.{tbl_name}", "alias": alias})
        except Exception:
            pass
    return {"status": "ok", "tables": tables}


def get_connected_catalogs():
    """Return list of connected catalogs."""
    return {"status": "ok", "catalogs": [{"alias": a, "endpoint": v["endpoint"], "catalog": v["catalog"]} for a, v in polaris_catalogs.items()]}


def get_catalog_presets():
    """Return available presets (excluding secrets for display)."""
    safe = {}
    for k, v in CATALOG_PRESETS.items():
        safe[k] = {kk: vv for kk, vv in v.items()}
        # Mask secrets for display
        if "client_secret" in safe[k]:
            s = safe[k]["client_secret"]
            safe[k]["client_secret_masked"] = s[:4] + "..." + s[-4:] if len(s) > 8 else "***"
    return {"status": "ok", "presets": safe}


def update_polaris_credentials(data):
    """Update Polaris catalog credentials for a provider."""
    provider = data.get("provider", "").lower()
    if provider not in CATALOG_PRESETS:
        return {"status": "error", "message": f"Unknown provider: {provider}. Use gcs, azure, or aws."}
    endpoint = data.get("endpoint", "").strip()
    catalog = data.get("catalog", "").strip()
    client_id = data.get("client_id", "").strip()
    client_secret = data.get("client_secret", "").strip()
    if not endpoint or not catalog or not client_id or not client_secret:
        return {"status": "error", "message": "All fields are required: endpoint, catalog, client_id, client_secret"}
    CATALOG_PRESETS[provider]["endpoint"] = endpoint
    CATALOG_PRESETS[provider]["catalog"] = catalog
    CATALOG_PRESETS[provider]["client_id"] = client_id
    CATALOG_PRESETS[provider]["client_secret"] = client_secret
    # If this provider is currently connected, disconnect it so next connect uses new creds
    alias = CATALOG_PRESETS[provider].get("default_alias", provider)
    safe_alias = alias.replace("-", "_").replace(" ", "_").lower()
    if safe_alias in polaris_catalogs:
        try:
            disconnect_polaris(safe_alias)
        except Exception:
            pass
    return {"status": "ok", "message": f"Credentials updated for {CATALOG_PRESETS[provider].get('label', provider)}. Click the button above to reconnect."}


def _get_dir_stats(bucket, prefixes, parent_prefix):
    """Get file count and total size under each directory's data/ subfolder."""
    stats = {}
    if not prefixes:
        return stats
    # Do a deep listing of parent prefix and aggregate by directory
    try:
        all_blobs = gcs_client.list_blobs(bucket, prefix=parent_prefix)
        for blob in all_blobs:
            if not blob.name.endswith(".parquet") or "_delta_log" in blob.name:
                continue
            # Find which directory this blob belongs to
            rel = blob.name[len(parent_prefix):]
            parts = rel.split("/")
            if len(parts) >= 2:
                dir_name = parts[0]
                key = parent_prefix + dir_name + "/"
                if key in prefixes:
                    if key not in stats:
                        stats[key] = {"files": 0, "total_size": 0}
                    stats[key]["files"] += 1
                    stats[key]["total_size"] += (blob.size or 0)
    except Exception:
        pass
    return stats


def list_path(prefix, bucket_name=None):
    """List directories and files under a GCS prefix."""
    global current_bucket
    if bucket_name:
        current_bucket = bucket_name
    try:
        bucket = gcs_client.bucket(current_bucket)
        iterator = gcs_client.list_blobs(bucket, prefix=prefix, delimiter="/")
        blobs = list(iterator)
        prefixes = sorted(iterator.prefixes)

        # Gather stats only inside dataset directories under sap_cds_views/
        prefix_set = set(prefixes)
        is_dataset_dir = (current_bucket == BUCKET_NAME
                          and prefix.startswith(BASE_PREFIX)
                          and prefix.count("/") >= 2)
        dir_stats = _get_dir_stats(bucket, prefix_set, prefix) if is_dataset_dir else {}

        items = []
        for p in prefixes:
            name = p.rstrip("/").split("/")[-1]
            st = dir_stats.get(p, {})
            items.append({
                "name": name + "/",
                "path": p,
                "is_dir": True,
                "size": st.get("total_size", ""),
                "file_count": st.get("files", 0)
            })
        for blob in blobs:
            if blob.name == prefix:
                continue
            name = blob.name.split("/")[-1]
            if not name:
                continue
            items.append({
                "name": name,
                "path": blob.name,
                "is_dir": False,
                "size": blob.size,
                "file_count": 0
            })

        # Persist navigation state
        _save_state(current_bucket, prefix)

        return {"status": "ok", "items": items, "prefix": prefix, "bucket": current_bucket}
    except Exception as e:
        return {"status": "error", "message": str(e)}


def read_parquet(blob_path):
    """Read a parquet file from GCS into memory and return as table data."""
    _maybe_evict()
    try:
        bucket = gcs_client.bucket(current_bucket)
        blob = bucket.blob(blob_path)
        data = blob.download_as_bytes()
        table = pq.read_table(io.BytesIO(data))

        table_name = blob_path.split("/")[-1].replace(".parquet", "").replace("-", "_")
        if table_name[0:1].isdigit():
            table_name = "t_" + table_name
        parts = blob_path.split("/")
        dir_name = table_name
        if len(parts) >= 3:
            dir_name = parts[-3] if parts[-2] == "data" else parts[-2]
            dir_name = dir_name.replace("-", "_")
            if dir_name[0:1].isdigit():
                dir_name = "t_" + dir_name

        # Serialize and send to DuckDB subprocess for registration
        ipc_bytes = _serialize_arrow_table(table)
        if dir_name != table_name:
            duckdb_worker.send_command("register_table", {"name": dir_name, "ipc_bytes": ipc_bytes}, timeout=30)
            loaded_tables[dir_name] = {"rows": table.num_rows, "columns": list(table.column_names), "nbytes": table.nbytes}
        duckdb_worker.send_command("register_table", {"name": table_name, "ipc_bytes": ipc_bytes}, timeout=30)
        loaded_tables[table_name] = {"rows": table.num_rows, "columns": list(table.column_names), "nbytes": table.nbytes}

        columns = [field.name for field in table.schema]
        num_rows = table.num_rows
        limit = min(num_rows, 2000)
        rows = []
        for i in range(limit):
            rows.append([
                str(table.column(c)[i].as_py()) if table.column(c)[i].as_py() is not None else ""
                for c in range(len(columns))
            ])

        return {
            "status": "ok",
            "columns": columns,
            "rows": rows,
            "total_rows": num_rows,
            "displayed_rows": limit,
            "schema": str(table.schema),
            "registered_as": dir_name
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


def read_all_parquets_in_dir(dir_path):
    """Read all parquet files in a data/ directory and combine them."""
    _maybe_evict()
    try:
        bucket = gcs_client.bucket(current_bucket)
        data_prefix = dir_path if dir_path.endswith("data/") else dir_path + "data/"
        blobs = list(gcs_client.list_blobs(bucket, prefix=data_prefix))
        parquet_blobs = [b for b in blobs if b.name.endswith(".parquet")]

        if not parquet_blobs:
            return {"status": "error", "message": "No parquet files found in " + data_prefix}

        tables = []
        for blob in parquet_blobs:
            data = blob.download_as_bytes()
            tables.append(pq.read_table(io.BytesIO(data)))

        combined = pyarrow.concat_tables(tables)

        parts = dir_path.rstrip("/").split("/")
        dir_name = parts[-1] if parts[-1] != "data" else parts[-2]
        dir_name = dir_name.replace("-", "_")
        if dir_name[0:1].isdigit():
            dir_name = "t_" + dir_name

        # Serialize and send to DuckDB subprocess
        ipc_bytes = _serialize_arrow_table(combined)
        duckdb_worker.send_command("register_table", {"name": dir_name, "ipc_bytes": ipc_bytes}, timeout=30)
        loaded_tables[dir_name] = {"rows": combined.num_rows, "columns": list(combined.column_names), "nbytes": combined.nbytes}

        columns = [field.name for field in combined.schema]
        num_rows = combined.num_rows
        limit = min(num_rows, 2000)
        rows = []
        for i in range(limit):
            rows.append([
                str(combined.column(c)[i].as_py()) if combined.column(c)[i].as_py() is not None else ""
                for c in range(len(columns))
            ])

        return {
            "status": "ok",
            "columns": columns,
            "rows": rows,
            "total_rows": num_rows,
            "displayed_rows": limit,
            "schema": str(combined.schema),
            "registered_as": dir_name,
            "files_read": len(parquet_blobs)
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


def auto_load_table(table_name, browse_prefix=""):
    """Auto-load a table from GCS. Looks under the current browse prefix first."""
    _maybe_evict()
    try:
        bucket = gcs_client.bucket(current_bucket)
        search_name = table_name.lower()

        # Build candidate prefixes to search — current dataset first
        candidates = []
        if browse_prefix:
            # e.g. browse_prefix = "sap_cds_views/cds_sql_only_12/"
            # or "sap_cds_views/cds_sql_only_12/adcp_metadata/"
            parts = browse_prefix.rstrip("/").split("/")
            # The dataset prefix is typically the first 2 levels
            if len(parts) >= 2:
                dataset = "/".join(parts[:2]) + "/"
                candidates.append(dataset + search_name + "/data/")
                candidates.append(dataset + search_name + "/")

        for candidate in candidates:
            blobs = list(gcs_client.list_blobs(bucket, prefix=candidate, max_results=20))
            parquet_blobs = [b for b in blobs if b.name.endswith(".parquet") and "_delta_log" not in b.name]
            if parquet_blobs:
                tables = []
                for blob in parquet_blobs:
                    data = blob.download_as_bytes()
                    tables.append(pq.read_table(io.BytesIO(data)))
                combined = pyarrow.concat_tables(tables) if len(tables) > 1 else tables[0]
                ipc_bytes = _serialize_arrow_table(combined)
                duckdb_worker.send_command("register_table", {"name": search_name, "ipc_bytes": ipc_bytes}, timeout=30)
                loaded_tables[search_name] = {"rows": combined.num_rows, "columns": list(combined.column_names), "nbytes": combined.nbytes}
                print(f"  Auto-loaded: {search_name} ({combined.num_rows} rows)")
                return True
        return False
    except Exception as e:
        print(f"  Auto-load failed for {table_name}: {e}")
        return False


import re as _re

def _extract_missing_table(error_msg):
    """Extract the missing table name from a DuckDB error message."""
    m = _re.search(r'Table with name (\S+) does not exist', error_msg)
    return m.group(1).strip('"').lower() if m else None


def run_sql(query, browse_prefix=""):
    """Execute a SQL query via DuckDB. Auto-loads tables from GCS if not found."""
    def _exec(q, timeout_sec=45):
        # send_command timeout is timeout_sec + 5s buffer so the subprocess can
        # return its own error before the main process kills it
        result = duckdb_worker.send_command("exec_sql", {"query": q}, timeout=timeout_sec + 5)
        return result["cols"], result["data"]

    # Rewrite AWS Polaris queries to use iceberg_scan() with vended S3 credentials
    query = _rewrite_aws_query(query)

    try:
        columns, rows = _exec(query)
    except Exception as e:
        error_msg = str(e)
        missing = _extract_missing_table(error_msg)
        if missing and missing not in loaded_tables:
            if auto_load_table(missing, browse_prefix):
                try:
                    columns, rows = _exec(query)
                except Exception as e2:
                    missing2 = _extract_missing_table(str(e2))
                    if missing2 and missing2 not in loaded_tables and auto_load_table(missing2, browse_prefix):
                        try:
                            columns, rows = _exec(query)
                        except Exception as e3:
                            return {"status": "error", "message": str(e3)}
                    else:
                        return {"status": "error", "message": str(e2)}
            else:
                return {"status": "error", "message": error_msg}
        else:
            return {"status": "error", "message": error_msg}

    try:

        safe_rows = []
        for row in rows:
            safe_row = []
            for v in row:
                if v is None:
                    safe_row.append("")
                elif isinstance(v, str):
                    safe_row.append(v)
                else:
                    safe_row.append(str(v))
            safe_rows.append(safe_row)

        return {
            "status": "ok",
            "columns": columns,
            "rows": safe_rows,
            "total_rows": len(rows)
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}


def read_file_text(blob_path):
    """Read a text file from GCS."""
    try:
        bucket = gcs_client.bucket(current_bucket)
        blob = bucket.blob(blob_path)
        content = blob.download_as_text()
        return {"status": "ok", "content": content[:100000]}
    except Exception as e:
        return {"status": "error", "message": str(e)}




def get_loaded_tables():
    """Return list of tables registered in DuckDB."""
    tables = []
    for name, meta in loaded_tables.items():
        if isinstance(meta, dict):
            tables.append({
                "name": name,
                "rows": meta.get("rows", 0),
                "columns": len(meta.get("columns", [])),
                "column_names": meta.get("columns", [])
            })
        else:
            # Legacy: meta is a PyArrow table (shouldn't happen after refactor)
            tables.append({
                "name": name,
                "rows": meta.num_rows,
                "columns": len(meta.column_names),
                "column_names": list(meta.column_names)
            })
    return {"status": "ok", "tables": tables}


HTML_PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Fivetran - GCS Parquet Explorer</title>
<style>
/* ===== RESET & BASE ===== */
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
html, body { height: 100%; overflow: hidden; }
body {
  font-family: 'Inter', 'Segoe UI', system-ui, -apple-system, sans-serif;
  font-size: 14px;
  color: #1a1a2e;
  background: #f8f9fa;
  display: flex;
}

/* ===== SCROLLBAR ===== */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: #c1c7cd; border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: #a0a8b2; }

/* ===== SIDEBAR (light, matching Fivetran dashboard) ===== */
.sidebar {
  width: 220px;
  min-width: 220px;
  height: 100vh;
  background: #ffffff;
  border-right: 1px solid #e5e7eb;
  display: flex;
  flex-direction: column;
  overflow-y: auto;
  overflow-x: hidden;
  z-index: 100;
}
.sidebar-logo {
  padding: 18px 16px 14px;
  border-bottom: 1px solid #f0f0f0;
  flex-shrink: 0;
}
.sidebar-logo img { width: 100%; height: auto; display: block; }

.sidebar-nav {
  flex: 1;
  padding: 8px 0;
  overflow-y: auto;
}
.nav-section-label {
  font-size: 11px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: #9ca3af;
  padding: 16px 16px 6px;
}
.nav-item {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 9px 16px;
  color: #4b5563;
  cursor: pointer;
  font-size: 13.5px;
  font-weight: 500;
  border-left: 3px solid transparent;
  transition: background 0.15s, color 0.15s;
  user-select: none;
  text-decoration: none;
}
.nav-item:hover {
  background: #f8f9fa;
  color: #1a1a2e;
}
.nav-item.active {
  color: #0073FF;
  font-weight: 600;
  border-left-color: transparent;
  background: transparent;
}
.nav-item .nav-icon {
  width: 20px;
  height: 20px;
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  font-size: 15px;
  line-height: 1;
  color: #6b7280;
}
.nav-item.active .nav-icon { color: #0073FF; }
.nav-item .nav-dot {
  width: 10px;
  height: 10px;
  border-radius: 50%;
  flex-shrink: 0;
  margin: 0 5px;
}
.nav-dot.green { background: #2ea44f; }
.nav-dot.blue { background: #0078d4; }
.nav-dot.orange { background: #f59e0b; }

.sidebar-sep {
  height: 1px;
  background: #e5e7eb;
  margin: 8px 14px;
}

.sidebar-bottom {
  flex-shrink: 0;
  border-top: 1px solid #e5e7eb;
  padding: 12px 16px;
}
.sidebar-user {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 4px 0;
}
.sidebar-user-avatar {
  width: 30px;
  height: 30px;
  border-radius: 50%;
  background: #e5e7eb;
  display: flex;
  align-items: center;
  justify-content: center;
  color: #6b7280;
  font-size: 12px;
  font-weight: 600;
  flex-shrink: 0;
}
.sidebar-user-info {
  overflow: hidden;
  flex: 1;
}
.sidebar-user-email {
  color: #1a1a2e;
  font-size: 12px;
  font-weight: 500;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}
.sidebar-user-logout {
  color: #9ca3af;
  font-size: 11px;
  cursor: pointer;
  text-decoration: none;
  display: inline-block;
  margin-top: 1px;
}
.sidebar-user-logout:hover { color: #ef4444; }

/* ===== MAIN CONTENT AREA ===== */
.main-wrapper {
  flex: 1;
  display: flex;
  flex-direction: column;
  height: 100vh;
  overflow: hidden;
  min-width: 0;
}

/* --- Top Bar --- */
.top-bar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 14px 24px;
  background: #ffffff;
  border-bottom: 1px solid #e5e7eb;
  flex-shrink: 0;
  min-height: 54px;
}
.top-bar-title {
  font-size: 17px;
  font-weight: 700;
  color: #1a1a2e;
}
.status-badge {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 4px 12px;
  border-radius: 20px;
  font-size: 12px;
  font-weight: 600;
}
.status-badge .dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
}
.status-badge.connected { background: #dcfce7; color: #166534; }
.status-badge.connected .dot { background: #22c55e; }
.status-badge.pending { background: #fef9c3; color: #854d0e; }
.status-badge.pending .dot { background: #eab308; }
.status-badge.error { background: #fee2e2; color: #991b1b; }
.status-badge.error .dot { background: #ef4444; }
.status-badge.disconnected { background: #f1f3f5; color: #6b7280; }
.status-badge.disconnected .dot { background: #9ca3af; }

/* --- Content Scroll Area --- */
.content-scroll {
  flex: 1;
  overflow: hidden;
  padding: 0;
  background: #f8f9fa;
  display: flex;
  flex-direction: column;
}

/* ===== CONTENT SECTIONS ===== */
.content-section {
  display: none;
  flex-direction: column;
  flex: 1;
  overflow: hidden;
  padding: 20px 24px 0;
}
.content-section.active { display: flex; }

/* Scrollable area inside each section */
.section-scrollable {
  flex: 1;
  overflow-y: auto;
  overflow-x: hidden;
  padding-bottom: 16px;
}

/* --- Shared Card Styles --- */
.card {
  background: #ffffff;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  padding: 20px;
  margin-bottom: 16px;
}
.card-header {
  font-size: 15px;
  font-weight: 700;
  color: #1a1a2e;
  margin-bottom: 12px;
}

/* ===== GCS EXPLORER SECTION ===== */
.breadcrumb-bar {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 10px 0;
  font-size: 13px;
  color: #6b7280;
  flex-wrap: wrap;
}
.breadcrumb-bar a {
  color: #0073FF;
  text-decoration: none;
  cursor: pointer;
  font-weight: 500;
}
.breadcrumb-bar a:hover { text-decoration: underline; }
.breadcrumb-bar .sep { color: #d1d5db; margin: 0 2px; }

.search-filter-bar {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 10px;
}
.search-input {
  flex: 1;
  padding: 9px 14px 9px 36px;
  border: 1px solid #e5e7eb;
  border-radius: 6px;
  font-size: 13.5px;
  background: #ffffff;
  color: #1a1a2e;
  outline: none;
  transition: border-color 0.15s;
  font-family: inherit;
}
.search-input:focus { border-color: #0073FF; }
.search-input-wrap {
  position: relative;
  flex: 1;
}
.search-input-wrap .search-icon {
  position: absolute;
  left: 12px;
  top: 50%;
  transform: translateY(-50%);
  color: #9ca3af;
  font-size: 14px;
  pointer-events: none;
}

.sort-bar {
  display: flex;
  align-items: center;
  gap: 4px;
  padding: 8px 12px;
  background: #f1f3f5;
  border-radius: 6px;
  font-size: 12.5px;
  font-weight: 600;
  color: #6b7280;
  margin-bottom: 8px;
}
.sort-bar .sort-btn {
  cursor: pointer;
  padding: 4px 10px;
  border-radius: 4px;
  transition: background 0.12s, color 0.12s;
  user-select: none;
}
.sort-bar .sort-btn:hover { background: #e5e7eb; color: #1a1a2e; }
.sort-bar .sort-btn.active { background: #0073FF; color: #ffffff; }
.sort-bar .sort-spacer { flex: 1; }

.file-list {
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  background: #ffffff;
  overflow: hidden;
}
.file-item {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 10px 16px;
  border-bottom: 1px solid #f1f3f5;
  cursor: pointer;
  transition: background 0.1s;
  font-size: 13.5px;
}
.file-item:last-child { border-bottom: none; }
.file-item:hover { background: #f8f9fa; }
.file-item .file-icon {
  width: 20px;
  text-align: center;
  flex-shrink: 0;
  font-size: 16px;
}
.file-item .file-name { flex: 1; font-weight: 500; color: #1a1a2e; }
.file-item .file-meta { color: #8b949e; font-size: 12px; white-space: nowrap; }

/* Data table */
.data-table-wrap {
  overflow-x: auto;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  background: #ffffff;
  margin-top: 16px;
}
.data-table-wrap table {
  width: 100%;
  border-collapse: collapse;
  font-size: 13px;
}
.data-table-wrap th {
  background: #f1f3f5;
  font-weight: 600;
  color: #374151;
  padding: 10px 14px;
  text-align: left;
  border-bottom: 1px solid #e5e7eb;
  white-space: nowrap;
  position: sticky;
  top: 0;
  z-index: 1;
}
.data-table-wrap th.sortable { cursor: pointer; }
.data-table-wrap th.sortable:hover { color: #0073FF; }
.data-table-wrap th.sort-asc::after { content: ' \25B2'; color: #0073FF; font-size: 10px; }
.data-table-wrap th.sort-desc::after { content: ' \25BC'; color: #0073FF; font-size: 10px; }
.data-table-wrap td {
  padding: 8px 14px;
  border-bottom: 1px solid #f1f3f5;
  color: #374151;
  max-width: 260px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: default;
  font-family: 'SF Mono', 'Consolas', 'Menlo', monospace;
  font-size: 12.5px;
}
.data-table-wrap tr:hover td { background: #f8fafc; }

.data-info-bar {
  display: flex;
  align-items: center;
  gap: 16px;
  padding: 8px 0;
  font-size: 12.5px;
  color: #6b7280;
  margin-top: 8px;
}
.data-info-bar .tag {
  background: #e8ecf1;
  padding: 2px 8px;
  border-radius: 4px;
  font-weight: 600;
}

/* ===== POLARIS SECTION (shared by Google/Azure/AWS) ===== */
.polaris-section .connect-area {
  display: flex;
  align-items: center;
  gap: 16px;
  margin-bottom: 16px;
}
.btn {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 9px 20px;
  border: none;
  border-radius: 6px;
  font-size: 13.5px;
  font-weight: 600;
  cursor: pointer;
  font-family: inherit;
  transition: background 0.15s, box-shadow 0.15s;
  user-select: none;
}
.btn:active { transform: scale(0.98); }
.btn-primary { background: #0073FF; color: #ffffff; }
.btn-primary:hover { background: #005ecb; }
.btn-green { background: #2ea44f; color: #ffffff; }
.btn-green:hover { background: #258a3e; }
.btn-blue { background: #0078d4; color: #ffffff; }
.btn-blue:hover { background: #0063b1; }
.btn-orange { background: #f59e0b; color: #ffffff; }
.btn-orange:hover { background: #d97706; }
.btn-sm { padding: 6px 14px; font-size: 12.5px; }
.btn-outline {
  background: transparent;
  border: 1px solid #e5e7eb;
  color: #374151;
}
.btn-outline:hover { background: #f1f3f5; }

.polaris-msg {
  padding: 10px 14px;
  border-radius: 6px;
  font-size: 13px;
  margin-bottom: 14px;
  display: none;
}
.polaris-msg.info { background: #eff6ff; color: #1e40af; display: block; }
.polaris-msg.success { background: #dcfce7; color: #166534; display: block; }
.polaris-msg.error { background: #fee2e2; color: #991b1b; display: block; }

/* Credential Panel */
.cred-panel {
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  background: #ffffff;
  margin-bottom: 16px;
  overflow: hidden;
}
.cred-panel-toggle {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 12px 16px;
  cursor: pointer;
  font-size: 13.5px;
  font-weight: 600;
  color: #374151;
  user-select: none;
  transition: background 0.12s;
}
.cred-panel-toggle:hover { background: #f8f9fa; }
.cred-panel-toggle .arrow {
  font-size: 11px;
  color: #9ca3af;
  transition: transform 0.2s;
}
.cred-panel-toggle.open .arrow { transform: rotate(180deg); }
.cred-panel-body {
  display: none;
  padding: 4px 16px 16px;
}
.cred-panel-body.open { display: block; }
.cred-field {
  margin-bottom: 12px;
}
.cred-field label {
  display: block;
  font-size: 12.5px;
  font-weight: 600;
  color: #374151;
  margin-bottom: 4px;
}
.cred-field input {
  width: 100%;
  padding: 8px 12px;
  border: 1px solid #e5e7eb;
  border-radius: 6px;
  font-size: 13px;
  color: #1a1a2e;
  font-family: inherit;
  outline: none;
  transition: border-color 0.15s;
}
.cred-field input:focus { border-color: #0073FF; }
.cred-msg {
  font-size: 12.5px;
  margin-top: 8px;
  min-height: 18px;
}

/* Connected Catalogs Bar */
.connected-catalogs-bar {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 14px;
  background: #f1f3f5;
  border-radius: 6px;
  margin-bottom: 16px;
  font-size: 12.5px;
  color: #6b7280;
  flex-wrap: wrap;
  min-height: 38px;
}
.catalog-tag {
  background: #dbeafe;
  color: #1e40af;
  padding: 3px 10px;
  border-radius: 4px;
  font-weight: 600;
  font-size: 12px;
  cursor: pointer;
}
.catalog-tag:hover { background: #bfdbfe; }

/* Polaris Browser (two-column) */
.polaris-browser {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 16px;
  flex: 1;
  min-height: 0;
}
.polaris-col {
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  background: #ffffff;
  display: flex;
  flex-direction: column;
  min-height: 0;
  overflow: hidden;
}
.polaris-col-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 10px 14px;
  border-bottom: 1px solid #e5e7eb;
  flex-shrink: 0;
}
.polaris-col-title {
  font-size: 13px;
  font-weight: 700;
  color: #374151;
}
.polaris-col-count {
  font-size: 12px;
  color: #9ca3af;
  font-weight: 500;
}
.polaris-col-search {
  padding: 8px 12px;
  border-bottom: 1px solid #f1f3f5;
  flex-shrink: 0;
}
.polaris-col-search input {
  width: 100%;
  padding: 7px 10px;
  border: 1px solid #e5e7eb;
  border-radius: 5px;
  font-size: 12.5px;
  color: #1a1a2e;
  font-family: inherit;
  outline: none;
}
.polaris-col-search input:focus { border-color: #0073FF; }
.polaris-col-sort {
  display: flex;
  align-items: center;
  gap: 6px;
  padding: 6px 12px;
  border-bottom: 1px solid #f1f3f5;
  flex-shrink: 0;
  font-size: 11.5px;
  color: #9ca3af;
}
.polaris-col-sort .sort-toggle {
  cursor: pointer;
  padding: 2px 6px;
  border-radius: 3px;
  user-select: none;
}
.polaris-col-sort .sort-toggle:hover { background: #f1f3f5; color: #374151; }
.polaris-col-list {
  flex: 1;
  overflow-y: auto;
  padding: 4px 0;
}
.polaris-list-item {
  padding: 8px 14px;
  font-size: 13px;
  color: #374151;
  cursor: pointer;
  transition: background 0.1s;
  display: flex;
  align-items: center;
  gap: 8px;
}
.polaris-list-item:hover { background: #f8f9fa; }
.polaris-list-item.active { background: #eff6ff; color: #0073FF; font-weight: 600; }
.polaris-ns-label {
  font-size: 12px;
  color: #6b7280;
  padding: 6px 14px 2px;
  font-weight: 500;
}

/* ===== DOCUMENTATION SECTION ===== */
.doc-content {
  max-width: 780px;
  line-height: 1.7;
  color: #374151;
  flex: 1;
  overflow-y: auto;
  padding-bottom: 16px;
}
.doc-content h2 {
  font-size: 20px;
  font-weight: 700;
  color: #1a1a2e;
  margin: 24px 0 10px;
}
.doc-content h3 {
  font-size: 16px;
  font-weight: 700;
  color: #1a1a2e;
  margin: 18px 0 8px;
}
.doc-content p { margin-bottom: 10px; }
.doc-content code {
  background: #f1f3f5;
  padding: 2px 6px;
  border-radius: 4px;
  font-size: 12.5px;
  font-family: 'SF Mono', 'Consolas', 'Menlo', monospace;
}
.doc-content ul, .doc-content ol { padding-left: 24px; margin-bottom: 12px; }
.doc-content li { margin-bottom: 4px; }

/* ===== AUTH ERROR OVERLAY ===== */
.auth-overlay {
  display: none;
  position: fixed;
  inset: 0;
  background: rgba(26,31,46,0.7);
  z-index: 500;
  align-items: center;
  justify-content: center;
}
.auth-overlay.show { display: flex; }
.auth-box {
  background: #ffffff;
  border-radius: 12px;
  padding: 32px;
  max-width: 420px;
  width: 90%;
  text-align: center;
}
.auth-box h2 { font-size: 18px; margin-bottom: 10px; color: #1a1a2e; }
.auth-box p { font-size: 13.5px; color: #6b7280; margin-bottom: 18px; }
.auth-box .auth-msg { font-size: 12.5px; color: #ef4444; margin-top: 10px; min-height: 18px; }

/* ===== DRAG SPLITTER ===== */
.sql-drag-splitter {
  height: 5px;
  background: #e5e7eb;
  cursor: ns-resize;
  flex-shrink: 0;
  position: relative;
  z-index: 51;
  transition: background 0.15s;
}
.sql-drag-splitter:hover, .sql-drag-splitter.dragging {
  background: #0073FF;
}

/* ===== SQL EDITOR (BOTTOM PANEL) ===== */
.sql-panel {
  flex-shrink: 0;
  background: #ffffff;
  border-top: 1px solid #e5e7eb;
  display: flex;
  flex-direction: column;
  z-index: 50;
}
.sql-toggle-bar {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 7px 16px;
  background: #f9fafb;
  cursor: pointer;
  user-select: none;
  flex-shrink: 0;
  border-bottom: 1px solid #e5e7eb;
}
.sql-toggle-label {
  font-size: 13px;
  font-weight: 700;
  color: #1f2937;
}
.sql-toggle-arrow {
  font-size: 11px;
  color: #6b7280;
  transition: transform 0.2s;
  margin-left: 4px;
}
.sql-toggle-bar.collapsed .sql-toggle-arrow { transform: rotate(180deg); }
.sql-loaded-tags {
  display: flex;
  align-items: center;
  gap: 6px;
  margin-left: auto;
  overflow-x: auto;
  max-width: 50%;
}
.sql-loaded-tag {
  background: rgba(0,115,255,0.1);
  color: #0073FF;
  padding: 2px 8px;
  border-radius: 3px;
  font-size: 11px;
  font-weight: 600;
  white-space: nowrap;
}

.sql-body {
  display: flex;
  flex-direction: column;
  height: 250px;
  overflow: hidden;
  transition: height 0.2s;
}
.sql-body.collapsed { height: 0; }

.sql-editor-row {
  display: flex;
  align-items: flex-start;
  gap: 10px;
  padding: 10px 16px 6px;
  flex-shrink: 0;
  position: relative;
}
.sql-textarea-wrap {
  flex: 1;
  position: relative;
}
.sql-textarea {
  width: 100%;
  height: 70px;
  background: #f9fafb;
  border: 1px solid #d1d5db;
  border-radius: 6px;
  color: #1f2937;
  font-family: 'SF Mono', 'Consolas', 'Menlo', monospace;
  font-size: 13px;
  padding: 10px 12px;
  resize: vertical;
  outline: none;
  line-height: 1.5;
}
.sql-textarea:focus { border-color: #0073FF; box-shadow: 0 0 0 2px rgba(0,115,255,0.1); }
.sql-textarea::placeholder { color: #9ca3af; }

/* Autocomplete dropdown */
.ac-list {
  display: none;
  position: absolute;
  bottom: 100%;
  left: 0;
  min-width: 220px;
  max-height: 180px;
  overflow-y: auto;
  background: #ffffff;
  border: 1px solid #d1d5db;
  border-radius: 6px;
  box-shadow: 0 -4px 16px rgba(0,0,0,0.1);
  z-index: 60;
  margin-bottom: 4px;
}
.ac-list.show { display: block; }
.ac-item {
  padding: 7px 12px;
  font-size: 12.5px;
  color: #1f2937;
  cursor: pointer;
  font-family: 'SF Mono', 'Consolas', 'Menlo', monospace;
  display: flex;
  justify-content: space-between;
  gap: 16px;
}
.ac-item:hover, .ac-item.active { background: #0073FF; color: #ffffff; }
.ac-item .ac-type { font-size: 11px; color: #6b7280; white-space: nowrap; }

.sql-actions {
  display: flex;
  flex-direction: column;
  gap: 6px;
  flex-shrink: 0;
  padding-top: 2px;
}
.btn-run {
  background: #0073FF;
  color: #ffffff;
  border: none;
  border-radius: 6px;
  padding: 8px 18px;
  font-size: 13px;
  font-weight: 700;
  cursor: pointer;
  font-family: inherit;
  white-space: nowrap;
}
.btn-run:hover { background: #005ecb; }
.btn-show-tables {
  background: transparent;
  border: 1px solid #d1d5db;
  color: #6b7280;
  border-radius: 6px;
  padding: 6px 14px;
  font-size: 12px;
  font-weight: 600;
  cursor: pointer;
  font-family: inherit;
  white-space: nowrap;
}
.btn-show-tables:hover { border-color: #0073FF; color: #1f2937; }

.sql-hint {
  padding: 0 16px 6px;
  font-size: 11.5px;
  color: #9ca3af;
  flex-shrink: 0;
}

.sql-results-area {
  flex: 1;
  overflow: auto;
  padding: 0 16px 8px;
}
.sql-results-area table {
  width: 100%;
  border-collapse: collapse;
  font-size: 12.5px;
  font-family: 'SF Mono', 'Consolas', 'Menlo', monospace;
}
.sql-results-area th {
  background: #f3f4f6;
  color: #6b7280;
  font-weight: 600;
  padding: 7px 12px;
  text-align: left;
  border-bottom: 1px solid #e5e7eb;
  white-space: nowrap;
  position: sticky;
  top: 0;
  z-index: 1;
}
.sql-results-area th.sortable { cursor: pointer; }
.sql-results-area th.sortable:hover { color: #0073FF; }
.sql-results-area th.sort-asc::after { content: ' ▲'; color: #0073FF; font-size: 9px; }
.sql-results-area th.sort-desc::after { content: ' ▼'; color: #0073FF; font-size: 9px; }
.sql-results-area td {
  padding: 6px 12px;
  color: #1f2937;
  border-bottom: 1px solid #f3f4f6;
  max-width: 220px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
.sql-results-area tr:hover td { background: rgba(0,115,255,0.04); }

.sql-info-bar {
  display: flex;
  align-items: center;
  gap: 14px;
  padding: 6px 16px 8px;
  font-size: 11.5px;
  color: #9ca3af;
  flex-shrink: 0;
  border-top: 1px solid #e5e7eb;
}
.sql-info-bar .tag {
  color: #0073FF;
  font-weight: 600;
}

/* ===== CELL POPUP ===== */
.cell-popup {
  display: none;
  position: fixed;
  z-index: 600;
  background: #ffffff;
  border: 1px solid #e5e7eb;
  border-radius: 8px;
  box-shadow: 0 8px 32px rgba(0,0,0,0.12);
  padding: 14px 18px;
  max-width: 80vw;
  max-height: 80vh;
  overflow: auto;
  font-size: 13px;
  color: #374151;
  word-break: break-word;
  white-space: pre-wrap;
}
.cell-popup.show { display: block; }
.overlay { position: fixed; top:0; left:0; width:100%; height:100%; background: rgba(0,0,0,0.4); z-index: 599; }

/* ===== RESPONSIVE ===== */
@media (max-width: 900px) {
  .polaris-browser {
    grid-template-columns: 1fr;
  }
}
</style>
</head>
<body>

<!-- ===== SIDEBAR ===== -->
<aside class="sidebar">
  <div class="sidebar-logo">
    <img src="data:image/png;base64,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BETFNwEABAgQIECAgIBIGyBAgAABAgSKFxAQFd8EABAgQIAAAQICIm2AAAECBAgQKF5AQFR8EwBAgAABAgQICIi0AQIECBAgQKB4AQFR8U0AAAECBAgQICAg0gYIECBAgACB4gUERMU3AQAECBAgQICAgEgbIECAAAECBIoXEBAV3wQAECBAgAABAgIibYAAAQIECBAoXkBAVHwTAECAAAECBAgIiLQBAgQIECBAoHgBAVHxTQAAAQIECBAgICDSBggQIECAAIHiBQRExTcBAAQIECBAgICASBsgQIAAAQIEihcQEBXfBAAQIECAAAECAiJtgAABAgQIECheQEBUfBMAQIAAAQIECAiItAECBAgQIECgeAEBUfFNAAABAgQIECAgINIGCBAgQIAAgeIFBETFNwEABAgQIECAgIBIGyBAgAABAgSKFxAQFd8EABAgQIAAAQICIm2AAAECBAgQKF5AQFR8EwBAgAABAgQICIi0AQIECBAgQKB4AQFR8U0AAAECBAgQICAg0gYIECBAgACB4gUERMU3AQAECBAgQICAgEgbIECAAAECBIoXEBAV3wQAECBAgAABAgIibYAAAQIECBAoXkBAVHwTAECAAAECBAgIiLQBAgQIECBAoHgBAVHxTQAAAQIECBAgICDSBggQIECAAIHiBQRExTcBAAQIECBAgICASBsgQIAAAQIEihcQEBXfBAAQIECAAAECAiJtgAABAgQIECheQEBUfBMAQIAAAQIECAiItAECBAgQIECgeAEBUfFNAAABAgQIECAgINIGCBAgQIAAgeIFBETFNwEABAgQIECAgIBIGyBAgAABAgSKFxAQFd8EABAgQIAAAQICIm2AAAECBAgQKF5AQFR8EwBAgAABAgQICIi0AQIECBAgQKB4AQFR8U0AAAECBAgQICAg0gYIECBAgACB4gUERMU3AQAECBAgQICAgEgbIECAAAECBIoXEBAV3wQAECBAgAABAgIibYAAAQIECBAoXkBAVHwTAECAAAECBAgIiLQBAgQIECBAoHgBAVHxTQAAAQIECBAgICDSBggQIECAAIHiBQRExTcBAAQIECBAgICASBsgQIAAAQIEihcQEBXfBAAQIECAAAECAiJtgAABAgQIECheQEBUfBMAQIAAAQIECAiItAECBAgQIECgeAEBUfFNAAABAgQIECAgINIG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IECAAIHMBAREmVWI4hAgQIAAAQLTCwiIpjeXIwECBAgQIJCZgIAoswpRHAIECBAgQGB6AQHR9OZyJECAAAECBDITEBBlViGKQ4AAAQIECEwvICCa3lyOBAgQIECAQGYCAqLMKkRxCBAgQIAAgekFBETTm8uRAAECBAgQyExAQJRZhSgOAQIECBAgML2AgGh6czkSIECAAAECmQkIiDKrEMUhQIAAAQIEphcQEE1vLkcCBAgQIEAgMwEBUWYVojgECBAgQIDA9AICounN5UiAAAECBAhkJiAgyqxCFIcAAQIECBCYXkBANL25HAkQIECAAIHMBAREmVWI4hAgQIAAAQLTCwiIpjeXIwECBAgQIJCZgIAoswpRHAIECBAgQGB6AQHR9OZyJECAAAECBDITEBBlViGKQ4AAAQIECEwvICCa3lyOBAgQIECAQGYCAqLMKkRxCBAgQIAAgekFBETTm8uRAAECBAgQyExAQJRZhSgOAQIECBAgML2AgGh6czkSIECAAAECmQkIiDKrEMUhQIAAAQIEphcQEE1vLkcCBAgQIEAgMwEBUWYVojgECBAgQIDA9AICounN5UiAAAECBAhkJiAgyqxCFIcAAQIECBCYXkBANL25HAkQIECAAIHMBAREmVWI4hAgQIAAAQLTCwiIpjeXIwECBAgQIJCZgIAoswpRHAIECBAgQGB6AQHR9OZyJECAAAECBDITEBBlViGKQ4AAAQIECEwvICCa3lyOBAgQIECAQGYCAqLMKkRxCBAgQIAAgekFBETTm8uRAAECBAgQyExAQJRZhSgOAQIECBAgML2AgGh6czkSIECAAAECmQkIiDKrEMUhQIAAAQIEphcQEE1vLkcCBAgQIEAgMwEBUWYVojgECBAgQIDA9AICounN5UiAAAECBAhkJiAgyqxCFIcAAQIECBCYXkBANL25HAkQIECAAIHMBAREmVWI4hAgQIAAAQLTCwiIpjeXIwECBAgQIJCZgIAoswpRHAIECBAgQGB6AQHR9OZyJECAAAECBDITEBBlViGKQ4AAAQIECEwvICCa3lyOBAgQIECAQGYCAqLMKkRxCBAgQIAAgekFBETTm8uRAAECBAgQyExAQJRZhSgOAQIECBAgML2AgGh6czkSIECAAAECmQkIiDKrEMUhQIAAAQIEphcQEE1vLkcCBAgQIEAgMwEBUWYVojgECBAgQIDA9AICounN5UiAAAECBAhkJvD/AJ71kMZ6QictAAAAAElFTkSuQmCC" alt="Fivetran">
  </div>

  <nav class="sidebar-nav">
    <div class="nav-section-label">Explorer</div>
    <div class="nav-item active" onclick="switchNav('gcs')" data-section="gcs">
      <span class="nav-icon">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/></svg>
      </span>
      <span>GCS Parquet Explorer</span>
    </div>

    <div class="nav-section-label">Polaris Catalogs</div>
    <div class="nav-item" onclick="switchNav('google')" data-section="google">
      <span class="nav-icon"><span class="nav-dot green"></span></span>
      <span>Google Polaris</span>
    </div>
    <div class="nav-item" onclick="switchNav('azure')" data-section="azure">
      <span class="nav-icon"><span class="nav-dot blue"></span></span>
      <span>Azure Polaris</span>
    </div>
    <div class="nav-item" onclick="switchNav('aws')" data-section="aws">
      <span class="nav-icon"><span class="nav-dot orange"></span></span>
      <span>AWS Polaris</span>
    </div>

    <div class="sidebar-sep"></div>

    <div class="nav-item" onclick="switchNav('docs')" data-section="docs">
      <span class="nav-icon">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M4 19.5A2.5 2.5 0 0 1 6.5 17H20"/><path d="M6.5 2H20v20H6.5A2.5 2.5 0 0 1 4 19.5v-15A2.5 2.5 0 0 1 6.5 2z"/></svg>
      </span>
      <span>Documentation</span>
    </div>
  </nav>

  <div class="sidebar-bottom">
    <div class="nav-item" onclick="switchNav('restart')" data-section="restart" style="padding:8px 16px;margin-bottom:8px">
      <span class="nav-icon">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="23 4 23 10 17 10"/><path d="M20.49 15a9 9 0 1 1-2.12-9.36L23 10"/></svg>
      </span>
      <span>Restart Application</span>
    </div>
    <div class="sidebar-user">
      <div class="sidebar-user-avatar">U</div>
      <div class="sidebar-user-info">
        <div class="sidebar-user-email" id="userEmail">user@fivetran.com</div>
        <a class="sidebar-user-logout" onclick="logout()">Sign out</a>
      </div>
    </div>
  </div>
</aside>

<!-- ===== MAIN CONTENT ===== -->
<div class="main-wrapper">

  <!-- Top Bar -->
  <header class="top-bar">
    <span class="top-bar-title" id="topBarTitle">GCS Parquet Explorer</span>
    <span class="status-badge pending" id="authStatus">
      <span class="dot"></span>
      <span>Connecting...</span>
    </span>
  </header>

  <!-- Content Scroll Area -->
  <div class="content-scroll">

    <!-- === GCS PARQUET EXPLORER === -->
    <div class="content-section active" id="section-gcs">

      <!-- Breadcrumb -->
      <div class="breadcrumb-bar" id="breadcrumb">
        <a>Buckets</a>
      </div>

      <!-- Search & Filter -->
      <div class="search-filter-bar">
        <div class="search-input-wrap">
          <span class="search-icon">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><line x1="21" y1="21" x2="16.65" y2="16.65"/></svg>
          </span>
          <input class="search-input" type="text" id="sidebarSearch" placeholder="Filter files and folders..." oninput="filterSidebar(this.value)" autocomplete="off">
        </div>
      </div>

      <!-- Sort Bar -->
      <div class="sort-bar" id="sortBar" style="display:none">
        <span class="sort-btn active" data-sort="name" onclick="toggleSort('name')">Name</span>
        <span class="sort-btn" data-sort="size" onclick="toggleSort('size')">Size</span>
        <span class="sort-spacer"></span>
      </div>

      <!-- Scrollable content area -->
      <div class="section-scrollable">
        <!-- File List -->
        <div class="file-list" id="fileList">
          <div style="padding:30px;text-align:center;color:#6b7280">Loading...</div>
        </div>

        <!-- Fivetran Columns Toggle (static) -->
        <div id="ftColsBarData" style="display:none;text-align:right;padding:4px 0;flex-shrink:0"></div>
        <!-- Data Table -->
        <div class="data-table-wrap" id="dataTable" style="display:none;">
        </div>
        <div class="data-info-bar" id="dataInfo" style="display:none;">
        </div>
      </div>

    </div>

    <!-- === GOOGLE POLARIS === -->
    <div class="content-section polaris-section" id="section-google">

      <div class="connect-area">
        <button class="btn btn-green" id="btnConnectGcs" onclick="connectCloud('gcs')">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
          Connect to Google Polaris
        </button>
        <span class="status-badge disconnected" id="cloudConnectStatusGcs">
          <span class="dot"></span>
          <span>Not connected</span>
        </span>
      </div>

      <div class="polaris-msg" id="polarisMsgGcs"></div>

      <!-- Credentials -->
      <div class="cred-panel" id="credPanelGcs">
        <div class="cred-panel-toggle" onclick="toggleCredPanel('Gcs')">
          <span>Change Credentials</span>
          <span class="arrow">&#9660;</span>
        </div>
        <div class="cred-panel-body" id="credBodyGcs">
          <div class="cred-field">
            <label>Endpoint URL</label>
            <input type="text" id="credGcsEndpoint" placeholder="https://..." autocomplete="off">
          </div>
          <div class="cred-field">
            <label>Catalog Name</label>
            <input type="text" id="credGcsCatalog" placeholder="my-catalog" autocomplete="off">
          </div>
          <div class="cred-field">
            <label>Client ID</label>
            <input type="text" id="credGcsClientId" placeholder="client_id" autocomplete="off">
          </div>
          <div class="cred-field">
            <label>Client Secret</label>
            <input type="password" id="credGcsClientSecret" placeholder="client_secret" autocomplete="off">
          </div>
          <button class="btn btn-primary btn-sm" onclick="saveCredentials('gcs')">Save Credentials</button>
          <div class="cred-msg" id="credGcsMsg"></div>
        </div>
      </div>

      <div class="connected-catalogs-bar" id="connectedCatalogsGcs">
        <span>Connected catalogs:</span>
      </div>

      <div class="polaris-browser" id="polarisBrowserGcs" style="display:none">
        <!-- Namespaces -->
        <div class="polaris-col">
          <div class="polaris-col-header">
            <span class="polaris-col-title">Namespaces</span>
            <span class="polaris-col-count" id="polarisNsCountGcs">0</span>
          </div>
          <div class="polaris-col-search">
            <input type="text" id="polarisNsSearchGcs" placeholder="Search namespaces..." oninput="filterPolarisList('polarisNamespacesGcs', this.value)" autocomplete="off">
          </div>
          <div class="polaris-col-sort">
            <span>Sort:</span>
            <span class="sort-toggle" id="polarisNsSortBtnGcs" onclick="sortPolarisNs()">A-Z</span>
          </div>
          <div class="polaris-col-list" id="polarisNamespacesGcs">
          </div>
        </div>
        <!-- Tables -->
        <div class="polaris-col">
          <div class="polaris-col-header">
            <span class="polaris-col-title">Tables</span>
            <span class="polaris-col-count" id="polarisTblCountGcs">0</span>
          </div>
          <div class="polaris-ns-label" id="polarisNsLabelGcs">Select a namespace</div>
          <div class="polaris-col-search">
            <input type="text" id="polarisTblSearchGcs" placeholder="Search tables..." oninput="filterPolarisList('polarisTablesGcs', this.value)" autocomplete="off">
          </div>
          <div class="polaris-col-sort">
            <span>Sort:</span>
            <span class="sort-toggle" id="polarisTblSortBtnGcs" onclick="sortPolarisTbl()">A-Z</span>
          </div>
          <div class="polaris-col-list" id="polarisTablesGcs">
          </div>
        </div>
      </div>
    </div>

    <!-- === AZURE POLARIS === -->
    <div class="content-section polaris-section" id="section-azure">

      <div class="connect-area">
        <button class="btn btn-blue" id="btnConnectAzure" onclick="connectCloud('azure')">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
          Connect to Azure Polaris
        </button>
        <span class="status-badge disconnected" id="cloudConnectStatusAzure">
          <span class="dot"></span>
          <span>Not connected</span>
        </span>
      </div>

      <div class="polaris-msg" id="polarisMsgAzure"></div>

      <div class="cred-panel" id="credPanelAzure">
        <div class="cred-panel-toggle" onclick="toggleCredPanel('Azure')">
          <span>Change Credentials</span>
          <span class="arrow">&#9660;</span>
        </div>
        <div class="cred-panel-body" id="credBodyAzure">
          <div class="cred-field">
            <label>Endpoint URL</label>
            <input type="text" id="credAzureEndpoint" placeholder="https://..." autocomplete="off">
          </div>
          <div class="cred-field">
            <label>Catalog Name</label>
            <input type="text" id="credAzureCatalog" placeholder="my-catalog" autocomplete="off">
          </div>
          <div class="cred-field">
            <label>Client ID</label>
            <input type="text" id="credAzureClientId" placeholder="client_id" autocomplete="off">
          </div>
          <div class="cred-field">
            <label>Client Secret</label>
            <input type="password" id="credAzureClientSecret" placeholder="client_secret" autocomplete="off">
          </div>
          <button class="btn btn-primary btn-sm" onclick="saveCredentials('azure')">Save Credentials</button>
          <div class="cred-msg" id="credAzureMsg"></div>
        </div>
      </div>

      <div class="connected-catalogs-bar" id="connectedCatalogsAzure">
        <span>Connected catalogs:</span>
      </div>

      <div class="polaris-browser" id="polarisBrowserAzure" style="display:none">
        <div class="polaris-col">
          <div class="polaris-col-header">
            <span class="polaris-col-title">Namespaces</span>
            <span class="polaris-col-count" id="polarisNsCountAzure">0</span>
          </div>
          <div class="polaris-col-search">
            <input type="text" id="polarisNsSearchAzure" placeholder="Search namespaces..." oninput="filterPolarisList('polarisNamespacesAzure', this.value)" autocomplete="off">
          </div>
          <div class="polaris-col-sort">
            <span>Sort:</span>
            <span class="sort-toggle" id="polarisNsSortBtnAzure" onclick="sortPolarisNs()">A-Z</span>
          </div>
          <div class="polaris-col-list" id="polarisNamespacesAzure">
          </div>
        </div>
        <div class="polaris-col">
          <div class="polaris-col-header">
            <span class="polaris-col-title">Tables</span>
            <span class="polaris-col-count" id="polarisTblCountAzure">0</span>
          </div>
          <div class="polaris-ns-label" id="polarisNsLabelAzure">Select a namespace</div>
          <div class="polaris-col-search">
            <input type="text" id="polarisTblSearchAzure" placeholder="Search tables..." oninput="filterPolarisList('polarisTablesAzure', this.value)" autocomplete="off">
          </div>
          <div class="polaris-col-sort">
            <span>Sort:</span>
            <span class="sort-toggle" id="polarisTblSortBtnAzure" onclick="sortPolarisTbl()">A-Z</span>
          </div>
          <div class="polaris-col-list" id="polarisTablesAzure">
          </div>
        </div>
      </div>
    </div>

    <!-- === AWS POLARIS === -->
    <div class="content-section polaris-section" id="section-aws">

      <div class="connect-area">
        <button class="btn btn-orange" id="btnConnectAws" onclick="connectCloud('aws')">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>
          Connect to AWS Polaris
        </button>
        <span class="status-badge disconnected" id="cloudConnectStatusAws">
          <span class="dot"></span>
          <span>Not connected</span>
        </span>
      </div>

      <div class="polaris-msg" id="polarisMsgAws"></div>

      <div class="cred-panel" id="credPanelAws">
        <div class="cred-panel-toggle" onclick="toggleCredPanel('Aws')">
          <span>Change Credentials</span>
          <span class="arrow">&#9660;</span>
        </div>
        <div class="cred-panel-body" id="credBodyAws">
          <div class="cred-field">
            <label>Endpoint URL</label>
            <input type="text" id="credAwsEndpoint" placeholder="https://..." autocomplete="off">
          </div>
          <div class="cred-field">
            <label>Catalog Name</label>
            <input type="text" id="credAwsCatalog" placeholder="my-catalog" autocomplete="off">
          </div>
          <div class="cred-field">
            <label>Client ID</label>
            <input type="text" id="credAwsClientId" placeholder="client_id" autocomplete="off">
          </div>
          <div class="cred-field">
            <label>Client Secret</label>
            <input type="password" id="credAwsClientSecret" placeholder="client_secret" autocomplete="off">
          </div>
          <button class="btn btn-primary btn-sm" onclick="saveCredentials('aws')">Save Credentials</button>
          <div class="cred-msg" id="credAwsMsg"></div>
        </div>
      </div>

      <div class="connected-catalogs-bar" id="connectedCatalogsAws">
        <span>Connected catalogs:</span>
      </div>

      <div class="polaris-browser" id="polarisBrowserAws" style="display:none">
        <div class="polaris-col">
          <div class="polaris-col-header">
            <span class="polaris-col-title">Namespaces</span>
            <span class="polaris-col-count" id="polarisNsCountAws">0</span>
          </div>
          <div class="polaris-col-search">
            <input type="text" id="polarisNsSearchAws" placeholder="Search namespaces..." oninput="filterPolarisList('polarisNamespacesAws', this.value)" autocomplete="off">
          </div>
          <div class="polaris-col-sort">
            <span>Sort:</span>
            <span class="sort-toggle" id="polarisNsSortBtnAws" onclick="sortPolarisNs()">A-Z</span>
          </div>
          <div class="polaris-col-list" id="polarisNamespacesAws">
          </div>
        </div>
        <div class="polaris-col">
          <div class="polaris-col-header">
            <span class="polaris-col-title">Tables</span>
            <span class="polaris-col-count" id="polarisTblCountAws">0</span>
          </div>
          <div class="polaris-ns-label" id="polarisNsLabelAws">Select a namespace</div>
          <div class="polaris-col-search">
            <input type="text" id="polarisTblSearchAws" placeholder="Search tables..." oninput="filterPolarisList('polarisTablesAws', this.value)" autocomplete="off">
          </div>
          <div class="polaris-col-sort">
            <span>Sort:</span>
            <span class="sort-toggle" id="polarisTblSortBtnAws" onclick="sortPolarisTbl()">A-Z</span>
          </div>
          <div class="polaris-col-list" id="polarisTablesAws">
          </div>
        </div>
      </div>
    </div>

    <!-- === DOCUMENTATION === -->
    <div class="content-section" id="section-docs">
      <div class="doc-content" id="docContent">
        <h2>GCS Parquet Explorer AND Iceberg Table Explorer</h2>
        <p style="color:#6b7280;font-style:italic">Engineered by the Fivetran SAP Specialist Team</p>

        <h3>What Is This?</h3>
        <p>A web-based data exploration tool for browsing and querying SAP CDS view data across multiple cloud providers. It connects to <b>Google Cloud Storage</b> (raw Parquet files) and <b>Fivetran Polaris</b> (Apache Iceberg catalogs on GCS, Azure, and AWS). All queries run through <b>DuckDB</b>, an in-memory SQL engine.</p>

        <h3>Getting Started</h3>
        <ol>
          <li><b>Open</b> &mdash; Navigate to this URL in your browser. You will see a clean, light-themed login screen.</li>
          <li><b>Sign in</b> &mdash; Enter your <b>Fivetran email address</b>. This email is used to authenticate with Google Cloud, Azure, and AWS storage backends.</li>
          <li><b>Browse</b> &mdash; Once signed in, the sidebar on the left shows navigation sections. Use <b>GCS Parquet Explorer</b> to browse bucket contents, or the <b>Polaris Catalog</b> sections to connect to Iceberg catalogs.</li>
        </ol>

        <h3>Layout &amp; Navigation</h3>
        <p>The application uses a Fivetran dashboard-style layout with a light sidebar, a main content area, and a resizable SQL panel.</p>
        <ul>
          <li><b>Sidebar</b> &mdash; A white sidebar on the left with the Fivetran logo at the top. Navigation items include GCS Parquet Explorer, three Polaris catalog sections (Google, Azure, AWS), and Documentation. The <b>Restart Application</b> button and user sign-out are at the bottom.</li>
          <li><b>Fixed headers</b> &mdash; Breadcrumbs, search filters, and sort controls stay pinned at the top of each section. Only the file list, data tables, and Polaris browser columns scroll.</li>
          <li><b>Resizable SQL panel</b> &mdash; A drag splitter bar sits between the content area and the SQL editor. Drag it up or down to resize the SQL panel. The splitter turns blue when hovered or dragged.</li>
          <li><b>Auto-expand</b> &mdash; When a query returns results, the SQL panel automatically expands to 60% of the viewport height so you can see the data.</li>
        </ul>

        <h3>Browsing GCS Parquet Data</h3>
        <p>The GCS Explorer section lets you navigate raw Parquet files stored in Google Cloud Storage buckets.</p>
        <ul>
          <li><b>Navigate</b> &mdash; Click directories to drill down. The breadcrumb at the top shows your current path; click any segment to jump back.</li>
          <li><b>Home</b> &mdash; Click the house icon in the breadcrumb to return to the bucket list and switch buckets.</li>
          <li><b>Filter</b> &mdash; Type in the filter box to narrow down directories and files by name.</li>
          <li><b>Sort</b> &mdash; Click <b>Name</b> or <b>Size</b> buttons to sort. Click again to reverse.</li>
          <li><b>Load a table</b> &mdash; Click a <code>.parquet</code> file to load it, or click the <b>Load all parquet data</b> button to load all files in a directory as one table.</li>
          <li><b>View data</b> &mdash; After loading, a data table appears below the file list. Click any column header to sort. Click any truncated cell to open it in a full-screen popup.</li>
        </ul>

        <h3>Fivetran Column Visibility</h3>
        <p>Fivetran internal columns are automatically managed for a cleaner data view.</p>
        <ul>
          <li><b>Hidden by default</b> &mdash; The columns <code>extracted_at</code>, <code>_fivetran_synced</code>, <code>_fivetran_deleted</code>, and <code>_fivetran_sap_archived</code> are hidden from all table results by default, both in the GCS data view and SQL query results.</li>
          <li><b>Show Fivetran Columns</b> &mdash; When hidden columns exist, a <b>Show Fivetran Columns</b> button appears above the table (pinned, does not scroll with the data). Click it to reveal the hidden columns; click again to hide them.</li>
          <li><b>Always hidden</b> &mdash; The <code>_fivetran_id</code> column is always hidden and never displayed, even when "Show Fivetran Columns" is toggled on. This column is a Fivetran internal identifier that is not useful for data analysis.</li>
        </ul>

        <h3>Polaris Iceberg Catalogs (Multi-Cloud)</h3>
        <p>The <b>Google Polaris</b>, <b>Azure Polaris</b>, and <b>AWS Polaris</b> sections in the sidebar each connect to Fivetran Polaris, which manages Apache Iceberg tables across three cloud providers.</p>
        <ul>
          <li><b>One-click connect</b> &mdash; Click the connect button in any Polaris section. This connects the Polaris catalog <i>and</i> sets up storage credentials in a single step.</li>
          <li><b>Browse namespaces</b> &mdash; After connecting, namespaces (schemas) appear on the left. Click one to see its tables on the right. Both panels fill the available space and scroll independently.</li>
          <li><b>Query a table</b> &mdash; Click a table name to populate the SQL editor, or click the play icon to run a <code>SELECT * LIMIT 100</code> immediately.</li>
          <li><b>Multiple catalogs</b> &mdash; You can connect GCS, Azure, and AWS at the same time. Each section manages its own catalog connection independently.</li>
          <li><b>Cross-cloud joins</b> &mdash; All catalogs are in the same DuckDB instance, so you can join data across clouds: <code>SELECT * FROM gcs.ns.tbl JOIN ts_adls_destination_demo.ns.tbl USING(id)</code></li>
          <li><b>Search &amp; Sort</b> &mdash; Use the search boxes above namespace and table lists to filter. Click sort toggles to sort.</li>
          <li><b>Disconnect</b> &mdash; Click the &times; on a connected catalog tag to detach it.</li>
        </ul>

        <h3>Managing Polaris Credentials</h3>
        <p>Each Polaris section has a <b>Change Credentials</b> panel that lets you view and update the OAuth client credentials used to connect to that provider's Polaris catalog.</p>
        <ul>
          <li><b>Open the panel</b> &mdash; Click <b>Change Credentials</b> in any Polaris section. Fields appear for endpoint URL, catalog name, client ID, and client secret.</li>
          <li><b>View current settings</b> &mdash; Fields show the current values. The client secret is masked for security.</li>
          <li><b>Update credentials</b> &mdash; Edit any field and click <b>Save Credentials</b>. To keep the existing client secret, leave the secret field empty.</li>
          <li><b>Reconnect</b> &mdash; After saving new credentials, click the connect button to reconnect with the updated settings.</li>
          <li><b>Runtime only</b> &mdash; Credential changes persist in memory until the server restarts.</li>
        </ul>

        <h3>SQL Queries</h3>
        <p>The SQL editor at the bottom of the screen is always available, regardless of which section you are viewing.</p>
        <ul>
          <li><b>Run a query</b> &mdash; Type your SQL and press <b>Ctrl+Enter</b> (or click Run).</li>
          <li><b>Auto-load</b> &mdash; If you reference a GCS table that is not loaded yet, the tool automatically downloads it from the current browse path and retries the query.</li>
          <li><b>Autocomplete</b> &mdash; Start typing after <code>FROM</code> or <code>JOIN</code> to see table name suggestions. All loaded GCS tables and connected Polaris Iceberg tables are included.</li>
          <li><b>Table addressing</b> &mdash; GCS tables are referenced by name (e.g., <code>dd02l_all</code>). Polaris tables use three-part names: <code>catalog.namespace.table</code>.</li>
          <li><b>Loaded tables</b> &mdash; Tags in the SQL toggle bar show currently loaded tables. Click a tag to populate a quick query.</li>
          <li><b>Toggle panel</b> &mdash; Click the SQL toggle bar to expand or collapse the SQL editor.</li>
          <li><b>Resize panel</b> &mdash; Drag the splitter bar above the SQL panel to adjust its height. Minimum 80px, maximum 70% of the viewport.</li>
        </ul>

        <h3>Cell Detail Popup</h3>
        <p>When a cell value is too long to display in the table, it is truncated and shown in blue. Click it to open a <b>full-screen popup</b> (up to 80% of the viewport) showing the complete value. Click the gray overlay to close.</p>

        <h3>GCS Parquet vs Polaris Iceberg</h3>
        <ul>
          <li><b>GCS Parquet</b> &mdash; Reads raw Parquet files directly from a GCS bucket. Best for ad-hoc file inspection, exploring raw data exports, and debugging.</li>
          <li><b>Polaris Iceberg</b> &mdash; Reads through the Iceberg catalog with snapshot management, schema evolution, and partition pruning. Best for querying production Fivetran destination data across all three clouds.</li>
        </ul>

        <h3>How It Works</h3>
        <ul>
          <li>The server runs on an internal VM with HTTPS. All data stays in-memory on the server.</li>
          <li><b>DuckDB</b> is the SQL engine &mdash; tables are registered in-memory and queries execute locally on the server.</li>
          <li><b>GCS browsing</b> uses the Google Cloud Storage API with application default credentials configured on the server.</li>
          <li><b>Polaris catalogs</b> connect via the Iceberg REST protocol. Namespace and table listings use the Polaris REST API directly for fast response times.</li>
          <li><b>Storage access</b> for Iceberg data files uses DuckDB extensions (httpfs, azure, iceberg) with credential chain authentication.</li>
          <li><b>Memory management</b> &mdash; When server memory reaches 85%, the oldest loaded table is automatically evicted to free space.</li>
        </ul>

        <h3>Restart Application</h3>
        <p>The <b>Restart Application</b> button at the bottom of the sidebar sends a restart command to the server. This clears all in-memory tables and reloads credentials from environment variables. The page will automatically reconnect after the restart completes.</p>

        <h3>Deployment Note</h3>
        <p>The source code in the <b>GitHub repository</b> does not contain any secrets. All credentials (Polaris OAuth client IDs/secrets, login password, AWS keys) are loaded from <b>environment variables</b> at runtime.</p>
        <ul>
          <li>On the server, credentials are stored in <code>/usr/sap/gcs_explorer.env</code> and loaded by systemd via <code>EnvironmentFile</code>.</li>
          <li>When deploying from the repo to a <b>new server</b>, copy <code>server/gcs_explorer.env.example</code> to <code>/usr/sap/gcs_explorer.env</code> and fill in real values.</li>
          <li>You can also update credentials at runtime using the <b>Change Credentials</b> panel in each Polaris section (changes persist in memory until restart).</li>
        </ul>

        <h3>Keyboard Shortcuts</h3>
        <p><code>Ctrl+Enter</code> &mdash; Run SQL query</p>
        <p><code>Tab</code> &mdash; Accept autocomplete suggestion</p>
        <p><code>Escape</code> &mdash; Close popups and overlays</p>

        <h3>Troubleshooting</h3>
        <ul>
          <li><b>Cannot load Polaris tables</b> &mdash; Make sure you clicked the connect button in the appropriate Polaris section (Google / Azure / AWS) first.</li>
          <li><b>Storage authentication error</b> &mdash; The connect buttons handle storage auth automatically. If it still fails, the server administrator may need to refresh credentials on the server.</li>
          <li><b>Query returns no results</b> &mdash; Check the table name and namespace. Use the Polaris section to browse and verify available tables.</li>
          <li><b>Slow query on large tables</b> &mdash; Add a <code>LIMIT</code> clause or filter with <code>WHERE</code> to reduce the data scanned.</li>
          <li><b>Table not found in SQL</b> &mdash; For GCS tables, navigate to the directory in the GCS Explorer first, then the auto-loader can find it. For Polaris tables, use the full three-part name.</li>
        </ul>
      </div>
    </div>

  </div><!-- /content-scroll -->

  <!-- ===== DRAG SPLITTER ===== -->
  <div class="sql-drag-splitter" id="sqlDragSplitter"></div>

  <!-- ===== SQL EDITOR (always visible) ===== -->
  <div class="sql-panel" id="sqlPanel">
    <div class="sql-toggle-bar" id="sqlToggleBar" onclick="toggleSqlPanel()">
      <span class="sql-toggle-label">SQL Query</span>
      <span class="sql-toggle-arrow">&#9650;</span>
      <div class="sql-loaded-tags" id="loadedTables">
      </div>
    </div>
    <div class="sql-body" id="sqlBody">
      <div class="sql-editor-row">
        <div class="sql-textarea-wrap">
          <textarea class="sql-textarea" id="sqlInput" placeholder="SELECT * FROM table_name LIMIT 100" spellcheck="false"></textarea>
          <div class="ac-list" id="acList">
          </div>
        </div>
        <div class="sql-actions">
          <button class="btn-run" onclick="runQuery()">Run &#9654;</button>
        </div>
      </div>
      <div class="sql-hint" id="sqlHint">Tip: Use Tab for autocomplete. Ctrl+Enter to run.</div>
      <div id="ftColsBarSql" style="display:none;text-align:right;padding:2px 16px;flex-shrink:0"></div>
      <div class="sql-results-area" id="sqlResults">
      </div>
      <div class="sql-info-bar" id="sqlInfo">
      </div>
    </div>
  </div>

</div><!-- /main-wrapper -->

<!-- Hidden Polaris form fields (used by loadPreset/connectCloud) -->
<input type="hidden" id="polarisAlias" value="">
<input type="hidden" id="polarisEndpoint" value="">
<input type="hidden" id="polarisCatalog" value="">
<input type="hidden" id="polarisClientId" value="">
<input type="hidden" id="polarisClientSecret" value="">

<!-- Auth error overlay -->
<div class="auth-overlay" id="authOverlay">
  <div class="auth-box">
    <h2>Authentication Required</h2>
    <p>Your session has expired or authentication failed. Please sign in again.</p>
    <button class="btn btn-primary" id="authBtn" onclick="retryAuth()">Sign In</button>
    <div class="auth-msg" id="authMsg"></div>
  </div>
</div>

<!-- Cell popup for truncated values -->
<div class="cell-popup" id="cellPopup"></div>

<script>
// ===== PROVIDER SUFFIX HELPERS =====
function getProviderSuffix(alias) {
  if (!alias) return 'Gcs';
  var a = alias.toLowerCase();
  if (a === 'gcs' || a === 'obeisance_plaintive') return 'Gcs';
  if (a === 'azure' || a.indexOf('adls') >= 0 || a === 'log_pseudo' || a === 'ts_adls_destination_demo') return 'Azure';
  if (a === 'aws' || a === 'surfacing_caramel') return 'Aws';
  return 'Gcs';
}

function polarisEl(base, suffix) {
  return document.getElementById(base + (suffix || getProviderSuffix(activeCatalogAlias)));
}

function providerKeyToSuffix(provider) {
  if (provider === 'gcs') return 'Gcs';
  if (provider === 'azure') return 'Azure';
  if (provider === 'aws') return 'Aws';
  return 'Gcs';
}

// ===== NAVIGATION =====
var navTitles = {
  gcs: 'GCS Parquet Explorer',
  google: 'Google Polaris Catalog',
  azure: 'Azure Polaris Catalog',
  aws: 'AWS Polaris Catalog',
  docs: 'Documentation'
};

function switchNav(section) {
  if (section === 'restart') {
    restartServer();
    return;
  }
  // Update nav items
  var items = document.querySelectorAll('.nav-item');
  for (var i = 0; i < items.length; i++) {
    var ds = items[i].getAttribute('data-section');
    if (ds === section) items[i].classList.add('active');
    else items[i].classList.remove('active');
  }
  // Show matching content section
  var sections = document.querySelectorAll('.content-section');
  for (var j = 0; j < sections.length; j++) {
    if (sections[j].id === 'section-' + section) sections[j].classList.add('active');
    else sections[j].classList.remove('active');
  }
  // Update top bar title
  var title = navTitles[section] || section;
  document.getElementById('topBarTitle').textContent = title;
}

function logout() {
  window.location.href = BASE_PATH + '/logout';
}

function toggleSqlPanel() {
  var body = document.getElementById('sqlBody');
  var bar = document.getElementById('sqlToggleBar');
  if (body.classList.contains('collapsed')) {
    body.classList.remove('collapsed');
    bar.classList.remove('collapsed');
  } else {
    body.classList.add('collapsed');
    bar.classList.add('collapsed');
  }
}

// ===== DRAG SPLITTER =====
(function() {
  var splitter = document.getElementById('sqlDragSplitter');
  var sqlBody = document.getElementById('sqlBody');
  var dragging = false;
  var startY = 0;
  var startH = 0;

  splitter.addEventListener('mousedown', function(e) {
    if (sqlBody.classList.contains('collapsed')) return;
    dragging = true;
    startY = e.clientY;
    startH = sqlBody.offsetHeight;
    splitter.classList.add('dragging');
    document.body.style.cursor = 'ns-resize';
    document.body.style.userSelect = 'none';
    e.preventDefault();
  });

  document.addEventListener('mousemove', function(e) {
    if (!dragging) return;
    var delta = startY - e.clientY;
    var newH = Math.max(80, Math.min(startH + delta, window.innerHeight * 0.7));
    sqlBody.style.height = newH + 'px';
    sqlBody.style.transition = 'none';
  });

  document.addEventListener('mouseup', function() {
    if (!dragging) return;
    dragging = false;
    splitter.classList.remove('dragging');
    document.body.style.cursor = '';
    document.body.style.userSelect = '';
    sqlBody.style.transition = '';
  });
})();

// ===== GLOBAL STATE =====
var currentPrefix = '';
var allRows = null;
var browsedTables = [];
var polarisTables = [];
var lastBrowseItems = [];
var lastBrowsePrefix = '';
var lastHasDataDir = false;
var sortField = 'name';
var sortAsc = true;

var BASE_PATH = '/datalake_reader';
function api(endpoint, params) {
  params = params || {};
  var url = new URL(BASE_PATH + endpoint, location.origin);
  var keys = Object.keys(params);
  for (var i = 0; i < keys.length; i++) url.searchParams.append(keys[i], params[keys[i]]);
  return fetch(url).then(function(r) { return r.json(); });
}

var currentBucket = '';

// ===== INIT =====
function init() {
  // Show logged-in user
  api('/api/whoami').then(function(who) {
    if (who.status === 'ok') {
      document.getElementById('userEmail').textContent = who.email;
      // Set avatar initial
      var avatar = document.querySelector('.sidebar-user-avatar');
      if (avatar && who.email) avatar.textContent = who.email.charAt(0).toUpperCase();
    }
  }).catch(function(){});

  api('/api/init').then(function(r) {
    var el = document.getElementById('authStatus');
    if (r.status === 'ok') {
      el.className = 'status-badge connected';
      el.innerHTML = '<span class="dot"></span><span>Connected</span>';
      currentBucket = r.last_bucket || '';
      if (r.last_prefix !== undefined && r.last_prefix !== null) {
        browse(r.last_prefix, r.last_bucket);
      } else {
        browse('sap_cds_views/', 'sap_cds_dbt');
      }
    } else {
      el.className = 'status-badge error';
      el.innerHTML = '<span class="dot"></span><span>Error</span>';
      document.getElementById('fileList').innerHTML = '<div style="padding:30px 20px;text-align:left">'
        + '<div style="color:#ef4444;font-size:15px;margin-bottom:12px">GCS connection failed</div>'
        + '<div style="color:#6b7280;margin-bottom:16px;font-size:13px">' + escHtml(r.message) + '</div>'
        + '<button class="btn btn-primary" onclick="runAuth()" id="authBtn">Authenticate with Google Cloud</button>'
        + '<div id="authMsg" style="margin-top:12px;font-size:13px;color:#6b7280"></div>'
        + '</div>';
    }
  });
}

// ===== BROWSING =====
function browseBuckets() {
  document.getElementById('sidebarSearch').value = '';
  document.getElementById('fileList').innerHTML = '<div style="padding:30px;text-align:center;color:#6b7280">Loading buckets...</div>';
  api('/api/buckets').then(function(r) {
    if (r.status !== 'ok') {
      document.getElementById('fileList').innerHTML = '<div style="padding:30px;text-align:center;color:#6b7280">' + r.message + '</div>';
      return;
    }
    currentPrefix = '';
    currentBucket = '';
    document.getElementById('breadcrumb').innerHTML = '<a style="color:#f59e0b;cursor:default">All Buckets</a>';
    var html = '';
    for (var i = 0; i < r.buckets.length; i++) {
      var b = r.buckets[i];
      html += '<div class="file-item" onclick="browse(\'\', \'' + b.name + '\')"><span class="file-icon">&#128230;</span><span class="file-name" style="color:#0073FF">' + escHtml('gs://' + b.name) + '</span></div>';
    }
    if (!r.buckets.length) html = '<div style="padding:30px;text-align:center;color:#6b7280">No accessible buckets</div>';
    document.getElementById('fileList').innerHTML = html;
  });
}

function browse(prefix, bucket) {
  if (prefix === undefined) prefix = currentPrefix;
  if (bucket) currentBucket = bucket;
  currentPrefix = prefix;
  document.getElementById('sidebarSearch').value = '';
  document.getElementById('fileList').innerHTML = '<div style="padding:30px;text-align:center;color:#6b7280">Loading...</div>';
  var params = { prefix: prefix };
  if (currentBucket) params.bucket = currentBucket;
  api('/api/ls', params).then(function(r) {
    if (r.status !== 'ok') {
      document.getElementById('fileList').innerHTML = '<div style="padding:30px;text-align:center;color:#6b7280">' + r.message + '</div>';
      return;
    }
    if (r.bucket) currentBucket = r.bucket;
    updateBreadcrumb(prefix);

    lastBrowseItems = r.items;
    lastBrowsePrefix = prefix;
    lastHasDataDir = false;
    for (var i = 0; i < r.items.length; i++) { if (r.items[i].name === 'data/') { lastHasDataDir = true; break; } }
    browsedTables = [];
    for (var j = 0; j < r.items.length; j++) {
      var it = r.items[j];
      if (it.is_dir && it.name !== 'data/' && it.name !== '_delta_log/') browsedTables.push(it.name.replace('/', ''));
    }

    var hasStats = false;
    for (var k = 0; k < r.items.length; k++) { if (r.items[k].is_dir && r.items[k].file_count) { hasStats = true; break; } }
    document.getElementById('sortBar').style.display = hasStats ? 'flex' : 'none';

    renderFileList();
  });
}

function renderFileList() {
  var prefix = lastBrowsePrefix;
  var items = lastBrowseItems.slice();

  var dirs = [];
  var files = [];
  for (var i = 0; i < items.length; i++) {
    if (items[i].is_dir) dirs.push(items[i]);
    else files.push(items[i]);
  }
  dirs.sort(function(a, b) {
    var cmp = 0;
    if (sortField === 'files') cmp = (a.file_count || 0) - (b.file_count || 0);
    else if (sortField === 'size') cmp = (a.size || 0) - (b.size || 0);
    else cmp = a.name.localeCompare(b.name);
    return sortAsc ? cmp : -cmp;
  });

  var html = '';
  if (prefix) {
    var parts = prefix.replace(/\/+$/, '').split('/');
    if (parts.length > 1) {
      var parent = parts.slice(0, -1).join('/') + '/';
      html += '<div class="file-item" onclick="browse(\'' + parent + '\')"><span class="file-icon">&#11014;</span><span class="file-name">..</span></div>';
    } else {
      html += '<div class="file-item" onclick="browse(\'\')"><span class="file-icon">&#11014;</span><span class="file-name">..</span></div>';
    }
  } else if (currentBucket) {
    html += '<div class="file-item" onclick="browseBuckets()"><span class="file-icon">&#11014;</span><span class="file-name">.. (all buckets)</span></div>';
  }

  if (lastHasDataDir) {
    html += '<div class="file-item" style="background:#fffbeb;border-left:3px solid #f59e0b"><span class="file-icon">&#9889;</span><span class="file-name" style="color:#b45309;font-weight:600">Load all parquet data</span><button class="btn btn-orange btn-sm" onclick="event.stopPropagation();loadDir(\'' + prefix + '\')" style="margin-left:auto">Load</button></div>';
  }

  for (var d = 0; d < dirs.length; d++) {
    var dir = dirs[d];
    var meta = '';
    if (dir.file_count) meta += dir.file_count + ' file' + (dir.file_count > 1 ? 's' : '') + '  ';
    if (dir.size) meta += formatSize(dir.size);
    html += '<div class="file-item" onclick="browse(\'' + dir.path + '\')"><span class="file-icon">&#128193;</span><span class="file-name" style="color:#0073FF">' + escHtml(dir.name) + '</span><span class="file-meta">' + meta + '</span></div>';
  }
  for (var f = 0; f < files.length; f++) {
    var file = files[f];
    var isPq = file.name.indexOf('.parquet') >= 0;
    var isJson = file.name.indexOf('.json') >= 0;
    var icon = isPq ? '&#128202;' : isJson ? '&#128196;' : '&#128206;';
    var size = file.size ? formatSize(file.size) : '';
    var action = isPq ? "loadParquet('" + file.path + "')" : "loadText('" + file.path + "')";
    html += '<div class="file-item" onclick="' + action + '"><span class="file-icon">' + icon + '</span><span class="file-name">' + escHtml(file.name) + '</span><span class="file-meta">' + size + '</span></div>';
  }
  if (!items.length) html = '<div style="padding:30px;text-align:center;color:#6b7280">Empty directory</div>';
  document.getElementById('fileList').innerHTML = html;
}

function toggleSort(field) {
  if (sortField === field) { sortAsc = !sortAsc; }
  else { sortField = field; sortAsc = (field === 'name'); }
  var btns = document.querySelectorAll('.sort-bar .sort-btn');
  for (var i = 0; i < btns.length; i++) {
    var f = btns[i].getAttribute('data-sort');
    if (f === sortField) btns[i].classList.add('active');
    else btns[i].classList.remove('active');
  }
  renderFileList();
  var q = document.getElementById('sidebarSearch').value;
  if (q) filterSidebar(q);
}

function updateBreadcrumb(prefix) {
  var html = '<a onclick="browseBuckets()" title="All buckets">&#127968;</a>';
  if (currentBucket) {
    html += '<span class="sep">/</span>';
    html += '<a onclick="browse(\'\', \'' + currentBucket + '\')" title="gs://' + currentBucket + '">' + currentBucket + '</a>';
  }
  if (prefix) {
    var parts = prefix.replace(/\/+$/, '').split('/');
    var acc = '';
    for (var i = 0; i < parts.length; i++) {
      acc += parts[i] + '/';
      html += '<span class="sep">/</span>';
      html += '<a onclick="browse(\'' + acc + '\')">' + parts[i] + '</a>';
    }
  }
  document.getElementById('breadcrumb').innerHTML = html;
}

// ===== DATA LOADING =====
function loadParquet(path) {
  var dt = document.getElementById('dataTable');
  var di = document.getElementById('dataInfo');
  dt.style.display = 'block';
  di.style.display = 'flex';
  dt.innerHTML = '<div style="padding:20px;text-align:center;color:#6b7280">Loading parquet...</div>';
  di.textContent = '';
  api('/api/parquet', { path: path }).then(function(r) {
    if (r.status !== 'ok') {
      dt.innerHTML = '<div style="padding:20px;color:#ef4444">' + escHtml(r.message) + '</div>';
      return;
    }
    allRows = r.rows;
    renderTable('dataTable', r.columns, r.rows);
    di.textContent = r.total_rows + ' rows | ' + r.columns.length + ' cols | registered as: ' + (r.registered_as || 'N/A');
    updateLoadedTables();
  });
  dt.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function loadDir(prefix) {
  var dt = document.getElementById('dataTable');
  var di = document.getElementById('dataInfo');
  dt.style.display = 'block';
  di.style.display = 'flex';
  dt.innerHTML = '<div style="padding:20px;text-align:center;color:#6b7280">Loading all parquet files...</div>';
  di.textContent = '';
  api('/api/load_dir', { prefix: prefix }).then(function(r) {
    if (r.status !== 'ok') {
      dt.innerHTML = '<div style="padding:20px;color:#ef4444">' + escHtml(r.message) + '</div>';
      return;
    }
    allRows = r.rows;
    renderTable('dataTable', r.columns, r.rows);
    di.textContent = r.total_rows + ' rows | ' + r.columns.length + ' cols | ' + r.files_read + ' files | registered as: ' + r.registered_as;
    updateLoadedTables();
  });
  dt.scrollIntoView({ behavior: 'smooth', block: 'start' });
}

function loadText(path) {
  var dt = document.getElementById('dataTable');
  dt.style.display = 'block';
  dt.innerHTML = '<div style="padding:20px;text-align:center;color:#6b7280">Loading...</div>';
  api('/api/cat', { path: path }).then(function(r) {
    dt.innerHTML = '<pre style="padding:16px;font-family:SF Mono,Consolas,Menlo,monospace;font-size:13px;white-space:pre-wrap;color:#374151;overflow:auto">' + escHtml(r.status === 'ok' ? r.content : r.message) + '</pre>';
  });
}

// ===== SQL =====
function runQuery() {
  var q = document.getElementById('sqlInput').value.trim();
  if (!q) return;
  // Ensure SQL panel is expanded
  var body = document.getElementById('sqlBody');
  if (body.classList.contains('collapsed')) toggleSqlPanel();
  document.getElementById('sqlResults').innerHTML = '<div style="padding:12px;color:#8b949e;text-align:center">Running...</div>';
  document.getElementById('sqlInfo').textContent = '';
  var t0 = Date.now();
  api('/api/sql', { query: q, prefix: currentPrefix }).then(function(r) {
    var ms = Date.now() - t0;
    if (r.status !== 'ok') {
      document.getElementById('sqlResults').innerHTML = '<div style="color:#ef4444;white-space:pre-wrap;text-align:left;padding:12px;font-family:monospace;font-size:12.5px">' + escHtml(r.message) + '</div>';
      document.getElementById('sqlInfo').textContent = 'Error | ' + ms + 'ms';
      return;
    }
    renderTable('sqlResults', r.columns, r.rows);
    document.getElementById('sqlInfo').textContent = r.total_rows + ' rows | ' + r.columns.length + ' cols | ' + ms + 'ms';
    updateLoadedTables();
  });
}

function showTables() {
  api('/api/tables').then(function(r) {
    if (r.status === 'ok' && r.tables.length) {
      document.getElementById('sqlInput').value = '-- Available tables:\n' + r.tables.map(function(t) { return '-- ' + t.name + ' (' + t.rows + ' rows)'; }).join('\n') + '\n\nSELECT * FROM ' + r.tables[0].name + ' LIMIT 10';
    }
  });
}

function updateLoadedTables() {
  api('/api/tables').then(function(r) {
    var el = document.getElementById('loadedTables');
    if (r.status === 'ok' && r.tables.length) {
      var html = '';
      for (var i = 0; i < r.tables.length; i++) {
        var t = r.tables[i];
        html += '<span class="sql-loaded-tag" onclick="setSQL(\'SELECT * FROM ' + t.name + ' LIMIT 100\')" title="' + t.rows + ' rows, ' + t.columns + ' cols" style="cursor:pointer">' + t.name + '</span>';
      }
      el.innerHTML = html;
    } else {
      el.innerHTML = '';
    }
  });
}

function setSQL(q) {
  document.getElementById('sqlInput').value = q;
  var body = document.getElementById('sqlBody');
  if (body.classList.contains('collapsed')) toggleSqlPanel();
}

// ===== TABLE RENDERING =====
// Fivetran internal columns to hide by default
var FIVETRAN_COLS = ['extracted_at', '_fivetran_synced', '_fivetran_deleted', '_fivetran_sap_archived'];
var FIVETRAN_ALWAYS_HIDE = ['_fivetran_id'];
var showFivetranCols = false;

function isFivetranCol(name) {
  return FIVETRAN_COLS.indexOf(name.toLowerCase()) >= 0;
}
function isAlwaysHiddenCol(name) {
  return FIVETRAN_ALWAYS_HIDE.indexOf(name.toLowerCase()) >= 0;
}

function getFtBar(id) {
  if (id === 'sqlResults') return document.getElementById('ftColsBarSql');
  if (id === 'dataTable') return document.getElementById('ftColsBarData');
  return null;
}
function toggleFivetranCols(id) {
  showFivetranCols = !showFivetranCols;
  var el = document.getElementById(id);
  var d = el._data;
  if (d) _renderTableHTML(id, d.columns, d.rows, d.sortCol, d.sortAsc);
}

function renderTable(id, columns, rows) {
  var el = document.getElementById(id);
  if (!columns || !columns.length) { el.innerHTML = '<div style="padding:20px;text-align:center;color:#6b7280">No data</div>'; return; }
  el._data = { columns: columns, rows: rows, sortCol: -1, sortAsc: true };
  _renderTableHTML(id, columns, rows);
  // Auto-expand SQL body to show results — use 60% of viewport
  var body = document.getElementById('sqlBody');
  if (body && rows.length > 0) {
    var targetH = Math.floor(window.innerHeight * 0.6);
    if (body.offsetHeight < targetH) {
      body.style.height = targetH + 'px';
      body.style.transition = 'none';
    }
  }
}

function _renderTableHTML(id, columns, rows, sortCol, sortAsc2) {
  var el = document.getElementById(id);
  // Check if any fivetran columns exist
  var hasFtCols = false;
  for (var fi = 0; fi < columns.length; fi++) {
    if (isFivetranCol(columns[fi])) { hasFtCols = true; break; }
  }
  // Build visible column indices
  var visibleCols = [];
  for (var ci = 0; ci < columns.length; ci++) {
    if (isAlwaysHiddenCol(columns[ci])) continue;
    if (!showFivetranCols && isFivetranCol(columns[ci])) continue;
    visibleCols.push(ci);
  }
  // Render Fivetran toggle button in static bar
  var bar = getFtBar(id);
  if (bar) {
    if (hasFtCols) {
      bar.style.display = 'block';
      bar.innerHTML = '<button onclick="toggleFivetranCols(\'' + id + '\')" style="background:' + (showFivetranCols ? '#0073FF' : 'transparent') + ';color:' + (showFivetranCols ? '#fff' : '#6b7280') + ';border:1px solid ' + (showFivetranCols ? '#0073FF' : '#d1d5db') + ';border-radius:4px;padding:4px 10px;font-size:11.5px;font-weight:600;cursor:pointer;font-family:inherit">' + (showFivetranCols ? 'Hide' : 'Show') + ' Fivetran Columns</button>';
    } else {
      bar.style.display = 'none';
      bar.innerHTML = '';
    }
  }
  var h = '<table><thead><tr><th>#</th>';
  for (var vi = 0; vi < visibleCols.length; vi++) {
    var ci = visibleCols[vi];
    var c = columns[ci];
    var cls = sortCol === ci ? (sortAsc2 ? 'sortable sort-asc' : 'sortable sort-desc') : 'sortable';
    h += '<th class="' + cls + '" onclick="sortTable(\'' + id + '\',' + ci + ')">' + escHtml(c) + '</th>';
  }
  h += '</tr></thead><tbody>';
  for (var i = 0; i < rows.length; i++) {
    h += '<tr><td style="color:#9ca3af">' + (i+1) + '</td>';
    for (var vi2 = 0; vi2 < visibleCols.length; vi2++) {
      var j = visibleCols[vi2];
      var v = rows[i][j] || '';
      var trunc = v.length > 80 ? v.substring(0,80) + '...' : v;
      var click = v.length > 80 ? ' onclick="showCell(\'' + id + '\',' + i + ',' + j + ')" style="cursor:pointer;color:#0073FF"' : '';
      h += '<td' + click + ' title="' + escAttr(v.substring(0,200)) + '">' + escHtml(trunc) + '</td>';
    }
    h += '</tr>';
  }
  h += '</tbody></table>';
  el.innerHTML = h;
}

function sortTable(id, colIdx) {
  var el = document.getElementById(id);
  var d = el._data;
  if (!d) return;
  var asc = (d.sortCol === colIdx) ? !d.sortAsc : true;
  var sorted = d.rows.slice().sort(function(a, b) {
    var va = a[colIdx] || '', vb = b[colIdx] || '';
    var na = parseFloat(va), nb = parseFloat(vb);
    if (!isNaN(na) && !isNaN(nb)) return asc ? na - nb : nb - na;
    return asc ? va.localeCompare(vb) : vb.localeCompare(va);
  });
  d.sortCol = colIdx;
  d.sortAsc = asc;
  d.rows = sorted;
  _renderTableHTML(id, d.columns, sorted, colIdx, asc);
}

function showCell(containerId, row, col) {
  var d = document.getElementById(containerId)._data;
  if (!d) return;
  var val = d.rows[row][col];
  var colName = d.columns[col];
  var ov = document.createElement('div'); ov.className = 'overlay';
  ov.onclick = function() { ov.remove(); popup.remove(); };
  document.body.appendChild(ov);
  var popup = document.createElement('div'); popup.className = 'cell-popup show';
  popup.style.top = '50%'; popup.style.left = '50%'; popup.style.transform = 'translate(-50%,-50%)';
  popup.innerHTML = '<div style="margin-bottom:8px;color:#0073FF;font-weight:600">' + escHtml(colName) + ' (row ' + (row+1) + ')</div>' + escHtml(val);
  document.body.appendChild(popup);
}

// Keep switchTab for internal use
function switchTab(name) {
  // Map old tab names to new nav sections
  if (name === 'polaris') { switchNav('google'); return; }
  if (name === 'docs') { switchNav('docs'); return; }
  if (name === 'data') { switchNav('gcs'); return; }
  // sql: expand SQL panel
  if (name === 'sql') {
    var body = document.getElementById('sqlBody');
    if (body.classList.contains('collapsed')) toggleSqlPanel();
    return;
  }
}

// ===== UTILITY =====
function formatSize(bytes) {
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1048576) return (bytes/1024).toFixed(1) + ' KB';
  return (bytes/1048576).toFixed(1) + ' MB';
}

function filterSidebar(query) {
  var q = query.toLowerCase();
  var items = document.querySelectorAll('#fileList .file-item');
  for (var i = 0; i < items.length; i++) {
    var el = items[i];
    var name = el.querySelector('.file-name');
    if (!name) continue;
    var text = name.textContent.toLowerCase();
    if (text === '..' || text === '.. (all buckets)') { el.style.display = ''; continue; }
    el.style.display = (!q || text.indexOf(q) >= 0) ? '' : 'none';
  }
}

function escHtml(s) { return s ? s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;') : ''; }
function escAttr(s) { return s ? s.replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;') : ''; }

// ===== AUTH =====
function runAuth() {
  var btn = document.getElementById('authBtn');
  var msg = document.getElementById('authMsg');
  btn.disabled = true;
  btn.textContent = 'Authenticating...';
  msg.textContent = 'A browser window should open for Google login. Complete the sign-in there.';
  msg.style.color = '#f59e0b';
  api('/api/auth').then(function(r) {
    if (r.status === 'ok') {
      msg.textContent = r.message;
      msg.style.color = '#22c55e';
      btn.textContent = 'Authenticated!';
      var st = document.getElementById('authStatus');
      st.className = 'status-badge connected';
      st.innerHTML = '<span class="dot"></span><span>Connected</span>';
      setTimeout(function() { init(); }, 1000);
    } else {
      msg.textContent = r.message;
      msg.style.color = '#ef4444';
      btn.disabled = false;
      btn.textContent = 'Retry Authentication';
    }
  }).catch(function(e) {
    msg.textContent = 'Request failed: ' + e.message;
    msg.style.color = '#ef4444';
    btn.disabled = false;
    btn.textContent = 'Retry Authentication';
  });
}

function retryAuth() {
  window.location.href = BASE_PATH + '/login';
}

// ===== CLOUD CONNECT =====
function connectCloud(provider) {
  var suffix = providerKeyToSuffix(provider);
  var cap = provider.charAt(0).toUpperCase() + provider.slice(1);
  var btn = document.getElementById('btnConnect' + cap);
  var statusEl = document.getElementById('cloudConnectStatus' + suffix);
  var msgEl = document.getElementById('polarisMsg' + suffix);
  var labels = { gcs: 'Google Cloud', azure: 'Azure', aws: 'AWS' };
  var origText = btn.textContent;
  btn.disabled = true;
  btn.textContent = 'Connecting...';
  msgEl.className = 'polaris-msg';
  msgEl.textContent = '';

  // Update status badge
  statusEl.className = 'status-badge pending';
  statusEl.innerHTML = '<span class="dot"></span><span>Connecting...</span>';

  loadPreset(provider);
  api('/api/polaris/connect', {
    alias: document.getElementById('polarisAlias').value,
    endpoint: document.getElementById('polarisEndpoint').value,
    catalog: document.getElementById('polarisCatalog').value,
    client_id: document.getElementById('polarisClientId').value,
    client_secret: document.getElementById('polarisClientSecret').value
  }).then(function(r) {
    if (r.status === 'ok') {
      activeCatalogAlias = r.alias;
      refreshConnectedCatalogs(suffix);
      var browserEl = document.getElementById('polarisBrowser' + suffix);
      if (browserEl) browserEl.style.display = 'grid';
      loadPolarisNamespaces(r.alias);

      // Step 2: Storage auth
      if (provider === 'aws') {
        btn.disabled = false; btn.textContent = origText;
        statusEl.className = 'status-badge connected';
        statusEl.innerHTML = '<span class="dot"></span><span>Connected (Polaris vends S3 creds)</span>';
        msgEl.className = 'polaris-msg success';
        msgEl.textContent = 'AWS Polaris connected. S3 credentials are vended automatically.';
      } else {
        statusEl.innerHTML = '<span class="dot"></span><span>Catalog connected, authenticating storage...</span>';
        msgEl.className = 'polaris-msg info';
        msgEl.textContent = 'Catalog connected. Setting up storage credentials...';

        var authEndpoint = provider === 'gcs' ? '/api/auth' : '/api/azure_auth';
        api(authEndpoint).then(function(authResult) {
          btn.disabled = false; btn.textContent = origText;
          if (authResult && authResult.status === 'ok') {
            statusEl.className = 'status-badge connected';
            statusEl.innerHTML = '<span class="dot"></span><span>Connected (catalog + storage)</span>';
            msgEl.className = 'polaris-msg success';
            msgEl.textContent = labels[provider] + ' connected successfully.';
          } else {
            statusEl.className = 'status-badge pending';
            statusEl.innerHTML = '<span class="dot"></span><span>Catalog OK, storage auth issue</span>';
            msgEl.className = 'polaris-msg error';
            msgEl.textContent = 'Storage auth: ' + ((authResult || {}).message || 'failed');
          }
        }).catch(function(e) {
          btn.disabled = false; btn.textContent = origText;
          statusEl.className = 'status-badge pending';
          statusEl.innerHTML = '<span class="dot"></span><span>Catalog OK, storage error</span>';
          msgEl.className = 'polaris-msg error';
          msgEl.textContent = 'Storage auth error: ' + e.message;
        });
      }
    } else {
      statusEl.className = 'status-badge error';
      statusEl.innerHTML = '<span class="dot"></span><span>Failed</span>';
      msgEl.className = 'polaris-msg error';
      msgEl.textContent = 'Catalog connect failed: ' + (r.message || 'Unknown error');
      btn.disabled = false; btn.textContent = origText;
    }
  }).catch(function(e) {
    statusEl.className = 'status-badge error';
    statusEl.innerHTML = '<span class="dot"></span><span>Error</span>';
    msgEl.className = 'polaris-msg error';
    msgEl.textContent = 'Connect error: ' + e.message;
    btn.disabled = false; btn.textContent = origText;
  });
}

// ===== POLARIS CATALOG =====
var catalogPresets = {};
var activeCatalogAlias = '';
var polarisNsData = [];
var polarisTblData = [];
var polarisNsSortAsc = true;
var polarisTblSortAsc = true;
var polarisActiveNs = '';

function initPolarisForm() {
  api('/api/polaris/presets').then(function(r) {
    if (r.status === 'ok') catalogPresets = r.presets;
    loadCredFieldsAll();
  }).catch(function(){});
  loadPreset('gcs');
}

function loadPreset(key) {
  var p = catalogPresets[key];
  if (!p) return;
  document.getElementById('polarisAlias').value = p.default_alias || key;
  document.getElementById('polarisEndpoint').value = p.endpoint || '';
  document.getElementById('polarisCatalog').value = p.catalog || '';
  document.getElementById('polarisClientId').value = p.client_id || '';
  document.getElementById('polarisClientSecret').value = p.client_secret || '';
}

function restartServer() {
  if (!confirm('Restart the server? The page will reload automatically.')) return;
  var st = document.getElementById('authStatus');
  st.className = 'status-badge pending';
  st.innerHTML = '<span class="dot"></span><span>Restarting...</span>';
  try { fetch(BASE_PATH + '/api/restart', {method:'POST', headers:{'Content-Type':'application/json'}, body:'{}'}); } catch(e) {}
  var attempts = 0;
  var poll = setInterval(function() {
    attempts++;
    st.innerHTML = '<span class="dot"></span><span>Reconnecting... (' + attempts + ')</span>';
    fetch(BASE_PATH + '/api/init', {signal: AbortSignal.timeout(2000)}).then(function(r2) {
      if (r2.ok) { clearInterval(poll); location.reload(); }
    }).catch(function(){});
    if (attempts > 30) { clearInterval(poll); st.className = 'status-badge error'; st.innerHTML = '<span class="dot"></span><span>Restart failed</span>'; }
  }, 2000);
}

function toggleCredPanel(suffix) {
  var body = document.getElementById('credBody' + suffix);
  var toggle = body.previousElementSibling;
  if (body.classList.contains('open')) {
    body.classList.remove('open');
    toggle.classList.remove('open');
  } else {
    body.classList.add('open');
    toggle.classList.add('open');
    loadCredFields(suffix);
  }
}

function loadCredFields(suffix) {
  var key = suffix.toLowerCase();
  var p = catalogPresets[key];
  if (!p) return;
  var el;
  el = document.getElementById('cred' + suffix + 'Endpoint'); if (el) el.value = p.endpoint || '';
  el = document.getElementById('cred' + suffix + 'Catalog'); if (el) el.value = p.catalog || '';
  el = document.getElementById('cred' + suffix + 'ClientId'); if (el) el.value = p.client_id || '';
  el = document.getElementById('cred' + suffix + 'ClientSecret');
  if (el) { el.placeholder = p.client_secret_masked || '(current)'; el.value = ''; }
}

function loadCredFieldsAll() {
  loadCredFields('Gcs');
  loadCredFields('Azure');
  loadCredFields('Aws');
}

function saveCredentials(provider) {
  var suffix = providerKeyToSuffix(provider);
  var endpoint = (document.getElementById('cred' + suffix + 'Endpoint').value || '').trim();
  var catalog = (document.getElementById('cred' + suffix + 'Catalog').value || '').trim();
  var client_id = (document.getElementById('cred' + suffix + 'ClientId').value || '').trim();
  var client_secret = (document.getElementById('cred' + suffix + 'ClientSecret').value || '').trim();
  if (!client_secret && catalogPresets[provider]) client_secret = catalogPresets[provider].client_secret || '';
  var msgEl = document.getElementById('cred' + suffix + 'Msg');
  if (!endpoint || !catalog || !client_id || !client_secret) {
    msgEl.style.color = '#ef4444';
    msgEl.textContent = 'All fields are required.';
    return;
  }
  msgEl.style.color = '#6b7280';
  msgEl.textContent = 'Saving...';
  fetch(BASE_PATH + '/api/polaris/update_credentials', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({ provider: provider, endpoint: endpoint, catalog: catalog, client_id: client_id, client_secret: client_secret })
  }).then(function(resp) { return resp.json(); }).then(function(r) {
    if (r.status === 'ok') {
      msgEl.style.color = '#22c55e';
      msgEl.textContent = r.message;
      api('/api/polaris/presets').then(function(pr) {
        if (pr.status === 'ok') catalogPresets = pr.presets;
        loadCredFields(suffix);
      });
    } else {
      msgEl.style.color = '#ef4444';
      msgEl.textContent = r.message || 'Failed to save.';
    }
  }).catch(function(e) {
    msgEl.style.color = '#ef4444';
    msgEl.textContent = 'Error: ' + e.message;
  });
}

function disconnectCatalog(alias) {
  var suffix = getProviderSuffix(alias);
  api('/api/polaris/disconnect', { alias: alias }).then(function() {
    refreshConnectedCatalogs(suffix);
    if (activeCatalogAlias === alias) {
      activeCatalogAlias = '';
      var browserEl = document.getElementById('polarisBrowser' + suffix);
      if (browserEl) browserEl.style.display = 'none';
      var nsEl = document.getElementById('polarisNamespaces' + suffix);
      if (nsEl) nsEl.innerHTML = '';
      var tblEl = document.getElementById('polarisTables' + suffix);
      if (tblEl) tblEl.innerHTML = '';
    }
    api('/api/polaris/all_tables').then(function(pt) {
      if (pt.status === 'ok') polarisTables = pt.tables;
    }).catch(function() { polarisTables = []; });
  });
}

function refreshConnectedCatalogs(suffix) {
  api('/api/polaris/catalogs').then(function(r) {
    // Update all three provider sections
    var suffixes = ['Gcs', 'Azure', 'Aws'];
    for (var s = 0; s < suffixes.length; s++) {
      var sf = suffixes[s];
      var el = document.getElementById('connectedCatalogs' + sf);
      if (!el) continue;
      if (r.status !== 'ok' || !r.catalogs.length) {
        el.innerHTML = '<span>Connected catalogs:</span>';
        continue;
      }
      var html = '<span>Connected catalogs:</span>';
      for (var i = 0; i < r.catalogs.length; i++) {
        var c = r.catalogs[i];
        var isActive = c.alias === activeCatalogAlias;
        html += '<span class="catalog-tag" style="' + (isActive ? 'background:#bbf7d0;color:#166534' : '') + '" onclick="switchCatalog(\'' + c.alias + '\')">'
          + escHtml(c.alias)
          + ' <span style="cursor:pointer;opacity:0.6" onclick="event.stopPropagation();disconnectCatalog(\'' + c.alias + '\')" title="Disconnect">&#10005;</span>'
          + '</span>';
      }
      el.innerHTML = html;
    }
  });
}

function switchCatalog(alias) {
  activeCatalogAlias = alias;
  var suffix = getProviderSuffix(alias);
  refreshConnectedCatalogs(suffix);
  var browserEl = document.getElementById('polarisBrowser' + suffix);
  if (browserEl) browserEl.style.display = 'grid';
  loadPolarisNamespaces(alias);
}

function loadPolarisNamespaces(alias) {
  if (!alias) alias = activeCatalogAlias;
  activeCatalogAlias = alias;
  var suffix = getProviderSuffix(alias);
  var el = document.getElementById('polarisNamespaces' + suffix);
  if (!el) return;
  el.innerHTML = '<div style="padding:10px;color:#6b7280">Loading...</div>';
  var nsSearch = document.getElementById('polarisNsSearch' + suffix);
  if (nsSearch) nsSearch.value = '';
  api('/api/polaris/namespaces', { alias: alias }).then(function(r) {
    if (r.status !== 'ok') { el.innerHTML = '<div style="padding:10px;color:#ef4444">' + escHtml(r.message) + '</div>'; return; }
    polarisNsData = r.namespaces || [];
    polarisActiveNs = '';
    var nsCount = document.getElementById('polarisNsCount' + suffix);
    if (nsCount) nsCount.textContent = polarisNsData.length;
    var tblEl = document.getElementById('polarisTables' + suffix);
    if (tblEl) tblEl.innerHTML = '';
    var nsLabel = document.getElementById('polarisNsLabel' + suffix);
    if (nsLabel) nsLabel.textContent = 'Select a namespace';
    var tblCount = document.getElementById('polarisTblCount' + suffix);
    if (tblCount) tblCount.textContent = '0';
    renderPolarisNs(suffix);
  });
}

function renderPolarisNs(suffix) {
  if (!suffix) suffix = getProviderSuffix(activeCatalogAlias);
  var el = document.getElementById('polarisNamespaces' + suffix);
  if (!el) return;
  var sorted = polarisNsData.slice().sort(function(a, b) { return polarisNsSortAsc ? a.localeCompare(b) : b.localeCompare(a); });
  if (!sorted.length) { el.innerHTML = '<div style="padding:10px;color:#6b7280">No namespaces found</div>'; return; }
  var html = '';
  for (var i = 0; i < sorted.length; i++) {
    var ns = sorted[i];
    var active = ns === polarisActiveNs;
    html += '<div class="polaris-list-item' + (active ? ' active' : '') + '" data-name="' + escAttr(ns) + '" onclick="loadPolarisTables(\'' + escAttr(ns) + '\')">' + escHtml(ns) + '</div>';
  }
  el.innerHTML = html;
}

function sortPolarisNs() {
  polarisNsSortAsc = !polarisNsSortAsc;
  var suffix = getProviderSuffix(activeCatalogAlias);
  var btn = document.getElementById('polarisNsSortBtn' + suffix);
  if (btn) btn.textContent = polarisNsSortAsc ? 'A-Z' : 'Z-A';
  renderPolarisNs(suffix);
  var nsSearch = document.getElementById('polarisNsSearch' + suffix);
  if (nsSearch && nsSearch.value) filterPolarisList('polarisNamespaces' + suffix, nsSearch.value);
}

function loadPolarisTables(namespace) {
  polarisActiveNs = namespace;
  var suffix = getProviderSuffix(activeCatalogAlias);
  renderPolarisNs(suffix);
  var nsSearch = document.getElementById('polarisNsSearch' + suffix);
  if (nsSearch && nsSearch.value) filterPolarisList('polarisNamespaces' + suffix, nsSearch.value);

  var el = document.getElementById('polarisTables' + suffix);
  if (!el) return;
  var nsLabel = document.getElementById('polarisNsLabel' + suffix);
  if (nsLabel) nsLabel.textContent = namespace;
  var tblSearch = document.getElementById('polarisTblSearch' + suffix);
  if (tblSearch) tblSearch.value = '';
  el.innerHTML = '<div style="padding:10px;color:#6b7280">Loading...</div>';
  api('/api/polaris/tables', { alias: activeCatalogAlias, namespace: namespace }).then(function(r) {
    if (r.status !== 'ok') { el.innerHTML = '<div style="padding:10px;color:#ef4444">' + escHtml(r.message) + '</div>'; return; }
    polarisTblData = (r.tables || []).map(function(t) { return { name: t, namespace: namespace, alias: activeCatalogAlias }; });
    var tblCount = document.getElementById('polarisTblCount' + suffix);
    if (tblCount) tblCount.textContent = polarisTblData.length;
    renderPolarisTbl(suffix);
  });
}

function renderPolarisTbl(suffix) {
  if (!suffix) suffix = getProviderSuffix(activeCatalogAlias);
  var el = document.getElementById('polarisTables' + suffix);
  if (!el) return;
  var sorted = polarisTblData.slice().sort(function(a, b) { return polarisTblSortAsc ? a.name.localeCompare(b.name) : b.name.localeCompare(a.name); });
  if (!sorted.length) { el.innerHTML = '<div style="padding:10px;color:#6b7280">No tables in this namespace</div>'; return; }
  var html = '';
  for (var i = 0; i < sorted.length; i++) {
    var item = sorted[i];
    var fqn = item.alias + '.' + item.namespace + '.' + item.name;
    html += '<div class="polaris-list-item" data-name="' + escAttr(item.name) + '" style="display:flex;justify-content:space-between">';
    html += '<span onclick="setSQL(\'SELECT * FROM ' + fqn + ' LIMIT 100\')">' + escHtml(item.name) + '</span>';
    html += '<span style="font-size:11px;color:#9ca3af;cursor:pointer" onclick="setSQL(\'SELECT * FROM ' + fqn + ' LIMIT 100\');runQuery()">&#9654; query</span>';
    html += '</div>';
  }
  el.innerHTML = html;
}

function sortPolarisTbl() {
  polarisTblSortAsc = !polarisTblSortAsc;
  var suffix = getProviderSuffix(activeCatalogAlias);
  var btn = document.getElementById('polarisTblSortBtn' + suffix);
  if (btn) btn.textContent = polarisTblSortAsc ? 'A-Z' : 'Z-A';
  renderPolarisTbl(suffix);
  var tblSearch = document.getElementById('polarisTblSearch' + suffix);
  if (tblSearch && tblSearch.value) filterPolarisList('polarisTables' + suffix, tblSearch.value);
}

function filterPolarisList(containerId, query) {
  var q = query.toLowerCase();
  var items = document.querySelectorAll('#' + containerId + ' .polaris-list-item');
  for (var i = 0; i < items.length; i++) {
    var el = items[i];
    var name = (el.getAttribute('data-name') || '').toLowerCase();
    el.style.display = (!q || name.indexOf(q) >= 0) ? '' : 'none';
  }
}

// ===== KEYBOARD SHORTCUT =====
document.addEventListener('keydown', function(e) { if (e.ctrlKey && e.key === 'Enter') { runQuery(); e.preventDefault(); } });

// ===== SQL AUTOCOMPLETE =====
(function() {
  var ta = document.getElementById('sqlInput');
  var acList = document.getElementById('acList');
  var acItems = [], acIdx = -1, acWord = '', acStart = 0;

  var SQL_KW = ['SELECT','FROM','WHERE','AND','OR','NOT','IN','LIKE','BETWEEN','IS','NULL',
    'ORDER BY','GROUP BY','HAVING','LIMIT','OFFSET','JOIN','LEFT JOIN','RIGHT JOIN','INNER JOIN',
    'CROSS JOIN','FULL JOIN','ON','AS','DISTINCT','COUNT','SUM','AVG','MIN','MAX','CASE','WHEN',
    'THEN','ELSE','END','UNION','UNION ALL','EXCEPT','INTERSECT','INSERT','UPDATE','DELETE',
    'CREATE','TABLE','VIEW','WITH','EXISTS','CAST','COALESCE','IFNULL','TRIM','UPPER','LOWER',
    'LENGTH','SUBSTR','REPLACE','CONCAT','DESC','ASC','TRUE','FALSE','OVER','PARTITION BY',
    'ROW_NUMBER','RANK','DENSE_RANK','LAG','LEAD','FIRST_VALUE','LAST_VALUE','FILTER','USING',
    'LATERAL','UNNEST','EXPLAIN','DESCRIBE','SHOW','TABLES','COLUMNS','TYPEOF','LIST','STRUCT',
    'ARRAY_AGG','STRING_AGG','GROUP_CONCAT','PIVOT','UNPIVOT','QUALIFY','SAMPLE','TABLESAMPLE'];

  function getCompletions(prefix) {
    var pfx = prefix.toUpperCase();
    var completions = [];
    var seen = {};
    return api('/api/tables').then(function(r) {
      if (r.status === 'ok') {
        for (var ti = 0; ti < r.tables.length; ti++) {
          var t = r.tables[ti];
          if (t.name.toUpperCase().indexOf(pfx) === 0) {
            completions.push({ text: t.name, type: 'table (' + t.rows + ' rows)' });
            seen[t.name.toUpperCase()] = true;
          }
          var cols = t.column_names || [];
          for (var ci = 0; ci < cols.length; ci++) {
            var c = cols[ci];
            if (c.toUpperCase().indexOf(pfx) === 0 && !seen[c.toUpperCase()]) {
              completions.push({ text: '"' + c + '"', type: 'column (' + t.name + ')' });
              seen[c.toUpperCase()] = true;
            }
          }
        }
      }
      for (var bi = 0; bi < browsedTables.length; bi++) {
        var bn = browsedTables[bi];
        if (bn.toUpperCase().indexOf(pfx) === 0 && !seen[bn.toUpperCase()]) {
          completions.push({ text: bn, type: 'table (not loaded)' });
          seen[bn.toUpperCase()] = true;
        }
      }
      for (var pi = 0; pi < polarisTables.length; pi++) {
        var pt = polarisTables[pi];
        if (pt.fqn.toUpperCase().indexOf(pfx) === 0 && !seen[pt.fqn.toUpperCase()]) {
          completions.push({ text: pt.fqn, type: 'iceberg (' + pt.namespace + ')' });
          seen[pt.fqn.toUpperCase()] = true;
        }
        if (pt.name.toUpperCase().indexOf(pfx) === 0 && !seen[pt.fqn.toUpperCase()]) {
          completions.push({ text: pt.fqn, type: 'iceberg (' + pt.namespace + ')' });
          seen[pt.fqn.toUpperCase()] = true;
        }
      }
      for (var ki = 0; ki < SQL_KW.length; ki++) {
        var kw = SQL_KW[ki];
        if (kw.indexOf(pfx) === 0 && !seen[kw])
          completions.push({ text: kw, type: 'keyword' });
      }
      return completions.slice(0, 20);
    }).catch(function() { return []; });
  }

  function getWordAtCursor() {
    var pos = ta.selectionStart;
    var text = ta.value.substring(0, pos);
    var m = text.match(/[a-zA-Z_][a-zA-Z0-9_.]*$/);
    if (m) return { word: m[0], start: pos - m[0].length };
    return { word: '', start: pos };
  }

  function renderAc() {
    if (!acItems.length) { acList.style.display = 'none'; return; }
    acList.style.display = 'block';
    var html = '';
    for (var i = 0; i < acItems.length; i++) {
      html += '<div class="ac-item' + (i === acIdx ? ' active' : '') + '" data-i="' + i + '">'
        + '<span>' + escHtml(acItems[i].text) + '</span><span class="ac-type">' + escHtml(acItems[i].type) + '</span></div>';
    }
    acList.innerHTML = html;
    var acEls = acList.querySelectorAll('.ac-item');
    for (var j = 0; j < acEls.length; j++) {
      (function(el) {
        el.onmousedown = function(e) { e.preventDefault(); acceptAc(+el.getAttribute('data-i')); };
      })(acEls[j]);
    }
    var activeEl = acList.querySelector('.ac-item.active');
    if (activeEl) activeEl.scrollIntoView({ block: 'nearest' });
  }

  function acceptAc(idx) {
    if (idx < 0 || idx >= acItems.length) return;
    var item = acItems[idx];
    var before = ta.value.substring(0, acStart);
    var after = ta.value.substring(ta.selectionStart);
    var insert = item.text + (item.type === 'keyword' ? ' ' : '');
    ta.value = before + insert + after;
    var newPos = acStart + insert.length;
    ta.selectionStart = ta.selectionEnd = newPos;
    hideAc();
    ta.focus();
  }

  function hideAc() { acItems = []; acIdx = -1; acList.style.display = 'none'; }

  function prevKeyword() {
    var text = ta.value.substring(0, ta.selectionStart).toUpperCase();
    var m = text.match(/\b(FROM|JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|INNER\s+JOIN|CROSS\s+JOIN|FULL\s+JOIN|INTO)\s+$/);
    return m ? 'table' : null;
  }

  var debounce = null;
  ta.addEventListener('input', function() {
    clearTimeout(debounce);
    debounce = setTimeout(function() {
      var wc = getWordAtCursor();
      var word = wc.word, start = wc.start;
      var ctx = prevKeyword();
      if (word.length < 2 && !ctx) { hideAc(); return; }
      acWord = word; acStart = start;
      if (ctx === 'table' && word.length < 2) {
        var seen2 = {};
        acItems = [];
        api('/api/tables').then(function(r2) {
          if (r2.status === 'ok') {
            for (var i = 0; i < r2.tables.length; i++) {
              var t2 = r2.tables[i];
              acItems.push({ text: t2.name, type: 'table (' + t2.rows + ' rows)' });
              seen2[t2.name.toUpperCase()] = true;
            }
          }
          for (var bi = 0; bi < browsedTables.length; bi++) {
            var bn = browsedTables[bi];
            if (!seen2[bn.toUpperCase()]) acItems.push({ text: bn, type: 'table (not loaded)' });
          }
          for (var pi = 0; pi < polarisTables.length; pi++) {
            var pt = polarisTables[pi];
            if (!seen2[pt.fqn.toUpperCase()]) {
              acItems.push({ text: pt.fqn, type: 'iceberg (' + pt.namespace + ')' });
              seen2[pt.fqn.toUpperCase()] = true;
            }
          }
          acIdx = acItems.length ? 0 : -1;
          renderAc();
        }).catch(function() {});
      } else {
        getCompletions(word).then(function(items) {
          acItems = items;
          acIdx = acItems.length ? 0 : -1;
          renderAc();
        });
      }
    }, 150);
  });

  ta.addEventListener('keydown', function(e) {
    if (acList.style.display === 'none' || !acItems.length) return;
    if (e.key === 'ArrowDown') { e.preventDefault(); acIdx = Math.min(acIdx + 1, acItems.length - 1); renderAc(); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); acIdx = Math.max(acIdx - 1, 0); renderAc(); }
    else if (e.key === 'Tab' || e.key === 'Enter') {
      if (acIdx >= 0) { e.preventDefault(); acceptAc(acIdx); }
    }
    else if (e.key === 'Escape') { e.preventDefault(); hideAc(); }
  });

  ta.addEventListener('blur', function() { setTimeout(hideAc, 200); });
})();

init();
initPolarisForm();
</script>
</body>
</html>"""


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, fmt, *args): pass

    def _strip_base(self, path):
        """Strip BASE_PATH prefix from the request path."""
        if path == BASE_PATH:
            return "/"
        if path.startswith(BASE_PATH + "/"):
            return path[len(BASE_PATH):]
        return None  # path doesn't match base

    def _get_session(self):
        return get_session(self.headers.get("Cookie", ""))

    def _require_auth(self):
        """Check session. Returns session dict or None (and sends redirect)."""
        session = self._get_session()
        if session:
            return session
        # Not authenticated — redirect to login
        self.send_response(302)
        self.send_header("Location", BASE_PATH + "/login")
        self.end_headers()
        return None

    def send_json(self, data):
        body = json.dumps(data, default=str).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        try:
            self._do_POST()
        except (BrokenPipeError, ConnectionResetError):
            pass  # Client disconnected — ignore silently

    def _do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        route = self._strip_base(parsed.path)
        if route is None:
            self.send_response(404)
            self.end_headers()
            return
        if route == "/api/login":
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            try:
                data = json.loads(body)
            except Exception:
                self.send_json({"status": "error", "message": "Invalid request"})
                return
            email = data.get("email", "").strip()
            if not email or "@" not in email:
                self.send_json({"status": "error", "message": "Valid email required"})
                return
            token = create_session(email)
            resp = json.dumps({"status": "ok"}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Set-Cookie", f"session={token}; Path=/; HttpOnly; Secure; SameSite=Lax")
            self.send_header("Content-Length", str(len(resp)))
            self.end_headers()
            self.wfile.write(resp)
        elif route == "/api/polaris/update_credentials":
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            try:
                data = json.loads(body)
            except Exception:
                self.send_json({"status": "error", "message": "Invalid JSON"})
                return
            self.send_json(update_polaris_credentials(data))
        elif route == "/api/restart":
            self.send_json({"status": "ok", "message": "Restarting..."})
            threading.Timer(0.5, lambda: os.system("systemctl restart gcs-explorer")).start()
        else:
            self.send_response(404)
            self.end_headers()

    def do_GET(self):
        try:
            self._do_GET()
        except (BrokenPipeError, ConnectionResetError):
            pass  # Client disconnected — ignore silently

    def _do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = dict(urllib.parse.parse_qsl(parsed.query))

        # Redirect bare /datalake_reader to /datalake_reader/
        if parsed.path == BASE_PATH:
            self.send_response(301)
            self.send_header("Location", BASE_PATH + "/")
            self.end_headers()
            return

        route = self._strip_base(parsed.path)
        if route is None:
            self.send_response(404)
            self.end_headers()
            return

        # Login page — no auth required
        if route == "/login":
            body = LOGIN_PAGE.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        # Logout
        if route == "/logout":
            session = self._get_session()
            if session:
                cookie_header = self.headers.get("Cookie", "")
                cookies = http.cookies.SimpleCookie()
                try:
                    cookies.load(cookie_header)
                    token = cookies.get("session").value
                    active_sessions.pop(token, None)
                except Exception:
                    pass
            self.send_response(302)
            self.send_header("Location", BASE_PATH + "/login")
            self.send_header("Set-Cookie", "session=; Path=/; HttpOnly; Secure; Max-Age=0")
            self.end_headers()
            return

        # Health check — no auth required, no db_lock, no GCS calls
        if route == "/api/health":
            self.send_json({"status": "ok", "uptime": int(time.time() - _server_start_time)})
            return

        # All other routes require auth
        if route == "/api/login":
            self.send_response(405)
            self.end_headers()
            return

        session = self._require_auth()
        if not session:
            return

        if route == "/":
            body = HTML_PAGE.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        elif route == "/api/whoami":
            self.send_json({"status": "ok", "email": session["email"]})

        elif route == "/api/init":
            self.send_json(init_gcs())

        elif route == "/api/buckets":
            self.send_json(list_buckets())

        elif route == "/api/ls":
            prefix = params.get("prefix", "")
            bucket_name = params.get("bucket", "")
            self.send_json(list_path(prefix, bucket_name or None))

        elif route == "/api/parquet":
            self.send_json(read_parquet(params.get("path", "")))

        elif route == "/api/load_dir":
            self.send_json(read_all_parquets_in_dir(params.get("prefix", "")))

        elif route == "/api/sql":
            self.send_json(run_sql(params.get("query", ""), params.get("prefix", "")))


        elif route == "/api/cat":
            self.send_json(read_file_text(params.get("path", "")))

        elif route == "/api/tables":
            self.send_json(get_loaded_tables())

        elif route == "/api/auth":
            self.send_json(run_gcloud_auth())

        elif route == "/api/azure_auth":
            self.send_json(run_azure_auth())

        elif route == "/api/aws_auth":
            self.send_json(run_aws_auth(
                mode=params.get("mode", "keys"),
                access_key=params.get("access_key", ""),
                secret_key=params.get("secret_key", ""),
                region=params.get("region", "us-west-2"),
                role_arn=params.get("role_arn", "")
            ))

        elif route == "/api/polaris/connect":
            self.send_json(connect_polaris(
                params.get("alias", ""), params.get("endpoint", ""),
                params.get("catalog", ""), params.get("client_id", ""),
                params.get("client_secret", "")
            ))

        elif route == "/api/polaris/disconnect":
            self.send_json(disconnect_polaris(params.get("alias", "")))

        elif route == "/api/polaris/catalogs":
            self.send_json(get_connected_catalogs())

        elif route == "/api/polaris/presets":
            self.send_json(get_catalog_presets())

        elif route == "/api/polaris/namespaces":
            self.send_json(list_polaris_namespaces(params.get("alias", "")))

        elif route == "/api/polaris/tables":
            self.send_json(list_polaris_tables(params.get("alias", ""), params.get("namespace", "")))

        elif route == "/api/polaris/all_tables":
            self.send_json(list_all_polaris_tables())

        else:
            self.send_response(404)
            self.end_headers()


def run_gcloud_auth():
    """Re-initialize GCS client using VM service account credentials."""
    try:
        # On GCE, remove stale ADC so the compute engine service account is used
        adc_path = os.path.expanduser("~/.config/gcloud/application_default_credentials.json")
        if os.path.exists(adc_path):
            os.rename(adc_path, adc_path + ".bak")
            print("  Removed stale ADC, falling back to VM service account")
        r = init_gcs()
        if r["status"] == "ok":
            return {"status": "ok", "message": "Connected using VM service account credentials"}
        return {"status": "error", "message": "GCS connection failed: " + r["message"]}
    except Exception as e:
        return {"status": "error", "message": str(e)}


def run_azure_auth():
    """Create DuckDB Azure credential chain secret."""
    try:
        duckdb_worker.send_command("exec_multi", {"statements": [
            "DROP SECRET IF EXISTS azure_storage_secret;",
            """CREATE SECRET azure_storage_secret (
                TYPE AZURE,
                PROVIDER CREDENTIAL_CHAIN
            );""",
        ]}, timeout=15)
        print("  Azure credential chain secret created")
        return {"status": "ok", "message": "Azure storage credentials configured."}
    except Exception as e:
        return {"status": "error", "message": f"Azure credential chain failed: {e}"}


def run_aws_auth(mode="keys", access_key="", secret_key="", region="us-west-2", role_arn=""):
    """Create DuckDB S3 secret via IAM role assumption or direct credentials."""
    region = region or "us-west-2"
    if mode == "arn":
        if not role_arn:
            return {"status": "error", "message": "IAM Role ARN is required"}
        import subprocess
        try:
            result = subprocess.run(
                ["aws", "sts", "assume-role",
                 "--role-arn", role_arn,
                 "--role-session-name", "gcs-explorer-session",
                 "--duration-seconds", "3600",
                 "--output", "json"],
                capture_output=True, text=True, timeout=30
            )
            if result.returncode != 0:
                return {"status": "error", "message": result.stderr or "assume-role failed"}
            import json
            creds = json.loads(result.stdout)["Credentials"]
            ak = creds["AccessKeyId"]
            sk = creds["SecretAccessKey"]
            token = creds["SessionToken"]
            duckdb_worker.send_command("exec_multi", {"statements": [
                "DROP SECRET IF EXISTS aws_storage_secret;",
                f"""CREATE SECRET aws_storage_secret (
                    TYPE S3,
                    KEY_ID '{ak}',
                    SECRET '{sk}',
                    SESSION_TOKEN '{token}',
                    REGION '{region}',
                    ENDPOINT 's3.{region}.amazonaws.com',
                    URL_STYLE 'vhost'
                );""",
            ]}, timeout=15)
            print(f"  AWS S3 secret created via assume-role (region: {region})")
            return {"status": "ok", "message": f"Role assumed successfully (region: {region}). Temporary credentials expire in 1 hour."}
        except FileNotFoundError:
            return {"status": "error", "message": "AWS CLI not found. Install: brew install awscli"}
        except subprocess.TimeoutExpired:
            return {"status": "error", "message": "assume-role timed out"}
        except Exception as e:
            return {"status": "error", "message": str(e)}
    else:
        if not access_key or not secret_key:
            return {"status": "error", "message": "Access Key ID and Secret Access Key are required"}
        try:
            duckdb_worker.send_command("exec_multi", {"statements": [
                "DROP SECRET IF EXISTS aws_storage_secret;",
                f"""CREATE SECRET aws_storage_secret (
                    TYPE S3,
                    KEY_ID '{access_key}',
                    SECRET '{secret_key}',
                    REGION '{region}',
                    ENDPOINT 's3.{region}.amazonaws.com',
                    URL_STYLE 'vhost'
                );""",
                f"SET s3_region='{region}';",
                f"SET s3_access_key_id='{access_key}';",
                f"SET s3_secret_access_key='{secret_key}';",
                f"SET s3_endpoint='s3.{region}.amazonaws.com';",
                "SET s3_url_style='vhost';",
            ]}, timeout=15)
            print(f"  AWS S3 secret + global config set (region: {region})")
            return {"status": "ok", "message": f"AWS authentication successful (region: {region})."}
        except Exception as e:
            return {"status": "error", "message": str(e)}


def main():
    print(f"GCS Parquet Explorer (Server Mode)")
    print(f"DuckDB {duckdb.__version__} | PyArrow {pyarrow.__version__}")
    print()

    init_duckdb()

    http.server.ThreadingHTTPServer.allow_reuse_address = True
    http.server.ThreadingHTTPServer.request_queue_size = 64  # default 5 is too low — blocks new connections when db_lock is contended
    server = http.server.ThreadingHTTPServer((BIND_ADDR, PORT), Handler)

    # Wrap socket with SSL
    if os.path.exists(SSL_CERT) and os.path.exists(SSL_KEY):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(certfile=SSL_CERT, keyfile=SSL_KEY)
        server.socket = ctx.wrap_socket(server.socket, server_side=True)
        proto = "https"
        print(f"  SSL: {SSL_CERT}")
    else:
        proto = "http"
        print(f"  WARNING: No SSL cert found — running plain HTTP")
        print(f"  Place cert at {SSL_CERT} and key at {SSL_KEY} to enable HTTPS")

    print(f"Server: {proto}://{FQDN}{BASE_PATH}/")
    print(f"Login with your email + password")
    print("Ctrl+C to stop")
    print(f"  DuckDB running in subprocess (PID {duckdb_worker._proc.pid})")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
        server.shutdown()


if __name__ == "__main__":
    main()
