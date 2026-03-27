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
import os
import psutil
import secrets
import ssl
import sys
import threading
import urllib.parse
import urllib.request
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
body { font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; background: #0f1117; color: #e1e4e8; display: flex; justify-content: center; align-items: center; min-height: 100vh; }
.login-box { background: #13161f; border: 1px solid #2d3348; border-radius: 12px; padding: 40px; width: 420px; }
.login-box h1 { font-size: 24px; color: #58a6ff; margin-bottom: 8px; }
.login-box .sub { font-size: 14px; color: #6e7681; margin-bottom: 28px; }
.login-box label { display: block; font-size: 14px; color: #8b949e; margin-bottom: 6px; margin-top: 16px; }
.login-box input { width: 100%; background: #1c2030; border: 1px solid #2d3348; color: #e1e4e8; padding: 10px 12px; border-radius: 6px; font-size: 16px; font-family: inherit; }
.login-box input:focus { outline: none; border-color: #58a6ff; }
.login-box button { width: 100%; margin-top: 24px; padding: 12px; background: #238636; color: #fff; border: none; border-radius: 6px; font-size: 16px; font-weight: 600; cursor: pointer; }
.login-box button:hover { background: #2ea043; }
.error { color: #f06060; font-size: 14px; margin-top: 12px; display: none; }
.info { font-size: 12px; color: #6e7681; margin-top: 20px; text-align: center; }
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
db_conn = None
db_lock = threading.Lock()  # DuckDB connections are not thread-safe
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
        name, tbl = loaded_tables.popitem(last=False)  # oldest first
        try:
            db_conn.unregister(name)
        except Exception:
            pass
        mb = tbl.nbytes / (1024 * 1024)
        del tbl
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
    """Initialize DuckDB in-memory connection with Iceberg extensions."""
    global db_conn
    # Ensure HOME is set — systemd services may not have it
    if not os.environ.get("HOME"):
        os.environ["HOME"] = "/root"
    db_conn = duckdb.connect(":memory:")
    try:
        db_conn.execute(f"SET home_directory='{os.environ['HOME']}';")
        db_conn.execute("INSTALL httpfs; LOAD httpfs;")
        db_conn.execute("INSTALL iceberg; LOAD iceberg;")
        db_conn.execute("INSTALL azure; LOAD azure;")
        db_conn.execute("SET azure_transport_option_type='curl';")
        print("  DuckDB extensions: httpfs, iceberg, azure loaded (azure transport: curl)")
    except Exception as e:
        print(f"  Warning: Could not load extensions: {e}")


def connect_polaris(alias, endpoint, catalog, client_id, client_secret):
    """Connect to a Polaris Iceberg catalog via DuckDB with a given alias."""
    if not alias or not endpoint or not catalog or not client_id or not client_secret:
        return {"status": "error", "message": "All fields are required"}
    # Sanitize alias for use as DuckDB identifier
    safe_alias = alias.replace("-", "_").replace(" ", "_").lower()
    secret_name = f"secret_{safe_alias}"
    oauth_uri = endpoint.rstrip("/") + "/v1/oauth/tokens"
    try:
        with db_lock:
            db_conn.execute(f"DETACH DATABASE IF EXISTS {safe_alias};")
            db_conn.execute(f"DROP SECRET IF EXISTS {secret_name};")
            db_conn.execute(f"""
                CREATE SECRET {secret_name} (
                    TYPE iceberg,
                    CLIENT_ID '{client_id}',
                    CLIENT_SECRET '{client_secret}',
                    OAUTH2_SCOPE 'PRINCIPAL_ROLE:ALL',
                    OAUTH2_SERVER_URI '{oauth_uri}'
                );
            """)
            db_conn.execute(f"""
                ATTACH '{catalog}' AS {safe_alias} (
                    TYPE ICEBERG,
                    ENDPOINT '{endpoint}',
                    SECRET {secret_name}
                );
            """)
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
        with db_lock:
            db_conn.execute(f"DETACH DATABASE IF EXISTS {safe_alias};")
            db_conn.execute(f"DROP SECRET IF EXISTS secret_{safe_alias};")
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


def _polaris_rest_get(endpoint, path, token):
    """GET a Polaris REST API path with bearer token."""
    url = endpoint.rstrip("/") + path
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


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
        # Register in DuckDB for SQL queries
        table_name = blob_path.split("/")[-1].replace(".parquet", "").replace("-", "_")
        # Also register by directory name
        parts = blob_path.split("/")
        dir_name = table_name
        if len(parts) >= 3:
            dir_name = parts[-3] if parts[-2] == "data" else parts[-2]
            dir_name = dir_name.replace("-", "_")
            loaded_tables[dir_name] = table
            with db_lock:
                db_conn.register(dir_name, table)

        loaded_tables[table_name] = table
        with db_lock:
            db_conn.register(table_name, table)

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

        # Register for SQL
        parts = dir_path.rstrip("/").split("/")
        dir_name = parts[-1] if parts[-1] != "data" else parts[-2]
        dir_name = dir_name.replace("-", "_")
        loaded_tables[dir_name] = combined
        with db_lock:
            db_conn.register(dir_name, combined)

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
                loaded_tables[search_name] = combined
                with db_lock:
                    db_conn.register(search_name, combined)
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
    def _exec(q):
        with db_lock:
            r = db_conn.execute(q)
            cols = [desc[0] for desc in r.description] if r.description else []
            data = r.fetchall()
        return cols, data

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


def read_delta_log(table_prefix):
    """Read Delta Lake transaction log for a table."""
    try:
        bucket = gcs_client.bucket(current_bucket)
        log_prefix = table_prefix + "_delta_log/"
        blobs = list(gcs_client.list_blobs(bucket, prefix=log_prefix))
        json_blobs = sorted([b for b in blobs if b.name.endswith(".json")], key=lambda b: b.name)

        if not json_blobs:
            return {"status": "error", "message": "No _delta_log found"}

        entries = []
        for blob in json_blobs[-5:]:
            content = blob.download_as_text()
            name = blob.name.split("/")[-1]
            parsed = []
            for line in content.strip().split("\n"):
                try:
                    parsed.append(json.loads(line))
                except Exception:
                    parsed.append({"raw": line[:300]})
            entries.append({"file": name, "entries": parsed})

        return {"status": "ok", "log_entries": entries}
    except Exception as e:
        return {"status": "error", "message": str(e)}


def get_loaded_tables():
    """Return list of tables registered in DuckDB."""
    tables = []
    for name, tbl in loaded_tables.items():
        tables.append({
            "name": name,
            "rows": tbl.num_rows,
            "columns": len(tbl.column_names),
            "column_names": list(tbl.column_names)
        })
    return {"status": "ok", "tables": tables}


HTML_PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>GCS Parquet Explorer — SAP CDS Data</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; background: #0f1117; color: #e1e4e8; font-size: 16px; }
.header { background: linear-gradient(135deg, #1a1f35 0%, #0d1025 100%); padding: 14px 20px; border-bottom: 1px solid #2d3348; display: flex; align-items: center; gap: 14px; flex-wrap: wrap; }
.header h1 { font-size: 26px; font-weight: 600; color: #58a6ff; }
.header .sub { font-size: 18px; color: #6e7681; }
.header .team { font-size: 13px; color: #8b949e; margin-left: auto; text-align: right; line-height: 1.4; }
.header .fivetran-logo { height: 42px; vertical-align: middle; }
.status { font-size: 16px; padding: 4px 14px; border-radius: 12px; }
.status.ok { background: #1b4332; color: #40c057; }
.status.pending { background: #3d2e00; color: #f0c040; }
.status.error { background: #3d1010; color: #f06060; }
.container { display: flex; height: calc(100vh - 50px); }
.sidebar { width: 500px; min-width: 200px; border-right: none; display: flex; flex-direction: column; background: #13161f; overflow: hidden; }
.splitter { width: 6px; cursor: col-resize; background: #2d3348; flex-shrink: 0; transition: background 0.15s; }
.splitter:hover, .splitter.active { background: #58a6ff; }
.main { flex: 1; display: flex; flex-direction: column; overflow: hidden; }
.btn { padding: 8px 16px; border: none; border-radius: 5px; cursor: pointer; font-size: 18px; font-weight: 500; transition: all 0.15s; }
.btn-primary { background: #238636; color: #fff; }
.btn-primary:hover { background: #2ea043; }
.btn-blue { background: #1f6feb; color: #fff; }
.btn-blue:hover { background: #388bfd; }
.btn-secondary { background: #2d3348; color: #c9d1d9; }
.btn-secondary:hover { background: #3d4458; }
.btn-sm { padding: 6px 12px; font-size: 16px; }
.btn-orange { background: #9a6700; color: #fff; }
.btn-orange:hover { background: #bb8009; }
.file-list { flex: 1; overflow-y: auto; padding: 2px 0; }
.file-item { padding: 8px 12px; cursor: pointer; display: flex; align-items: center; gap: 10px; font-size: 18px; border-left: 3px solid transparent; }
.file-item:hover { background: #1c2030; }
.file-item.active { background: #1c2030; border-left-color: #58a6ff; }
.file-item .icon { font-size: 20px; width: 24px; text-align: center; flex-shrink: 0; }
.file-item .name { flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.file-item .size { font-size: 15px; color: #6e7681; flex-shrink: 0; }
.file-item .meta { display: flex; gap: 10px; flex-shrink: 0; margin-left: auto; }
.file-item .meta .dir-files { font-size: 13px; color: #8b949e; min-width: 55px; text-align: right; }
.file-item .meta .dir-size { font-size: 13px; color: #6e7681; min-width: 60px; text-align: right; }
.file-item.dir .name { color: #58a6ff; }
.file-item.file .name { color: #c9d1d9; }
.breadcrumb { padding: 8px 12px; font-size: 16px; color: #6e7681; border-bottom: 1px solid #1e2233; display: flex; flex-wrap: wrap; gap: 2px; align-items: center; }
.search-bar { padding: 6px 10px; border-bottom: 1px solid #1e2233; }
.search-bar input { width: 100%; background: #1c2030; border: 1px solid #2d3348; color: #e1e4e8; padding: 7px 10px; border-radius: 5px; font-size: 15px; font-family: inherit; }
.search-bar input:focus { outline: none; border-color: #58a6ff; }
.search-bar input::placeholder { color: #4d5566; }
.sort-bar { display: flex; padding: 5px 12px; border-bottom: 1px solid #1e2233; background: #13161f; align-items: center; }
.sort-bar .sort-name { flex: 1; padding-left: 34px; }
.sort-bar .sort-meta { display: flex; gap: 10px; flex-shrink: 0; margin-right: 14px; }
.sort-col { font-size: 12px; color: #6e7681; cursor: pointer; padding: 2px 8px; border-radius: 3px; user-select: none; white-space: nowrap; }
.sort-col:hover { color: #c9d1d9; background: #1c2030; }
.sort-col.active { color: #58a6ff; }
.sort-col .arrow { font-size: 10px; }
.breadcrumb span { cursor: pointer; color: #58a6ff; }
.breadcrumb span:hover { text-decoration: underline; }
.breadcrumb .sep { color: #3d4458; cursor: default; margin: 0 1px; }
.tab-bar { display: flex; border-bottom: 1px solid #2d3348; background: #13161f; }
.tab { padding: 12px 20px; cursor: pointer; font-size: 18px; color: #8b949e; border-bottom: 2px solid transparent; transition: all 0.15s; }
.tab:hover { color: #c9d1d9; }
.tab.active { color: #58a6ff; border-bottom-color: #58a6ff; }
.panel { flex: 1; overflow: auto; display: none; scrollbar-width: none; -ms-overflow-style: none; }
.panel::-webkit-scrollbar { display: none; }
#panel-docs > div::-webkit-scrollbar { display: none; }
.panel.active { display: flex; flex-direction: column; }
.sql-area { padding: 10px; border-bottom: 1px solid #2d3348; }
.sql-area textarea { width: 100%; height: 110px; background: #1c2030; border: 1px solid #2d3348; color: #e1e4e8; padding: 10px; border-radius: 5px; font-family: 'SF Mono', 'Fira Code', monospace; font-size: 18px; resize: vertical; }
.sql-area textarea:focus { outline: none; border-color: #58a6ff; }
.ac-wrap { position: relative; }
.ac-list { position: absolute; left: 10px; background: #1a1f35; border: 1px solid #58a6ff; border-radius: 5px; max-height: 220px; overflow-y: auto; z-index: 50; display: none; min-width: 260px; box-shadow: 0 4px 16px rgba(0,0,0,0.5); }
.ac-list .ac-item { padding: 6px 12px; cursor: pointer; font-family: 'SF Mono', 'Fira Code', monospace; font-size: 16px; color: #c9d1d9; display: flex; justify-content: space-between; gap: 16px; }
.ac-list .ac-item:hover, .ac-list .ac-item.active { background: #2d3348; }
.ac-list .ac-item .ac-type { font-size: 13px; color: #6e7681; white-space: nowrap; }
.sql-bar { display: flex; gap: 8px; margin-top: 8px; align-items: center; }
.sql-bar .hint { font-size: 15px; color: #6e7681; flex: 1; }
.loaded-tables { padding: 8px 12px; border-bottom: 1px solid #2d3348; font-size: 16px; color: #6e7681; max-height: 80px; overflow-y: auto; }
.loaded-tables .tag { display: inline-block; background: #1a1f35; color: #58a6ff; padding: 2px 7px; border-radius: 3px; margin: 1px 2px; cursor: pointer; font-family: monospace; }
.loaded-tables .tag:hover { background: #2d3348; }
.table-wrap { flex: 1; overflow: auto; }
table { width: 100%; border-collapse: collapse; font-size: 16px; }
table th { position: sticky; top: 0; background: #1a1f35; color: #8b949e; text-align: left; padding: 8px 10px; font-weight: 600; text-transform: uppercase; font-size: 15px; letter-spacing: 0.4px; border-bottom: 2px solid #2d3348; white-space: nowrap; z-index: 1; user-select: none; }
table th.sortable { cursor: pointer; }
table th.sortable:hover { color: #58a6ff; }
table th.sort-asc::after { content: ' ▲'; color: #58a6ff; font-size: 11px; }
table th.sort-desc::after { content: ' ▼'; color: #58a6ff; font-size: 11px; }
table td { padding: 7px 10px; border-bottom: 1px solid #1e2233; color: #c9d1d9; max-width: 400px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-family: 'SF Mono', 'Fira Code', monospace; font-size: 16px; }
table tr:hover td { background: #1c2030; }
.info-bar { padding: 8px 12px; background: #13161f; border-top: 1px solid #2d3348; font-size: 16px; color: #8b949e; display: flex; justify-content: space-between; }
.schema-view { padding: 16px; font-family: 'SF Mono', 'Fira Code', monospace; font-size: 17px; white-space: pre-wrap; color: #8b949e; overflow: auto; flex: 1; }
.delta-view { padding: 14px; overflow: auto; flex: 1; }
.delta-entry { margin-bottom: 12px; border: 1px solid #2d3348; border-radius: 5px; overflow: hidden; }
.delta-entry-header { background: #1a1f35; padding: 8px 12px; font-weight: 600; color: #58a6ff; font-size: 17px; }
.delta-entry-body { padding: 10px 12px; white-space: pre-wrap; color: #8b949e; max-height: 300px; overflow: auto; font-family: monospace; font-size: 16px; }
.loading { text-align: center; padding: 30px; color: #6e7681; }
.empty { text-align: center; padding: 40px 16px; color: #6e7681; font-size: 18px; }
.cell-popup { position: fixed; top: 50%; left: 50%; transform: translate(-50%,-50%); background: #1c2030; border: 1px solid #58a6ff; border-radius: 8px; padding: 20px; max-width: 80vw; max-height: 80vh; overflow: auto; z-index: 100; white-space: pre-wrap; font-family: monospace; font-size: 17px; color: #e1e4e8; }
.overlay { position: fixed; top:0; left:0; width:100%; height:100%; background: rgba(0,0,0,0.6); z-index: 99; }
</style>
</head>
<body>
<div class="header">
  <svg class="fivetran-logo" viewBox="0 0 260 50" xmlns="http://www.w3.org/2000/svg"><g transform="translate(2,0)"><polygon points="4,48 14,2 22,2 12,48" fill="#0073FF" rx="2"/><polygon points="17,48 27,2 35,2 25,48" fill="#0073FF"/><polygon points="30,48 40,2 48,2 38,48" fill="#0073FF"/></g><text x="60" y="37" font-family="Arial,Helvetica,sans-serif" font-size="34" font-weight="700" fill="#0073FF">Fivetran</text></svg>
  <svg style="height:38px;vertical-align:middle" viewBox="0 0 100 100" xmlns="http://www.w3.org/2000/svg"><path d="M15 30 L50 10 L85 30 L85 70 L50 90 L15 70 Z" fill="#4285F4" stroke="#3367D6" stroke-width="1.5"/><rect x="25" y="36" width="50" height="6" rx="1" fill="#AECBFA"/><rect x="25" y="46" width="50" height="6" rx="1" fill="#AECBFA"/><rect x="25" y="56" width="50" height="6" rx="1" fill="#AECBFA"/><text x="50" y="82" text-anchor="middle" font-family="Arial,sans-serif" font-size="8" fill="#4285F4" font-weight="600">GCS</text></svg>
  <h1>GCS Parquet Explorer</h1>
  <span class="sub">SAP CDS Views | DuckDB SQL Engine</span>
  <span id="authStatus" class="status pending">Connecting...</span>
  <span class="team">
    <span id="userEmail" style="color:#58a6ff"></span><br>
    Engineered by the SAP Specialist Team
    <span style="margin-left:12px"><a href="/datalake_reader/logout" style="color:#6e7681;font-size:12px;text-decoration:none" onmouseover="this.style.color='#f06060'" onmouseout="this.style.color='#6e7681'">Logout</a></span>
  </span>
</div>
<div class="container">
  <div class="sidebar">
    <div class="breadcrumb" id="breadcrumb"></div>
    <div class="search-bar"><input type="text" id="sidebarSearch" placeholder="Filter..." oninput="filterSidebar(this.value)"></div>
    <div class="sort-bar" id="sortBar" style="display:none">
      <div class="sort-name"><span class="sort-col active" data-sort="name" onclick="toggleSort('name')">Name <span class="arrow">&#9650;</span></span></div>
      <div class="sort-meta">
        <span class="sort-col" data-sort="files" onclick="toggleSort('files')">Files <span class="arrow"></span></span>
        <span class="sort-col" data-sort="size" onclick="toggleSort('size')">Size <span class="arrow"></span></span>
      </div>
    </div>
    <div class="file-list" id="fileList">
      <div class="empty">Loading...</div>
    </div>
  </div>
  <div class="splitter" id="splitter"></div>
  <div class="main">
    <div class="tab-bar">
      <div class="tab active" data-tab="data" onclick="switchTab('data')">Data</div>
      <div class="tab" data-tab="schema" onclick="switchTab('schema')">Schema</div>
      <div class="tab" data-tab="sql" onclick="switchTab('sql')">SQL Query</div>
      <div class="tab" data-tab="delta" onclick="switchTab('delta')">Delta Log</div>
      <div class="tab" data-tab="polaris" onclick="switchTab('polaris')">Polaris Catalog</div>
      <div class="tab" data-tab="docs" onclick="switchTab('docs')">Documentation</div>
    </div>
    <div class="panel active" id="panel-data">
      <div class="table-wrap" id="dataTable"><div class="empty">Select a parquet file or folder to view data</div></div>
      <div class="info-bar" id="dataInfo"></div>
    </div>
    <div class="panel" id="panel-schema">
      <div class="schema-view" id="schemaView">Select a parquet file to view its Arrow schema</div>
    </div>
    <div class="panel" id="panel-sql">
      <div class="sql-area">
        <div class="ac-wrap">
        <textarea id="sqlInput" placeholder="SQL query against loaded tables — Tab to autocomplete.

Examples after loading chain_objects:
  SELECT * FROM chain_objects LIMIT 100
  SELECT object_type, count(*) as cnt FROM chain_objects GROUP BY 1 ORDER BY 2 DESC
  SELECT * FROM chain_resolution WHERE level = 1
  SELECT c.parent_object, c.child_object, o.object_type FROM chain_resolution c JOIN chain_objects o ON c.child_object = o.object_name WHERE c.level = 1"></textarea>
        <div class="ac-list" id="acList"></div>
        </div>
        <div class="sql-bar">
          <button class="btn btn-blue" onclick="runQuery()">Run (Ctrl+Enter)</button>
          <button class="btn btn-secondary btn-sm" onclick="showTables()">Show Tables</button>
          <span class="hint" id="sqlHint">Load parquet files to query them with SQL</span>
        </div>
      </div>
      <div class="loaded-tables" id="loadedTables"></div>
      <div class="table-wrap" id="sqlResults"><div class="empty">Run a SQL query to see results</div></div>
      <div class="info-bar" id="sqlInfo"></div>
    </div>
    <div class="panel" id="panel-delta">
      <div class="delta-view" id="deltaView"><div class="empty">Browse to a table directory to view its Delta transaction log</div></div>
    </div>
    <div class="panel" id="panel-polaris">
      <div style="padding:20px;overflow:auto">
        <div style="margin-bottom:16px;display:flex;align-items:center;gap:10px;flex-wrap:wrap">
          <span style="color:#8b949e;font-size:14px;font-weight:600">Connect to:</span>
          <button class="btn btn-sm" id="btnConnectGcs" onclick="connectCloud('gcs')" style="background:#2a5a3a;color:#58d68d;border:1px solid #58d68d;font-weight:600;padding:10px 22px;font-size:15px;display:flex;align-items:center;gap:8px">
            <svg width="20" height="20" viewBox="0 0 100 100"><path d="M50 10 C25 10, 5 28, 5 50 C5 65, 12 72, 25 78 L25 78 C30 80, 38 82, 50 82 C62 82, 70 80, 75 78 C88 72, 95 65, 95 50 C95 28, 75 10, 50 10 Z" fill="#4285F4"/><path d="M50 10 C25 10, 5 28, 5 50 C5 65, 12 72, 25 78 L50 50 Z" fill="#EA4335"/><path d="M5 50 C5 65, 12 72, 25 78 L25 78 C30 80, 38 82, 50 82 L50 50 Z" fill="#34A853"/><path d="M25 78 C30 80, 38 82, 50 82 L50 50 L5 50 C5 65, 12 72, 25 78Z" fill="#34A853"/><path d="M50 82 C62 82, 70 80, 75 78 L50 50 Z" fill="#FBBC05" opacity="0.5"/><path d="M50 10 C25 10, 5 28, 5 50 L50 50Z" fill="#EA4335"/><path d="M50 10 C75 10, 95 28, 95 50 L50 50Z" fill="#4285F4"/><path d="M95 50 C95 65, 88 72, 75 78 L50 50Z" fill="#4285F4" opacity="0.8"/><path d="M50 25 C35 25, 22 36, 22 50 C22 60, 28 66, 35 70 C40 72, 44 73, 50 73 C56 73, 60 72, 65 70 C72 66, 78 60, 78 50 C78 36, 65 25, 50 25 Z" fill="#fff"/></svg>
            Google Cloud
          </button>
          <button class="btn btn-sm" id="btnConnectAzure" onclick="connectCloud('azure')" style="background:#2a3a5a;color:#5dade2;border:1px solid #5dade2;font-weight:600;padding:10px 22px;font-size:15px;display:flex;align-items:center;gap:8px">
            <svg width="20" height="20" viewBox="0 0 100 110"><ellipse cx="50" cy="12" rx="30" ry="10" fill="#a0c843" stroke="#fff" stroke-width="2"/><ellipse cx="50" cy="12" rx="24" ry="7" fill="#7fad2b"/><path d="M20 12 L20 75 Q20 80, 25 82 L50 82 L50 12" fill="#3c3c3c"/><path d="M80 12 L80 75 Q80 80, 75 82 L50 82 L50 12" fill="#8c8c8c"/><path d="M20 68 Q28 60, 35 65 Q42 58, 50 65 Q58 58, 65 65 Q72 60, 80 68 L80 100 Q80 108, 75 110 L25 110 Q20 108, 20 100 Z" fill="#0078d4"/><path d="M50 65 Q58 58, 65 65 Q72 60, 80 68 L80 100 Q80 108, 75 110 L50 110 Z" fill="#50b0f0"/><path d="M20 68 Q28 60, 35 65 Q42 58, 50 65 L50 110 L25 110 Q20 108, 20 100 Z" fill="#0078d4"/></svg>
            Azure
          </button>
          <button class="btn btn-sm" id="btnConnectAws" onclick="connectCloud('aws')" style="background:#5a4a2a;color:#f5b041;border:1px solid #f5b041;font-weight:600;padding:10px 22px;font-size:15px;display:flex;align-items:center;gap:8px">
            <svg width="20" height="20" viewBox="0 0 100 100"><path d="M30 15 L30 85 L10 85 L10 15 Z" fill="#C1272D"/><path d="M30 15 L30 85 L20 85 L20 15 Z" fill="#9B1D22"/><path d="M70 15 L70 85 L90 85 L90 15 Z" fill="#C1272D"/><path d="M80 15 L80 85 L90 85 L90 15 Z" fill="#9B1D22"/><path d="M35 30 L35 70 L65 70 L65 30 Z" fill="#D4372C"/><path d="M50 30 L50 70 L65 70 L65 30 Z" fill="#B12A25"/><path d="M40 5 L40 35 L60 35 L60 5 Z" fill="#E8523F"/><path d="M50 5 L50 35 L60 35 L60 5 Z" fill="#C1272D"/><path d="M40 65 L40 95 L60 95 L60 65 Z" fill="#E8523F"/><path d="M50 65 L50 95 L60 95 L60 65 Z" fill="#C1272D"/></svg>
            AWS
          </button>
        </div>
        <div style="margin-bottom:12px">
          <button class="btn" onclick="toggleCredPanel()" style="background:#3a3520;color:#f0c040;border:1px solid #f0c040;padding:12px 28px;font-size:16px;font-weight:600;cursor:pointer;display:flex;align-items:center;gap:10px;border-radius:6px">
            <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 00.33 1.82l.06.06a2 2 0 01-2.83 2.83l-.06-.06a1.65 1.65 0 00-1.82-.33 1.65 1.65 0 00-1 1.51V21a2 2 0 01-4 0v-.09A1.65 1.65 0 008.4 19.4a1.65 1.65 0 00-1.82.33l-.06.06a2 2 0 01-2.83-2.83l.06-.06a1.65 1.65 0 00.33-1.82 1.65 1.65 0 00-1.51-1H3a2 2 0 010-4h.09A1.65 1.65 0 004.6 8.4a1.65 1.65 0 00-.33-1.82l-.06-.06a2 2 0 012.83-2.83l.06.06a1.65 1.65 0 001.82.33H9a1.65 1.65 0 001-1.51V3a2 2 0 014 0v.09a1.65 1.65 0 001 1.51 1.65 1.65 0 001.82-.33l.06-.06a2 2 0 012.83 2.83l-.06.06a1.65 1.65 0 00-.33 1.82V9c.26.604.852.997 1.51 1H21a2 2 0 010 4h-.09a1.65 1.65 0 00-1.51 1z"/></svg>
            Manage Credentials
          </button>
        </div>
        <div id="credPanel" style="display:none;margin-bottom:16px;padding:16px;background:#13161f;border:1px solid #2d3348;border-radius:8px">
          <h4 style="color:#58a6ff;margin:0 0 12px 0;font-size:15px">Polaris Catalog Credentials</h4>
          <p style="color:#6e7681;font-size:13px;margin:0 0 16px 0">Update OAuth client credentials for each cloud provider. Changes take effect on next connect.</p>
          <div style="display:flex;gap:16px;flex-wrap:wrap">
            <div style="flex:1;min-width:280px;background:#1c2030;border:1px solid #2d3348;border-radius:6px;padding:14px">
              <div style="color:#58d68d;font-weight:600;font-size:14px;margin-bottom:10px">Google Cloud (GCS)</div>
              <label style="display:block;color:#8b949e;font-size:12px;margin-bottom:3px">Endpoint</label>
              <input id="credGcsEndpoint" style="width:100%;background:#13161f;border:1px solid #2d3348;color:#e1e4e8;padding:6px 8px;border-radius:4px;font-size:12px;margin-bottom:8px;box-sizing:border-box;font-family:monospace" />
              <label style="display:block;color:#8b949e;font-size:12px;margin-bottom:3px">Catalog Name</label>
              <input id="credGcsCatalog" style="width:100%;background:#13161f;border:1px solid #2d3348;color:#e1e4e8;padding:6px 8px;border-radius:4px;font-size:12px;margin-bottom:8px;box-sizing:border-box;font-family:monospace" />
              <label style="display:block;color:#8b949e;font-size:12px;margin-bottom:3px">Client ID</label>
              <input id="credGcsClientId" style="width:100%;background:#13161f;border:1px solid #2d3348;color:#e1e4e8;padding:6px 8px;border-radius:4px;font-size:12px;margin-bottom:8px;box-sizing:border-box;font-family:monospace" />
              <label style="display:block;color:#8b949e;font-size:12px;margin-bottom:3px">Client Secret</label>
              <input id="credGcsClientSecret" type="password" style="width:100%;background:#13161f;border:1px solid #2d3348;color:#e1e4e8;padding:6px 8px;border-radius:4px;font-size:12px;margin-bottom:10px;box-sizing:border-box;font-family:monospace" />
              <button class="btn btn-sm" onclick="saveCred('gcs')" style="background:#238636;color:#fff;border:none;padding:6px 16px;font-size:13px;width:100%">Save GCS Credentials</button>
              <div id="credGcsMsg" style="font-size:12px;margin-top:6px;color:#6e7681"></div>
            </div>
            <div style="flex:1;min-width:280px;background:#1c2030;border:1px solid #2d3348;border-radius:6px;padding:14px">
              <div style="color:#5dade2;font-weight:600;font-size:14px;margin-bottom:10px">Azure Data Lake</div>
              <label style="display:block;color:#8b949e;font-size:12px;margin-bottom:3px">Endpoint</label>
              <input id="credAzureEndpoint" style="width:100%;background:#13161f;border:1px solid #2d3348;color:#e1e4e8;padding:6px 8px;border-radius:4px;font-size:12px;margin-bottom:8px;box-sizing:border-box;font-family:monospace" />
              <label style="display:block;color:#8b949e;font-size:12px;margin-bottom:3px">Catalog Name</label>
              <input id="credAzureCatalog" style="width:100%;background:#13161f;border:1px solid #2d3348;color:#e1e4e8;padding:6px 8px;border-radius:4px;font-size:12px;margin-bottom:8px;box-sizing:border-box;font-family:monospace" />
              <label style="display:block;color:#8b949e;font-size:12px;margin-bottom:3px">Client ID</label>
              <input id="credAzureClientId" style="width:100%;background:#13161f;border:1px solid #2d3348;color:#e1e4e8;padding:6px 8px;border-radius:4px;font-size:12px;margin-bottom:8px;box-sizing:border-box;font-family:monospace" />
              <label style="display:block;color:#8b949e;font-size:12px;margin-bottom:3px">Client Secret</label>
              <input id="credAzureClientSecret" type="password" style="width:100%;background:#13161f;border:1px solid #2d3348;color:#e1e4e8;padding:6px 8px;border-radius:4px;font-size:12px;margin-bottom:10px;box-sizing:border-box;font-family:monospace" />
              <button class="btn btn-sm" onclick="saveCred('azure')" style="background:#238636;color:#fff;border:none;padding:6px 16px;font-size:13px;width:100%">Save Azure Credentials</button>
              <div id="credAzureMsg" style="font-size:12px;margin-top:6px;color:#6e7681"></div>
            </div>
            <div style="flex:1;min-width:280px;background:#1c2030;border:1px solid #2d3348;border-radius:6px;padding:14px">
              <div style="color:#f5b041;font-weight:600;font-size:14px;margin-bottom:10px">AWS S3</div>
              <label style="display:block;color:#8b949e;font-size:12px;margin-bottom:3px">Endpoint</label>
              <input id="credAwsEndpoint" style="width:100%;background:#13161f;border:1px solid #2d3348;color:#e1e4e8;padding:6px 8px;border-radius:4px;font-size:12px;margin-bottom:8px;box-sizing:border-box;font-family:monospace" />
              <label style="display:block;color:#8b949e;font-size:12px;margin-bottom:3px">Catalog Name</label>
              <input id="credAwsCatalog" style="width:100%;background:#13161f;border:1px solid #2d3348;color:#e1e4e8;padding:6px 8px;border-radius:4px;font-size:12px;margin-bottom:8px;box-sizing:border-box;font-family:monospace" />
              <label style="display:block;color:#8b949e;font-size:12px;margin-bottom:3px">Client ID</label>
              <input id="credAwsClientId" style="width:100%;background:#13161f;border:1px solid #2d3348;color:#e1e4e8;padding:6px 8px;border-radius:4px;font-size:12px;margin-bottom:8px;box-sizing:border-box;font-family:monospace" />
              <label style="display:block;color:#8b949e;font-size:12px;margin-bottom:3px">Client Secret</label>
              <input id="credAwsClientSecret" type="password" style="width:100%;background:#13161f;border:1px solid #2d3348;color:#e1e4e8;padding:6px 8px;border-radius:4px;font-size:12px;margin-bottom:10px;box-sizing:border-box;font-family:monospace" />
              <button class="btn btn-sm" onclick="saveCred('aws')" style="background:#238636;color:#fff;border:none;padding:6px 16px;font-size:13px;width:100%">Save AWS Credentials</button>
              <div id="credAwsMsg" style="font-size:12px;margin-top:6px;color:#6e7681"></div>
            </div>
          </div>
        </div>
        <div id="cloudConnectStatus" style="margin-bottom:14px;font-size:14px;color:#6e7681"></div>
        <div id="polarisMsg" style="margin-bottom:12px;font-size:14px;color:#6e7681"></div>
        <input id="polarisAlias" type="hidden" />
        <input id="polarisEndpoint" type="hidden" />
        <input id="polarisCatalog" type="hidden" />
        <input id="polarisClientId" type="hidden" />
        <input id="polarisClientSecret" type="hidden" />
        <div id="connectedCatalogs" style="margin-bottom:16px"></div>
        <div id="polarisBrowser" style="display:none">
          <div style="display:flex;gap:16px">
            <div style="min-width:220px;display:flex;flex-direction:column">
              <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
                <h4 style="color:#58a6ff;margin:0;font-size:15px">Namespaces</h4>
                <span id="polarisNsCount" style="font-size:12px;color:#6e7681"></span>
                <span style="cursor:pointer;font-size:12px;color:#6e7681;margin-left:auto" onclick="sortPolarisNs()" id="polarisNsSortBtn" title="Sort">Name &#9650;</span>
              </div>
              <input type="text" id="polarisNsSearch" placeholder="Filter namespaces..." oninput="filterPolarisList('polarisNamespaces', this.value)" style="background:#1c2030;border:1px solid #2d3348;color:#e1e4e8;padding:6px 8px;border-radius:4px;font-size:13px;margin-bottom:6px;width:100%;box-sizing:border-box" />
              <div id="polarisNamespaces" style="border:1px solid #2d3348;border-radius:5px;flex:1;max-height:500px;overflow:auto;background:#1c2030"></div>
            </div>
            <div style="flex:1;display:flex;flex-direction:column">
              <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px">
                <h4 style="color:#58a6ff;margin:0;font-size:15px">Tables</h4>
                <span id="polarisNsLabel" style="color:#6e7681;font-weight:normal;font-size:14px"></span>
                <span id="polarisTblCount" style="font-size:12px;color:#6e7681"></span>
                <span style="cursor:pointer;font-size:12px;color:#6e7681;margin-left:auto" onclick="sortPolarisTbl()" id="polarisTblSortBtn" title="Sort">Name &#9650;</span>
              </div>
              <input type="text" id="polarisTblSearch" placeholder="Filter tables..." oninput="filterPolarisList('polarisTables', this.value)" style="background:#1c2030;border:1px solid #2d3348;color:#e1e4e8;padding:6px 8px;border-radius:4px;font-size:13px;margin-bottom:6px;width:100%;box-sizing:border-box" />
              <div id="polarisTables" style="border:1px solid #2d3348;border-radius:5px;flex:1;max-height:500px;overflow:auto;background:#1c2030"></div>
            </div>
          </div>
          <div style="margin-top:16px;padding:12px;background:#1a1f35;border-radius:5px;border:1px solid #2d3348">
            <span style="color:#6e7681;font-size:14px">Query Iceberg tables in the <b>SQL Query</b> tab using:</span>
            <code style="display:block;margin-top:8px;color:#f0c040;font-size:15px">SELECT * FROM &lt;connection&gt;.&lt;namespace&gt;.&lt;table&gt; LIMIT 100</code>
          </div>
        </div>
      </div>
    </div>
    <div class="panel" id="panel-docs">
      <div style="padding:24px 32px;overflow:auto;color:#c9d1d9;font-size:16px;line-height:1.7;scrollbar-width:none;-ms-overflow-style:none">
        <h2 style="color:#58a6ff;margin-top:0">GCS Parquet Explorer</h2>
        <p style="color:#6e7681;font-style:italic">Engineered by the Fivetran SAP Specialist Team</p>

        <h3 style="color:#f0c040">What Is This?</h3>
        <p>A web-based data exploration tool for browsing and querying SAP CDS view data across multiple cloud providers. It connects to <b>Google Cloud Storage</b> (raw Parquet files) and <b>Fivetran Polaris</b> (Apache Iceberg catalogs on GCS, Azure, and AWS). All queries run through <b>DuckDB</b>, an in-memory SQL engine.</p>

        <h3 style="color:#f0c040">Getting Started</h3>
        <ol>
          <li><b>Open</b> &mdash; Navigate to this URL in your browser. You will see a login screen.</li>
          <li><b>Sign in</b> &mdash; Enter your <b>Fivetran email address</b>. This email is used to authenticate with Google Cloud, Azure, and AWS storage backends.</li>
          <li><b>Browse</b> &mdash; Once signed in, the sidebar shows the GCS bucket contents. Click directories to navigate, or use the <b>Polaris Catalog</b> tab to connect to Iceberg catalogs.</li>
        </ol>

        <h3 style="color:#f0c040">Browsing GCS Parquet Data</h3>
        <p>The sidebar lets you navigate raw Parquet files stored in Google Cloud Storage buckets.</p>
        <ul>
          <li><b>Navigate</b> &mdash; Click directories in the sidebar to drill down. The breadcrumb at the top shows your current path; click any segment to jump back.</li>
          <li><b>Home</b> &mdash; Click the house icon in the breadcrumb to return to the bucket list and switch buckets.</li>
          <li><b>Filter</b> &mdash; Type in the filter box to narrow down directories and files by name.</li>
          <li><b>Sort</b> &mdash; Click <b>Name</b>, <b>Files</b>, or <b>Size</b> column headers to sort. Click again to reverse.</li>
          <li><b>Load a table</b> &mdash; Click a <code style="background:#1a1f35;padding:2px 6px;border-radius:3px">.parquet</code> file to load it, or click the <span style="color:#f0c040">Load all parquet data</span> button to load all files in a directory as one table.</li>
          <li><b>View data</b> &mdash; After loading, the <b>Data</b> tab shows the table contents. Click any column header to sort. The <b>Schema</b> tab shows column names and types.</li>
        </ul>

        <h3 style="color:#f0c040">Polaris Iceberg Catalogs (Multi-Cloud)</h3>
        <p>The <b>Polaris Catalog</b> tab connects to Fivetran Polaris, which manages Apache Iceberg tables across three cloud providers. This is the recommended way to query production SAP CDS data.</p>
        <ul>
          <li><b>One-click connect</b> &mdash; Click <b>Google Cloud</b>, <b>Azure</b>, or <b>AWS</b>. This connects the Polaris catalog <i>and</i> sets up storage credentials in a single step.</li>
          <li><b>Browse namespaces</b> &mdash; After connecting, namespaces (schemas) appear on the left. Click one to see its tables on the right.</li>
          <li><b>Query a table</b> &mdash; Click a table name to populate the SQL tab, or click the play icon to run a <code style="background:#1a1f35;padding:2px 6px;border-radius:3px">SELECT * LIMIT 100</code> immediately.</li>
          <li><b>Multiple catalogs</b> &mdash; You can connect GCS, Azure, and AWS at the same time. Each appears as a button in the "Connected" bar. Click to switch between them.</li>
          <li><b>Cross-cloud joins</b> &mdash; All catalogs are in the same DuckDB instance, so you can join data across clouds:<br><code style="background:#1a1f35;padding:2px 6px;border-radius:3px;display:inline-block;margin-top:6px">SELECT * FROM gcs.ns.tbl JOIN ts_adls_destination_demo.ns.tbl USING(id)</code></li>
          <li><b>Search &amp; Sort</b> &mdash; Use the search boxes above namespace and table lists to filter. Click column headers to sort.</li>
          <li><b>Disconnect</b> &mdash; Click the &times; on a connected catalog button to detach it.</li>
        </ul>

        <h3 style="color:#f0c040">Managing Polaris Credentials</h3>
        <p>The <b>Manage Credentials</b> button in the Polaris Catalog tab lets you view and update the OAuth client credentials used to connect to each cloud provider's Polaris catalog.</p>
        <ul>
          <li><b>Open the panel</b> &mdash; Click <b>Manage Credentials</b> (gear icon) below the connect buttons. Three cards appear, one for each provider: Google Cloud, Azure, and AWS.</li>
          <li><b>View current settings</b> &mdash; Each card shows the current Polaris endpoint URL, catalog name, and client ID. The client secret is masked for security.</li>
          <li><b>Update credentials</b> &mdash; Edit any field and click <b>Save</b>. To keep the existing client secret, leave the secret field empty. To change it, type the new value.</li>
          <li><b>Reconnect</b> &mdash; After saving new credentials, click the cloud provider button (Google Cloud / Azure / AWS) to reconnect with the updated settings. Any previously connected catalog using old credentials is automatically disconnected.</li>
          <li><b>Runtime only</b> &mdash; Credential changes are stored in memory and persist until the server restarts. To make changes permanent, the server configuration file must be updated.</li>
        </ul>

        <h3 style="color:#f0c040">SQL Queries</h3>
        <p>The <b>SQL Query</b> tab gives you a full DuckDB SQL editor.</p>
        <ul>
          <li><b>Run a query</b> &mdash; Type your SQL and press <b>Ctrl+Enter</b> (or click Run).</li>
          <li><b>Auto-load</b> &mdash; If you reference a GCS table that is not loaded yet, the tool automatically downloads it from the current browse path and retries the query.</li>
          <li><b>Autocomplete</b> &mdash; Start typing after <code style="background:#1a1f35;padding:2px 6px;border-radius:3px">FROM</code> or <code style="background:#1a1f35;padding:2px 6px;border-radius:3px">JOIN</code> to see table name suggestions. All loaded GCS tables and connected Polaris Iceberg tables are included.</li>
          <li><b>Table addressing</b> &mdash; GCS tables are referenced by name (e.g., <code style="background:#1a1f35;padding:2px 6px;border-radius:3px">dd02l_all</code>). Polaris tables use three-part names: <code style="background:#1a1f35;padding:2px 6px;border-radius:3px">catalog.namespace.table</code>.</li>
          <li><b>Show Tables</b> &mdash; Click the button to list all currently loaded tables with row and column counts.</li>
        </ul>

        <h3 style="color:#f0c040">GCS Parquet vs Polaris Iceberg</h3>
        <ul>
          <li><b>GCS Parquet</b> &mdash; Reads raw Parquet files directly from a GCS bucket. Best for ad-hoc file inspection, exploring raw data exports, and debugging.</li>
          <li><b>Polaris Iceberg</b> &mdash; Reads through the Iceberg catalog with snapshot management, schema evolution, and partition pruning. Best for querying production Fivetran destination data across all three clouds.</li>
        </ul>

        <h3 style="color:#f0c040">Tabs Reference</h3>
        <ul>
          <li><b>Data</b> &mdash; View loaded table contents with sortable columns. Click any cell to see the full value.</li>
          <li><b>Schema</b> &mdash; Column names, data types, and row count for the last loaded table.</li>
          <li><b>SQL Query</b> &mdash; DuckDB SQL editor with autocomplete and results grid.</li>
          <li><b>Delta Log</b> &mdash; View Delta Lake transaction log entries for the current directory.</li>
          <li><b>Polaris Catalog</b> &mdash; Connect and browse Polaris Iceberg catalogs (GCS, Azure, AWS).</li>
          <li><b>Documentation</b> &mdash; This page.</li>
        </ul>

        <h3 style="color:#f0c040">How It Works</h3>
        <ul>
          <li>The server runs on an internal VM with HTTPS. All data stays in-memory on the server.</li>
          <li><b>DuckDB</b> is the SQL engine &mdash; tables are registered in-memory and queries execute locally on the server.</li>
          <li><b>GCS browsing</b> uses the Google Cloud Storage API with application default credentials configured on the server.</li>
          <li><b>Polaris catalogs</b> connect via the Iceberg REST protocol. Namespace and table listings use the Polaris REST API directly for fast response times.</li>
          <li><b>Storage access</b> for Iceberg data files uses DuckDB extensions (httpfs, azure, iceberg) with credential chain authentication.</li>
          <li><b>Memory management</b> &mdash; When server memory reaches 85%, the oldest loaded table is automatically evicted to free space.</li>
        </ul>

        <h3 style="color:#f0c040">Deployment Note</h3>
        <p>The source code in the <b>GitHub repository</b> does not contain any secrets. All credentials (Polaris OAuth client IDs/secrets, login password, AWS keys) are loaded from <b>environment variables</b> at runtime.</p>
        <ul>
          <li>On the server, credentials are stored in <code style="background:#1a1f35;padding:2px 6px;border-radius:3px">/usr/sap/gcs_explorer.env</code> and loaded by systemd via <code style="background:#1a1f35;padding:2px 6px;border-radius:3px">EnvironmentFile</code>.</li>
          <li>When deploying from the repo to a <b>new server</b>, copy <code style="background:#1a1f35;padding:2px 6px;border-radius:3px">server/gcs_explorer.env.example</code> to <code style="background:#1a1f35;padding:2px 6px;border-radius:3px">/usr/sap/gcs_explorer.env</code> and fill in real values.</li>
          <li>The <b>current production server</b> (sapidesecc8) has credentials hardcoded in its deployed copy and continues to work without the env file.</li>
          <li>You can also update credentials at runtime using the <b>Manage Credentials</b> panel in the Polaris Catalog tab (changes persist in memory until restart).</li>
        </ul>

        <h3 style="color:#f0c040">Troubleshooting</h3>
        <ul>
          <li><b>Cannot load Polaris tables</b> &mdash; Make sure you clicked one of the cloud provider buttons (Google Cloud / Azure / AWS) to connect the catalog first.</li>
          <li><b>Storage authentication error</b> &mdash; The cloud connect buttons handle storage auth automatically. If it still fails, the server administrator may need to refresh credentials on the server.</li>
          <li><b>Query returns no results</b> &mdash; Check the table name and namespace. Use the Polaris Catalog tab to browse and verify available tables.</li>
          <li><b>Slow query on large tables</b> &mdash; Add a <code style="background:#1a1f35;padding:2px 6px;border-radius:3px">LIMIT</code> clause or filter with <code style="background:#1a1f35;padding:2px 6px;border-radius:3px">WHERE</code> to reduce the data scanned.</li>
          <li><b>Table not found in SQL</b> &mdash; For GCS tables, navigate to the directory in the sidebar first, then the auto-loader can find it. For Polaris tables, use the full three-part name.</li>
        </ul>
      </div>
    </div>
  </div>
</div>
<script>
// Draggable splitter
(function() {
  const splitter = document.getElementById('splitter');
  const sidebar = document.querySelector('.sidebar');
  let dragging = false;
  splitter.addEventListener('mousedown', (e) => { dragging = true; splitter.classList.add('active'); document.body.style.cursor = 'col-resize'; document.body.style.userSelect = 'none'; e.preventDefault(); });
  document.addEventListener('mousemove', (e) => { if (!dragging) return; const newW = e.clientX; if (newW >= 200 && newW <= window.innerWidth - 200) sidebar.style.width = newW + 'px'; });
  document.addEventListener('mouseup', () => { if (dragging) { dragging = false; splitter.classList.remove('active'); document.body.style.cursor = ''; document.body.style.userSelect = ''; } });
})();

let currentPrefix = '';
let allRows = null;
let browsedTables = [];  // table names from sidebar for autocomplete
let polarisTables = [];  // Polaris Iceberg tables for autocomplete
let lastBrowseItems = [];  // cached items for re-sorting
let lastBrowsePrefix = '';
let lastHasDataDir = false;
let lastHasDeltaLog = false;
let sortField = 'name';
let sortAsc = true;

const BASE_PATH = '/datalake_reader';
async function api(endpoint, params = {}) {
  const url = new URL(BASE_PATH + endpoint, location.origin);
  Object.entries(params).forEach(([k,v]) => url.searchParams.append(k, v));
  const r = await fetch(url);
  return r.json();
}

let currentBucket = '';

async function init() {
  // Show logged-in user
  try {
    const who = await api('/api/whoami');
    if (who.status === 'ok') document.getElementById('userEmail').textContent = who.email;
  } catch(e) {}
  const r = await api('/api/init');
  const el = document.getElementById('authStatus');
  if (r.status === 'ok') {
    el.textContent = 'Connected'; el.className = 'status ok';
    currentBucket = r.last_bucket || '';
    if (r.last_prefix !== undefined && r.last_prefix !== null) {
      // Restore last browsed location
      browse(r.last_prefix, r.last_bucket);
    } else {
      // First time — default to sap_cds_dbt / sap_cds_views/
      browse('sap_cds_views/', 'sap_cds_dbt');
    }
  } else {
    el.textContent = 'Error'; el.className = 'status error';
    document.getElementById('fileList').innerHTML = '<div class="empty" style="padding:30px 20px;text-align:left">'
      + '<div style="color:#f06060;font-size:18px;margin-bottom:12px">GCS connection failed</div>'
      + '<div style="color:#8b949e;margin-bottom:16px;font-size:15px">' + escHtml(r.message) + '</div>'
      + '<button class="btn btn-blue" onclick="runAuth()" id="authBtn" style="font-size:16px;padding:10px 20px">Authenticate with Google Cloud</button>'
      + '<div id="authMsg" style="margin-top:12px;font-size:14px;color:#6e7681"></div>'
      + '</div>';
  }
}

async function browseBuckets() {
  document.getElementById('sidebarSearch').value = '';
  document.getElementById('fileList').innerHTML = '<div class="loading">Loading buckets...</div>';
  const r = await api('/api/buckets');
  if (r.status !== 'ok') {
    document.getElementById('fileList').innerHTML = '<div class="empty">' + r.message + '</div>';
    return;
  }
  currentPrefix = '';
  currentBucket = '';
  document.getElementById('breadcrumb').innerHTML = '<span style="color:#f0c040;cursor:default">All Buckets</span>';
  let html = '';
  for (const b of r.buckets) {
    html += '<div class="file-item dir" onclick="browse(\'\', \'' + b.name + '\')"><span class="icon">&#128230;</span><span class="name">gs://' + b.name + '</span></div>';
  }
  if (!r.buckets.length) html = '<div class="empty">No accessible buckets</div>';
  document.getElementById('fileList').innerHTML = html;
}

async function browse(prefix, bucket) {
  if (prefix === undefined) prefix = currentPrefix;
  if (bucket) currentBucket = bucket;
  currentPrefix = prefix;
  document.getElementById('sidebarSearch').value = '';
  document.getElementById('fileList').innerHTML = '<div class="loading">Loading...</div>';
  const params = { prefix };
  if (currentBucket) params.bucket = currentBucket;
  const r = await api('/api/ls', params);
  if (r.status !== 'ok') {
    document.getElementById('fileList').innerHTML = '<div class="empty">' + r.message + '</div>';
    return;
  }
  if (r.bucket) currentBucket = r.bucket;
  updateBreadcrumb(prefix);

  lastBrowseItems = r.items;
  lastBrowsePrefix = prefix;
  lastHasDataDir = r.items.some(i => i.name === 'data/');
  lastHasDeltaLog = r.items.some(i => i.name === '_delta_log/');
  browsedTables = r.items.filter(i => i.is_dir && i.name !== 'data/' && i.name !== '_delta_log/').map(i => i.name.replace('/', ''));

  // Show sort bar only when dirs have stats
  const hasStats = r.items.some(i => i.is_dir && i.file_count);
  document.getElementById('sortBar').style.display = hasStats ? 'flex' : 'none';

  renderFileList();
  if (lastHasDeltaLog) loadDeltaLog(prefix);
}

function renderFileList() {
  const prefix = lastBrowsePrefix;
  const items = [...lastBrowseItems];

  // Sort directories
  const dirs = items.filter(i => i.is_dir);
  const files = items.filter(i => !i.is_dir);
  dirs.sort((a, b) => {
    let cmp = 0;
    if (sortField === 'files') cmp = (a.file_count || 0) - (b.file_count || 0);
    else if (sortField === 'size') cmp = (a.size || 0) - (b.size || 0);
    else cmp = a.name.localeCompare(b.name);
    return sortAsc ? cmp : -cmp;
  });

  let html = '';
  // Parent dir
  if (prefix) {
    const parts = prefix.replace(/\/+$/, '').split('/');
    if (parts.length > 1) {
      const parent = parts.slice(0, -1).join('/') + '/';
      html += '<div class="file-item dir" onclick="browse(\'' + parent + '\')"><span class="icon">&#11014;</span><span class="name">..</span></div>';
    } else {
      html += '<div class="file-item dir" onclick="browse(\'\')"><span class="icon">&#11014;</span><span class="name">..</span></div>';
    }
  } else if (currentBucket) {
    html += '<div class="file-item dir" onclick="browseBuckets()"><span class="icon">&#11014;</span><span class="name">.. (all buckets)</span></div>';
  }

  if (lastHasDataDir) {
    html += '<div class="file-item" style="background:#1a1f35;border-left:3px solid #f0c040"><span class="icon">&#9889;</span><span class="name" style="color:#f0c040">Load all parquet data</span><button class="btn btn-orange btn-sm" onclick="event.stopPropagation();loadDir(\'' + prefix + '\')">Load</button></div>';
  }

  for (const item of dirs) {
    let meta = '';
    if (item.file_count) meta += '<span class="dir-files">' + item.file_count + ' file' + (item.file_count > 1 ? 's' : '') + '</span>';
    if (item.size) meta += '<span class="dir-size">' + formatSize(item.size) + '</span>';
    html += '<div class="file-item dir" onclick="browse(\'' + item.path + '\')"><span class="icon">&#128193;</span><span class="name">' + item.name + '</span><span class="meta">' + meta + '</span></div>';
  }
  for (const item of files) {
    const isPq = item.name.endsWith('.parquet');
    const isJson = item.name.endsWith('.json');
    const icon = isPq ? '&#128202;' : isJson ? '&#128196;' : '&#128206;';
    const size = item.size ? formatSize(item.size) : '';
    const action = isPq ? "loadParquet('" + item.path + "')" : "loadText('" + item.path + "')";
    html += '<div class="file-item file" onclick="' + action + '"><span class="icon">' + icon + '</span><span class="name">' + item.name + '</span><span class="size">' + size + '</span></div>';
  }
  if (!items.length) html = '<div class="empty">Empty directory</div>';
  document.getElementById('fileList').innerHTML = html;
}

function toggleSort(field) {
  if (sortField === field) { sortAsc = !sortAsc; }
  else { sortField = field; sortAsc = (field === 'name'); }
  // Update sort bar UI
  document.querySelectorAll('.sort-col').forEach(el => {
    const f = el.dataset.sort;
    el.classList.toggle('active', f === sortField);
    const arrow = el.querySelector('.arrow');
    if (f === sortField) arrow.innerHTML = sortAsc ? '&#9650;' : '&#9660;';
    else arrow.innerHTML = '';
  });
  renderFileList();
  // Re-apply filter if active
  const q = document.getElementById('sidebarSearch').value;
  if (q) filterSidebar(q);
}

function updateBreadcrumb(prefix) {
  let html = '<span onclick="browseBuckets()" title="All buckets">&#127968;</span>';
  if (currentBucket) {
    html += '<span class="sep">/</span>';
    html += '<span onclick="browse(\'\', \'' + currentBucket + '\')" title="gs://' + currentBucket + '">' + currentBucket + '</span>';
  }
  if (prefix) {
    const parts = prefix.replace(/\/+$/, '').split('/');
    let acc = '';
    for (let i = 0; i < parts.length; i++) {
      acc += parts[i] + '/';
      const p = acc;
      html += '<span class="sep">/</span>';
      html += '<span onclick="browse(\'' + p + '\')">' + parts[i] + '</span>';
    }
  }
  document.getElementById('breadcrumb').innerHTML = html;
}

async function loadParquet(path) {
  switchTab('data');
  document.getElementById('dataTable').innerHTML = '<div class="loading">Loading parquet...</div>';
  document.getElementById('dataInfo').textContent = '';
  const r = await api('/api/parquet', { path });
  if (r.status !== 'ok') {
    document.getElementById('dataTable').innerHTML = '<div class="empty" style="color:#f06060">' + r.message + '</div>';
    return;
  }
  allRows = r.rows;
  renderTable('dataTable', r.columns, r.rows);
  document.getElementById('dataInfo').textContent = r.total_rows + ' rows | ' + r.columns.length + ' cols | registered as: ' + (r.registered_as || 'N/A');
  document.getElementById('schemaView').textContent = r.schema || '';
  updateLoadedTables();
}

async function loadDir(prefix) {
  switchTab('data');
  document.getElementById('dataTable').innerHTML = '<div class="loading">Loading all parquet files...</div>';
  const r = await api('/api/load_dir', { prefix });
  if (r.status !== 'ok') {
    document.getElementById('dataTable').innerHTML = '<div class="empty" style="color:#f06060">' + r.message + '</div>';
    return;
  }
  allRows = r.rows;
  renderTable('dataTable', r.columns, r.rows);
  document.getElementById('dataInfo').textContent = r.total_rows + ' rows | ' + r.columns.length + ' cols | ' + r.files_read + ' files | registered as: ' + r.registered_as;
  document.getElementById('schemaView').textContent = r.schema || '';
  updateLoadedTables();
}

async function loadText(path) {
  switchTab('schema');
  document.getElementById('schemaView').textContent = 'Loading...';
  const r = await api('/api/cat', { path });
  document.getElementById('schemaView').textContent = r.status === 'ok' ? r.content : r.message;
}

async function loadDeltaLog(prefix) {
  const r = await api('/api/delta', { prefix });
  const el = document.getElementById('deltaView');
  if (r.status !== 'ok') { el.innerHTML = '<div class="empty">' + r.message + '</div>'; return; }
  let html = '';
  for (const e of r.log_entries) {
    html += '<div class="delta-entry"><div class="delta-entry-header">' + e.file + '</div><div class="delta-entry-body">' + escHtml(JSON.stringify(e.entries, null, 2)) + '</div></div>';
  }
  el.innerHTML = html || '<div class="empty">No delta log entries</div>';
}

async function runQuery() {
  const q = document.getElementById('sqlInput').value.trim();
  if (!q) return;
  document.getElementById('sqlResults').innerHTML = '<div class="loading">Running...</div>';
  document.getElementById('sqlInfo').textContent = '';
  const t0 = Date.now();
  const r = await api('/api/sql', { query: q, prefix: currentPrefix });
  const ms = Date.now() - t0;
  if (r.status !== 'ok') {
    document.getElementById('sqlResults').innerHTML = '<div class="empty" style="color:#f06060;white-space:pre-wrap;text-align:left;padding:20px;font-family:monospace;font-size:17px">' + escHtml(r.message) + '</div>';
    document.getElementById('sqlInfo').textContent = 'Error | ' + ms + 'ms';
    return;
  }
  renderTable('sqlResults', r.columns, r.rows);
  document.getElementById('sqlInfo').textContent = r.total_rows + ' rows | ' + r.columns.length + ' cols | ' + ms + 'ms';
  updateLoadedTables();
}

async function showTables() {
  const r = await api('/api/tables');
  if (r.status === 'ok' && r.tables.length) {
    let msg = 'Loaded tables:\\n';
    for (const t of r.tables) msg += '  ' + t.name + ' (' + t.rows + ' rows, ' + t.columns + ' cols)\\n';
    document.getElementById('sqlInput').value = '-- Available tables:\\n' + r.tables.map(t => '-- ' + t.name + ' (' + t.rows + ' rows)').join('\\n') + '\\n\\nSELECT * FROM ' + r.tables[0].name + ' LIMIT 10';
  }
}

async function updateLoadedTables() {
  const r = await api('/api/tables');
  const el = document.getElementById('loadedTables');
  if (r.status === 'ok' && r.tables.length) {
    el.innerHTML = 'Loaded: ' + r.tables.map(t => '<span class="tag" onclick="setSQL(\'SELECT * FROM ' + t.name + ' LIMIT 100\')" title="' + t.rows + ' rows, ' + t.columns + ' cols">' + t.name + '</span>').join('');
  } else {
    el.innerHTML = '';
  }
}

function setSQL(q) { document.getElementById('sqlInput').value = q; switchTab('sql'); }

function renderTable(id, columns, rows) {
  const el = document.getElementById(id);
  if (!columns || !columns.length) { el.innerHTML = '<div class="empty">No data</div>'; return; }
  el._data = { columns, rows, sortCol: -1, sortAsc: true };
  _renderTableHTML(id, columns, rows);
}

function _renderTableHTML(id, columns, rows, sortCol, sortAsc) {
  const el = document.getElementById(id);
  let h = '<table><thead><tr><th>#</th>';
  for (let ci = 0; ci < columns.length; ci++) {
    const c = columns[ci];
    const isFivetranId = c.toUpperCase() === '_FIVETRAN_ID';
    if (isFivetranId) {
      h += '<th>' + escHtml(c) + '</th>';
    } else {
      const cls = sortCol === ci ? (sortAsc ? 'sortable sort-asc' : 'sortable sort-desc') : 'sortable';
      h += '<th class="' + cls + '" onclick="sortTable(\'' + id + '\',' + ci + ')">' + escHtml(c) + '</th>';
    }
  }
  h += '</tr></thead><tbody>';
  for (let i = 0; i < rows.length; i++) {
    h += '<tr><td style="color:#6e7681">' + (i+1) + '</td>';
    for (let j = 0; j < rows[i].length; j++) {
      const v = rows[i][j] || '';
      const trunc = v.length > 80 ? v.substring(0,80) + '...' : v;
      const click = v.length > 80 ? ' onclick="showCell(\'' + id + '\',' + i + ',' + j + ')" style="cursor:pointer;color:#58a6ff"' : '';
      h += '<td' + click + ' title="' + escAttr(v.substring(0,200)) + '">' + escHtml(trunc) + '</td>';
    }
    h += '</tr>';
  }
  h += '</tbody></table>';
  el.innerHTML = h;
}

function sortTable(id, colIdx) {
  const el = document.getElementById(id);
  const d = el._data;
  if (!d) return;
  const asc = (d.sortCol === colIdx) ? !d.sortAsc : true;
  const sorted = [...d.rows].sort((a, b) => {
    const va = a[colIdx] || '', vb = b[colIdx] || '';
    const na = parseFloat(va), nb = parseFloat(vb);
    if (!isNaN(na) && !isNaN(nb)) return asc ? na - nb : nb - na;
    return asc ? va.localeCompare(vb) : vb.localeCompare(va);
  });
  d.sortCol = colIdx;
  d.sortAsc = asc;
  d.rows = sorted;
  _renderTableHTML(id, d.columns, sorted, colIdx, asc);
}

function showCell(containerId, row, col) {
  const d = document.getElementById(containerId)._data;
  if (!d) return;
  const val = d.rows[row][col];
  const colName = d.columns[col];
  const ov = document.createElement('div'); ov.className = 'overlay';
  ov.onclick = () => { ov.remove(); popup.remove(); };
  document.body.appendChild(ov);
  const popup = document.createElement('div'); popup.className = 'cell-popup';
  popup.innerHTML = '<div style="margin-bottom:8px;color:#58a6ff;font-weight:600">' + escHtml(colName) + ' (row ' + (row+1) + ')</div>' + escHtml(val);
  document.body.appendChild(popup);
}

function switchTab(name) {
  document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t.dataset.tab === name));
  document.querySelectorAll('.panel').forEach(p => p.classList.toggle('active', p.id === 'panel-' + name));
}

function formatSize(bytes) {
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1048576) return (bytes/1024).toFixed(1) + ' KB';
  return (bytes/1048576).toFixed(1) + ' MB';
}

function filterSidebar(query) {
  const q = query.toLowerCase();
  const items = document.querySelectorAll('#fileList .file-item');
  items.forEach(el => {
    const name = el.querySelector('.name');
    if (!name) return;
    const text = name.textContent.toLowerCase();
    // Always show ".." parent nav
    if (text === '..' || text === '.. (all buckets)') { el.style.display = ''; return; }
    el.style.display = (!q || text.includes(q)) ? '' : 'none';
  });
}

function escHtml(s) { return s ? s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;') : ''; }

async function runAuth() {
  const btn = document.getElementById('authBtn');
  const msg = document.getElementById('authMsg');
  btn.disabled = true;
  btn.textContent = 'Authenticating... (check your browser)';
  msg.textContent = 'A browser window should open for Google login. Complete the sign-in there.';
  msg.style.color = '#f0c040';
  try {
    const r = await api('/api/auth');
    if (r.status === 'ok') {
      msg.textContent = r.message;
      msg.style.color = '#3fb950';
      btn.textContent = 'Authenticated!';
      document.getElementById('authStatus').textContent = 'Connected';
      document.getElementById('authStatus').className = 'status ok';
      setTimeout(() => init(), 1000);
    } else {
      msg.textContent = r.message;
      msg.style.color = '#f06060';
      btn.disabled = false;
      btn.textContent = 'Retry Authentication';
    }
  } catch(e) {
    msg.textContent = 'Request failed: ' + e.message;
    msg.style.color = '#f06060';
    btn.disabled = false;
    btn.textContent = 'Retry Authentication';
  }
}
async function connectCloud(provider) {
  const btn = document.getElementById('btnConnect' + provider.charAt(0).toUpperCase() + provider.slice(1));
  const status = document.getElementById('cloudConnectStatus');
  const msg = document.getElementById('polarisMsg');
  const labels = { gcs: 'Google Cloud', azure: 'Azure', aws: 'AWS' };
  const origText = btn.textContent;
  btn.disabled = true;
  btn.textContent = 'Connecting...';
  msg.textContent = '';

  // Step 1: Connect Polaris catalog (fast — just OAuth + DuckDB attach)
  status.innerHTML = '<span style="color:#f0c040">Connecting ' + labels[provider] + ' Polaris catalog...</span>';
  loadPreset(provider);
  try {
    const r = await api('/api/polaris/connect', {
      alias: document.getElementById('polarisAlias').value,
      endpoint: document.getElementById('polarisEndpoint').value,
      catalog: document.getElementById('polarisCatalog').value,
      client_id: document.getElementById('polarisClientId').value,
      client_secret: document.getElementById('polarisClientSecret').value
    });
    if (r.status === 'ok') {
      activeCatalogAlias = r.alias;
      refreshConnectedCatalogs();
      document.getElementById('polarisBrowser').style.display = 'block';
      loadPolarisNamespaces(r.alias);
    } else {
      status.innerHTML = '<span style="color:#f06060">Catalog connect failed: ' + escHtml(r.message) + '</span>';
      msg.textContent = r.message; msg.style.color = '#f06060';
      btn.disabled = false; btn.textContent = origText;
      return;
    }
  } catch(e) {
    status.innerHTML = '<span style="color:#f06060">Connect error: ' + escHtml(e.message) + '</span>';
    btn.disabled = false; btn.textContent = origText;
    return;
  }

  // Step 2: Storage auth (may be slow — run after catalog is connected)
  status.innerHTML = '<span style="color:#f0c040">' + labels[provider] + ' catalog connected. Authenticating storage...</span>';
  msg.textContent = 'Catalog connected. Setting up storage credentials...';
  msg.style.color = '#3fb950';
  try {
    let authResult;
    if (provider === 'gcs') {
      authResult = await api('/api/auth');
    } else if (provider === 'azure') {
      authResult = await api('/api/azure_auth');
    } else if (provider === 'aws') {
      authResult = await api('/api/aws_auth', {
        mode: 'keys',
        access_key: '',  // Set via environment or UI
        secret_key: '',  // Set via environment or UI
        region: 'us-west-2'
      });
    }
    btn.disabled = false; btn.textContent = origText;
    if (authResult && authResult.status === 'ok') {
      status.innerHTML = '<span style="color:#3fb950">&#10003; ' + labels[provider] + ' connected (catalog + storage)</span>';
    } else {
      status.innerHTML = '<span style="color:#f0c040">&#10003; ' + labels[provider] + ' catalog connected. &#9888; Storage auth: ' + escHtml((authResult||{}).message||'failed') + '</span>';
    }
  } catch(e) {
    btn.disabled = false; btn.textContent = origText;
    status.innerHTML = '<span style="color:#f0c040">&#10003; ' + labels[provider] + ' catalog connected. &#9888; Storage auth error: ' + escHtml(e.message) + '</span>';
  }
}
function escAttr(s) { return s ? s.replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;') : ''; }

// Polaris Catalog
let catalogPresets = {};
let activeCatalogAlias = '';
let polarisNsData = [];
let polarisTblData = [];
let polarisNsSortAsc = true;
let polarisTblSortAsc = true;
let polarisActiveNs = '';

async function initPolarisForm() {
  try {
    const r = await api('/api/polaris/presets');
    if (r.status === 'ok') catalogPresets = r.presets;
  } catch(e) {}
  loadPreset('gcs');
}

function loadPreset(key) {
  const p = catalogPresets[key];
  if (!p) return;
  document.getElementById('polarisAlias').value = p.default_alias || key;
  document.getElementById('polarisEndpoint').value = p.endpoint || '';
  document.getElementById('polarisCatalog').value = p.catalog || '';
  document.getElementById('polarisClientId').value = p.client_id || '';
  document.getElementById('polarisClientSecret').value = p.client_secret || '';
}

function toggleCredPanel() {
  const panel = document.getElementById('credPanel');
  if (panel.style.display === 'none') {
    panel.style.display = 'block';
    loadCredFields();
  } else {
    panel.style.display = 'none';
  }
}
function loadCredFields() {
  const p = catalogPresets;
  if (p.gcs) {
    document.getElementById('credGcsEndpoint').value = p.gcs.endpoint || '';
    document.getElementById('credGcsCatalog').value = p.gcs.catalog || '';
    document.getElementById('credGcsClientId').value = p.gcs.client_id || '';
    document.getElementById('credGcsClientSecret').placeholder = p.gcs.client_secret_masked || '(current)';
    document.getElementById('credGcsClientSecret').value = '';
  }
  if (p.azure) {
    document.getElementById('credAzureEndpoint').value = p.azure.endpoint || '';
    document.getElementById('credAzureCatalog').value = p.azure.catalog || '';
    document.getElementById('credAzureClientId').value = p.azure.client_id || '';
    document.getElementById('credAzureClientSecret').placeholder = p.azure.client_secret_masked || '(current)';
    document.getElementById('credAzureClientSecret').value = '';
  }
  if (p.aws) {
    document.getElementById('credAwsEndpoint').value = p.aws.endpoint || '';
    document.getElementById('credAwsCatalog').value = p.aws.catalog || '';
    document.getElementById('credAwsClientId').value = p.aws.client_id || '';
    document.getElementById('credAwsClientSecret').placeholder = p.aws.client_secret_masked || '(current)';
    document.getElementById('credAwsClientSecret').value = '';
  }
}
async function saveCred(provider) {
  const cap = provider.charAt(0).toUpperCase() + provider.slice(1);
  const endpoint = document.getElementById('cred' + cap + 'Endpoint').value.trim();
  const catalog = document.getElementById('cred' + cap + 'Catalog').value.trim();
  const client_id = document.getElementById('cred' + cap + 'ClientId').value.trim();
  let client_secret = document.getElementById('cred' + cap + 'ClientSecret').value.trim();
  // If secret field left empty, use the existing one from presets
  if (!client_secret) client_secret = catalogPresets[provider]?.client_secret || '';
  const msgEl = document.getElementById('cred' + cap + 'Msg');
  if (!endpoint || !catalog || !client_id || !client_secret) {
    msgEl.style.color = '#f06060';
    msgEl.textContent = 'All fields are required.';
    return;
  }
  msgEl.style.color = '#6e7681';
  msgEl.textContent = 'Saving...';
  try {
    const resp = await fetch(BASE_PATH + '/api/polaris/update_credentials', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ provider, endpoint, catalog, client_id, client_secret })
    });
    const r = await resp.json();
    if (r.status === 'ok') {
      msgEl.style.color = '#58d68d';
      msgEl.textContent = r.message;
      // Refresh presets cache
      const pr = await api('/api/polaris/presets');
      if (pr.status === 'ok') catalogPresets = pr.presets;
      loadCredFields();
    } else {
      msgEl.style.color = '#f06060';
      msgEl.textContent = r.message || 'Failed to save.';
    }
  } catch (e) {
    msgEl.style.color = '#f06060';
    msgEl.textContent = 'Error: ' + e.message;
  }
}

async function connectPolaris() {
  // Legacy — use connectCloud() instead
}

async function disconnectCatalog(alias) {
  await api('/api/polaris/disconnect', { alias });
  refreshConnectedCatalogs();
  if (activeCatalogAlias === alias) {
    activeCatalogAlias = '';
    document.getElementById('polarisBrowser').style.display = 'none';
    document.getElementById('polarisNamespaces').innerHTML = '';
    document.getElementById('polarisTables').innerHTML = '';
  }
  try {
    const pt = await api('/api/polaris/all_tables');
    if (pt.status === 'ok') polarisTables = pt.tables;
  } catch(e) { polarisTables = []; }
}

async function refreshConnectedCatalogs() {
  const r = await api('/api/polaris/catalogs');
  const el = document.getElementById('connectedCatalogs');
  if (r.status !== 'ok' || !r.catalogs.length) { el.innerHTML = ''; return; }
  let html = '<div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center"><span style="font-size:13px;color:#6e7681">Connected:</span>';
  for (const c of r.catalogs) {
    const active = c.alias === activeCatalogAlias;
    html += '<span style="display:inline-flex;align-items:center;gap:6px;padding:4px 10px;border-radius:4px;font-size:13px;cursor:pointer;'
      + (active ? 'background:#1a3a1a;border:1px solid #3fb950;color:#3fb950' : 'background:#1a1f35;border:1px solid #2d3348;color:#58a6ff')
      + '" onclick="switchCatalog(\'' + c.alias + '\')">'
      + c.alias
      + '<span style="color:#6e7681;font-size:11px;cursor:pointer" onclick="event.stopPropagation();disconnectCatalog(\'' + c.alias + '\')" title="Disconnect">&#10005;</span>'
      + '</span>';
  }
  html += '</div>';
  el.innerHTML = html;
}

function switchCatalog(alias) {
  activeCatalogAlias = alias;
  refreshConnectedCatalogs();
  document.getElementById('polarisBrowser').style.display = 'block';
  loadPolarisNamespaces(alias);
}

async function loadPolarisNamespaces(alias) {
  if (!alias) alias = activeCatalogAlias;
  activeCatalogAlias = alias;
  const el = document.getElementById('polarisNamespaces');
  el.innerHTML = '<div style="padding:10px;color:#6e7681">Loading...</div>';
  document.getElementById('polarisNsSearch').value = '';
  const r = await api('/api/polaris/namespaces', { alias });
  if (r.status !== 'ok') { el.innerHTML = '<div style="padding:10px;color:#f06060">' + escHtml(r.message) + '</div>'; return; }
  polarisNsData = r.namespaces || [];
  polarisActiveNs = '';
  document.getElementById('polarisNsCount').textContent = '(' + polarisNsData.length + ')';
  document.getElementById('polarisTables').innerHTML = '';
  document.getElementById('polarisNsLabel').textContent = '';
  document.getElementById('polarisTblCount').textContent = '';
  renderPolarisNs();
}

function renderPolarisNs() {
  const el = document.getElementById('polarisNamespaces');
  const sorted = [...polarisNsData].sort((a, b) => polarisNsSortAsc ? a.localeCompare(b) : b.localeCompare(a));
  if (!sorted.length) { el.innerHTML = '<div style="padding:10px;color:#6e7681">No namespaces found</div>'; return; }
  let html = '';
  for (const ns of sorted) {
    const active = ns === polarisActiveNs;
    html += '<div class="polaris-item" data-name="' + escAttr(ns) + '" style="padding:8px 12px;cursor:pointer;color:#58a6ff;border-bottom:1px solid #2d3348;font-size:15px;'
      + (active ? 'background:#1a1f35;border-left:3px solid #58a6ff' : 'border-left:3px solid transparent')
      + '" onmouseover="if(!this.classList.contains(\'active\'))this.style.background=\'#1a1f35\'" onmouseout="if(!this.classList.contains(\'active\'))this.style.background=\'\'" onclick="loadPolarisTables(\'' + escAttr(ns) + '\')">' + escHtml(ns) + '</div>';
  }
  el.innerHTML = html;
}

function sortPolarisNs() {
  polarisNsSortAsc = !polarisNsSortAsc;
  document.getElementById('polarisNsSortBtn').innerHTML = 'Name ' + (polarisNsSortAsc ? '&#9650;' : '&#9660;');
  renderPolarisNs();
  const q = document.getElementById('polarisNsSearch').value;
  if (q) filterPolarisList('polarisNamespaces', q);
}

async function loadPolarisTables(namespace) {
  polarisActiveNs = namespace;
  renderPolarisNs();
  const q = document.getElementById('polarisNsSearch').value;
  if (q) filterPolarisList('polarisNamespaces', q);

  const el = document.getElementById('polarisTables');
  document.getElementById('polarisNsLabel').textContent = '(' + namespace + ')';
  document.getElementById('polarisTblSearch').value = '';
  el.innerHTML = '<div style="padding:10px;color:#6e7681">Loading...</div>';
  const r = await api('/api/polaris/tables', { alias: activeCatalogAlias, namespace });
  if (r.status !== 'ok') { el.innerHTML = '<div style="padding:10px;color:#f06060">' + escHtml(r.message) + '</div>'; return; }
  polarisTblData = (r.tables || []).map(t => ({ name: t, namespace, alias: activeCatalogAlias }));
  document.getElementById('polarisTblCount').textContent = '(' + polarisTblData.length + ')';
  renderPolarisTbl();
}

function renderPolarisTbl() {
  const el = document.getElementById('polarisTables');
  const sorted = [...polarisTblData].sort((a, b) => polarisTblSortAsc ? a.name.localeCompare(b.name) : b.name.localeCompare(a.name));
  if (!sorted.length) { el.innerHTML = '<div style="padding:10px;color:#6e7681">No tables in this namespace</div>'; return; }
  let html = '';
  for (const item of sorted) {
    const fqn = item.alias + '.' + item.namespace + '.' + item.name;
    html += '<div class="polaris-item" data-name="' + escAttr(item.name) + '" style="padding:8px 12px;cursor:pointer;color:#c9d1d9;border-bottom:1px solid #2d3348;font-size:15px;display:flex;justify-content:space-between;align-items:center" onmouseover="this.style.background=\'#1a1f35\'" onmouseout="this.style.background=\'\'">';
    html += '<span onclick="setSQL(\'SELECT * FROM ' + fqn + ' LIMIT 100\');switchTab(\'sql\')">' + escHtml(item.name) + '</span>';
    html += '<span style="font-size:12px;color:#6e7681;cursor:pointer" onclick="setSQL(\'SELECT * FROM ' + fqn + ' LIMIT 100\');switchTab(\'sql\');runQuery()">&#9654; query</span>';
    html += '</div>';
  }
  el.innerHTML = html;
}

function sortPolarisTbl() {
  polarisTblSortAsc = !polarisTblSortAsc;
  document.getElementById('polarisTblSortBtn').innerHTML = 'Name ' + (polarisTblSortAsc ? '&#9650;' : '&#9660;');
  renderPolarisTbl();
  const q = document.getElementById('polarisTblSearch').value;
  if (q) filterPolarisList('polarisTables', q);
}

function filterPolarisList(containerId, query) {
  const q = query.toLowerCase();
  document.querySelectorAll('#' + containerId + ' .polaris-item').forEach(el => {
    const name = (el.dataset.name || '').toLowerCase();
    el.style.display = (!q || name.includes(q)) ? '' : 'none';
  });
}

document.addEventListener('keydown', e => { if (e.ctrlKey && e.key === 'Enter') { runQuery(); e.preventDefault(); } });

// SQL Autocomplete
(function() {
  const ta = document.getElementById('sqlInput');
  const acList = document.getElementById('acList');
  let acItems = [], acIdx = -1, acWord = '', acStart = 0;

  const SQL_KW = ['SELECT','FROM','WHERE','AND','OR','NOT','IN','LIKE','BETWEEN','IS','NULL',
    'ORDER BY','GROUP BY','HAVING','LIMIT','OFFSET','JOIN','LEFT JOIN','RIGHT JOIN','INNER JOIN',
    'CROSS JOIN','FULL JOIN','ON','AS','DISTINCT','COUNT','SUM','AVG','MIN','MAX','CASE','WHEN',
    'THEN','ELSE','END','UNION','UNION ALL','EXCEPT','INTERSECT','INSERT','UPDATE','DELETE',
    'CREATE','TABLE','VIEW','WITH','EXISTS','CAST','COALESCE','IFNULL','TRIM','UPPER','LOWER',
    'LENGTH','SUBSTR','REPLACE','CONCAT','DESC','ASC','TRUE','FALSE','OVER','PARTITION BY',
    'ROW_NUMBER','RANK','DENSE_RANK','LAG','LEAD','FIRST_VALUE','LAST_VALUE','FILTER','USING',
    'LATERAL','UNNEST','EXPLAIN','DESCRIBE','SHOW','TABLES','COLUMNS','TYPEOF','LIST','STRUCT',
    'ARRAY_AGG','STRING_AGG','GROUP_CONCAT','PIVOT','UNPIVOT','QUALIFY','SAMPLE','TABLESAMPLE'];

  async function getCompletions(prefix) {
    const pfx = prefix.toUpperCase();
    const completions = [];
    const seen = new Set();
    // Table names from loaded tables
    try {
      const r = await api('/api/tables');
      if (r.status === 'ok') {
        for (const t of r.tables) {
          if (t.name.toUpperCase().startsWith(pfx)) {
            completions.push({ text: t.name, type: 'table (' + t.rows + ' rows)' });
            seen.add(t.name.toUpperCase());
          }
        }
        // Column names from loaded tables
        for (const t of r.tables) {
          const cols = t.column_names || [];
          for (const c of cols) {
            if (c.toUpperCase().startsWith(pfx) && !seen.has(c.toUpperCase())) {
              completions.push({ text: '"' + c + '"', type: 'column (' + t.name + ')' });
              seen.add(c.toUpperCase());
            }
          }
        }
      }
    } catch(e) {}
    // Table names from browsed directories (not yet loaded)
    for (const name of browsedTables) {
      if (name.toUpperCase().startsWith(pfx) && !seen.has(name.toUpperCase())) {
        completions.push({ text: name, type: 'table (not loaded)' });
        seen.add(name.toUpperCase());
      }
    }
    // Polaris Iceberg tables
    for (const pt of polarisTables) {
      if (pt.fqn.toUpperCase().startsWith(pfx) && !seen.has(pt.fqn.toUpperCase())) {
        completions.push({ text: pt.fqn, type: 'iceberg (' + pt.namespace + ')' });
        seen.add(pt.fqn.toUpperCase());
      }
      if (pt.name.toUpperCase().startsWith(pfx) && !seen.has(pt.fqn.toUpperCase())) {
        completions.push({ text: pt.fqn, type: 'iceberg (' + pt.namespace + ')' });
        seen.add(pt.fqn.toUpperCase());
      }
    }
    // SQL keywords
    for (const kw of SQL_KW) {
      if (kw.startsWith(pfx) && !seen.has(kw))
        completions.push({ text: kw, type: 'keyword' });
    }
    return completions.slice(0, 20);
  }

  function getWordAtCursor() {
    const pos = ta.selectionStart;
    const text = ta.value.substring(0, pos);
    const m = text.match(/[a-zA-Z_][a-zA-Z0-9_.]*$/);
    if (m) return { word: m[0], start: pos - m[0].length };
    return { word: '', start: pos };
  }

  function renderAc() {
    if (!acItems.length) { acList.style.display = 'none'; return; }
    acList.style.display = 'block';
    // Position near cursor
    const linesBefore = ta.value.substring(0, acStart).split('\n');
    const lineH = 22;
    const topPx = Math.min(linesBefore.length * lineH + 4, ta.offsetHeight);
    acList.style.top = topPx + 'px';
    acList.innerHTML = acItems.map((it, i) =>
      '<div class="ac-item' + (i === acIdx ? ' active' : '') + '" data-i="' + i + '">' +
      '<span>' + escHtml(it.text) + '</span><span class="ac-type">' + escHtml(it.type) + '</span></div>'
    ).join('');
    acList.querySelectorAll('.ac-item').forEach(el => {
      el.onmousedown = (e) => { e.preventDefault(); acceptAc(+el.dataset.i); };
    });
    // Scroll active into view
    const activeEl = acList.querySelector('.ac-item.active');
    if (activeEl) activeEl.scrollIntoView({ block: 'nearest' });
  }

  function acceptAc(idx) {
    if (idx < 0 || idx >= acItems.length) return;
    const item = acItems[idx];
    const before = ta.value.substring(0, acStart);
    const after = ta.value.substring(ta.selectionStart);
    const insert = item.text + (item.type === 'keyword' ? ' ' : '');
    ta.value = before + insert + after;
    const newPos = acStart + insert.length;
    ta.selectionStart = ta.selectionEnd = newPos;
    hideAc();
    ta.focus();
  }

  function hideAc() { acItems = []; acIdx = -1; acList.style.display = 'none'; }

  function prevKeyword() {
    const text = ta.value.substring(0, ta.selectionStart).toUpperCase();
    const m = text.match(/\b(FROM|JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|INNER\s+JOIN|CROSS\s+JOIN|FULL\s+JOIN|INTO)\s+$/);
    return m ? 'table' : null;
  }

  let debounce = null;
  ta.addEventListener('input', () => {
    clearTimeout(debounce);
    debounce = setTimeout(async () => {
      const { word, start } = getWordAtCursor();
      const ctx = prevKeyword();
      if (word.length < 2 && !ctx) { hideAc(); return; }
      acWord = word; acStart = start;
      if (ctx === 'table' && word.length < 2) {
        // Show all table names after FROM/JOIN (loaded + browsed)
        const seen = new Set();
        acItems = [];
        try {
          const r = await api('/api/tables');
          if (r.status === 'ok') for (const t of r.tables) {
            acItems.push({ text: t.name, type: 'table (' + t.rows + ' rows)' });
            seen.add(t.name.toUpperCase());
          }
        } catch(e) {}
        for (const name of browsedTables) {
          if (!seen.has(name.toUpperCase())) acItems.push({ text: name, type: 'table (not loaded)' });
        }
        for (const pt of polarisTables) {
          if (!seen.has(pt.fqn.toUpperCase())) {
            acItems.push({ text: pt.fqn, type: 'iceberg (' + pt.namespace + ')' });
            seen.add(pt.fqn.toUpperCase());
          }
        }
      } else {
        acItems = await getCompletions(word);
      }
      acIdx = acItems.length ? 0 : -1;
      renderAc();
    }, 150);
  });

  ta.addEventListener('keydown', (e) => {
    if (acList.style.display === 'none' || !acItems.length) return;
    if (e.key === 'ArrowDown') { e.preventDefault(); acIdx = Math.min(acIdx + 1, acItems.length - 1); renderAc(); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); acIdx = Math.max(acIdx - 1, 0); renderAc(); }
    else if (e.key === 'Tab' || e.key === 'Enter') {
      if (acIdx >= 0) { e.preventDefault(); acceptAc(acIdx); }
    }
    else if (e.key === 'Escape') { e.preventDefault(); hideAc(); }
  });

  ta.addEventListener('blur', () => { setTimeout(hideAc, 200); });
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
        else:
            self.send_response(404)
            self.end_headers()

    def do_GET(self):
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

        elif route == "/api/delta":
            self.send_json(read_delta_log(params.get("prefix", "")))

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
        with db_lock:
            db_conn.execute("DROP SECRET IF EXISTS azure_storage_secret;")
            db_conn.execute("""
                CREATE SECRET azure_storage_secret (
                    TYPE AZURE,
                    PROVIDER CREDENTIAL_CHAIN
                );
            """)
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
            with db_lock:
                db_conn.execute("DROP SECRET IF EXISTS aws_storage_secret;")
                db_conn.execute(f"""
                    CREATE SECRET aws_storage_secret (
                        TYPE S3,
                        KEY_ID '{ak}',
                        SECRET '{sk}',
                        SESSION_TOKEN '{token}',
                        REGION '{region}',
                        ENDPOINT 's3.{region}.amazonaws.com',
                        URL_STYLE 'vhost'
                    );
                """)
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
            with db_lock:
                db_conn.execute("DROP SECRET IF EXISTS aws_storage_secret;")
                db_conn.execute(f"""
                    CREATE SECRET aws_storage_secret (
                        TYPE S3,
                        KEY_ID '{access_key}',
                        SECRET '{secret_key}',
                        REGION '{region}',
                        ENDPOINT 's3.{region}.amazonaws.com',
                        URL_STYLE 'vhost'
                    );
                """)
                # Also set global S3 config for httpfs/iceberg extension
                db_conn.execute(f"SET s3_region='{region}';")
                db_conn.execute(f"SET s3_access_key_id='{access_key}';")
                db_conn.execute(f"SET s3_secret_access_key='{secret_key}';")
                db_conn.execute(f"SET s3_endpoint='s3.{region}.amazonaws.com';")
                db_conn.execute("SET s3_url_style='vhost';")
            print(f"  AWS S3 secret + global config set (region: {region})")
            return {"status": "ok", "message": f"AWS authentication successful (region: {region})."}
        except Exception as e:
            return {"status": "error", "message": str(e)}


def main():
    print(f"GCS Parquet Explorer (Server Mode)")
    print(f"DuckDB {duckdb.__version__} | PyArrow {pyarrow.__version__}")
    print()

    init_duckdb()

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

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
        server.shutdown()


if __name__ == "__main__":
    main()
