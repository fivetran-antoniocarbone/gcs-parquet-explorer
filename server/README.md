# GCS Parquet Explorer — Server Version

HTTPS server deployment with email login, DuckDB subprocess isolation, Polaris REST API, Fivetran dashboard UI, and systemd service management.

## Current Deployment

### Production
- **URL**: https://sapidesecc8.fivetran-internal-sales.com/datalake_reader/
- **Port**: 443 (HTTPS)
- **Service**: systemd `gcs-explorer.service`

### Dev
- **URL**: https://sapidesecc8.fivetran-internal-sales.com/datalake_reader_dev/
- **Port**: 8443 (HTTPS)
- **Service**: systemd `gcs-explorer-dev.service`

### Host
- **Host**: sapidesecc8 (SUSE Linux Enterprise Server 15 SP5)
- **Python**: `/root/miniconda/bin/python3` (3.13)
- **SSL**: ZeroSSL certificate (expires Jun 25, 2026)

## Files

| File | Purpose |
|------|---------|
| `gcs_explorer_server.py` | Main application (single file, ~4000 lines) |
| `gcs-explorer.service` | systemd unit file (production, port 443) |
| `deploy.sh` | One-command deployment script |
| `requirements.txt` | Python dependencies |

## Deploy

```bash
./deploy.sh sapidesecc8
```

## Architecture: DuckDB Subprocess Isolation

DuckDB runs in a **separate child process** (`multiprocessing.Process`) to prevent its C extension
from holding the Python GIL and freezing the HTTP server.

```
┌─ Main Process (HTTP Server) ─────┐     ┌─ Child Process (DuckDB) ──────┐
│ ThreadingHTTPServer               │     │ _duckdb_worker()              │
│ All HTTP handlers                 │     │ Owns duckdb.Connection        │
│ GCS operations (list, download)   │ Q   │ Owns arrow_tables dict        │
│ Polaris REST API calls            │────>│ Command loop:                 │
│ DuckDBWorker (sends commands)     │<────│   exec_sql, exec_multi,       │
│ loaded_tables (metadata only)     │ Q   │   register_table,             │
│ polaris_catalogs dict             │     │   unregister_table            │
└───────────────────────────────────┘     └───────────────────────────────┘
```

- **Main process** handles HTTP, GCS, and Polaris REST — never touches DuckDB directly
- **Child process** owns the DuckDB connection and processes commands via `multiprocessing.Queue`
- **PyArrow tables** are serialized via Arrow IPC for cross-process transfer (`register_table`)
- **Timeouts**: 65s for ATTACH, 50s for queries, 30s for table registration, 15s for secrets
- **Hang recovery**: on timeout, subprocess is killed (SIGKILL) and respawned automatically

### Why This Matters

DuckDB's C extension holds the Python GIL during network I/O (ATTACH TYPE ICEBERG, iceberg_scan
over S3). In the old in-process design, this froze ALL Python threads — including the HTTP server.
With subprocess isolation, the child's GIL cannot affect the parent. The HTTP server stays
responsive even when DuckDB is completely hung.

## Server Stability

### Proactive Measures (Prevent Freezes)

| Measure | What it prevents |
|---------|-----------------|
| Subprocess isolation (separate GIL) | DuckDB C code can never freeze the HTTP server |
| Per-command timeouts (65s ATTACH, 50s query) | Hung operations auto-kill the worker, server stays up |
| Skip-if-already-connected | Redundant ATTACH calls that caused contention |
| Auto-respawn on worker death | Fresh DuckDB after any crash, no manual restart needed |
| `/api/health` endpoint (no auth, no DuckDB) | Always responds even when worker is hung |

### Reactive Backup Measures

| Measure | What it catches |
|---------|----------------|
| External systemd watchdog timer (every 60s) | Curls `/api/health`, restarts if unresponsive |
| systemd `Restart=always` | Process-level crashes |
| BrokenPipeError handling | Client disconnects during response |
| Socket backlog = 64 | Handles queued connections under load |

### Watchdog Services

```
/etc/systemd/system/gcs-explorer-watchdog.timer    — Runs every 60s
/etc/systemd/system/gcs-explorer-watchdog.service   — Curls /api/health, restarts if down
/var/log/gcs_explorer_watchdog.log                  — Watchdog restart log
```

## Configuration

Edit the top of `gcs_explorer_server.py`:

```python
PORT = 443                           # HTTPS port (8443 for dev)
BIND_ADDR = "0.0.0.0"               # Listen on all interfaces
FQDN = "sapidesecc8.fivetran-internal-sales.com"
BASE_PATH = "/datalake_reader"       # URL prefix ("/datalake_reader_dev" for dev)
LOGIN_PASSWORD = os.environ.get("GCS_EXPLORER_PASSWORD", "changeme")
BUCKET_NAME = "sap_cds_dbt"          # Default GCS bucket
BASE_PREFIX = "sap_cds_views/"       # Default prefix
MEMORY_THRESHOLD = 0.85              # Evict tables at 85% memory
```

SSL certificate and key paths are also at the top of the file.

## UI Features (Fivetran Dashboard Layout)

- **Light theme** — White sidebar (220px), light content panels, Fivetran brand colors
- **Fivetran logo** — Base64-embedded PNG fills sidebar width
- **Fixed headers** — Breadcrumbs, search bar, sort buttons stay pinned; only file lists and data tables scroll
- **Draggable SQL splitter** — Vertical drag handle between content area and SQL panel for resizable results
- **SQL panel auto-expand** — Expands to 60% viewport height when query results arrive
- **Fivetran column visibility** — Hides `extracted_at`, `_fivetran_synced`, `_fivetran_deleted`, `_fivetran_sap_archived` by default; toggle with "Show Fivetran Columns" button (static, does not scroll with table). `_fivetran_id` is always hidden everywhere.
- **Cell popup** — Click truncated cells to view full content in 80vw x 80vh modal
- **Light login page** — Clean white login box with blue submit button
- **Restart Application** — Moved to sidebar bottom near Sign Out
- **In-app documentation** — Built-in help page with all features documented

## Server-Specific Features

- **DuckDB subprocess isolation** — Separate process with own GIL, prevents server freezes
- **DuckDBWorker class** — Queue-based command/response protocol with per-command timeouts
- **Arrow IPC table transfer** — PyArrow tables serialized across process boundary
- **Auto-respawn** — Hung worker killed and replaced, HTTP server stays up
- **AWS S3 vended credentials** — `_rewrite_aws_query()` fetches vended S3 creds from Polaris REST API, rewrites queries to `iceberg_scan()`
- **Query timeout** — 50s per command (subprocess killed if exceeded)
- **Restart Server button** — `POST /api/restart` + frontend auto-reconnect
- **Health endpoint** — `GET /api/health` (no auth, no DuckDB — always responds)
- **External watchdog timer** — systemd timer curls health endpoint every 60s
- **SO_REUSEADDR** — Prevents "Address already in use" on quick restarts
- **Unbuffered output** — `python3 -u` in systemd for immediate log visibility
- **Polaris REST API** for namespace/table listing (~0.3s vs 70s)
- **Email login** with session cookies (`HttpOnly; Secure; SameSite=Lax`)
- **Azure curl transport** (`SET azure_transport_option_type='curl'`) for SUSE SSL compatibility
- **SSL CA cert auto-detection** for SUSE Linux paths
- **DuckDB home_directory** set explicitly for systemd compatibility
- **Credential management UI** for updating Polaris OAuth credentials at runtime
- **No pandas dependency** — pure PyArrow for row extraction
