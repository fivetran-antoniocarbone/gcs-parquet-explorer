# GCS Parquet Explorer — Server Version

HTTPS server deployment with email login, multi-threading, Polaris REST API, Fivetran dashboard UI, and systemd service management.

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
| `gcs_explorer_server.py` | Main application (single file, ~3900 lines) |
| `gcs-explorer.service` | systemd unit file (production, port 443) |
| `deploy.sh` | One-command deployment script |
| `requirements.txt` | Python dependencies |

## Deploy

```bash
./deploy.sh sapidesecc8
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

- **ThreadingHTTPServer** with `threading.Lock()` for DuckDB thread safety
- **AWS S3 vended credentials** — `_rewrite_aws_query()` fetches vended S3 creds from Polaris REST API, rewrites queries to `iceberg_scan()`
- **45s query timeout** — `threading.Timer` + `db_conn.interrupt()` prevents queries from hanging
- **Restart Server button** — `POST /api/restart` + frontend auto-reconnect
- **SO_REUSEADDR** — Prevents "Address already in use" on quick restarts
- **Unbuffered output** — `python3 -u` in systemd for immediate log visibility
- **Polaris REST API** for namespace/table listing (~0.3s vs 70s)
- **Email login** with session cookies (`HttpOnly; Secure; SameSite=Lax`)
- **Azure curl transport** (`SET azure_transport_option_type='curl'`) for SUSE SSL compatibility
- **SSL CA cert auto-detection** for SUSE Linux paths
- **DuckDB home_directory** set explicitly for systemd compatibility
- **Credential management UI** for updating Polaris OAuth credentials at runtime
- **No pandas dependency** — pure PyArrow for row extraction
