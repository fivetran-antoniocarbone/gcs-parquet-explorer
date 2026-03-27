# GCS Parquet Explorer — Server Version

HTTPS server deployment with email login, multi-threading, Polaris REST API, and systemd service management.

## Current Deployment

- **URL**: https://sapidesecc8.fivetran-internal-sales.com/datalake_reader/
- **Host**: sapidesecc8 (SUSE Linux Enterprise Server 15 SP5)
- **Python**: `/root/miniconda/bin/python3` (3.13)
- **SSL**: ZeroSSL certificate (expires Jun 25, 2026)
- **Service**: systemd `gcs-explorer.service` (auto-restart, start on boot)

## Files

| File | Purpose |
|------|---------|
| `gcs_explorer_server.py` | Main application (single file, ~2200 lines) |
| `gcs-explorer.service` | systemd unit file |
| `deploy.sh` | One-command deployment script |
| `requirements.txt` | Python dependencies |

## Deploy

```bash
./deploy.sh sapidesecc8
```

## Configuration

Edit the top of `gcs_explorer_server.py`:

```python
PORT = 443                           # HTTPS port
BIND_ADDR = "0.0.0.0"               # Listen on all interfaces
FQDN = "sapidesecc8.fivetran-internal-sales.com"
BASE_PATH = "/datalake_reader"       # URL prefix
LOGIN_PASSWORD = os.environ.get("GCS_EXPLORER_PASSWORD", "changeme")
BUCKET_NAME = "sap_cds_dbt"          # Default GCS bucket
BASE_PREFIX = "sap_cds_views/"       # Default prefix
MEMORY_THRESHOLD = 0.85              # Evict tables at 85% memory
```

SSL certificate and key paths are also at the top of the file.

## Server-Specific Features

- **ThreadingHTTPServer** with `threading.Lock()` for DuckDB thread safety
- **Polaris REST API** for namespace/table listing (~0.3s vs 70s)
- **Email login** with session cookies (`HttpOnly; Secure; SameSite=Lax`)
- **Azure curl transport** (`SET azure_transport_option_type='curl'`) for SUSE SSL compatibility
- **SSL CA cert auto-detection** for SUSE Linux paths
- **DuckDB home_directory** set explicitly for systemd compatibility
- **Credential management UI** for updating Polaris OAuth credentials at runtime
- **No pandas dependency** — pure PyArrow for row extraction
