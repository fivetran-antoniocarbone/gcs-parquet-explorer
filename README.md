# GCS Parquet Explorer

A web application for browsing and querying SAP CDS Parquet data stored in Google Cloud Storage, Azure Data Lake (ADLS Gen2), and AWS S3. Uses DuckDB as the SQL engine, PyArrow for Parquet file reading, and Fivetran Polaris (Apache Iceberg) for multi-cloud catalog access.

Engineered by the Fivetran SAP Specialist Team.

## Live Instance

**https://sapidesecc8.fivetran-internal-sales.com/datalake_reader/**

Sign in with your Fivetran email address to start browsing and querying data.

## Features

- **GCS Parquet Browsing** — Navigate bucket directories, load Parquet files, view data and schemas
- **Polaris Iceberg Catalogs** — One-click connect to Google Cloud, Azure, and AWS Polaris catalogs
- **AWS S3 Vended Credentials** — Automatic S3 credential vending via Polaris REST API with query rewrite to `iceberg_scan()`
- **DuckDB SQL Engine** — Full SQL queries with auto-load, autocomplete, and cross-catalog joins
- **Query Timeout** — 45-second watchdog timer prevents queries from hanging forever on unauthorized tables
- **Per-User Polaris Credentials** *(Portal deployment)* — Each signed-in user gets a private credential overlay and their own DuckDB worker, so two users with two different Polaris scopes don't see each other's catalogs, tables, or vended S3 keys. "Reset to Defaults" reverts to the shared baseline
- **Credential Management** — View and update Polaris OAuth credentials from the UI (per-user in the Portal deployment; in-memory shared in the standalone)
- **Multi-Threaded HTTPS Server** — Thread-safe DuckDB access, SSL/TLS, email-based login
- **Restart Server Button** — One-click server restart from the web UI header
- **Self-Healing** — systemd service with auto-restart on crash and start on boot
- **Fast Catalog Browsing** — Polaris REST API for namespace/table listing (~0.3s vs 70s)
- **Zero Client Dependencies** — Pure browser-based, no pandas or other client-side installs needed
- **Dark Theme UI** — Interactive data tables, sidebar file browser, resizable panels

## Architecture

Single-file Python application with embedded HTML/CSS/JS frontend:

```
┌─────────────────────────────────────────────────────┐
│ Header: Fivetran Logo + Title + Status              │
├────────────┬──────┬─────────────────────────────────┤
│ Sidebar    │Split │ Main Content                    │
│ (500px)    │(6px) │ Tabs: Data|Schema|SQL|Delta|    │
│            │drag  │       Polaris Catalog|Docs      │
│ Breadcrumb │      │                                 │
│ Search     │      │ Tab panels with data tables,    │
│ Sort bar   │      │ SQL editor, catalog browser,    │
│ File list  │      │ credential mgmt, documentation  │
└────────────┴──────┴─────────────────────────────────┘
```

### Stack

| Layer | Technology |
|-------|-----------|
| Server | Python `http.server.ThreadingHTTPServer` (multi-threaded) |
| Thread Safety | `threading.Lock()` around all DuckDB operations |
| SQL Engine | DuckDB (in-memory, httpfs + iceberg + azure extensions) |
| Azure Transport | `curl` mode (fixes SSL on SUSE Linux) |
| Parquet | PyArrow (`pyarrow.parquet`) — **no pandas** |
| GCS | `google-cloud-storage` with Application Default Credentials |
| Iceberg | DuckDB Iceberg extension + Polaris REST catalog |
| Polaris REST API | Direct HTTP calls for fast namespace/table listing |
| Memory | `psutil` for LRU eviction at 85% system memory |
| Auth | Email-based login with secure session cookies |
| SSL | ZeroSSL certificate with chained PEM |
| Process Mgmt | systemd (auto-restart on crash, start on boot) |
| UI | Embedded HTML/CSS/JS, dark theme, no frameworks |

### Polaris Iceberg Catalogs

Three pre-configured Polaris catalog presets with OAuth2 client credentials:

| Provider | Catalog | Storage Backend |
|----------|---------|-----------------|
| Google Cloud | `obeisance_plaintive` | GCS buckets |
| Azure | `log_pseudo` | ADLS Gen2 (`abfss://`) |
| AWS | `surfacing_caramel` | S3 buckets |

Namespace and table listing uses the Polaris REST API directly (~0.3s) instead of DuckDB `information_schema` queries (~70s).

### AWS S3 Vended Credentials & Query Rewrite

DuckDB's Iceberg extension does **not** send the `X-Iceberg-Access-Delegation: vended-credentials`
HTTP header when querying via `ATTACH ... TYPE ICEBERG`. Without this header, the Polaris REST
catalog does not return S3 credentials in `loadTable` responses, causing data queries to hang.

**Workaround** (transparent to the user): When a SQL query references an AWS Polaris catalog
(e.g., `SELECT * FROM aws.namespace.table`), the server:

1. Detects the AWS catalog reference via regex
2. Resolves the table's `metadata-location` via the Polaris REST API with `X-Iceberg-Access-Delegation: vended-credentials`
3. Extracts vended S3 credentials (`s3.access-key-id`, `s3.secret-access-key`, `s3.session-token`) from the response
4. Creates a DuckDB S3 secret with those credentials
5. Rewrites the query to use `iceberg_scan('s3://...metadata.json')` instead of the catalog reference

This is handled by `_rewrite_aws_query()` and `_get_aws_polaris_token_and_creds()`. Vended credentials are cached for ~50 minutes.

GCS and Azure do **not** need this — GCS uses `gcloud auth` (Application Default Credentials)
and Azure uses `CREDENTIAL_CHAIN`, both providing storage auth independently of Polaris.

### Query Timeout

All SQL queries have a 45-second watchdog timer. If a query exceeds this limit (e.g., unauthorized Polaris table), `db_conn.interrupt()` is called from a background thread and the user sees: *"Query timed out after 45s. You may not have access to this table."*

### Restart Server

The web UI header includes a "Restart Server" link. It sends `POST /api/restart`, the server responds OK then triggers `systemctl restart gcs-explorer` via a 0.5s timer. The frontend polls `/api/init` every 2 seconds and auto-reloads when the server is back.

## Usage

### Getting Started

1. **Open** — Navigate to the URL in your browser. You will see a login screen.
2. **Sign in** — Enter your Fivetran email address.
3. **Browse** — The sidebar shows the GCS bucket contents. Click directories to navigate, or use the **Polaris Catalog** tab to connect to Iceberg catalogs.

### GCS Parquet Browsing

- **Navigate** — Click directories in the sidebar to drill down. The breadcrumb shows your current path.
- **Home** — Click the house icon to return to the bucket list and switch buckets.
- **Filter & Sort** — Type in the filter box to narrow results. Click Name/Files/Size headers to sort.
- **Load a table** — Click a `.parquet` file, or the "Load all parquet data" button for an entire directory.
- **View data** — The **Data** tab shows table contents with sortable columns. The **Schema** tab shows column names and types.

### Polaris Iceberg Catalogs

1. **Connect** — Go to the Polaris Catalog tab. Click **Google Cloud**, **Azure**, or **AWS** to connect the catalog. GCS and Azure also set up storage credentials; AWS credentials are vended automatically at query time via the Polaris REST API.
2. **Browse** — Click namespaces to see tables. Click a table name to populate the SQL tab, or the play icon to query immediately.
3. **Query** — Tables are addressed as `<alias>.<namespace>.<table>`:
   ```sql
   SELECT * FROM gcs.my_namespace.my_table LIMIT 100
   ```
4. **Cross-catalog joins** — All catalogs share one DuckDB instance:
   ```sql
   SELECT * FROM gcs.ns.tbl JOIN ts_adls_destination_demo.ns.tbl USING(id)
   ```

### SQL Queries

- **Run a query** — Type SQL in the SQL Query tab and press **Ctrl+Enter**.
- **Auto-load** — Reference a GCS table not yet loaded and it's automatically fetched.
- **Autocomplete** — Start typing after `FROM` or `JOIN` for table name suggestions (GCS tables + Polaris Iceberg tables).
- **Table addressing** — GCS tables by name (e.g., `dd02l_all`), Polaris tables as `catalog.namespace.table`.

### Managing Credentials

Click the **Manage Credentials** button in the Polaris Catalog tab to view and update OAuth client credentials for each cloud provider.

**Standalone deployment:** changes mutate the in-memory `CATALOG_PRESETS` dict and persist until server restart.

**Portal deployment (live instance):** changes are scoped to your account only.

- **Save Credentials** writes a private overlay row to a Postgres table (`user_polaris_creds`), encrypted column-wise with `pgcrypto`, joined to the `users` table by email.
- **Reset to Defaults** removes your overlay so you fall back to the shared baseline.
- A small caption under the form tells you whether you're currently using the shared baseline or your personal overlay.
- Other users are never affected. New users are auto-seeded with a copy of the shared baseline on first sign-in (or on Okta JIT provisioning).

### Per-User DuckDB Workers (Portal deployment)

In the live (Portal) deployment, the DuckDB engine is **per-user** rather than singleton. Every logged-in user gets their own subprocess, lazily spawned on the first DuckDB-touching request, with their own ATTACHed Polaris catalogs, registered tables, and storage secrets (S3 / Azure / GCS). This guarantees that two users with two different Polaris scopes can be connected at the same time without their credentials, vended S3 keys, or attached catalogs leaking across each other.

| Worker lifecycle event | Behaviour |
|---|---|
| First `/api/polaris/connect` or `/api/sql` for a user | Lazy spawn |
| `/auth/logout` | Worker killed immediately |
| Idle ≥ 30 min | Reaped by background thread |
| `>10` workers active *or* memory ≥ 85 % | LRU eviction |
| Hung query | Per-user kill + respawn (other users unaffected) |
| `systemctl restart gcs-explorer` | All workers die together (existing behaviour) |

The standalone version in this repository keeps the original singleton-worker design — the per-user split requires a user model and a Postgres-backed credentials table that don't exist outside the Portal.

## Deployment

### Current Deployment

| Property | Value |
|----------|-------|
| URL | `https://sapidesecc8.fivetran-internal-sales.com/datalake_reader/` |
| Host | sapidesecc8 (SUSE Linux Enterprise Server 15 SP5) |
| Python | `/root/miniconda/bin/python3` (3.13) |
| SSL | ZeroSSL certificate (expires Jun 25, 2026) |
| Service | `gcs-explorer.service` (systemd, auto-restart, boot start) |

### Important: Environment Variables

The GitHub repository does **not** contain any secrets. All credentials are loaded from
environment variables at runtime. When deploying from this repo, you must create
`/usr/sap/gcs_explorer.env` on the server with the real values. Use
`server/gcs_explorer.env.example` as a template:

```bash
scp server/gcs_explorer.env.example sapidesecc8:/usr/sap/gcs_explorer.env
# Then edit /usr/sap/gcs_explorer.env on the server with real credentials
```

The systemd service loads this file automatically via `EnvironmentFile`.

The **current production server** (sapidesecc8) already has credentials hardcoded in its
deployed copy of `gcs_explorer_server.py` and will continue to work. This note only
applies to fresh deployments from the repository.

### Deploy Updates

```bash
# One-command deploy from this repo:
./server/deploy.sh sapidesecc8

# Or manually:
scp server/gcs_explorer_server.py sapidesecc8:/tmp/
ssh sapidesecc8 "sudo cp /tmp/gcs_explorer_server.py /usr/sap/ && sudo systemctl restart gcs-explorer"
```

### Deploy to a New Server

1. **Install dependencies** on the server:
   ```bash
   pip3 install -r server/requirements.txt
   ```

2. **Copy the application**:
   ```bash
   scp server/gcs_explorer_server.py yourserver:/usr/sap/
   ```

3. **Configure SSL** — place your certificate and key:
   ```
   /usr/sap/gcs_explorer_cert.pem   # Server cert + intermediate CA (chained PEM)
   /usr/sap/gcs_explorer_key.pem    # Private key
   ```

4. **Edit configuration** at the top of `gcs_explorer_server.py`:
   ```python
   PORT = 443
   BIND_ADDR = "0.0.0.0"
   FQDN = "yourserver.example.com"
   BASE_PATH = "/datalake_reader"
   LOGIN_PASSWORD = "YourPassword"
   ```

5. **Install systemd service**:
   ```bash
   sudo cp server/gcs-explorer.service /etc/systemd/system/
   # Edit ExecStart path if your python3 is elsewhere
   sudo systemctl daemon-reload
   sudo systemctl enable gcs-explorer
   sudo systemctl start gcs-explorer
   ```

6. **Verify**:
   ```bash
   sudo systemctl status gcs-explorer
   ```

### Service Management

```bash
sudo systemctl start gcs-explorer      # Start
sudo systemctl stop gcs-explorer       # Stop
sudo systemctl restart gcs-explorer    # Restart after deploy
sudo systemctl status gcs-explorer     # Check status
journalctl -u gcs-explorer -f          # Follow logs
```

### Configuration Reference

| Setting | Default | Description |
|---------|---------|-------------|
| `PORT` | `443` | HTTPS port |
| `BIND_ADDR` | `0.0.0.0` | Listen on all interfaces |
| `FQDN` | `sapidesecc8.fivetran-internal-sales.com` | Server hostname |
| `BASE_PATH` | `/datalake_reader` | URL prefix for all routes |
| `LOGIN_PASSWORD` | `changeme` | Shared login password (set via `GCS_EXPLORER_PASSWORD` env var) |
| `BUCKET_NAME` | `sap_cds_dbt` | Default GCS bucket |
| `BASE_PREFIX` | `sap_cds_views/` | Default prefix in bucket |
| `MEMORY_THRESHOLD` | `0.85` | Evict tables at 85% memory |

## API Endpoints

### GET

| Endpoint | Description |
|----------|-------------|
| `/api/init` | Initialize GCS client, return connection status |
| `/api/buckets` | List all accessible GCS buckets |
| `/api/ls?prefix=&bucket=` | List dirs/files under a GCS prefix |
| `/api/parquet?path=` | Read single parquet file, register in DuckDB |
| `/api/load_dir?prefix=` | Read all parquets in a directory |
| `/api/sql?query=&prefix=` | Execute SQL via DuckDB |
| `/api/delta?prefix=` | Read Delta Lake transaction log |
| `/api/tables` | List tables registered in DuckDB |
| `/api/auth` | Run `gcloud auth application-default login` |
| `/api/azure_auth` | Create DuckDB Azure credential chain secret |
| `/api/aws_auth?mode=&...` | Create DuckDB S3 secret |
| `/api/polaris/connect` | Attach Iceberg catalog to DuckDB |
| `/api/polaris/disconnect` | Detach catalog |
| `/api/polaris/catalogs` | List connected catalogs |
| `/api/polaris/presets` | Return preset configurations (secrets masked) |
| `/api/polaris/namespaces?alias=` | List namespaces via Polaris REST API |
| `/api/polaris/tables?alias=&namespace=` | List tables via Polaris REST API |
| `/api/polaris/all_tables` | All tables across all catalogs via REST API |

### POST

| Endpoint | Description |
|----------|-------------|
| `/api/login` | Email login, sets session cookie |
| `/api/restart` | Restart the server via `systemctl restart gcs-explorer` |
| `/api/polaris/update_credentials` | Update Polaris OAuth credentials at runtime |

## Dependencies

```
duckdb>=1.0.0
pyarrow>=15.0.0
google-cloud-storage>=2.14.0
psutil>=5.9.0
pytz>=2024.1
```

**No pandas required.** Row extraction uses pure PyArrow.

Optional CLIs on the server: `gcloud`, `az`, `aws`.

## Repository Structure

```
gcs-parquet-explorer/
├── README.md                          # This file
├── .gitignore
├── local/                             # Legacy local version (laptop, HTTP, port 8765)
│   ├── README.md
│   ├── gcs_explorer.py
│   ├── requirements.txt
│   └── setup.sh
└── server/                            # Production server version (HTTPS, systemd)
    ├── README.md
    ├── gcs_explorer_server.py         # Main application (~2200 lines)
    ├── requirements.txt
    ├── gcs-explorer.service           # systemd unit file
    └── deploy.sh                      # One-command deploy script
```

## Troubleshooting

| Problem | Fix |
|---------|-----|
| "Application Default Credentials" | Run `gcloud auth application-default login` on the server |
| Azure storage error | Click Azure button (auto-configures credential chain) |
| AWS queries hang forever | Fixed: queries are rewritten to `iceberg_scan()` with vended S3 credentials |
| AWS queries fail after restart | Re-click the AWS button to re-attach the catalog (DuckDB state is in-memory) |
| "Query timed out after 45s" | You may not have access to this table, or Polaris credentials expired |
| DuckDB "Can't find home directory" | Fixed: `HOME` env var and `SET home_directory` set in init |
| Azure SSL error on SUSE | Fixed: `SET azure_transport_option_type='curl'` configured |
| Slow Polaris listing | Fixed: uses Polaris REST API (~0.3s instead of 70s) |
| Port in use | Fixed: `allow_reuse_address = True`; or click Restart Server in UI |
| `no module named pandas` | Fixed: uses pure PyArrow, no pandas needed |
| Server won't start under sudo | Use `/root/miniconda/bin/python3`, not `/usr/bin/python3` |
| Logs not appearing | Fixed: `python3 -u` in systemd ExecStart for unbuffered output |
| Browser shows "not secure" | Clear browser cache (ZeroSSL cert is valid) |

## Tested On

- SUSE Linux Enterprise Server 15 SP5
- macOS 14+ (Apple Silicon and Intel)
- Python 3.11–3.13
- DuckDB 1.4–1.5
- PyArrow 15–22
- google-cloud-storage 2.14+
