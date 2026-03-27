# GCS Parquet Explorer

A web application for browsing and querying SAP CDS Parquet data stored in Google Cloud Storage, Azure Data Lake (ADLS Gen2), and AWS S3. Uses DuckDB as the SQL engine, PyArrow for Parquet file reading, and Fivetran Polaris (Apache Iceberg) for multi-cloud catalog access.

Engineered by the Fivetran SAP Specialist Team.

## Live Instance

**https://sapidesecc8.fivetran-internal-sales.com/datalake_reader/**

Sign in with your Fivetran email address to start browsing and querying data.

## Features

- **GCS Parquet Browsing** — Navigate bucket directories, load Parquet files, view data and schemas
- **Polaris Iceberg Catalogs** — One-click connect to Google Cloud, Azure, and AWS Polaris catalogs
- **DuckDB SQL Engine** — Full SQL queries with auto-load, autocomplete, and cross-catalog joins
- **Credential Management** — View and update Polaris OAuth credentials from the UI
- **Multi-Threaded HTTPS Server** — Thread-safe DuckDB access, SSL/TLS, email-based login
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

1. **Connect** — Go to the Polaris Catalog tab. Click **Google Cloud**, **Azure**, or **AWS** to connect the catalog and set up storage credentials in one step.
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

Click the yellow **Manage Credentials** button in the Polaris Catalog tab to view and update OAuth client credentials for each cloud provider. Changes persist in memory until server restart.

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
| AWS S3 HTTP 400 | Click AWS button, check region matches bucket |
| DuckDB "Can't find home directory" | Fixed: `HOME` env var and `SET home_directory` set in init |
| Azure SSL error on SUSE | Fixed: `SET azure_transport_option_type='curl'` configured |
| Slow Polaris listing | Fixed: uses Polaris REST API (~0.3s instead of 70s) |
| Port in use | `sudo systemctl restart gcs-explorer` |
| `no module named pandas` | Fixed: uses pure PyArrow, no pandas needed |
| Server won't start under sudo | Use `/root/miniconda/bin/python3`, not `/usr/bin/python3` |
| Browser shows "not secure" | Clear browser cache (ZeroSSL cert is valid) |

## Tested On

- SUSE Linux Enterprise Server 15 SP5
- macOS 14+ (Apple Silicon and Intel)
- Python 3.11–3.13
- DuckDB 1.4–1.5
- PyArrow 15–22
- google-cloud-storage 2.14+
