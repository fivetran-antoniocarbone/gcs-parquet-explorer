# GCS Parquet Explorer — Local Version

Lightweight version for running on a developer's laptop. HTTP on port 8765, no login required.

## Quick Start

```bash
# Automated setup (installs deps, checks gcloud, authenticates)
./setup.sh

# Or manually
pip3 install -r requirements.txt
gcloud auth application-default login
python3 gcs_explorer.py
```

Then open **http://localhost:8765** in your browser.

## Prerequisites

- Python 3.9+
- Google Cloud SDK (`gcloud` CLI) — for GCS browsing
- Azure CLI (`az`) — optional, for Azure Polaris catalog
- AWS CLI (`aws`) — optional, for AWS Polaris catalog
- GCS bucket access (`roles/storage.objectViewer`)

## Configuration

Edit the top of `gcs_explorer.py`:

```python
PORT = 8765                          # HTTP port
BUCKET_NAME = "sap_cds_dbt"          # Default GCS bucket
BASE_PREFIX = "sap_cds_views/"       # Default prefix
MEMORY_THRESHOLD = 0.85              # Evict tables at 85% memory
```

## Distribution

To create a distributable zip:

```bash
cd local/
zip -r ../gcs_parquet_explorer.zip . -x '.*' '__pycache__/*'
```
