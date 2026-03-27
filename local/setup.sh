#!/bin/bash
# GCS Parquet Explorer — One-time setup script for macOS
set -e

echo "=== GCS Parquet Explorer Setup ==="
echo ""

# Check Python 3
if ! command -v python3 &>/dev/null; then
    echo "ERROR: Python 3 is required."
    echo "Install from https://www.python.org/downloads/ or run: brew install python3"
    exit 1
fi
PYVER=$(python3 --version)
echo "✓ $PYVER"

# Install Python dependencies
echo ""
echo "Installing Python packages..."
pip3 install --user -r requirements.txt
echo "✓ Python packages installed"

# Check gcloud CLI
echo ""
if ! command -v gcloud &>/dev/null; then
    echo "ERROR: Google Cloud SDK (gcloud) is required."
    echo ""
    echo "Install it:"
    echo "  brew install google-cloud-sdk"
    echo "  OR download from https://cloud.google.com/sdk/docs/install"
    echo ""
    echo "After installing, run this script again."
    exit 1
fi
echo "✓ $(gcloud --version 2>/dev/null | head -1)"

# Authenticate
echo ""
echo "Checking GCS authentication..."
if gcloud auth application-default print-access-token &>/dev/null 2>&1; then
    echo "✓ Already authenticated (Application Default Credentials)"
else
    echo "You need to authenticate with Google Cloud."
    echo "This will open a browser window for login."
    echo ""
    read -p "Press Enter to continue (or Ctrl+C to skip)..."
    gcloud auth application-default login
fi

# Verify access to the bucket
echo ""
echo "Verifying access to gs://sap_cds_dbt..."
if python3 -c "
from google.cloud import storage
c = storage.Client()
b = c.bucket('sap_cds_dbt')
b.reload()
print('✓ Bucket access confirmed: ' + b.name)
" 2>/dev/null; then
    echo ""
else
    echo "WARNING: Cannot access gs://sap_cds_dbt"
    echo "You may need to:"
    echo "  1. Run: gcloud auth application-default login"
    echo "  2. Ensure your Google account has access to the sap_cds_dbt bucket"
    echo ""
fi

echo ""
echo "=== Setup Complete ==="
echo ""
echo "To start the explorer:"
echo "  python3 gcs_explorer.py"
echo ""
echo "Then open http://localhost:8765 in your browser."
