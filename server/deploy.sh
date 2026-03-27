#!/bin/bash
# Deploy GCS Parquet Explorer to sapidesecc8
# Usage: ./deploy.sh [server_host]
set -e

SERVER="${1:-sapidesecc8}"
REMOTE_DIR="/usr/sap"
SERVICE="gcs-explorer"

echo "=== Deploying GCS Parquet Explorer to $SERVER ==="

# Upload server file
echo "Uploading gcs_explorer_server.py..."
scp "$(dirname "$0")/gcs_explorer_server.py" "$SERVER:/tmp/gcs_explorer_server.py"

# Deploy and restart
echo "Deploying and restarting service..."
ssh "$SERVER" "sudo cp /tmp/gcs_explorer_server.py $REMOTE_DIR/gcs_explorer_server.py && sudo systemctl restart $SERVICE"

# Verify
echo "Verifying..."
sleep 3
ssh "$SERVER" "sudo systemctl status $SERVICE --no-pager | head -8"

echo ""
echo "=== Deployment complete ==="
