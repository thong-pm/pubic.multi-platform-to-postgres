#!/bin/bash

# --- Configuration ---
set -e
set -u
set -o pipefail

# --- Script Start ---
echo "[$(date +%Y-%m-%dT%H:%M:%S%z)] --- Running pipeline-hubspot-to-postgres ---"

# Execute the main Python script for this pipeline directly
python runner/__init__.py
./format.sh

# --- Script End ---
echo "[$(date +%Y-%m-%dT%H:%M:%S%z)] --- pipeline-hubspot-to-postgres finished ---"
exit 0