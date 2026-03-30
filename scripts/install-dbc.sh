#!/usr/bin/env bash
# Install dbc (ADBC Driver Manager CLI) and the DuckDB driver.
#
# dbc is a CLI tool from Columnar Technologies for installing and managing
# ADBC drivers. For more information, see: https://github.com/columnar-tech/dbc
#
# Installation methods (choose one):
#
#   Homebrew (macOS/Linux):
#     brew install columnar-tech/tap/dbc
#
#   Standalone installer (macOS/Linux):
#     curl -LsSf https://dbc.columnar.tech/install.sh | sh
#
#   pipx (cross-platform, requires Python):
#     pipx install dbc
#
#   Docker:
#     docker run --rm -it columnar/dbc:latest
#
# After installing dbc, install the DuckDB driver:
#   dbc install duckdb

set -euo pipefail

# Check if dbc is installed
if ! command -v dbc &>/dev/null; then
    echo "dbc not found. Installing via standalone installer..."
    curl -LsSf https://dbc.columnar.tech/install.sh | sh
fi

# Install the DuckDB driver
echo "Installing DuckDB ADBC driver..."
dbc install duckdb

echo "Done! DuckDB driver installed successfully."
echo "You can verify with: dbc list"
