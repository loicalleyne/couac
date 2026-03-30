# Install dbc (ADBC Driver Manager CLI) and the DuckDB driver.
#
# dbc is a CLI tool from Columnar Technologies for installing and managing
# ADBC drivers. For more information, see: https://github.com/columnar-tech/dbc
#
# Installation methods (choose one):
#
#   Standalone installer (PowerShell):
#     powershell -ExecutionPolicy ByPass -c "irm https://dbc.columnar.tech/install.ps1 | iex"
#
#   MSI installer (direct download):
#     https://dbc.columnar.tech/latest/dbc-latest-x64.msi
#
#   pipx (cross-platform, requires Python):
#     pipx install dbc
#
# After installing dbc, install the DuckDB driver:
#   dbc install duckdb

$ErrorActionPreference = "Stop"

# Check if dbc is installed
$dbcPath = Get-Command dbc -ErrorAction SilentlyContinue
if (-not $dbcPath) {
    Write-Host "dbc not found. Installing via standalone installer..."
    powershell -ExecutionPolicy ByPass -c "irm https://dbc.columnar.tech/install.ps1 | iex"
}

# Install the DuckDB driver
Write-Host "Installing DuckDB ADBC driver..."
dbc install duckdb

Write-Host "Done! DuckDB driver installed successfully."
Write-Host "You can verify with: dbc list"
