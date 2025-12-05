# Fabric Scanner API — Cloud Connections Inventory

This package contains a ready‑to‑run PySpark notebook that:

- Performs a **full tenant scan** (including **Personal workspaces**) using the **Scanner Admin REST APIs**
- Supports **incremental scans** (workspaces modified since a timestamp)
- Flattens results into a unified cloud‑connections schema
- Persists results to **Parquet** in your Lakehouse and exposes a **SQL table** `tenant_cloud_connections`

## Files
- `fabric_scanner_cloud_connections_notebook.py` — the PySpark notebook code
- `README.md` — this guide

## Prerequisites
1. In the Fabric **Admin Portal** enable Admin API settings for metadata scanning (and optionally DAX/Mashup) so the Scanner API returns rich datasource details.
2. Choose authentication:
   - **Delegated Fabric Admin** (default): set `USE_DELEGATED = True`. Run inside Fabric notebooks.
   - **Service Principal (SPN)** for automation: set `USE_DELEGATED = False` and provide `TENANT_ID`, `CLIENT_ID`, `CLIENT_SECRET`.

## How to Use
1. Upload `fabric_scanner_cloud_connections_notebook.py` to your Fabric workspace notebook or copy/paste the cell contents.
2. Attach a Lakehouse and run either:
   - Full baseline: `full_tenant_scan(include_personal=True)`
   - Incremental (last 24h): `incremental_update(since_iso, include_personal=True)` where `since_iso` is UTC ISO8601 with `Z`.
3. Query the table:
   ```sql
   SELECT * FROM tenant_cloud_connections;
   ```

## Notes
- Limits: ≤100 workspace IDs per `getInfo`; ≤16 concurrent scans; poll 30–60s.
- Personal workspaces are **included** when `excludePersonalWorkspaces=false`.
- Extend `CLOUD_CONNECTORS` to match your estate.
