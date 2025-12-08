# Fabric Scanner API — Cloud Connections Inventory

This package contains a ready‑to‑run PySpark notebook that:

- Performs a **full tenant scan** (including **Personal workspaces**) using the **Scanner Admin REST APIs**
- Supports **incremental scans** (workspaces modified since a timestamp)
- **NEW**: Scans all JSON files in a lakehouse directory to identify cloud connections
- Allows you to **enable/disable any combination** of the three scanning features
- Flattens results into a unified cloud‑connections schema
- Persists results to **Parquet** in your Lakehouse and exposes a **SQL table** `tenant_cloud_connections`

## Files
- `fabric_scanner_cloud_connections_notebook.py` — the PySpark notebook code
- `README.md` — this guide

## Features

### 1. Full Tenant Scan
Scans all workspaces in your Fabric tenant using the Scanner API to create a baseline inventory.

### 2. Incremental Scan
Scans only workspaces modified since a specific timestamp for efficient updates.

### 3. JSON Directory Scan (NEW)
Scans all JSON files in a lakehouse directory (e.g., previously saved scanner API responses) and extracts cloud connection information. Useful for:
- Processing archived scan results
- Analyzing historical data
- Combining data from multiple sources

## Prerequisites
1. In the Fabric **Admin Portal** enable Admin API settings for metadata scanning (and optionally DAX/Mashup) so the Scanner API returns rich datasource details.
2. Choose authentication:
   - **Delegated Fabric Admin** (default): set `USE_DELEGATED = True`. Run inside Fabric notebooks.
   - **Service Principal (SPN)** for automation: set `USE_DELEGATED = False` and provide `TENANT_ID`, `CLIENT_ID`, `CLIENT_SECRET`.

## How to Use

### Quick Start - Using the Orchestrator Function

The `run_cloud_connection_scan()` function allows you to choose any combination of features:

```python
# Example 1: Run only incremental scan (default)
run_cloud_connection_scan(
    enable_incremental_scan=True,
    incremental_days_back=1
)

# Example 2: Run full baseline scan only
run_cloud_connection_scan(
    enable_full_scan=True,
    enable_incremental_scan=False
)

# Example 3: Scan JSON files in a directory only
run_cloud_connection_scan(
    enable_full_scan=False,
    enable_incremental_scan=False,
    enable_json_directory_scan=True,
    json_directory_path="lakehouse:/Default/Files/scanner/raw/full",
    json_merge_with_existing=True
)

# Example 4: Combine full scan + JSON directory scan
run_cloud_connection_scan(
    enable_full_scan=True,
    enable_incremental_scan=False,
    enable_json_directory_scan=True,
    json_directory_path="lakehouse:/Default/Files/scanner/raw",
    json_merge_with_existing=False
)

# Example 5: Enable all three features
run_cloud_connection_scan(
    enable_full_scan=True,
    enable_incremental_scan=True,
    enable_json_directory_scan=True,
    json_directory_path="lakehouse:/Default/Files/scanner/archived",
    incremental_days_back=7,
    include_personal=True
)
```

### Parameters

- **`enable_full_scan`** (bool): Run full tenant scan
- **`enable_incremental_scan`** (bool): Run incremental scan for modified workspaces
- **`enable_json_directory_scan`** (bool): Scan JSON files in a directory
- **`include_personal`** (bool): Include personal workspaces in API scans
- **`incremental_days_back`** (int): Days to look back for incremental scan
- **`json_directory_path`** (str): Path to directory with JSON files (required if JSON scan enabled)
- **`json_merge_with_existing`** (bool): Merge JSON results with existing data or overwrite
- **`curated_dir`** (str): Output directory for curated data
- **`table_name`** (str): SQL table name for results

### Direct Function Calls (Advanced)

You can also call individual functions directly:

```python
# Full tenant scan
full_tenant_scan(include_personal=True)

# Incremental scan
since_iso = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat(timespec="seconds").replace("+00:00","Z")
incremental_update(since_iso, include_personal=True)

# JSON directory scan
scan_json_directory_for_connections(
    json_dir_path="lakehouse:/Default/Files/scanner/raw",
    merge_with_existing=True
)
```

### Query Results

After running any scan, query the results table:

```sql
SELECT * FROM tenant_cloud_connections;

-- Filter for cloud connections only
SELECT * FROM tenant_cloud_connections WHERE cloud = true;

-- Group by connector type
SELECT connector, COUNT(*) as connection_count 
FROM tenant_cloud_connections 
GROUP BY connector 
ORDER BY connection_count DESC;
```

## Notes
- Limits: ≤100 workspace IDs per `getInfo`; ≤16 concurrent scans; poll 30–60s.
- Personal workspaces are **included** when `include_personal=True`.
- JSON directory scan requires JSON files in the format produced by the Scanner API (with `workspace_sidecar` metadata).
- All features can be run independently or in combination.
- Extend `CLOUD_CONNECTORS` set to match your estate's connector types.

## Output Schema

The unified connection schema includes:

**Workspace Information:**
- `workspace_id`, `workspace_name`, `workspace_kind`
- **`workspace_users`** - Comma-separated list of workspace admins/members (up to 5)

**Item Information:**
- `item_id`, `item_name`, `item_type`
- **`item_creator`** - User who created the item (if available)
- **`item_modified_by`** - User who last modified the item (if available)
- **`item_modified_date`** - Last modification timestamp

**Connection Information:**
- `connector` - The type of cloud connector (e.g., azuresqldatabase, snowflake, rest)
- **`target`** - **Consolidated target field** showing the destination in a readable format (e.g., "Server: server.database.windows.net | Database: mydb")
- `server`, `database`, `endpoint` - Individual target components (kept for backwards compatibility)
- `connection_scope` (Cloud/OnPremViaGateway)
- `cloud` (boolean flag)
- `generation` (for Dataflows)

### User/Ownership Information

The Scanner API provides two levels of user information:

1. **Workspace-level users** (`workspace_users`): Shows workspace admins and members who have access to manage the workspace and its items
2. **Item-level creator/modifier** (`item_creator`, `item_modified_by`): Shows who created or last modified specific items (when available in the API response)

> **Note:** The Scanner API does not provide connection-level user information (i.e., which specific user created or uses a particular data source connection). The user fields show workspace and item ownership, which can help identify responsibility and accountability.

### Example Queries

```sql
-- See all connections with workspace owners
SELECT connector, target, workspace_name, workspace_users, item_name
FROM tenant_cloud_connections
WHERE cloud = true
ORDER BY workspace_name;

-- Find connections in workspaces managed by specific user
SELECT connector, target, workspace_name, item_name
FROM tenant_cloud_connections
WHERE workspace_users LIKE '%john.doe@company.com%';

-- Show recently modified items with connections
SELECT item_name, item_modified_by, item_modified_date, connector, target
FROM tenant_cloud_connections
WHERE item_modified_date IS NOT NULL
ORDER BY item_modified_date DESC
LIMIT 20;

-- Group connections by workspace owner
SELECT workspace_users, COUNT(*) as connection_count, 
       COUNT(DISTINCT connector) as unique_connectors
FROM tenant_cloud_connections
WHERE workspace_users IS NOT NULL
GROUP BY workspace_users
ORDER BY connection_count DESC;
```

```sql
SELECT connector, target, workspace_name, item_name, item_type
FROM tenant_cloud_connections
WHERE cloud = true
ORDER BY connector;
```

**Sample Results:**
| connector | target | workspace_name | workspace_users | item_name | item_type |
|-----------|--------|----------------|-----------------|-----------|-----------|
| azuresqldatabase | Server: myserver.database.windows.net \| Database: analytics | Finance WS | john.doe@company.com, jane.smith@company.com | Sales Model | SemanticModel |
| snowflake | Server: xy12345.snowflakecomputing.com \| Database: DW | Data Science | data.team@company.com | Customer360 | SemanticModel |
| rest | Endpoint: https://api.example.com/data | Marketing | marketing.admin@company.com | API Dataflow | Dataflow |
