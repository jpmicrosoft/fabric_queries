# Fabric Scanner API — Cloud Connections Inventory

This package contains a ready‑to‑run PySpark notebook that:

- Performs a **full tenant scan** (including **Personal workspaces**) using the **Scanner Admin REST APIs**
- Supports **incremental scans** (workspaces modified since a timestamp) with **flexible time windows** (hours or days)
- **Retrieves scan results by scan ID** for previously completed scans
- **Scans all JSON files** in a lakehouse directory to identify cloud connections
- **Single file mode** for debugging and testing individual JSON files
- Allows you to **enable/disable any combination** of scanning features
- Flattens results into a unified cloud‑connections schema
- Persists results to **Parquet** in your Lakehouse and exposes a **SQL table** `tenant_cloud_connections`

## Files
- `fabric_scanner_cloud_connections_notebook.py` — the PySpark notebook code
- `README.md` — this guide

## Features

### 1. Full Tenant Scan
Scans all workspaces in your Fabric tenant using the Scanner API to create a baseline inventory.

### 2. Full Tenant Scan (Chunked) - NEW
**For large tenants (10K+ workspaces)**: Automatically manages rate limits by processing workspaces in hourly chunks.
- **Rate limit safe**: Respects 500 API calls/hour limit
- **Automatic pausing**: Waits between chunks to avoid 429 errors
- **Progress tracking**: Shows completion status and estimated time
- **Incremental saving**: Saves results after each chunk (no data loss if interrupted)
- **Configurable speed**: Adjust `max_batches_per_hour` to balance speed vs. other users

### 3. Incremental Scan
Scans only workspaces modified since a specific timestamp for efficient updates.
- **Flexible time windows**: Specify lookback period in hours or days
- **Sub-hour precision**: Support for fractional hours (e.g., 0.5 = 30 minutes)

### 4. Scan ID Retrieval
Retrieves results from a previous scan using the WorkspaceInfo GetScanResult API.
- **Use scan IDs** from previous scans without re-scanning
- **24-hour window**: Works with scans completed within the last 24 hours
- **Includes personal workspaces**: Gets all workspaces from the original scan

### 5. JSON Directory Scan
Scans all JSON files in a lakehouse directory (e.g., previously saved scanner API responses) and extracts cloud connection information.
- **Single file mode**: Process one specific JSON file for debugging/testing
- **Batch mode**: Process all JSON files in a directory
- Useful for analyzing archived scan results and historical data

## Configuration

### General Configuration

```python
# Authentication mode
USE_DELEGATED = True  # True -> Delegated (Fabric Admin); False -> Service Principal

# Debug logging
DEBUG_MODE = False  # Set to True for detailed JSON structure logging

# JSON single file mode (for testing/debugging)
JSON_SINGLE_FILE_MODE = False  # Set to True to process only one specific JSON file
JSON_TARGET_FILE = "Files/scanner/raw/scan_result_20241208.json"  # Target file path
```
## Prerequisites
1. In the Fabric **Admin Portal** enable Admin API settings for metadata scanning (and optionally DAX/Mashup) so the Scanner API returns rich datasource details.
2. Choose authentication:
   - **Delegated Fabric Admin** (default): set `USE_DELEGATED = True`. Run inside Fabric notebooks.
   - **Service Principal (SPN)** for automation: set `USE_DELEGATED = False` and provide `TENANT_ID`, `CLIENT_ID`, `CLIENT_SECRET`.
3. Ensure you have **Fabric Administrator** or **Power BI Administrator** role for Scanner API access.

## How to Use

### Quick Start - Using the Orchestrator Function

The `run_cloud_connection_scan()` function allows you to choose any combination of features:

```python
# Example 1: Run incremental scan (default - last 24 hours)
run_cloud_connection_scan(
    enable_incremental_scan=True,
    incremental_hours_back=24
)

# Example 2: Run incremental scan for last 6 hours
run_cloud_connection_scan(
    enable_incremental_scan=True,
    incremental_hours_back=6
)

# Example 3: Run incremental scan for last 30 minutes
run_cloud_connection_scan(
    enable_incremental_scan=True,
    incremental_hours_back=0.5
)

# Example 4: Run full baseline scan only (standard - may hit rate limits on large tenants)
run_cloud_connection_scan(
    enable_full_scan=True,
    enable_incremental_scan=False
)

# Example 4b: Run full baseline scan (chunked - for large tenants with 10K+ workspaces)
# Recommended for tenants with high workspace counts
run_cloud_connection_scan(
    enable_full_scan_chunked=True,
    enable_incremental_scan=False,
    max_batches_per_hour=250  # Conservative: 50% of 500/hour limit (leaves room for others)
)

# Example 4c: Run chunked scan during off-hours (faster completion)
run_cloud_connection_scan(
    enable_full_scan_chunked=True,
    enable_incremental_scan=False,
    max_batches_per_hour=450  # Aggressive: 90% of limit (run when others aren't using API)
)

# Example 5: Retrieve results from a previous scan using scan ID
run_cloud_connection_scan(
    enable_scan_id_retrieval=True,
    scan_id="e7d03602-4873-4760-b37e-1563ef5358e3",
    scan_id_merge_with_existing=True
)

# Example 6: Scan JSON files in a directory
run_cloud_connection_scan(
    enable_json_directory_scan=True,
    json_directory_path="Files/scanner/raw/full",
    json_merge_with_existing=True
)

# Example 7: Combine full scan + JSON directory scan
run_cloud_connection_scan(
    enable_full_scan=True,
    enable_json_directory_scan=True,
    json_directory_path="Files/scanner/raw",
    json_merge_with_existing=False
)

# Example 8: Enable all features
run_cloud_connection_scan(
    enable_full_scan=True,
    enable_incremental_scan=True,
    enable_json_directory_scan=True,
    enable_scan_id_retrieval=True,
    json_directory_path="Files/scanner/archived",
    scan_id="previous-scan-id",
    incremental_hours_back=12,
    include_personal=True
)
```

### Parameters

#### Feature Toggles
- **`enable_full_scan`** (bool): Run full tenant scan (standard - may hit rate limits on large tenants)
- **`enable_full_scan_chunked`** (bool): Run full tenant scan with automatic rate limit management (recommended for 10K+ workspaces)
- **`enable_incremental_scan`** (bool): Run incremental scan for modified workspaces
- **`enable_json_directory_scan`** (bool): Scan JSON files in a directory
- **`enable_scan_id_retrieval`** (bool): Retrieve results from a previous scan using scan ID

#### Time Windows
- **`incremental_hours_back`** (float): Hours to look back for incremental scan (takes precedence, supports fractions)
- **`incremental_days_back`** (float): Days to look back for incremental scan (can be fractional, e.g., 0.5 = 12 hours)

#### Scan Configuration
- **`include_personal`** (bool): Include personal workspaces in API scans
- **`max_batches_per_hour`** (int): Max API calls per hour for chunked scans (default: 250, provides 50% buffer for other users)
- **`json_directory_path`** (str): Path to directory with JSON files (required if JSON scan enabled)
- **`json_merge_with_existing`** (bool): Merge JSON results with existing data or overwrite
- **`scan_id`** (str): Scan ID to retrieve (required if scan ID retrieval enabled)
- **`scan_id_merge_with_existing`** (bool): Merge scan ID results with existing data or overwrite

#### Output Configuration
- **`curated_dir`** (str): Output directory for curated data (default: "Tables/dbo")
- **`table_name`** (str): SQL table name for results (default: "tenant_cloud_connections")

### Direct Function Calls (Advanced)

You can also call individual functions directly:

```python
# Full tenant scan (standard)
full_tenant_scan(include_personal=True)

# Full tenant scan (chunked - for large tenants)
full_tenant_scan_chunked(
    include_personal=True,
    max_batches_per_hour=250
)

# Incremental scan
since_iso = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat(timespec="seconds").replace("+00:00","Z")
incremental_update(since_iso, include_personal=True)

# Retrieve scan by ID
get_scan_result_by_id(
    scan_id="e7d03602-4873-4760-b37e-1563ef5358e3",
    merge_with_existing=True
)

# JSON directory scan
scan_json_directory_for_connections(
    json_dir_path="Files/scanner/raw",
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

## Path Configuration

The script uses **Spark-relative paths** for simplicity. You can customize the storage locations:

```python
# Spark-relative paths (recommended format)
RAW_DIR     = "Files/scanner/raw"           # Raw JSON responses
CURATED_DIR = "Tables/dbo"                  # Curated Parquet output

# Or use your own folder structure:
RAW_DIR     = "Files/myfolder/folder2/folder3"
CURATED_DIR = "Tables/myoutput"
```

**Path formats supported:**
- ✅ **Spark-relative**: `"Files/myfolder/subfolder"` or `"Tables/mytable"` (recommended)
- ✅ **Lakehouse URI**: `"lakehouse:/Default/Files/myfolder"` (also works)
- ✅ **Absolute paths**: `"/lakehouse/default/Files/myfolder"` (for advanced use)

The script automatically converts paths for `mssparkutils.fs` operations when needed.

## Notes
- **API Rate Limits**: 500 requests/hour (tenant-wide), 16 concurrent scans maximum
- **Large tenant recommendation**: Use `enable_full_scan_chunked=True` for tenants with 10,000+ workspaces to avoid rate limit errors
- **Chunked scan behavior**: Automatically processes workspaces in hourly batches, waits between chunks, and saves progress incrementally
- **Rate limit sharing**: The 500/hour limit is shared across all users in your organization. Use `max_batches_per_hour` to leave room for others
- **Retry logic**: Automatic retry with exponential backoff for 429 (rate limit) errors
- Limits: ≤100 workspace IDs per `getInfo`; poll 30–60s intervals.
- Personal workspaces are **included** when `include_personal=True`.
- **Scan ID retrieval**: Scan results are available for 24 hours after completion.
- **JSON directory scan**: Requires JSON files in the format produced by the Scanner API (with `workspace_sidecar` metadata).
- **Single file mode**: Enable `JSON_SINGLE_FILE_MODE = True` to test individual JSON files.
- **Debug mode**: Enable `DEBUG_MODE = True` to see detailed JSON structure logging.
- **Flexible time windows**: Use `incremental_hours_back` for sub-day precision (e.g., 6 hours, 30 minutes).
- All features can be run independently or in combination.
- Extend `CLOUD_CONNECTORS` set to match your estate's connector types.

## Performance Recommendations

### For Large Tenants (100K+ workspaces)

**Initial Baseline Scan:**
- Use `enable_full_scan_chunked=True` with `max_batches_per_hour=250-400`
- Run during off-hours or weekends to minimize impact
- Use **High Concurrency** session (16 cores, 32GB RAM) for optimal performance
- Expect 8-12 hours for completion with conservative settings

**Daily Updates:**
- Use `enable_incremental_scan=True` with `incremental_hours_back=24`
- Fast execution (minutes), well under rate limits
- Can use Standard session for daily incremental updates

**Example workflow for 247K workspaces:**
```python
# Week 1: Initial baseline (run once, Friday evening)
run_cloud_connection_scan(
    enable_full_scan_chunked=True,
    enable_incremental_scan=False,
    max_batches_per_hour=400  # Completes in ~6 hours
)

# Week 2+: Daily incremental updates (run every morning)
run_cloud_connection_scan(
    enable_full_scan=False,
    enable_incremental_scan=True,
    incremental_hours_back=24  # Only yesterday's changes
)
```

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
