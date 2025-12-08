
# Microsoft Fabric — Scanner API Cloud Connections Inventory (PySpark Notebook)
# Full tenant scan + Incremental scan (includes Personal workspaces)
# Auth: Delegated Fabric Admin (default) or Service Principal

import os
import json
import time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

from pyspark.sql import Row
import pyspark.sql.functions as F

try:
    from notebookutils import mssparkutils
except ImportError:
    try:
        import mssparkutils
    except ImportError:
        mssparkutils = None

# --- Authentication mode ---
USE_DELEGATED = True  # True -> Delegated; False -> Service Principal

# --- Service Principal secrets (override or use env/Key Vault) ---
TENANT_ID      = os.getenv("FABRIC_SP_TENANT_ID", "<YOUR_TENANT_ID>")
CLIENT_ID      = os.getenv("FABRIC_SP_CLIENT_ID", "<YOUR_APP_CLIENT_ID>")
CLIENT_SECRET  = os.getenv("FABRIC_SP_CLIENT_SECRET", "<YOUR_APP_CLIENT_SECRET>")

AUTH_URL       = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
FABRIC_SCOPE   = "https://analysis.windows.net/powerbi/api/.default"
PBI_ADMIN_BASE = "https://api.powerbi.com/v1.0/myorg/admin"

BATCH_SIZE_WORKSPACES  = 100
MAX_PARALLEL_SCANS     = 16
POLL_INTERVAL_SECONDS  = 30
SCAN_TIMEOUT_MINUTES   = 30

# Spark-relative paths (no lakehouse:// prefix needed for Spark operations)
RAW_DIR     = "Files/scanner/raw"
CURATED_DIR = "Tables/dbo"

# Helper function to convert Spark paths to mssparkutils paths
def _to_lakehouse_path(spark_path: str) -> str:
    """Convert Spark-relative path to mssparkutils lakehouse URI format."""
    if spark_path.startswith(("file:", "abfss:", "lakehouse:")):
        return spark_path
    # For Files/ paths, use file: prefix for mssparkutils
    if spark_path.startswith("Files/"):
        return f"file:/lakehouse/default/{spark_path}"
    # For absolute paths starting with /lakehouse/
    if spark_path.startswith("/lakehouse/"):
        return f"file:{spark_path}"
    # For Tables/ paths, they're managed tables and don't need filesystem operations
    return f"file:/lakehouse/default/{spark_path}"

if mssparkutils is not None:
    for path in [RAW_DIR]:  # Only create Files/ directories, Tables are managed by Spark
        try:
            lakehouse_path = _to_lakehouse_path(path)
            mssparkutils.fs.mkdirs(lakehouse_path)
        except Exception:
            pass

CLOUD_CONNECTORS = {
    "azuresqldatabase", "sqlserverless", "synapse", "kusto",
    "onelake", "adls", "abfss", "s3", "rest",
    "sharepointonline", "dynamics365", "salesforce", "snowflake",
    "fabriclakehouse"
}

# --- Auth ---
def get_access_token_spn() -> str:
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": FABRIC_SCOPE,
        "grant_type": "client_credentials",
    }
    r = requests.post(AUTH_URL, data=data)
    r.raise_for_status()
    return r.json().get("access_token")

HEADERS = None

if USE_DELEGATED:
    if mssparkutils is None:
        raise RuntimeError("Delegated auth selected, but not running inside Fabric. Set USE_DELEGATED=False or run in Fabric.")
    ACCESS_TOKEN = mssparkutils.credentials.getToken("powerbi")
    HEADERS = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Content-Type": "application/json"}
else:
    ACCESS_TOKEN = get_access_token_spn()
    HEADERS = {"Authorization": f"Bearer {ACCESS_TOKEN}", "Content-Type": "application/json"}

# --- Scanner API helpers ---

def get_all_workspaces(include_personal: bool = True) -> List[Dict[str, Any]]:
    url = f"{PBI_ADMIN_BASE}/workspaces/modified"
    params = {"excludePersonalWorkspaces": str(not include_personal).lower()}
    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    payload = r.json() or {}
    return payload.get("workspaces", [])


def modified_workspace_ids(modified_since_iso: str, include_personal: bool = True) -> List[Dict[str, Any]]:
    url = f"{PBI_ADMIN_BASE}/workspaces/modified"
    params = {
        "modifiedSince": modified_since_iso,
        "excludePersonalWorkspaces": str(not include_personal).lower(),
    }
    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    payload = r.json() or {}
    return payload.get("workspaces", [])


def post_workspace_info(workspace_ids: List[str]) -> str:
    if not workspace_ids:
        raise ValueError("workspace_ids cannot be empty.")
    url = f"{PBI_ADMIN_BASE}/workspaces/getInfo"
    body = {
        "workspaces": workspace_ids,
        "lineage": True,
        "users": True
    }
    r = requests.post(url, headers=HEADERS, json=body)
    r.raise_for_status()
    scan_id = (r.json() or {}).get("scanId")
    if not scan_id:
        raise RuntimeError("No scanId returned by getInfo.")
    return scan_id


def poll_scan_status(scan_id: str) -> None:
    url = f"{PBI_ADMIN_BASE}/workspaces/scanStatus/{scan_id}"
    start = time.time()
    while True:
        r = requests.get(url, headers=HEADERS)
        if r.status_code == 202:
            if time.time() - start > SCAN_TIMEOUT_MINUTES * 60:
                raise TimeoutError(f"Scan {scan_id} timed out after {SCAN_TIMEOUT_MINUTES} minutes.")
            time.sleep(POLL_INTERVAL_SECONDS)
            continue
        r.raise_for_status()
        status_json = r.json() or {}
        status = status_json.get("status")
        if status == "Succeeded":
            return
        if status in {"Failed", "Cancelled"}:
            raise RuntimeError(f"Scan {scan_id} ended with status: {status}")
        if time.time() - start > SCAN_TIMEOUT_MINUTES * 60:
            raise TimeoutError(f"Scan {scan_id} timed out after {SCAN_TIMEOUT_MINUTES} minutes.")
        time.sleep(POLL_INTERVAL_SECONDS)


def read_scan_result(scan_id: str) -> Dict[str, Any]:
    url = f"{PBI_ADMIN_BASE}/workspaces/scanResult/{scan_id}"
    r = requests.get(url, headers=HEADERS)
    r.raise_for_status()
    return r.json() or {}

# --- Batch runner ---

def run_one_batch(batch_meta: List[Dict[str, Any]]) -> Dict[str, Any]:
    ids = [w.get("id") for w in batch_meta if w.get("id")]
    scan_id = post_workspace_info(ids)
    poll_scan_status(scan_id)
    payload = read_scan_result(scan_id)
    
    # Extract workspace users/owners from scan result
    ws_users_map = {}
    for ws in (payload.get("workspaces") or []):
        ws_id = ws.get("id")
        users = ws.get("users") or []
        # Get workspace admins/owners
        admins = [u.get("emailAddress") or u.get("identifier") 
                  for u in users if u.get("workspaceUserAccessRight") in {"Admin", "Member"}]
        ws_users_map[ws_id] = ", ".join(admins[:5]) if admins else None  # Limit to first 5
    
    sidecar = {
        w.get("id"): {
            "name": w.get("name", ""),
            "kind": (str(w.get("type")).lower() if w.get("type") else "unknown"),
            "users": ws_users_map.get(w.get("id"))
        } for w in batch_meta if w.get("id")
    }
    payload["workspace_sidecar"] = sidecar
    if mssparkutils is not None:
        try:
            raw_path = f"{_to_lakehouse_path(RAW_DIR)}/full/{scan_id}.json"
            mssparkutils.fs.put(raw_path, json.dumps(payload))
        except Exception:
            pass
    return payload

# --- Flatten helpers ---

def _lower_or(x, default="unknown"):
    return (str(x).lower() if x is not None else default)


def _build_target(server, database, endpoint):
    """Build a consolidated target string from server, database, and endpoint."""
    parts = []
    if server:
        parts.append(f"Server: {server}")
    if database:
        parts.append(f"Database: {database}")
    if endpoint:
        parts.append(f"Endpoint: {endpoint}")
    return " | ".join(parts) if parts else None


def flatten_scan_payload(payload: Dict[str, Any], ws_sidecar: Dict[str, Dict[str, str]]) -> List[Row]:
    rows: List[Row] = []
    for ws in (payload.get("workspaces") or []):
        ws_id   = ws.get("id")
        itemset = ws.get("items") or []
        wmeta   = ws_sidecar.get(ws_id, {"name": ws.get("name") or "", "kind": _lower_or(ws.get("type")), "users": None})
        ws_name = wmeta.get("name", "")
        ws_kind = wmeta.get("kind", "unknown")
        ws_users = wmeta.get("users")

        for item in itemset:
            item_id   = item.get("id")
            item_name = item.get("name")
            item_type = _lower_or(item.get("type"))

            if item_type in {"semanticmodel", "dataset"}:
                # Extract item-level metadata
                item_creator = item.get("createdBy") or item.get("configuredBy")
                item_modified_by = item.get("modifiedBy")
                item_modified_date = item.get("modifiedDateTime")
                
                for ds in (item.get("datasources") or []):
                    conn      = ds.get("connectionDetails") or {}
                    connector = _lower_or(conn.get("datasourceType"))
                    server    = conn.get("server") or conn.get("host")
                    database  = conn.get("database") or conn.get("db")
                    gateway_id= ds.get("gatewayId")
                    connection_scope = "OnPremViaGateway" if gateway_id else "Cloud"
                    cloud_flag       = (connection_scope == "Cloud") or (connector in CLOUD_CONNECTORS)
                    target    = _build_target(server, database, None)
                    rows.append(Row(
                        workspace_id   = ws_id,
                        workspace_name = ws_name,
                        workspace_kind = ws_kind,
                        workspace_users = ws_users,
                        item_id        = item_id,
                        item_name      = item_name,
                        item_type      = "SemanticModel",
                        item_creator   = item_creator,
                        item_modified_by = item_modified_by,
                        item_modified_date = item_modified_date,
                        connector      = connector,
                        target         = target,
                        server         = server,
                        database       = database,
                        endpoint       = None,
                        connection_scope = connection_scope,
                        cloud          = cloud_flag,
                        generation     = None
                    ))

            elif item_type == "dataflow":
                generation = item.get("generation") or (item.get("properties") or {}).get("generation")
                item_creator = item.get("createdBy") or item.get("configuredBy")
                item_modified_by = item.get("modifiedBy")
                item_modified_date = item.get("modifiedDateTime")
                
                sources    = item.get("sources") or item.get("entities") or []
                for src in sources:
                    connector = _lower_or(src.get("type") or src.get("provider"))
                    endpoint  = src.get("url") or src.get("path")
                    connection_scope = "Cloud"
                    cloud_flag       = (connection_scope == "Cloud") or (connector in CLOUD_CONNECTORS)
                    target    = _build_target(None, None, endpoint)
                    rows.append(Row(
                        workspace_id   = ws_id,
                        workspace_name = ws_name,
                        workspace_kind = ws_kind,
                        workspace_users = ws_users,
                        item_id        = item_id,
                        item_name      = item_name,
                        item_type      = "Dataflow",
                        item_creator   = item_creator,
                        item_modified_by = item_modified_by,
                        item_modified_date = item_modified_date,
                        connector      = connector,
                        target         = target,
                        server         = None,
                        database       = None,
                        endpoint       = endpoint,
                        connection_scope = connection_scope,
                        cloud          = cloud_flag,
                        generation     = generation
                    ))

            elif item_type == "pipeline":
                item_creator = item.get("createdBy") or item.get("configuredBy")
                item_modified_by = item.get("modifiedBy")
                item_modified_date = item.get("modifiedDateTime")
                
                for act in (item.get("activities") or []):
                    ref       = act.get("linkedService") or {}
                    connector = _lower_or(ref.get("type") or act.get("type"))
                    endpoint  = ref.get("url") or ref.get("endpoint")
                    gateway_id= ref.get("gatewayId")
                    connection_scope = "OnPremViaGateway" if gateway_id else "Cloud"
                    cloud_flag       = (connection_scope == "Cloud") or (connector in CLOUD_CONNECTORS)
                    target    = _build_target(None, None, endpoint)
                    rows.append(Row(
                        workspace_id   = ws_id,
                        workspace_name = ws_name,
                        workspace_kind = ws_kind,
                        workspace_users = ws_users,
                        item_id        = item_id,
                        item_name      = item_name,
                        item_type      = "Pipeline",
                        item_creator   = item_creator,
                        item_modified_by = item_modified_by,
                        item_modified_date = item_modified_date,
                        connector      = connector,
                        target         = target,
                        server         = None,
                        database       = None,
                        endpoint       = endpoint,
                        connection_scope = connection_scope,
                        cloud          = cloud_flag,
                        generation     = None
                    ))

            elif item_type in {"lakehouse", "notebook"}:
                item_creator = item.get("createdBy") or item.get("configuredBy")
                item_modified_by = item.get("modifiedBy")
                item_modified_date = item.get("modifiedDateTime")
                
                references = (item.get("connections") or []) + (item.get("lineage") or [])
                for ref in references:
                    connector       = _lower_or(ref.get("type"))
                    endpoint        = ref.get("url") or ref.get("endpoint")
                    is_cloud_flag   = ref.get("isCloud", True)
                    connection_scope= "Cloud" if is_cloud_flag else "OnPremViaGateway"
                    cloud_flag      = (connection_scope == "Cloud") or (connector in CLOUD_CONNECTORS)
                    target          = _build_target(None, None, endpoint)
                    rows.append(Row(
                        workspace_id   = ws_id,
                        workspace_name = ws_name,
                        workspace_kind = ws_kind,
                        workspace_users = ws_users,
                        item_id        = item_id,
                        item_name      = item_name,
                        item_type      = item_type.capitalize(),
                        item_creator   = item_creator,
                        item_modified_by = item_modified_by,
                        item_modified_date = item_modified_date,
                        connector      = connector,
                        target         = target,
                        server         = None,
                        database       = None,
                        endpoint       = endpoint,
                        connection_scope = connection_scope,
                        cloud          = cloud_flag,
                        generation     = None
                    ))

            else:
                item_creator = item.get("createdBy") or item.get("configuredBy")
                item_modified_by = item.get("modifiedBy")
                item_modified_date = item.get("modifiedDateTime")
                
                rows.append(Row(
                    workspace_id   = ws_id,
                    workspace_name = ws_name,
                    workspace_kind = ws_kind,
                    workspace_users = ws_users,
                    item_id        = item_id,
                    item_name      = item_name,
                    item_type      = item_type.capitalize(),
                    item_creator   = item_creator,
                    item_modified_by = item_modified_by,
                    item_modified_date = item_modified_date,
                    connector      = "unknown",
                    target         = None,
                    server         = None,
                    database       = None,
                    endpoint       = None,
                    connection_scope = "Cloud",
                    cloud          = True,
                    generation     = None
                ))
    return rows

# --- Full tenant scan ---

def full_tenant_scan(include_personal: bool = True,
                     curated_dir: str = CURATED_DIR,
                     table_name: str = "tenant_cloud_connections") -> None:
    ws_min = get_all_workspaces(include_personal=include_personal)
    if not ws_min:
        print("No workspaces discovered.")
        return

    print(f"Discovered {len(ws_min)} workspaces (include_personal={include_personal}).")

    ws_list = [{
        "id":   w.get("id"),
        "name": w.get("name", ""),
        "type": (str(w.get("type")).lower() if w.get("type") else "unknown")
    } for w in ws_min if w.get("id")]

    batches = [ws_list[i:i+BATCH_SIZE_WORKSPACES] for i in range(0, len(ws_list), BATCH_SIZE_WORKSPACES)]
    scan_payloads: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_SCANS) as pool:
        futures = [pool.submit(run_one_batch, b) for b in batches]
        for fut in as_completed(futures):
            scan_payloads.append(fut.result())

    print(f"Completed {len(scan_payloads)} full scan batches.")

    all_rows = []
    for payload in scan_payloads:
        sidecar = payload.get("workspace_sidecar", {})
        all_rows.extend(flatten_scan_payload(payload, sidecar))

    if not all_rows:
        print("No connection rows produced by full scan.")
        return

    df = spark.createDataFrame(all_rows)
    df = (
        df.withColumn("connector", F.lower(F.coalesce(F.col("connector"), F.lit("unknown"))))
          .dropDuplicates(["workspace_id","item_id","connector","server","database","endpoint"])
    )

    df.write.mode("overwrite").parquet(curated_dir)

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f"CREATE TABLE {table_name} USING PARQUET LOCATION '{curated_dir}'")

    print(f"Full tenant scan completed. Curated path: {curated_dir} | SQL table: {table_name}")

# --- Incremental scan ---

def run_one_batch_incremental(batch_meta: List[Dict[str, Any]]) -> Dict[str, Any]:
    ids = [w.get("id") for w in batch_meta if w.get("id")]
    scan_id = post_workspace_info(ids)
    poll_scan_status(scan_id)
    payload = read_scan_result(scan_id)
    
    # Extract workspace users/owners from scan result
    ws_users_map = {}
    for ws in (payload.get("workspaces") or []):
        ws_id = ws.get("id")
        users = ws.get("users") or []
        # Get workspace admins/owners
        admins = [u.get("emailAddress") or u.get("identifier") 
                  for u in users if u.get("workspaceUserAccessRight") in {"Admin", "Member"}]
        ws_users_map[ws_id] = ", ".join(admins[:5]) if admins else None  # Limit to first 5
    
    sidecar = {
        w.get("id"): {
            "name": w.get("name", ""),
            "kind": (str(w.get("type")).lower() if w.get("type") else "unknown"),
            "users": ws_users_map.get(w.get("id"))
        } for w in batch_meta if w.get("id")
    }
    payload["workspace_sidecar"] = sidecar
    if mssparkutils is not None:
        try:
            raw_path = f"{_to_lakehouse_path(RAW_DIR)}/incremental/{scan_id}.json"
            mssparkutils.fs.put(raw_path, json.dumps(payload))
        except Exception:
            pass
    return payload


def incremental_update(modified_since_iso: str,
                       include_personal: bool = True,
                       curated_dir: str = CURATED_DIR,
                       table_name: str = "tenant_cloud_connections") -> None:
    changed_ws = modified_workspace_ids(modified_since_iso, include_personal=include_personal)
    if not changed_ws:
        print(f"No modified workspaces since {modified_since_iso}. Nothing to update.")
        return

    print(f"Found {len(changed_ws)} modified workspaces since {modified_since_iso} (include_personal={include_personal}).")

    batches = [changed_ws[i:i+BATCH_SIZE_WORKSPACES] for i in range(0, len(changed_ws), BATCH_SIZE_WORKSPACES)]
    scan_payloads: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_SCANS) as pool:
        futures = [pool.submit(run_one_batch_incremental, b) for b in batches]
        for fut in as_completed(futures):
            scan_payloads.append(fut.result())

    print(f"Completed {len(scan_payloads)} incremental scan batches.")

    all_rows = []
    for payload in scan_payloads:
        sidecar = payload.get("workspace_sidecar", {})
        all_rows.extend(flatten_scan_payload(payload, sidecar))

    if not all_rows:
        print("No connection rows produced by incremental scan.")
        return

    df_new = spark.createDataFrame(all_rows)
    df_new = (
        df_new.withColumn("connector", F.lower(F.coalesce(F.col("connector"), F.lit("unknown"))))
              .dropDuplicates(["workspace_id","item_id","connector","server","database","endpoint"])
    )

    try:
        df_existing = spark.read.parquet(curated_dir)
        df_merged = (
            df_existing.unionByName(df_new, allowMissingColumns=True)
            .dropDuplicates(["workspace_id","item_id","connector","server","database","endpoint"])
        )
    except Exception:
        df_merged = df_new

    df_merged.write.mode("overwrite").parquet(curated_dir)

    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f"CREATE TABLE {table_name} USING PARQUET LOCATION '{curated_dir}'")

    print(f"Incremental update completed. Curated path: {curated_dir} | SQL table: {table_name}")

# --- JSON Directory Scanner (Lakehouse) ---

def scan_json_directory_for_connections(
    json_dir_path: str,
    curated_dir: str = CURATED_DIR,
    table_name: str = "tenant_cloud_connections",
    merge_with_existing: bool = True
) -> None:
    """
    Scans all JSON files in a lakehouse directory and extracts cloud connection information.
    
    Args:
        json_dir_path: Path to directory containing JSON files (e.g., "lakehouse:/Default/Files/scanner/raw")
        curated_dir: Output directory for curated parquet files
        table_name: Name of the SQL table to create/update
        merge_with_existing: If True, merge with existing data; if False, overwrite
    """
    print(f"Scanning JSON files in directory: {json_dir_path}")
    
    if mssparkutils is None:
        raise RuntimeError("JSON directory scanning requires mssparkutils (Fabric environment)")
    
    # Convert to lakehouse path if needed
    lakehouse_json_path = json_dir_path if json_dir_path.startswith(("file:", "abfss:", "lakehouse:")) else _to_lakehouse_path(json_dir_path)
    
    try:
        # List all JSON files in directory
        files = mssparkutils.fs.ls(lakehouse_json_path)
        json_files = [f.path for f in files if f.path.endswith('.json')]
        
        if not json_files:
            print(f"No JSON files found in {json_dir_path}")
            return
        
        print(f"Found {len(json_files)} JSON file(s) to process")
        
        all_rows = []
        for json_path in json_files:
            try:
                # Read JSON file
                content = mssparkutils.fs.head(json_path, 10485760)  # Read up to 10MB
                payload = json.loads(content)
                
                # Extract workspace sidecar if present
                sidecar = payload.get("workspace_sidecar", {})
                
                # Flatten and extract connections
                rows = flatten_scan_payload(payload, sidecar)
                all_rows.extend(rows)
                print(f"  Processed {json_path}: {len(rows)} connection(s)")
                
            except Exception as e:
                print(f"  Warning: Failed to process {json_path}: {e}")
                continue
        
        if not all_rows:
            print("No connection rows extracted from JSON files.")
            return
        
        # Create DataFrame and deduplicate
        df_new = spark.createDataFrame(all_rows)
        df_new = (
            df_new.withColumn("connector", F.lower(F.coalesce(F.col("connector"), F.lit("unknown"))))
                  .dropDuplicates(["workspace_id","item_id","connector","server","database","endpoint"])
        )
        
        # Merge or overwrite
        if merge_with_existing:
            try:
                df_existing = spark.read.parquet(curated_dir)
                df_merged = (
                    df_existing.unionByName(df_new, allowMissingColumns=True)
                    .dropDuplicates(["workspace_id","item_id","connector","server","database","endpoint"])
                )
                print(f"Merged {df_new.count()} new rows with existing data")
            except Exception:
                df_merged = df_new
                print("No existing data found, creating new table")
        else:
            df_merged = df_new
            print("Overwriting existing data")
        
        # Write output
        df_merged.write.mode("overwrite").parquet(curated_dir)
        
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        spark.sql(f"CREATE TABLE {table_name} USING PARQUET LOCATION '{curated_dir}'")
        
        print(f"JSON directory scan completed. Total rows: {df_merged.count()}")
        print(f"Curated path: {curated_dir} | SQL table: {table_name}")
        
    except Exception as e:
        print(f"Error scanning JSON directory: {e}")
        raise


# --- Orchestrator: Choose Any Combination of Features ---

def run_cloud_connection_scan(
    enable_full_scan: bool = False,
    enable_incremental_scan: bool = True,
    enable_json_directory_scan: bool = False,
    include_personal: bool = True,
    incremental_days_back: int = 1,
    json_directory_path: str = None,
    json_merge_with_existing: bool = True,
    curated_dir: str = CURATED_DIR,
    table_name: str = "tenant_cloud_connections"
) -> None:
    """
    Orchestrates cloud connection scanning with configurable features.
    
    Args:
        enable_full_scan: Run full tenant scan (baseline)
        enable_incremental_scan: Run incremental scan for modified workspaces
        enable_json_directory_scan: Scan JSON files in a lakehouse directory
        include_personal: Include personal workspaces in API scans
        incremental_days_back: Number of days to look back for incremental scan
        json_directory_path: Path to directory containing JSON files (required if enable_json_directory_scan=True)
        json_merge_with_existing: Merge JSON scan results with existing data
        curated_dir: Output directory for curated data
        table_name: SQL table name for results
    """
    print("="*80)
    print("Cloud Connection Scanner - Feature Selection")
    print("="*80)
    print(f"Full Tenant Scan:        {'ENABLED' if enable_full_scan else 'DISABLED'}")
    print(f"Incremental Scan:        {'ENABLED' if enable_incremental_scan else 'DISABLED'}")
    print(f"JSON Directory Scan:     {'ENABLED' if enable_json_directory_scan else 'DISABLED'}")
    print(f"Include Personal WS:     {include_personal}")
    print(f"Incremental Days Back:   {incremental_days_back}")
    print(f"JSON Directory Path:     {json_directory_path or 'Not specified'}")
    print(f"Output Table:            {table_name}")
    print("="*80)
    
    features_enabled = sum([enable_full_scan, enable_incremental_scan, enable_json_directory_scan])
    if features_enabled == 0:
        print("\nWARNING: No features enabled. Nothing to do.")
        return
    
    # Feature 1: Full Tenant Scan
    if enable_full_scan:
        print("\n[1/3] Running FULL TENANT SCAN...")
        try:
            full_tenant_scan(
                include_personal=include_personal,
                curated_dir=curated_dir,
                table_name=table_name
            )
            print("✓ Full tenant scan completed successfully")
        except Exception as e:
            print(f"✗ Full tenant scan failed: {e}")
            raise
    
    # Feature 2: Incremental Scan
    if enable_incremental_scan:
        print("\n[2/3] Running INCREMENTAL SCAN...")
        try:
            since_iso = (
                datetime.now(timezone.utc) - timedelta(days=incremental_days_back)
            ).isoformat(timespec="seconds").replace("+00:00", "Z")
            
            incremental_update(
                modified_since_iso=since_iso,
                include_personal=include_personal,
                curated_dir=curated_dir,
                table_name=table_name
            )
            print("✓ Incremental scan completed successfully")
        except Exception as e:
            print(f"✗ Incremental scan failed: {e}")
            raise
    
    # Feature 3: JSON Directory Scan
    if enable_json_directory_scan:
        print("\n[3/3] Running JSON DIRECTORY SCAN...")
        if not json_directory_path:
            raise ValueError("json_directory_path is required when enable_json_directory_scan=True")
        
        try:
            scan_json_directory_for_connections(
                json_dir_path=json_directory_path,
                curated_dir=curated_dir,
                table_name=table_name,
                merge_with_existing=json_merge_with_existing
            )
            print("✓ JSON directory scan completed successfully")
        except Exception as e:
            print(f"✗ JSON directory scan failed: {e}")
            raise
    
    print("\n" + "="*80)
    print("SCAN COMPLETE - All enabled features executed successfully")
    print("="*80)


# --- Example Usage Patterns ---

if __name__ == "__main__":
    # EXAMPLE 1: Run only incremental scan (default behavior)
    # run_cloud_connection_scan(
    #     enable_incremental_scan=True,
    #     incremental_days_back=1
    # )
    
    # EXAMPLE 2: Run full scan only (baseline)
    # run_cloud_connection_scan(
    #     enable_full_scan=True,
    #     enable_incremental_scan=False
    # )
    
    # EXAMPLE 3: Run JSON directory scan only
    # run_cloud_connection_scan(
    #     enable_full_scan=False,
    #     enable_incremental_scan=False,
    #     enable_json_directory_scan=True,
    #     json_directory_path="lakehouse:/Default/Files/scanner/raw/full",
    #     json_merge_with_existing=True
    # )
    
    # EXAMPLE 4: Combine full scan + JSON directory scan
    # run_cloud_connection_scan(
    #     enable_full_scan=True,
    #     enable_incremental_scan=False,
    #     enable_json_directory_scan=True,
    #     json_directory_path="lakehouse:/Default/Files/scanner/raw",
    #     json_merge_with_existing=False
    # )
    
    # EXAMPLE 5: All features enabled
    # run_cloud_connection_scan(
    #     enable_full_scan=True,
    #     enable_incremental_scan=True,
    #     enable_json_directory_scan=True,
    #     json_directory_path="lakehouse:/Default/Files/scanner/archived",
    #     incremental_days_back=7,
    #     include_personal=True
    # )
    
    # Default: Run incremental scan only
    run_cloud_connection_scan(
        enable_incremental_scan=True,
        incremental_days_back=1
    )
