
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

RAW_DIR     = "lakehouse:/Default/Files/scanner/raw"
CURATED_DIR = "lakehouse:/Default/curated/connections"

if mssparkutils is not None:
    for path in [RAW_DIR, CURATED_DIR]:
        try:
            mssparkutils.fs.mkdirs(path)
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
    body = {"workspaces": workspace_ids}
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
    sidecar = {
        w.get("id"): {
            "name": w.get("name", ""),
            "kind": (str(w.get("type")).lower() if w.get("type") else "unknown")
        } for w in batch_meta if w.get("id")
    }
    payload["workspace_sidecar"] = sidecar
    if mssparkutils is not None:
        try:
            raw_path = f"{RAW_DIR}/full/{scan_id}.json"
            mssparkutils.fs.put(raw_path, json.dumps(payload))
        except Exception:
            pass
    return payload

# --- Flatten helpers ---

def _lower_or(x, default="unknown"):
    return (str(x).lower() if x is not None else default)


def flatten_scan_payload(payload: Dict[str, Any], ws_sidecar: Dict[str, Dict[str, str]]) -> List[Row]:
    rows: List[Row] = []
    for ws in (payload.get("workspaces") or []):
        ws_id   = ws.get("id")
        itemset = ws.get("items") or []
        wmeta   = ws_sidecar.get(ws_id, {"name": ws.get("name") or "", "kind": _lower_or(ws.get("type"))})
        ws_name = wmeta.get("name", "")
        ws_kind = wmeta.get("kind", "unknown")

        for item in itemset:
            item_id   = item.get("id")
            item_name = item.get("name")
            item_type = _lower_or(item.get("type"))

            if item_type in {"semanticmodel", "dataset"}:
                for ds in (item.get("datasources") or []):
                    conn      = ds.get("connectionDetails") or {}
                    connector = _lower_or(conn.get("datasourceType"))
                    server    = conn.get("server") or conn.get("host")
                    database  = conn.get("database") or conn.get("db")
                    gateway_id= ds.get("gatewayId")
                    connection_scope = "OnPremViaGateway" if gateway_id else "Cloud"
                    cloud_flag       = (connection_scope == "Cloud") or (connector in CLOUD_CONNECTORS)
                    rows.append(Row(
                        workspace_id   = ws_id,
                        workspace_name = ws_name,
                        workspace_kind = ws_kind,
                        item_id        = item_id,
                        item_name      = item_name,
                        item_type      = "SemanticModel",
                        connector      = connector,
                        server         = server,
                        database       = database,
                        endpoint       = None,
                        connection_scope = connection_scope,
                        cloud          = cloud_flag,
                        generation     = None
                    ))

            elif item_type == "dataflow":
                generation = item.get("generation") or (item.get("properties") or {}).get("generation")
                sources    = item.get("sources") or item.get("entities") or []
                for src in sources:
                    connector = _lower_or(src.get("type") or src.get("provider"))
                    endpoint  = src.get("url") or src.get("path")
                    connection_scope = "Cloud"
                    cloud_flag       = (connection_scope == "Cloud") or (connector in CLOUD_CONNECTORS)
                    rows.append(Row(
                        workspace_id   = ws_id,
                        workspace_name = ws_name,
                        workspace_kind = ws_kind,
                        item_id        = item_id,
                        item_name      = item_name,
                        item_type      = "Dataflow",
                        connector      = connector,
                        server         = None,
                        database       = None,
                        endpoint       = endpoint,
                        connection_scope = connection_scope,
                        cloud          = cloud_flag,
                        generation     = generation
                    ))

            elif item_type == "pipeline":
                for act in (item.get("activities") or []):
                    ref       = act.get("linkedService") or {}
                    connector = _lower_or(ref.get("type") or act.get("type"))
                    endpoint  = ref.get("url") or ref.get("endpoint")
                    gateway_id= ref.get("gatewayId")
                    connection_scope = "OnPremViaGateway" if gateway_id else "Cloud"
                    cloud_flag       = (connection_scope == "Cloud") or (connector in CLOUD_CONNECTORS)
                    rows.append(Row(
                        workspace_id   = ws_id,
                        workspace_name = ws_name,
                        workspace_kind = ws_kind,
                        item_id        = item_id,
                        item_name      = item_name,
                        item_type      = "Pipeline",
                        connector      = connector,
                        server         = None,
                        database       = None,
                        endpoint       = endpoint,
                        connection_scope = connection_scope,
                        cloud          = cloud_flag,
                        generation     = None
                    ))

            elif item_type in {"lakehouse", "notebook"}:
                references = (item.get("connections") or []) + (item.get("lineage") or [])
                for ref in references:
                    connector       = _lower_or(ref.get("type"))
                    endpoint        = ref.get("url") or ref.get("endpoint")
                    is_cloud_flag   = ref.get("isCloud", True)
                    connection_scope= "Cloud" if is_cloud_flag else "OnPremViaGateway"
                    cloud_flag      = (connection_scope == "Cloud") or (connector in CLOUD_CONNECTORS)
                    rows.append(Row(
                        workspace_id   = ws_id,
                        workspace_name = ws_name,
                        workspace_kind = ws_kind,
                        item_id        = item_id,
                        item_name      = item_name,
                        item_type      = item_type.capitalize(),
                        connector      = connector,
                        server         = None,
                        database       = None,
                        endpoint       = endpoint,
                        connection_scope = connection_scope,
                        cloud          = cloud_flag,
                        generation     = None
                    ))

            else:
                rows.append(Row(
                    workspace_id   = ws_id,
                    workspace_name = ws_name,
                    workspace_kind = ws_kind,
                    item_id        = item_id,
                    item_name      = item_name,
                    item_type      = item_type.capitalize(),
                    connector      = "unknown",
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
    sidecar = {
        w.get("id"): {
            "name": w.get("name", ""),
            "kind": (str(w.get("type")).lower() if w.get("type") else "unknown")
        } for w in batch_meta if w.get("id")
    }
    payload["workspace_sidecar"] = sidecar
    if mssparkutils is not None:
        try:
            raw_path = f"{RAW_DIR}/incremental/{scan_id}.json"
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

# --- Example runs ---
if __name__ == "__main__":
    # Uncomment to run a full baseline scan
    # full_tenant_scan(include_personal=True)

    # Run incremental for last 24h
    since_iso = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat(timespec="seconds").replace("+00:00","Z")
    incremental_update(since_iso, include_personal=True)
