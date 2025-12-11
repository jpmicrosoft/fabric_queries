# Service Principal Authentication + Direct Workspace Query
# Run this in a Fabric notebook or Python environment with requests library

import requests
import json
from typing import Dict, Any

# --- Configuration ---
TENANT_ID = "your-tenant-id"
CLIENT_ID = "your-app-client-id"
CLIENT_SECRET = "your-app-client-secret"

# Target workspace
WORKSPACE_ID = "12345678-1234-1234-1234-123456789abc"  # Your workspace ID

# Output options
SAVE_JSON_FILE = True  # Set to False to skip saving JSON file
PRINT_TO_SCREEN = True  # Set to False to skip console output

# --- Auth ---
def get_spn_token() -> str:
    """Get access token using Service Principal credentials."""
    auth_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    data = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://analysis.windows.net/powerbi/api/.default",
        "grant_type": "client_credentials",
    }
    r = requests.post(auth_url, data=data)
    r.raise_for_status()
    return r.json().get("access_token")

# --- Scanner API Call ---
def scan_workspace_for_cloud_connections(workspace_id: str, save_to_file: bool = True, print_to_screen: bool = True) -> Dict[str, Any]:
    """
    Scans a single workspace and returns cloud connection information.
    
    Args:
        workspace_id: The workspace ID to scan
        save_to_file: Whether to save results to JSON file (default: True)
        print_to_screen: Whether to print formatted output to console (default: True)
    
    Returns:
        Dictionary with workspace metadata and cloud connections
    """
    token = get_spn_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    base_url = "https://api.powerbi.com/v1.0/myorg/admin"
    
    # Step 1: Initiate scan for the workspace
    print(f"Initiating scan for workspace: {workspace_id}")
    scan_body = {
        "workspaces": [workspace_id],
        "lineage": True,
        "users": True
    }
    
    scan_response = requests.post(
        f"{base_url}/workspaces/getInfo",
        headers=headers,
        json=scan_body
    )
    scan_response.raise_for_status()
    scan_id = scan_response.json().get("id")
    print(f"Scan initiated. Scan ID: {scan_id}")
    
    # Step 2: Poll for scan completion
    import time
    max_wait = 600  # 10 minutes
    poll_interval = 10  # 10 seconds
    elapsed = 0
    
    while elapsed < max_wait:
        status_response = requests.get(
            f"{base_url}/workspaces/scanStatus/{scan_id}",
            headers=headers
        )
        
        if status_response.status_code == 200:
            status = status_response.json().get("status")
            if status == "Succeeded":
                print(f"Scan completed successfully!")
                break
            elif status in {"Failed", "Cancelled"}:
                raise RuntimeError(f"Scan failed with status: {status}")
        elif status_response.status_code == 202:
            print(f"Scan in progress... ({elapsed}s elapsed)")
            time.sleep(poll_interval)
            elapsed += poll_interval
        else:
            status_response.raise_for_status()
    
    if elapsed >= max_wait:
        raise TimeoutError("Scan timed out")
    
    # Step 3: Get scan results
    print("Retrieving scan results...")
    result_response = requests.get(
        f"{base_url}/workspaces/scanResult/{scan_id}",
        headers=headers
    )
    result_response.raise_for_status()
    scan_data = result_response.json()
    
    # Step 4: Extract cloud connections
    cloud_connectors = {
        "azuresqldatabase", "sqlserverless", "synapse", "kusto",
        "onelake", "adls", "abfss", "s3", "rest",
        "sharepointonline", "dynamics365", "salesforce", "snowflake",
        "fabriclakehouse"
    }
    
    results = {
        "workspace_id": workspace_id,
        "workspace_name": None,
        "workspace_type": None,
        "cloud_connections": []
    }
    
    for workspace in scan_data.get("workspaces", []):
        if workspace.get("id") == workspace_id:
            results["workspace_name"] = workspace.get("name")
            results["workspace_type"] = workspace.get("type")
            
            # Process all items in workspace
            for item in workspace.get("items", []):
                item_type = item.get("type", "").lower()
                item_name = item.get("name")
                item_id = item.get("id")
                
                # Semantic Models / Datasets
                if item_type in {"semanticmodel", "dataset"}:
                    for datasource in item.get("datasources", []):
                        conn_details = datasource.get("connectionDetails", {})
                        connector = conn_details.get("datasourceType", "").lower()
                        gateway_id = datasource.get("gatewayId")
                        
                        # Check if it's a cloud connection
                        is_cloud = (gateway_id is None) or (connector in cloud_connectors)
                        
                        if is_cloud:
                            results["cloud_connections"].append({
                                "item_name": item_name,
                                "item_type": "SemanticModel",
                                "item_id": item_id,
                                "connector": connector,
                                "server": conn_details.get("server"),
                                "database": conn_details.get("database"),
                                "connection_scope": "Cloud" if gateway_id is None else "OnPremViaGateway",
                                "has_gateway": gateway_id is not None
                            })
                
                # Dataflows
                elif item_type == "dataflow":
                    for source in item.get("sources", []):
                        connector = source.get("type", "").lower()
                        
                        if connector in cloud_connectors or connector != "unknown":
                            results["cloud_connections"].append({
                                "item_name": item_name,
                                "item_type": "Dataflow",
                                "item_id": item_id,
                                "connector": connector,
                                "endpoint": source.get("url"),
                                "connection_scope": "Cloud",
                                "has_gateway": False
                            })
                
                # Pipelines
                elif item_type == "pipeline":
                    for activity in item.get("activities", []):
                        linked_service = activity.get("linkedService", {})
                        connector = linked_service.get("type", "").lower()
                        gateway_id = linked_service.get("gatewayId")
                        
                        is_cloud = (gateway_id is None) or (connector in cloud_connectors)
                        
                        if is_cloud:
                            results["cloud_connections"].append({
                                "item_name": item_name,
                                "item_type": "Pipeline",
                                "item_id": item_id,
                                "connector": connector,
                                "endpoint": linked_service.get("url"),
                                "connection_scope": "Cloud" if gateway_id is None else "OnPremViaGateway",
                                "has_gateway": gateway_id is not None
                            })
                
                # Lakehouses / Notebooks
                elif item_type in {"lakehouse", "notebook"}:
                    for connection in item.get("connections", []):
                        connector = connection.get("type", "").lower()
                        is_cloud_flag = connection.get("isCloud", True)
                        
                        if is_cloud_flag or connector in cloud_connectors:
                            results["cloud_connections"].append({
                                "item_name": item_name,
                                "item_type": item_type.capitalize(),
                                "item_id": item_id,
                                "connector": connector,
                                "endpoint": connection.get("url"),
                                "connection_scope": "Cloud" if is_cloud_flag else "OnPremViaGateway",
                                "has_gateway": not is_cloud_flag
                            })
    
    # Save to JSON file if requested
    if save_to_file:
        filename = f"workspace_{workspace_id}_cloud_connections.json"
        with open(filename, "w") as f:
            json.dump(results, f, indent=2)
        print(f"\n✅ Results saved to: {filename}")
    
    # Print to console if requested
    if print_to_screen:
        print(f"\n{'='*60}")
        print(f"Workspace: {results['workspace_name']}")
        print(f"Type: {results['workspace_type']}")
        print(f"Total Cloud Connections: {len(results['cloud_connections'])}")
        print(f"{'='*60}\n")
        
        if results['cloud_connections']:
            print("Cloud Connections Found:")
            for idx, conn in enumerate(results['cloud_connections'], 1):
                print(f"\n{idx}. {conn['item_name']} ({conn['item_type']})")
                print(f"   Connector: {conn['connector']}")
                print(f"   Scope: {conn['connection_scope']}")
                if conn.get('server'):
                    print(f"   Server: {conn['server']}")
                if conn.get('database'):
                    print(f"   Database: {conn['database']}")
                if conn.get('endpoint'):
                    print(f"   Endpoint: {conn['endpoint']}")
        else:
            print("No cloud connections found in this workspace.")
    
    return results

# --- Execute ---
if __name__ == "__main__":
    try:
        results = scan_workspace_for_cloud_connections(
            WORKSPACE_ID, 
            save_to_file=SAVE_JSON_FILE,
            print_to_screen=PRINT_TO_SCREEN
        )
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
