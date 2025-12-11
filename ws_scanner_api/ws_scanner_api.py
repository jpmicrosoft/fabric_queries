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
PRINT_RAW_API_RESPONSE = False  # Set to True to see the original API JSON output
OUTPUT_PATH = None  # Set to a custom path (e.g., "/lakehouse/default/Files/") or leave as None for current directory

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

# --- Tenant Settings Check ---
def check_metadata_scanning_enabled() -> Dict[str, Any]:
    """
    Checks if enhanced metadata scanning is enabled in the tenant.
    Returns the tenant settings related to metadata scanning.
    """
    token = get_spn_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    base_url = "https://api.powerbi.com/v1.0/myorg/admin"
    
    print("\n🔍 Checking tenant settings for metadata scanning...")
    try:
        # Get tenant settings
        settings_response = requests.get(
            f"{base_url}/tenantsettings",
            headers=headers
        )
        settings_response.raise_for_status()
        settings = settings_response.json()
        
        # Look for metadata scanning related settings
        relevant_settings = {}
        if 'tenantSettings' in settings:
            for setting in settings['tenantSettings']:
                setting_name = setting.get('settingName', '')
                if any(keyword in setting_name.lower() for keyword in ['metadata', 'scan', 'admin', 'api']):
                    relevant_settings[setting_name] = {
                        'enabled': setting.get('enabled', False),
                        'canSpecifySecurityGroups': setting.get('canSpecifySecurityGroups', False),
                        'enabledSecurityGroups': setting.get('enabledSecurityGroups', [])
                    }
        
        # Display results
        print("\n📋 Relevant tenant settings:")
        if relevant_settings:
            for name, details in relevant_settings.items():
                status = "✅ ENABLED" if details['enabled'] else "❌ DISABLED"
                print(f"   {status}: {name}")
                if details['enabledSecurityGroups']:
                    print(f"      Applied to specific groups: {len(details['enabledSecurityGroups'])} group(s)")
        else:
            print("   ⚠️  Could not find metadata scanning settings")
        
        return relevant_settings
        
    except Exception as e:
        print(f"   ❌ Error checking tenant settings: {e}")
        print("   💡 Tip: Your SPN needs Tenant.Read.All permissions to read tenant settings")
        return {}

# --- Scanner API Call ---
def scan_workspace_for_cloud_connections(workspace_id: str, save_to_file: bool = True, print_to_screen: bool = True, print_raw_api: bool = False, output_path: str = None) -> Dict[str, Any]:
    """
    Scans a single workspace and returns cloud connection information.
    
    Args:
        workspace_id: The workspace ID to scan
        save_to_file: Whether to save results to JSON file (default: True)
        print_to_screen: Whether to print formatted output to console (default: True)
        print_raw_api: Whether to print the raw API JSON response (default: False)
        output_path: Custom directory path to save the file (default: None for current directory)
                     Example: "/lakehouse/default/Files/" or "C:/MyFolder/"
    
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
    
    # Request body contains only workspace IDs
    scan_body = {
        "workspaces": [workspace_id]
    }
    
    # Parameters go in the query string
    params = {
        "lineage": "True",
        "datasourceDetails": "True",
        "datasetSchema": "True",
        "datasetExpressions": "True",
        "getArtifactUsers": "True"
    }
    
    scan_response = requests.post(
        f"{base_url}/workspaces/getInfo",
        headers=headers,
        params=params,
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
    
    # Diagnostic: Show what keys are present in the response
    print(f"\n📊 Response contains these top-level keys: {list(scan_data.keys())}")
    if 'datasourceInstances' in scan_data:
        print(f"   ✅ datasourceInstances found: {len(scan_data['datasourceInstances'])} items")
    else:
        print(f"   ⚠️  datasourceInstances key is MISSING from response")
    if 'workspaces' in scan_data:
        print(f"   ✅ workspaces found: {len(scan_data['workspaces'])} items")
    
    # Return full API result without parsing
    results = scan_data
    
    # Save to JSON file if requested
    if save_to_file:
        filename = f"workspace_{workspace_id}_cloud_connections.json"
        
        if output_path:
            # Check if running in Fabric/Spark environment with lakehouse path
            if output_path.startswith("/lakehouse/") or output_path.startswith("abfss://"):
                try:
                    # Use mssparkutils for Fabric Lakehouse paths
                    from notebookutils import mssparkutils
                    full_path = output_path.rstrip('/') + '/' + filename
                    mssparkutils.fs.put(full_path, json.dumps(results, indent=2), True)
                    print(f"\n✅ Results saved to: {full_path}")
                except ImportError:
                    # Fallback if mssparkutils not available
                    print("⚠️  Warning: mssparkutils not available. Saving to local temp directory instead.")
                    import tempfile
                    full_path = f"{tempfile.gettempdir()}/{filename}"
                    with open(full_path, "w") as f:
                        json.dump(results, f, indent=2)
                    print(f"\n✅ Results saved to: {full_path}")
                    print(f"💡 Tip: Manually copy to lakehouse using: mssparkutils.fs.cp('{full_path}', '{output_path}{filename}')")
            else:
                # Local file system path
                import os
                os.makedirs(output_path, exist_ok=True)
                full_path = os.path.join(output_path, filename)
                with open(full_path, "w") as f:
                    json.dump(results, f, indent=2)
                print(f"\n✅ Results saved to: {full_path}")
        else:
            # No output path specified, save to current directory
            full_path = filename
            with open(full_path, "w") as f:
                json.dump(results, f, indent=2)
            print(f"\n✅ Results saved to: {full_path}")
    
    # Print to console if requested
    if print_to_screen:
        print("\n" + "="*60)
        print("FULL API JSON RESULT:")
        print("="*60)
        print(json.dumps(scan_data, indent=2))
        print("="*60 + "\n")
    
    return results

# --- Execute ---
if __name__ == "__main__":
    # Check tenant settings first
    print("="*60)
    print("TENANT SETTINGS CHECK")
    print("="*60)
    check_metadata_scanning_enabled()
    print("="*60 + "\n")
    
    # Only run if WORKSPACE_ID is set to an actual value (not the placeholder)
    if WORKSPACE_ID == "12345678-1234-1234-1234-123456789abc":
        print("⚠️  WARNING: WORKSPACE_ID is set to the default placeholder value.")
        print("Please update the WORKSPACE_ID variable or call the function directly:")
        print('results = scan_workspace_for_cloud_connections("your-workspace-id", save_to_file=True)')
    else:
        try:
            results = scan_workspace_for_cloud_connections(
                WORKSPACE_ID, 
                save_to_file=SAVE_JSON_FILE,
                print_to_screen=PRINT_TO_SCREEN,
                print_raw_api=PRINT_RAW_API_RESPONSE,
                output_path=OUTPUT_PATH
            )
            
        except Exception as e:
            print(f"❌ Error: {e}")
            import traceback
            traceback.print_exc()
