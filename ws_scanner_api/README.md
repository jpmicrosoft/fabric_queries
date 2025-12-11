# Workspace Scanner API

Python script for retrieving complete workspace metadata from Microsoft Fabric using the Scanner Admin REST API.

## Overview

This tool uses Service Principal authentication to scan a single workspace and returns the **complete raw API response** with all workspace details including:
- **Workspace metadata**: Name, type, state, capacity info
- **All items**: Semantic Models, Dataflows, Pipelines, Lakehouses, Notebooks, Reports, Dashboards
- **All connections**: Datasources, gateway info, connection details
- **Users and permissions**: Workspace access information
- **Lineage data**: Data flow and dependencies

The script returns the **unfiltered Scanner API JSON response** without any parsing or data transformation.

## Prerequisites

- Python 3.8+
- `requests` library (`pip install requests`)
- **Fabric Administrator** role in your tenant
- **Service Principal** with appropriate permissions:
  - `Tenant.Read.All` or `Tenant.ReadWrite.All` scope
  - Member of Fabric Admin security group

## Configuration

Edit the configuration section at the top of `ws_scanner_api.py`:

```python
# --- Configuration ---
TENANT_ID = "your-tenant-id"
CLIENT_ID = "your-app-client-id"
CLIENT_SECRET = "your-app-client-secret"

# Target workspace
WORKSPACE_ID = "12345678-1234-1234-1234-123456789abc"

# Output options
SAVE_JSON_FILE = True  # Set to False to skip saving JSON file
PRINT_TO_SCREEN = True  # Set to False to skip console output
OUTPUT_PATH = None  # Set to a custom path (e.g., "/lakehouse/default/Files/") or leave as None
```

## Usage

### Basic Usage

```bash
python ws_scanner_api.py
```

### Output

The script returns the **complete Scanner API JSON response** without any filtering or parsing.

**File Output (`SAVE_JSON_FILE`):**
- `True` (default): Saves full API response to `workspace_{WORKSPACE_ID}_cloud_connections.json`
- `False`: No file created

**Console Output (`PRINT_TO_SCREEN`):**
- `True` (default): Displays the complete Scanner API JSON response
- `False`: Silent mode, returns data only

**Custom Save Location (`OUTPUT_PATH`):**
- `None` (default): Saves to current working directory
- `"/lakehouse/default/Files/"`: Saves to Fabric Lakehouse (when running in Fabric notebook)
- `"C:/MyFolder/"`: Saves to custom local path

### Output Location

**Local Execution:**
JSON files are saved in the **current working directory** where you execute the script:

```powershell
cd C:\Users\jaiperez\Documents\Wells\Fabric_Work\fabric_queries\ws_scanner_api
python ws_scanner_api.py

# Output file will be saved at:
# C:\Users\jaiperez\Documents\Wells\Fabric_Work\fabric_queries\ws_scanner_api\workspace_{WORKSPACE_ID}_cloud_connections.json
```

**Fabric Lakehouse:**
When running in a Fabric notebook, save to your attached Lakehouse:

```python
# Set in configuration section
OUTPUT_PATH = "/lakehouse/default/Files/"

# Or pass as parameter
results = scan_workspace_for_cloud_connections(
    "workspace-id",
    output_path="/lakehouse/default/Files/"
)

# File will appear in: Lakehouse > Files > workspace_{id}_cloud_connections.json
```

**Note:** The `/lakehouse/default/` path automatically points to your notebook's attached lakehouse, regardless of the lakehouse name.

### Example Output

```json
{
  "workspaces": [
    {
      "id": "12345678-1234-1234-1234-123456789abc",
      "name": "Sales Analytics",
      "type": "Workspace",
      "state": "Active",
      "isReadOnly": false,
      "isOnDedicatedCapacity": true,
      "capacityId": "...",
      "users": [...],
      "items": [
        {
          "id": "...",
          "name": "Sales Report",
          "type": "Report",
          "datasources": [...]
        }
      ]
    }
  ]
}

1. SalesDataset (SemanticModel)
   Connector: azuresqldatabase
   Scope: Cloud
   Server: salesdb.database.windows.net
   Database: sales_prod

2. CustomerDataflow (Dataflow)
   Connector: salesforce
   Scope: Cloud
   Endpoint: https://company.salesforce.com

3. ETLPipeline (Pipeline)
   Connector: azuredatalakestorage
   Scope: Cloud
   Endpoint: https://storage.dfs.core.windows.net
```

## Scanner Admin REST API Workflow

The script follows this workflow:

```
1. Authenticate (Service Principal)
   ↓
2. POST /admin/workspaces/getInfo
   → Returns scanId
   ↓
3. GET /admin/workspaces/scanStatus/{scanId}
   → Poll until status = "Succeeded"
   ↓
4. GET /admin/workspaces/scanResult/{scanId}
   → Returns workspace metadata + connections
   ↓
5. Parse & Filter Cloud Connections
```

## API Rate Limits

**Scanner Admin REST API limits:**
- **500 requests/hour** (tenant-wide, shared across all users)
- **16 concurrent requests maximum**
- Single workspace scan uses approximately **3-4 API calls**

## Troubleshooting

### Authentication Errors (401)

```
Error: Authentication failed. Ensure you have Fabric Admin permissions.
```

**Solution:**
- Verify Service Principal credentials (TENANT_ID, CLIENT_ID, CLIENT_SECRET)
- Ensure Service Principal is member of Fabric Admin security group
- Check API permissions in Azure AD app registration

### Scan Not Found (404)

```
Error: Scan ID {scan_id} not found. The scan may have expired (>24 hours) or never existed.
```

**Solution:**
- Scan results expire after 24 hours
- Re-run the script to initiate a new scan

### Scan Timeout

```
Error: Scan timed out after 10 minutes
```

**Solution:**
- Check workspace size (very large workspaces may take longer)
- Verify network connectivity to `api.powerbi.com`
- Check Scanner API service health

### Rate Limit Exceeded (429)

```
⚠️ Rate limit exceeded (429). Waiting 60 seconds before retry...
```

**Solution:**
- Script automatically retries with exponential backoff
- Wait at least 1 hour if maximum retries exceeded
- Check if other processes are consuming the API quota

## Use Cases

- Export complete workspace metadata for analysis
- Retrieve all workspace items and their configurations
- Audit workspace connections and data sources
- Extract lineage and dependency information
- Backup workspace metadata as JSON
- Feed workspace data into custom analytics or reporting tools

## Advanced Usage

### Programmatic Usage

You can import and use the function to retrieve workspace data programmatically:

```python
from ws_scanner_api import scan_workspace_for_cloud_connections

# Get full API response with file + console output
results = scan_workspace_for_cloud_connections(
    "workspace-id",
    save_to_file=True,
    print_to_screen=True
)

# Console only (no file)
results = scan_workspace_for_cloud_connections(
    "workspace-id",
    save_to_file=False,
    print_to_screen=True
)

# Silent mode (return data only, no output)
results = scan_workspace_for_cloud_connections(
    "workspace-id",
    save_to_file=False,
    print_to_screen=False
)

# Save to Fabric Lakehouse
results = scan_workspace_for_cloud_connections(
    "workspace-id",
    save_to_file=True,
    output_path="/lakehouse/default/Files/"
)

# Save to custom local directory
results = scan_workspace_for_cloud_connections(
    "workspace-id",
    save_to_file=True,
    output_path="C:/Reports/FabricScans/"
)

# Access workspace data from API response
for workspace in results.get('workspaces', []):
    print(f"Workspace: {workspace['name']}")
    print(f"Total items: {len(workspace.get('items', []))}")
    
    # Access all items
    for item in workspace.get('items', []):
        print(f"  - {item['name']} ({item['type']})")
        
        # Access datasources if present
        for datasource in item.get('datasources', []):
            conn_details = datasource.get('connectionDetails', {})
            print(f"    Datasource: {conn_details.get('datasourceType')}")
```

### Function Signature

```python
def scan_workspace_for_cloud_connections(
    workspace_id: str,
    save_to_file: bool = True,
    print_to_screen: bool = True,
    output_path: str = None
) -> Dict[str, Any]
```

**Parameters:**
- `workspace_id`: Target workspace GUID
- `save_to_file`: Save complete API response to JSON file (default: True)
- `print_to_screen`: Display full Scanner API JSON response to console (default: True)
- `output_path`: Custom save directory path; None = current directory (default: None)

**Returns:**
- Complete Scanner API response as dictionary

## JSON Output Schema

The script returns the complete Scanner Admin API response structure:

```json
{
  "workspaces": [
    {
      "id": "guid",
      "name": "string",
      "type": "Workspace|PersonalWorkspace",
      "state": "Active|Deleted|Removing",
      "isReadOnly": boolean,
      "isOrphaned": boolean,
      "isOnDedicatedCapacity": boolean,
      "capacityId": "guid (optional)",
      "defaultDatasetStorageFormat": "string (optional)",
      "users": [
        {
          "identifier": "string",
          "displayName": "string",
          "emailAddress": "string",
          "workspaceUserAccessRight": "Admin|Member|Contributor|Viewer",
          "principalType": "User|Group|App"
        }
      ],
      "items": [
        {
          "id": "guid",
          "name": "string",
          "type": "Report|Dashboard|SemanticModel|Dataflow|Pipeline|Lakehouse|Notebook|...",
          "datasources": [
            {
              "datasourceId": "guid",
              "gatewayId": "guid (optional)",
              "connectionDetails": {
                "datasourceType": "string",
                "server": "string (optional)",
                "database": "string (optional)",
                "url": "string (optional)"
              }
            }
          ],
          "users": [...],
          "subscriptions": [...],
          "endorsementDetails": {...}
        }
      ]
    }
  ]
}
```

For complete schema documentation, see the [Scanner Admin REST API reference](https://learn.microsoft.com/rest/api/power-bi/admin/workspace-info-get-scan-result)

## Security Considerations

⚠️ **Important Security Notes:**

1. **Credentials Management:**
   - Never commit credentials to source control
   - Use environment variables or Azure Key Vault
   - Rotate Service Principal secrets regularly

2. **Service Principal Permissions:**
   - Grant minimum required permissions
   - Use dedicated Service Principal for scanning
   - Monitor Service Principal activity

3. **Output Files:**
   - JSON files may contain sensitive connection information
   - Store output files securely
   - Apply appropriate access controls

4. **Network Security:**
   - Ensure secure HTTPS communication
   - Use corporate network or VPN when required
   - Monitor outbound API calls

## Environment Variables (Optional)

Instead of hardcoding credentials, use environment variables:

```python
import os

TENANT_ID = os.getenv("FABRIC_TENANT_ID")
CLIENT_ID = os.getenv("FABRIC_CLIENT_ID")
CLIENT_SECRET = os.getenv("FABRIC_CLIENT_SECRET")
WORKSPACE_ID = os.getenv("FABRIC_WORKSPACE_ID")
```

Then set them:

```powershell
# PowerShell
$env:FABRIC_TENANT_ID = "your-tenant-id"
$env:FABRIC_CLIENT_ID = "your-client-id"
$env:FABRIC_CLIENT_SECRET = "your-secret"
$env:FABRIC_WORKSPACE_ID = "workspace-id"

python ws_scanner_api.py
```

```bash
# Bash
export FABRIC_TENANT_ID="your-tenant-id"
export FABRIC_CLIENT_ID="your-client-id"
export FABRIC_CLIENT_SECRET="your-secret"
export FABRIC_WORKSPACE_ID="workspace-id"

python ws_scanner_api.py
```

## Support

For issues or questions:
- Check the [Scanner Admin REST API documentation](https://learn.microsoft.com/rest/api/power-bi/admin)
- Review tenant-wide scanner documentation: `../scanner_api/README.md`
- Verify Service Principal permissions in Azure AD