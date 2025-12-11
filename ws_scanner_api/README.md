# Workspace Scanner API

Python script for scanning a specific Microsoft Fabric workspace to identify cloud connections using the Scanner Admin REST API.

## Overview

This tool uses Service Principal authentication to scan a single workspace and extract cloud connection information from:
- **Semantic Models/Datasets**: Database connections, data sources
- **Dataflows**: Data source connections
- **Pipelines**: Linked service connections
- **Lakehouses/Notebooks**: External connections

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
```

## Usage

### Basic Usage

```bash
python ws_scanner_api.py
```

### Output

The script can optionally generate a JSON file based on the `SAVE_JSON_FILE` configuration:

**When `SAVE_JSON_FILE = True` (default):**
- **`workspace_{WORKSPACE_ID}_cloud_connections.json`** - Full scan results saved to current directory
- **Console output** - Summary of findings with detailed connection information

**When `SAVE_JSON_FILE = False`:**
- **Console output only** - No file is created, results displayed in terminal

### Output Location

JSON files are saved in the **current working directory** where you execute the script:

```powershell
cd C:\Users\jaiperez\Documents\Wells\Fabric_Work\fabric_queries\ws_scanner_api
python ws_scanner_api.py

# Output file will be saved at:
# C:\Users\jaiperez\Documents\Wells\Fabric_Work\fabric_queries\ws_scanner_api\workspace_{WORKSPACE_ID}_cloud_connections.json
```

### Example Output

```
============================================================
Workspace: Sales Analytics
Type: Workspace
Total Cloud Connections: 3
============================================================

Cloud Connections Found:

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

## Cloud Connector Types Detected

The script identifies the following cloud connector types:

| Category | Connectors |
|----------|-----------|
| **Azure SQL** | `azuresqldatabase`, `sqlserverless`, `synapse` |
| **Azure Data** | `adls`, `abfss`, `onelake` |
| **Analytics** | `kusto`, `fabriclakehouse` |
| **Cloud Platforms** | `s3`, `snowflake` |
| **SaaS** | `salesforce`, `dynamics365`, `sharepointonline` |
| **Generic** | `rest` |

## Cloud Detection Logic

A connection is classified as "cloud" if:

1. **No Gateway Required** (`gatewayId` is `null`) **OR**
2. **Connector Type** is in the `CLOUD_CONNECTORS` set

### Examples:

- ✅ **Azure SQL Database** (no gateway, cloud connector) → Cloud
- ✅ **Salesforce** (no gateway, cloud connector) → Cloud
- ✅ **Snowflake via Gateway** (has gateway, but cloud connector) → Cloud
- ❌ **On-Prem SQL Server** (has gateway, not cloud connector) → Not Cloud

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

- Audit a specific workspace for cloud connections
- Quick connection check for development/test workspaces (set `SAVE_JSON_FILE = False` for console-only output)
- Verify external data dependencies for a single workspace
- Generate connection inventory for a project workspace
- Troubleshoot connection issues in a specific workspace

## Advanced Usage

### Console Output Only (No File)

For quick checks without creating files:

```python
# In ws_scanner_api.py, set:
SAVE_JSON_FILE = False

# Then run:
python ws_scanner_api.py
```

### Programmatic Usage

You can also import and use the function in your own scripts:

```python
from ws_scanner_api import scan_workspace_for_cloud_connections

# Save to file
results = scan_workspace_for_cloud_connections("workspace-id", save_to_file=True)

# Console only (no file)
results = scan_workspace_for_cloud_connections("workspace-id", save_to_file=False)

# Access results
print(f"Found {len(results['cloud_connections'])} cloud connections")
for conn in results['cloud_connections']:
    print(f"{conn['item_name']}: {conn['connector']}")
```

## JSON Output Schema

```json
{
  "workspace_id": "guid",
  "workspace_name": "string",
  "workspace_type": "Workspace|PersonalWorkspace",
  "cloud_connections": [
    {
      "item_name": "string",
      "item_type": "SemanticModel|Dataflow|Pipeline|Lakehouse|Notebook",
      "item_id": "guid",
      "connector": "string",
      "server": "string (optional)",
      "database": "string (optional)",
      "endpoint": "string (optional)",
      "connection_scope": "Cloud|OnPremViaGateway",
      "has_gateway": boolean
    }
  ]
}
```

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