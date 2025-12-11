# 1. Open PowerShell and navigate to the folder
cd C:\Users\jaiperez\Documents\Wells\Fabric_Work\pbi_helper

# 2. Load the script
. .\powerbi_admin_api.ps1

# 3. Set your credentials
$tenantId = "your-tenant-id"
$clientId = "your-client-id"
$clientSecret = "your-client-secret"
$workspaceId = "your-workspace-id"

# 4. Get token
$token = Get-PowerBIAdminToken -TenantId $tenantId -ClientId $clientId -ClientSecret $clientSecret

# 5. Scan workspace for cloud connections
$scanResult = Invoke-PowerBIWorkspaceScan -AccessToken $token `
                                           -WorkspaceIds @($workspaceId) `
                                           -IncludeDatasourceDetails `
                                           -IncludeLineage

# 6. Extract connections
$connections = Get-CloudConnectionsFromScan -ScanResult $scanResult -OutputPath "connections.csv"

# 7. View results
$connections | Format-Table