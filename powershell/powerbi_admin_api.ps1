# Power BI Admin API - PowerShell Scripts
# Direct API calls without Python

# ============================================================
# SCRIPT 1: Get OAuth Token (Service Principal)
# ============================================================

function Get-PowerBIAdminToken {
    param(
        [Parameter(Mandatory=$true)]
        [string]$TenantId,
        
        [Parameter(Mandatory=$true)]
        [string]$ClientId,
        
        [Parameter(Mandatory=$true)]
        [string]$ClientSecret
    )
    
    $tokenEndpoint = "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token"
    
    $body = @{
        grant_type    = "client_credentials"
        client_id     = $ClientId
        client_secret = $ClientSecret
        scope         = "https://analysis.windows.net/powerbi/api/.default"
    }
    
    $response = Invoke-RestMethod -Uri $tokenEndpoint -Method Post -Body $body -ContentType "application/x-www-form-urlencoded"
    
    return $response.access_token
}


# ============================================================
# SCRIPT 2: Scan Workspace for Cloud Connections
# ============================================================

function Invoke-PowerBIWorkspaceScan {
    param(
        [Parameter(Mandatory=$true)]
        [string]$AccessToken,
        
        [Parameter(Mandatory=$true)]
        [string[]]$WorkspaceIds,
        
        [switch]$IncludeLineage,
        [switch]$IncludeDatasourceDetails,
        [switch]$IncludeDatasetSchema,
        [switch]$IncludeDatasetExpressions,
        [switch]$IncludeArtifactUsers,
        
        [int]$MaxWaitSeconds = 600,
        [int]$PollIntervalSeconds = 10
    )
    
    $baseUrl = "https://api.powerbi.com/v1.0/myorg/admin"
    $headers = @{
        "Authorization" = "Bearer $AccessToken"
        "Content-Type"  = "application/json"
    }
    
    # Step 1: Initiate scan
    Write-Host "🔄 Initiating workspace scan..." -ForegroundColor Cyan
    
    $scanBody = @{
        workspaces = $WorkspaceIds
    } | ConvertTo-Json
    
    $queryParams = @()
    if ($IncludeLineage) { $queryParams += "lineage=True" }
    if ($IncludeDatasourceDetails) { $queryParams += "datasourceDetails=True" }
    if ($IncludeDatasetSchema) { $queryParams += "datasetSchema=True" }
    if ($IncludeDatasetExpressions) { $queryParams += "datasetExpressions=True" }
    if ($IncludeArtifactUsers) { $queryParams += "getArtifactUsers=True" }
    
    $queryString = if ($queryParams.Count -gt 0) { "?" + ($queryParams -join "&") } else { "" }
    
    $scanResponse = Invoke-RestMethod -Uri "$baseUrl/workspaces/getInfo$queryString" `
                                       -Method Post `
                                       -Headers $headers `
                                       -Body $scanBody
    
    $scanId = $scanResponse.id
    Write-Host "✅ Scan initiated. Scan ID: $scanId" -ForegroundColor Green
    
    # Step 2: Poll for completion
    Write-Host "⏳ Waiting for scan to complete..." -ForegroundColor Yellow
    
    $elapsed = 0
    $status = $null
    
    while ($elapsed -lt $MaxWaitSeconds) {
        Start-Sleep -Seconds $PollIntervalSeconds
        $elapsed += $PollIntervalSeconds
        
        try {
            $statusResponse = Invoke-RestMethod -Uri "$baseUrl/workspaces/scanStatus/$scanId" `
                                                  -Method Get `
                                                  -Headers $headers
            
            $status = $statusResponse.status
            Write-Host "  Status: $status (Elapsed: ${elapsed}s)" -ForegroundColor Gray
            
            if ($status -eq "Succeeded") {
                Write-Host "✅ Scan completed successfully!" -ForegroundColor Green
                break
            }
            elseif ($status -in @("Failed", "Cancelled")) {
                throw "Scan failed with status: $status"
            }
        }
        catch {
            # 202 Accepted means still processing
            if ($_.Exception.Response.StatusCode.value__ -ne 202) {
                throw $_
            }
        }
    }
    
    if ($elapsed -ge $MaxWaitSeconds) {
        throw "Scan timed out after $MaxWaitSeconds seconds"
    }
    
    # Step 3: Get results
    Write-Host "📥 Retrieving scan results..." -ForegroundColor Cyan
    
    $result = Invoke-RestMethod -Uri "$baseUrl/workspaces/scanResult/$scanId" `
                                 -Method Get `
                                 -Headers $headers
    
    Write-Host "✅ Scan results retrieved!" -ForegroundColor Green
    return $result
}


# ============================================================
# SCRIPT 3: Extract Cloud Connections from Scan Result
# ============================================================

function Get-CloudConnectionsFromScan {
    param(
        [Parameter(Mandatory=$true)]
        [object]$ScanResult,
        
        [string]$OutputPath
    )
    
    $datasources = $ScanResult.datasourceInstances
    
    Write-Host "`n============================================================" -ForegroundColor Cyan
    Write-Host "CLOUD CONNECTIONS FOUND" -ForegroundColor Cyan
    Write-Host "============================================================" -ForegroundColor Cyan
    Write-Host "Total datasource instances: $($datasources.Count)`n" -ForegroundColor Green
    
    $connections = @()
    
    foreach ($ds in $datasources) {
        $connection = [PSCustomObject]@{
            DatasourceType      = $ds.datasourceType
            DatasourceId        = $ds.datasourceId
            GatewayId          = $ds.gatewayId
            ConnectionDetails  = $ds.connectionDetails | ConvertTo-Json -Compress
            Server             = $ds.connectionDetails.server
            Database           = $ds.connectionDetails.database
            Url                = $ds.connectionDetails.url
            Account            = $ds.connectionDetails.account
        }
        
        $connections += $connection
        
        Write-Host "📊 Datasource Type: $($ds.datasourceType)" -ForegroundColor Yellow
        Write-Host "   Connection Details: $($ds.connectionDetails | ConvertTo-Json -Compress)" -ForegroundColor Gray
        Write-Host "   Datasource ID: $($ds.datasourceId)" -ForegroundColor Gray
        Write-Host "   Gateway ID: $($ds.gatewayId)`n" -ForegroundColor Gray
    }
    
    if ($OutputPath) {
        $connections | Export-Csv -Path $OutputPath -NoTypeInformation
        Write-Host "✅ Saved connections to: $OutputPath" -ForegroundColor Green
    }
    
    Write-Host "============================================================`n" -ForegroundColor Cyan
    
    return $connections
}


# ============================================================
# SCRIPT 4: Get All Workspaces
# ============================================================

function Get-PowerBIAdminWorkspaces {
    param(
        [Parameter(Mandatory=$true)]
        [string]$AccessToken,
        
        [int]$Top = 5000
    )
    
    $headers = @{
        "Authorization" = "Bearer $AccessToken"
    }
    
    $response = Invoke-RestMethod -Uri "https://api.powerbi.com/v1.0/myorg/admin/groups?`$top=$Top" `
                                   -Method Get `
                                   -Headers $headers
    
    return $response.value
}


# ============================================================
# SCRIPT 5: Get Dataflows in Workspace
# ============================================================

function Get-PowerBIAdminDataflows {
    param(
        [Parameter(Mandatory=$true)]
        [string]$AccessToken,
        
        [Parameter(Mandatory=$true)]
        [string]$WorkspaceId
    )
    
    $headers = @{
        "Authorization" = "Bearer $AccessToken"
    }
    
    $response = Invoke-RestMethod -Uri "https://api.powerbi.com/v1.0/myorg/admin/groups/$WorkspaceId/dataflows" `
                                   -Method Get `
                                   -Headers $headers
    
    return $response.value
}


# ============================================================
# EXAMPLE USAGE
# ============================================================

# Configuration
$tenantId = "your-tenant-id"
$clientId = "your-client-id"
$clientSecret = "your-client-secret"
$workspaceId = "your-workspace-id"

# Step 1: Get access token
Write-Host "`n🔑 Getting access token..." -ForegroundColor Cyan
$token = Get-PowerBIAdminToken -TenantId $tenantId -ClientId $clientId -ClientSecret $clientSecret
Write-Host "✅ Token acquired!`n" -ForegroundColor Green

# Step 2: Scan workspace for cloud connections
$scanResult = Invoke-PowerBIWorkspaceScan -AccessToken $token `
                                           -WorkspaceIds @($workspaceId) `
                                           -IncludeLineage `
                                           -IncludeDatasourceDetails `
                                           -IncludeDatasetSchema `
                                           -IncludeDatasetExpressions

# Step 3: Extract and display cloud connections
$connections = Get-CloudConnectionsFromScan -ScanResult $scanResult -OutputPath "cloud_connections.csv"

# Step 4: Save full scan result
$scanResult | ConvertTo-Json -Depth 10 | Out-File "scan_result.json"
Write-Host "✅ Full scan result saved to: scan_result.json" -ForegroundColor Green

# Display summary
Write-Host "`n📊 SUMMARY:" -ForegroundColor Cyan
Write-Host "  Datasource instances: $($scanResult.datasourceInstances.Count)" -ForegroundColor Green
Write-Host "  Dataflows: $($scanResult.dataflows.Count)" -ForegroundColor Green
Write-Host "  Datasets: $($scanResult.datasets.Count)" -ForegroundColor Green
Write-Host "  Workspaces scanned: $($scanResult.workspaces.Count)`n" -ForegroundColor Green


# ============================================================
# BONUS: Scan ALL Workspaces
# ============================================================

function Invoke-PowerBITenantScan {
    param(
        [Parameter(Mandatory=$true)]
        [string]$TenantId,
        
        [Parameter(Mandatory=$true)]
        [string]$ClientId,
        
        [Parameter(Mandatory=$true)]
        [string]$ClientSecret,
        
        [int]$BatchSize = 100,
        [string]$OutputFolder = "."
    )
    
    # Get token
    $token = Get-PowerBIAdminToken -TenantId $TenantId -ClientId $ClientId -ClientSecret $ClientSecret
    
    # Get all workspaces
    Write-Host "📋 Getting all workspaces..." -ForegroundColor Cyan
    $allWorkspaces = Get-PowerBIAdminWorkspaces -AccessToken $token
    $workspaceIds = $allWorkspaces | ForEach-Object { $_.id }
    
    Write-Host "✅ Found $($workspaceIds.Count) workspaces`n" -ForegroundColor Green
    
    # Process in batches
    $allConnections = @()
    $batchNum = 1
    
    for ($i = 0; $i -lt $workspaceIds.Count; $i += $BatchSize) {
        $batch = $workspaceIds[$i..[Math]::Min($i + $BatchSize - 1, $workspaceIds.Count - 1)]
        
        Write-Host "🔄 Processing batch $batchNum : $($batch.Count) workspaces" -ForegroundColor Yellow
        
        try {
            $scanResult = Invoke-PowerBIWorkspaceScan -AccessToken $token `
                                                       -WorkspaceIds $batch `
                                                       -IncludeDatasourceDetails `
                                                       -MaxWaitSeconds 900
            
            # Save batch
            $batchPath = Join-Path $OutputFolder "scan_batch_$batchNum.json"
            $scanResult | ConvertTo-Json -Depth 10 | Out-File $batchPath
            
            # Extract connections
            $connections = Get-CloudConnectionsFromScan -ScanResult $scanResult
            $allConnections += $connections
            
            Write-Host "  ✅ Batch $batchNum completed`n" -ForegroundColor Green
            
            # Rate limiting
            if ($i + $BatchSize -lt $workspaceIds.Count) {
                Write-Host "  ⏳ Waiting 60 seconds..." -ForegroundColor Gray
                Start-Sleep -Seconds 60
            }
        }
        catch {
            Write-Host "  ❌ Error: $_" -ForegroundColor Red
        }
        
        $batchNum++
    }
    
    # Save consolidated connections
    $allConnections | Export-Csv -Path (Join-Path $OutputFolder "all_cloud_connections.csv") -NoTypeInformation
    
    Write-Host "`n✅ COMPLETE!" -ForegroundColor Green
    Write-Host "Total connections found: $($allConnections.Count)" -ForegroundColor Cyan
    
    return $allConnections
}

# Example: Scan entire tenant
# $allConnections = Invoke-PowerBITenantScan -TenantId $tenantId -ClientId $clientId -ClientSecret $clientSecret
