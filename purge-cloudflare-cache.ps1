# Cloudflare Cache Purge Script
# You need your Cloudflare API token and Zone ID

# Configuration - UPDATE THESE
$ZONE_ID = "YOUR_ZONE_ID_HERE"  # Found in Cloudflare dashboard under domain overview
$API_TOKEN = "YOUR_API_TOKEN_HERE"  # Create at https://dash.cloudflare.com/profile/api-tokens

# Purge all cache
$headers = @{
    "Authorization" = "Bearer $API_TOKEN"
    "Content-Type" = "application/json"
}

$body = @{
    "purge_everything" = $true
} | ConvertTo-Json

try {
    Write-Host "Purging Cloudflare cache for zone: $ZONE_ID" -ForegroundColor Yellow
    
    $response = Invoke-RestMethod -Uri "https://api.cloudflare.com/client/v4/zones/$ZONE_ID/purge_cache" `
        -Method Post `
        -Headers $headers `
        -Body $body
    
    if ($response.success) {
        Write-Host "✓ Cache purged successfully!" -ForegroundColor Green
        Write-Host "Wait 30 seconds and try accessing your site again." -ForegroundColor Cyan
    } else {
        Write-Host "✗ Failed to purge cache:" -ForegroundColor Red
        Write-Host ($response.errors | ConvertTo-Json) -ForegroundColor Red
    }
} catch {
    Write-Host "✗ Error: $_" -ForegroundColor Red
    Write-Host "Make sure your API token has 'Cache Purge' permission" -ForegroundColor Yellow
}
