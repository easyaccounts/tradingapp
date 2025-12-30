# Save this as diagnose-525.ps1 and run when you see the error:

$timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
Write-Host "=== Diagnostic Report at $timestamp ===" -ForegroundColor Green

# Test 1: Direct server access (bypassing Cloudflare)
Write-Host "
1. Testing direct server access (bypassing Cloudflare)..." -ForegroundColor Yellow
try {
    # PowerShell 5.1 compatible certificate validation bypass
    add-type @"
        using System.Net;
        using System.Security.Cryptography.X509Certificates;
        public class TrustAllCertsPolicy : ICertificatePolicy {
            public bool CheckValidationResult(
                ServicePoint srvPoint, X509Certificate certificate,
                WebRequest request, int certificateProblem) {
                return true;
            }
        }
"@
    [System.Net.ServicePointManager]::CertificatePolicy = New-Object TrustAllCertsPolicy
    [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12
    
    $directTest = Invoke-WebRequest -Uri "https://82.180.144.255/health" -Headers @{"Host"="zopilot.in"} -TimeoutSec 5 -UseBasicParsing
    Write-Host " Server responding: HTTP $($directTest.StatusCode)" -ForegroundColor Green
    Write-Host " Content: $($directTest.Content.Substring(0, [Math]::Min(100, $directTest.Content.Length)))" -ForegroundColor Gray
} catch {
    Write-Host " Server not responding: $($_.Exception.Message)" -ForegroundColor Red
}

# Test 2: Through Cloudflare (using GET method)
Write-Host "
2. Testing through Cloudflare..." -ForegroundColor Yellow
$cfOutput = curl.exe -s -w "\nHTTP_CODE:%{http_code}" https://zopilot.in/health 2>&1
$httpCode = ($cfOutput | Select-String "HTTP_CODE:(\d+)").Matches.Groups[1].Value
$cfRay = curl.exe -I https://zopilot.in/ 2>&1 | Select-String "CF-RAY"
if ($httpCode -eq "200") {
    Write-Host " Cloudflare connecting successfully: HTTP $httpCode" -ForegroundColor Green
    Write-Host " $cfRay" -ForegroundColor Gray
} else {
    Write-Host " ERROR: HTTP $httpCode (expecting 200)" -ForegroundColor Red
    Write-Host " $cfRay" -ForegroundColor Gray
}

# Test 3: Check server load via SSH
Write-Host "
3. Checking server resources..." -ForegroundColor Yellow
ssh root@82.180.144.255 "uptime && free -h | grep Mem"

Write-Host "
=== End Report ===" -ForegroundColor Green
