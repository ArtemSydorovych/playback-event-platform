# Deploy Flink Jobs Script (Windows PowerShell 5.1+)
# Usage: .\scripts\deploy-flink-jobs.ps1
#
# This script:
# 1. Builds all Flink job JARs
# 2. Uploads them to Flink cluster
# 3. Optionally starts the jobs

param(
    [string]$FlinkUrl = "http://localhost:8083",
    [switch]$BuildOnly,
    [switch]$StartJobs
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot

# Function to upload file using multipart/form-data (PS 5.1 compatible)
function Upload-FlinkJar {
    param([string]$Url, [string]$FilePath)

    $fileName = Split-Path $FilePath -Leaf
    $fileBytes = [System.IO.File]::ReadAllBytes($FilePath)
    $boundary = [System.Guid]::NewGuid().ToString()

    $bodyLines = @(
        "--$boundary",
        "Content-Disposition: form-data; name=`"jarfile`"; filename=`"$fileName`"",
        "Content-Type: application/java-archive",
        "",
        [System.Text.Encoding]::GetEncoding("iso-8859-1").GetString($fileBytes),
        "--$boundary--"
    )
    $body = $bodyLines -join "`r`n"

    $response = Invoke-RestMethod -Uri $Url -Method Post `
        -ContentType "multipart/form-data; boundary=$boundary" `
        -Body $body

    return $response
}

Write-Host "=== Flink Jobs Deployment ===" -ForegroundColor Cyan
Write-Host "Flink REST API: $FlinkUrl"
Write-Host ""

# Step 1: Build all jobs
Write-Host "=== Step 1: Building Flink Jobs ===" -ForegroundColor Yellow
Push-Location $ProjectRoot
try {
    & ./gradlew.bat :flink-jobs:shadowJar --quiet
    if ($LASTEXITCODE -ne 0) { throw "Gradle build failed" }
} finally {
    Pop-Location
}
Write-Host "Build completed!" -ForegroundColor Green

if ($BuildOnly) {
    Write-Host ""
    Write-Host "JAR location: $ProjectRoot\flink-jobs\build\libs\playback-flink-jobs-all.jar"
    exit 0
}

# Step 2: Find all job JARs
$JobJars = Get-ChildItem -Path "$ProjectRoot\flink-jobs\build\libs" -Filter "*-all.jar"

if ($JobJars.Count -eq 0) {
    Write-Host "No job JARs found!" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "=== Step 2: Uploading JARs to Flink ===" -ForegroundColor Yellow

$UploadedJars = @()

foreach ($Jar in $JobJars) {
    Write-Host "Uploading: $($Jar.Name)..."

    try {
        # Use curl if available (more reliable for large files), otherwise use PowerShell
        $curlPath = Get-Command curl.exe -ErrorAction SilentlyContinue

        if ($curlPath) {
            $response = curl.exe -s -X POST -H "Expect:" `
                -F "jarfile=@$($Jar.FullName)" `
                "$FlinkUrl/jars/upload" | ConvertFrom-Json
        } else {
            $response = Upload-FlinkJar -Url "$FlinkUrl/jars/upload" -FilePath $Jar.FullName
        }

        $JarId = ($response.filename -split "/")[-1]
        $UploadedJars += @{
            Name = $Jar.Name
            JarId = $JarId
            EntryClass = "com.artemsydorovych.playback.flink.PlaybackPipelineJob"
        }
        Write-Host "  Uploaded: $JarId" -ForegroundColor Green
    }
    catch {
        Write-Host "  Failed to upload: $_" -ForegroundColor Red
        Write-Host "  Is Flink running? Check: $FlinkUrl" -ForegroundColor Yellow
        exit 1
    }
}

# Step 3: Start jobs if requested
if ($StartJobs) {
    Write-Host ""
    Write-Host "=== Step 3: Starting Jobs ===" -ForegroundColor Yellow

    foreach ($Job in $UploadedJars) {
        Write-Host "Starting: $($Job.Name)..."

        $Body = @{ entryClass = $Job.EntryClass } | ConvertTo-Json

        try {
            $Response = Invoke-RestMethod -Uri "$FlinkUrl/jars/$($Job.JarId)/run" `
                -Method Post `
                -ContentType "application/json" `
                -Body $Body `
                -ErrorAction Stop

            Write-Host "  Started! Job ID: $($Response.jobid)" -ForegroundColor Green
        }
        catch {
            Write-Host "  Failed to start: $_" -ForegroundColor Red
        }
    }
}

Write-Host ""
Write-Host "=== Deployment Complete ===" -ForegroundColor Cyan
Write-Host ""
Write-Host "Uploaded JARs:" -ForegroundColor White
foreach ($Job in $UploadedJars) {
    Write-Host "  - $($Job.JarId)"
}
Write-Host ""
Write-Host "Next steps:" -ForegroundColor White
Write-Host "  1. Open Flink UI: $FlinkUrl"
Write-Host "  2. Go to 'Submit New Job'"
Write-Host "  3. Select your JAR and click 'Submit'"
Write-Host ""
Write-Host "Or run with -StartJobs flag to auto-start:"
Write-Host "  .\scripts\deploy-flink-jobs.ps1 -StartJobs"
