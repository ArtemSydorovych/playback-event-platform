# Create Savepoint for Flink Job
#
# Usage:
#   .\scripts\create-savepoint.ps1 -JobName raw-events
#   .\scripts\create-savepoint.ps1 -JobName content-metrics -SavepointDir s3://custom-bucket/
#
# Prerequisites:
#   - Docker must be running
#   - Flink job must be running

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("raw-events", "content-metrics", "session-detection")]
    [string]$JobName,

    [Parameter(Mandatory=$false)]
    [string]$SavepointDir = "s3://flink-savepoints"
)

$ErrorActionPreference = "Stop"

Write-Host "========================================"
Write-Host "Creating savepoint for: $JobName"
Write-Host "========================================"

$containerName = "playback-$JobName-jm"

# Check if container is running
$containerStatus = docker ps --filter "name=$containerName" --format "{{.Status}}"
if (-not $containerStatus) {
    Write-Error "Container $containerName is not running!"
    exit 1
}

Write-Host "Container $containerName is running: $containerStatus"

# Get job ID from Flink REST API
Write-Host "Getting job ID..."
$jobsResponse = docker exec $containerName curl -s http://localhost:8081/jobs/overview
$jobsJson = $jobsResponse | ConvertFrom-Json

if ($jobsJson.jobs.Count -eq 0) {
    Write-Error "No jobs found running in $containerName"
    exit 1
}

$jobId = $jobsJson.jobs[0].jid
$jobName = $jobsJson.jobs[0].name
Write-Host "Found job: $jobName (ID: $jobId)"

# Create savepoint
$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$savepointPath = "$SavepointDir/$JobName/savepoint-$timestamp"

Write-Host "Creating savepoint at: $savepointPath"

$savepointResponse = docker exec $containerName `
    curl -s -X POST `
    -H "Content-Type: application/json" `
    -d "{`"target-directory`":`"$savepointPath`",`"cancel-job`":false}" `
    "http://localhost:8081/jobs/$jobId/savepoints"

$savepointJson = $savepointResponse | ConvertFrom-Json
$requestId = $savepointJson."request-id"

if (-not $requestId) {
    Write-Error "Failed to trigger savepoint: $savepointResponse"
    exit 1
}

Write-Host "Savepoint triggered. Request ID: $requestId"

# Wait for savepoint to complete
Write-Host "Waiting for savepoint to complete..."
$maxAttempts = 60
$attempt = 0

do {
    Start-Sleep -Seconds 2
    $attempt++

    $statusResponse = docker exec $containerName `
        curl -s "http://localhost:8081/jobs/$jobId/savepoints/$requestId"
    $statusJson = $statusResponse | ConvertFrom-Json

    $status = $statusJson.status.id

    if ($status -eq "COMPLETED") {
        $location = $statusJson.operation.location
        Write-Host ""
        Write-Host "========================================"
        Write-Host "Savepoint created successfully!"
        Write-Host "Location: $location"
        Write-Host "========================================"
        exit 0
    }
    elseif ($status -eq "FAILED") {
        $failure = $statusJson.operation."failure-cause"
        Write-Error "Savepoint failed: $failure"
        exit 1
    }

    Write-Host "Status: $status (attempt $attempt/$maxAttempts)"
} while ($attempt -lt $maxAttempts)

Write-Error "Savepoint timed out after $maxAttempts attempts"
exit 1
