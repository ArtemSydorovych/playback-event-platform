# Restore Flink Job from Savepoint
#
# Usage:
#   .\scripts\restore-from-savepoint.ps1 -JobName raw-events -SavepointPath s3://flink-savepoints/raw-events/savepoint-xxx
#
# Prerequisites:
#   - Docker must be running
#   - Flink job containers must be running (will restart with savepoint)

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("raw-events", "content-metrics", "session-detection")]
    [string]$JobName,

    [Parameter(Mandatory=$true)]
    [string]$SavepointPath
)

$ErrorActionPreference = "Stop"

Write-Host "========================================"
Write-Host "Restoring $JobName from savepoint"
Write-Host "Savepoint: $SavepointPath"
Write-Host "========================================"

$jobManagerContainer = "playback-$JobName-jm"
$taskManagerContainer = "playback-$JobName-tm"

# Map job name to class name
$classMap = @{
    "raw-events" = "com.artemsydorovych.playback.flink.job.RawEventsJob"
    "content-metrics" = "com.artemsydorovych.playback.flink.job.ContentMetricsJob"
    "session-detection" = "com.artemsydorovych.playback.flink.job.SessionDetectionJob"
}
$jobClass = $classMap[$JobName]

Write-Host ""
Write-Host "Step 1: Stopping current job..."

# Check if job is running and cancel it
$containerStatus = docker ps --filter "name=$jobManagerContainer" --format "{{.Status}}"
if ($containerStatus) {
    Write-Host "Cancelling current job..."

    $jobsResponse = docker exec $jobManagerContainer curl -s http://localhost:8081/jobs/overview 2>$null
    if ($jobsResponse) {
        $jobsJson = $jobsResponse | ConvertFrom-Json
        if ($jobsJson.jobs.Count -gt 0) {
            $jobId = $jobsJson.jobs[0].jid
            Write-Host "Cancelling job: $jobId"

            docker exec $jobManagerContainer `
                curl -s -X PATCH "http://localhost:8081/jobs/$jobId`?mode=cancel" | Out-Null

            Start-Sleep -Seconds 5
        }
    }
}

Write-Host ""
Write-Host "Step 2: Restarting containers with savepoint..."

# Restart containers
Write-Host "Stopping containers..."
docker stop $jobManagerContainer $taskManagerContainer 2>$null | Out-Null
docker rm $jobManagerContainer $taskManagerContainer 2>$null | Out-Null

Write-Host "Starting TaskManager..."
docker-compose -f docker/docker-compose.yml -f docker/flink/docker-compose.production.yml up -d "$JobName-taskmanager"
Start-Sleep -Seconds 10

Write-Host "Starting JobManager with savepoint..."
# The job will automatically restore from savepoint if FLINK_SAVEPOINT env var is set
# For manual restore, we need to override the command

$originalCommand = "standalone-job --job-classname $jobClass"
$savepointCommand = "standalone-job --job-classname $jobClass -s $SavepointPath --allowNonRestoredState"

Write-Host "Command: $savepointCommand"

# Start with savepoint
docker-compose -f docker/docker-compose.yml -f docker/flink/docker-compose.production.yml run -d `
    --name $jobManagerContainer `
    --entrypoint "/docker-entrypoint.sh" `
    "$JobName-jobmanager" $savepointCommand

Write-Host ""
Write-Host "Step 3: Waiting for job to start..."
$maxAttempts = 30
$attempt = 0

do {
    Start-Sleep -Seconds 5
    $attempt++

    $jobsResponse = docker exec $jobManagerContainer curl -s http://localhost:8081/jobs/overview 2>$null
    if ($jobsResponse) {
        $jobsJson = $jobsResponse | ConvertFrom-Json
        if ($jobsJson.jobs.Count -gt 0) {
            $jobStatus = $jobsJson.jobs[0].state
            Write-Host "Job status: $jobStatus"

            if ($jobStatus -eq "RUNNING") {
                Write-Host ""
                Write-Host "========================================"
                Write-Host "Job restored successfully!"
                Write-Host "Job ID: $($jobsJson.jobs[0].jid)"
                Write-Host "State: $jobStatus"
                Write-Host "========================================"
                exit 0
            }
        }
    }

    Write-Host "Waiting... (attempt $attempt/$maxAttempts)"
} while ($attempt -lt $maxAttempts)

Write-Error "Job failed to start after $maxAttempts attempts. Check logs: docker logs $jobManagerContainer"
exit 1
