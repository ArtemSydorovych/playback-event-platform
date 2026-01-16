#!/bin/bash
# Deploy Flink Jobs Script (Linux/Mac)
# Usage: ./scripts/deploy-flink-jobs.sh [--start]

set -e

FLINK_URL="${FLINK_URL:-http://localhost:8083}"
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
START_JOBS=false

# Parse args
for arg in "$@"; do
    case $arg in
        --start|-s) START_JOBS=true ;;
        --help|-h)
            echo "Usage: $0 [--start]"
            echo "  --start, -s   Start jobs after uploading"
            exit 0
            ;;
    esac
done

echo "=== Flink Jobs Deployment ==="
echo "Flink REST API: $FLINK_URL"
echo ""

# Step 1: Build
echo "=== Step 1: Building Flink Jobs ==="
cd "$PROJECT_ROOT"
./gradlew :flink-jobs:shadowJar --quiet
echo "Build completed!"

# Step 2: Upload
echo ""
echo "=== Step 2: Uploading JARs to Flink ==="

JAR_PATH="$PROJECT_ROOT/flink-jobs/build/libs/playback-flink-jobs-all.jar"

if [ ! -f "$JAR_PATH" ]; then
    echo "ERROR: JAR not found at $JAR_PATH"
    exit 1
fi

echo "Uploading: $(basename $JAR_PATH)..."

RESPONSE=$(curl -s -X POST -H "Expect:" \
    -F "jarfile=@$JAR_PATH" \
    "$FLINK_URL/jars/upload" 2>&1)

if echo "$RESPONSE" | grep -q "error"; then
    echo "Failed to upload JAR!"
    echo "Response: $RESPONSE"
    echo ""
    echo "Is Flink running? Check: $FLINK_URL"
    exit 1
fi

JAR_ID=$(echo "$RESPONSE" | grep -o '"filename":"[^"]*"' | cut -d'"' -f4 | xargs basename)
echo "  Uploaded: $JAR_ID"

# Step 3: Start if requested
if [ "$START_JOBS" = true ]; then
    echo ""
    echo "=== Step 3: Starting Job ==="

    RUN_RESPONSE=$(curl -s -X POST "$FLINK_URL/jars/$JAR_ID/run" \
        -H "Content-Type: application/json" \
        -d '{"entryClass": "com.artemsydorovych.playback.flink.PlaybackPipelineJob"}')

    JOB_ID=$(echo "$RUN_RESPONSE" | grep -o '"jobid":"[^"]*"' | cut -d'"' -f4)

    if [ -n "$JOB_ID" ]; then
        echo "  Started! Job ID: $JOB_ID"
    else
        echo "  Failed to start job"
        echo "  Response: $RUN_RESPONSE"
    fi
fi

echo ""
echo "=== Deployment Complete ==="
echo ""
echo "Uploaded JAR: $JAR_ID"
echo ""
echo "Next steps:"
echo "  1. Open Flink UI: $FLINK_URL"
echo "  2. Go to 'Submit New Job'"
echo "  3. Select your JAR and click 'Submit'"
echo ""
echo "Or run with --start flag to auto-start:"
echo "  ./scripts/deploy-flink-jobs.sh --start"
