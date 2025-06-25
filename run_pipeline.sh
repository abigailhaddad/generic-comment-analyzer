#!/bin/bash
# Simple script to run the comment analysis pipeline with caffeinate

set -e  # Exit on any error

# Default values
CSV_FILE=""
SAMPLE=""
MODEL="gpt-4o-mini"
TRUNCATE=""
TO_DATABASE=false
OUTPUT="analyzed_comments.parquet"
WORKERS="8"
BATCH_SIZE="50"
NO_PARALLEL=false

# Function to show usage
usage() {
    echo "Usage: $0 --csv <file> [options]"
    echo ""
    echo "Required:"
    echo "  --csv <file>              Path to comments CSV file"
    echo ""
    echo "Options:"
    echo "  --sample <N>              Process only N random comments (default: all)"
    echo "  --model <model>           LLM model to use (default: gpt-4o-mini)"
    echo "  --truncate <N>            Truncate comment text to N characters for LLM analysis"
    echo "  --output <file>           Output Parquet file (default: analyzed_comments.parquet)"
    echo "  --to-database             Store results in PostgreSQL database"
    echo "  --workers <N>             Number of parallel workers (default: 8)"
    echo "  --batch-size <N>          Batch size for parallel processing (default: 50)"
    echo "  --no-parallel             Disable parallel processing (slower but more stable)"
    echo "  --help                    Show this help"
    echo ""
    echo "Examples:"
    echo "  $0 --csv comments.csv --sample 100"
    echo "  $0 --csv comments.csv --truncate 5000 --to-database"
    echo "  $0 --csv comments.csv --workers 16 --batch-size 100"
    echo "  $0 --csv comments.csv --no-parallel  # Use for debugging"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --csv)
            CSV_FILE="$2"
            shift 2
            ;;
        --sample)
            SAMPLE="$2"
            shift 2
            ;;
        --model)
            MODEL="$2"
            shift 2
            ;;
        --truncate)
            TRUNCATE="$2"
            shift 2
            ;;
        --output)
            OUTPUT="$2"
            shift 2
            ;;
        --to-database)
            TO_DATABASE=true
            shift
            ;;
        --workers)
            WORKERS="$2"
            shift 2
            ;;
        --batch-size)
            BATCH_SIZE="$2"
            shift 2
            ;;
        --no-parallel)
            NO_PARALLEL=true
            shift
            ;;
        --help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Check required arguments
if [[ -z "$CSV_FILE" ]]; then
    echo "Error: --csv is required"
    usage
    exit 1
fi

if [[ ! -f "$CSV_FILE" ]]; then
    echo "Error: CSV file '$CSV_FILE' not found"
    exit 1
fi

# Build pipeline arguments
PIPELINE_ARGS="--csv $CSV_FILE --model $MODEL --output $OUTPUT"

if [[ -n "$SAMPLE" ]]; then
    PIPELINE_ARGS="$PIPELINE_ARGS --sample $SAMPLE"
fi

if [[ -n "$TRUNCATE" ]]; then
    PIPELINE_ARGS="$PIPELINE_ARGS --truncate $TRUNCATE"
fi

if [[ "$TO_DATABASE" == true ]]; then
    PIPELINE_ARGS="$PIPELINE_ARGS --to-database"
fi

if [[ "$NO_PARALLEL" == true ]]; then
    PIPELINE_ARGS="$PIPELINE_ARGS --no-parallel"
else
    PIPELINE_ARGS="$PIPELINE_ARGS --workers $WORKERS --batch-size $BATCH_SIZE"
fi

# Show what we're about to run
echo "🚀 Starting comment analysis pipeline..."
echo "📁 CSV file: $CSV_FILE"
if [[ -n "$SAMPLE" ]]; then
    echo "🎯 Sample size: $SAMPLE comments"
else
    echo "🎯 Processing: ALL comments"
fi
echo "🤖 Model: $MODEL"
if [[ -n "$TRUNCATE" ]]; then
    echo "✂️  Truncate: $TRUNCATE characters"
else
    echo "✂️  Truncate: Disabled"
fi
echo "💾 Output: $OUTPUT"
if [[ "$TO_DATABASE" == true ]]; then
    echo "🗄️  Database: PostgreSQL (enabled)"
else
    echo "🗄️  Database: Disabled (use --to-database to enable)"
fi
if [[ "$NO_PARALLEL" == true ]]; then
    echo "🔧 Processing: Sequential (parallel disabled)"
else
    echo "🔧 Processing: Parallel ($WORKERS workers, batch size $BATCH_SIZE)"
fi

echo ""
echo "💡 This will prevent your computer from sleeping during processing"
echo "💡 Press Ctrl+C to stop at any time"
echo ""

# Skip confirmation if database is enabled (Python script will handle it)
if [[ "$TO_DATABASE" != true ]]; then
    read -p "Continue? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Cancelled."
        exit 0
    fi
else
    echo "🗄️  Database storage enabled - pipeline will check database status first"
fi

# Get start time
START_TIME=$(date +%s)

echo ""
echo "⏰ Starting at $(date)"
echo "🔋 Preventing system sleep with caffeinate..."
echo ""

# Run with caffeinate to prevent sleep
if command -v caffeinate &> /dev/null; then
    caffeinate -i python pipeline.py $PIPELINE_ARGS
    EXIT_CODE=$?
else
    echo "⚠️  caffeinate not found (not on macOS?), running without sleep prevention"
    python pipeline.py $PIPELINE_ARGS
    EXIT_CODE=$?
fi

# Calculate runtime
END_TIME=$(date +%s)
RUNTIME=$((END_TIME - START_TIME))
HOURS=$((RUNTIME / 3600))
MINUTES=$(((RUNTIME % 3600) / 60))
SECONDS=$((RUNTIME % 60))

echo ""
echo "⏰ Completed at $(date)"
echo "⌛ Total runtime: ${HOURS}h ${MINUTES}m ${SECONDS}s"

if [[ $EXIT_CODE -eq 0 ]]; then
    echo "✅ Pipeline completed successfully!"
    if [[ "$TO_DATABASE" == true ]]; then
        echo "📊 Data stored in PostgreSQL database"
    fi
    echo "📁 Results saved to: $OUTPUT"
else
    echo "❌ Pipeline failed with exit code: $EXIT_CODE"
    exit $EXIT_CODE
fi