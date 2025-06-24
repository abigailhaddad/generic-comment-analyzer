#!/bin/bash
# Simple script to run the comment analysis pipeline with caffeinate

set -e  # Exit on any error

# Default values
CSV_FILE=""
SAMPLE=""
MODEL="gpt-4o-mini"
TO_DATABASE=false
OUTPUT="analyzed_comments.json"

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
    echo "  --output <file>           Output JSON file (default: analyzed_comments.json)"
    echo "  --to-database             Store results in PostgreSQL database"
    echo "  --help                    Show this help"
    echo ""
    echo "Examples:"
    echo "  $0 --csv comments.csv --sample 100"
    echo "  $0 --csv comments.csv --to-database"
    echo "  $0 --csv comments.csv --sample 50 --to-database"
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
        --output)
            OUTPUT="$2"
            shift 2
            ;;
        --to-database)
            TO_DATABASE=true
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

if [[ "$TO_DATABASE" == true ]]; then
    PIPELINE_ARGS="$PIPELINE_ARGS --to-database"
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
echo "💾 Output: $OUTPUT"
if [[ "$TO_DATABASE" == true ]]; then
    echo "🗄️  Database: PostgreSQL (enabled)"
else
    echo "🗄️  Database: Disabled (use --to-database to enable)"
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