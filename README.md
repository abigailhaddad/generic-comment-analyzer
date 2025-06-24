# Generic Regulation Comment Analysis

A simple, flexible system for analyzing public comments on federal regulations using LLMs.

## Quick Start

1. **Set up environment:**
   ```bash
   python3 -m venv myenv
   source myenv/bin/activate
   pip install -r requirements.txt
   ```

2. **Add your API keys to `.env`:**
   ```bash
   OPENAI_API_KEY=your_key_here
   GEMINI_API_KEY=your_key_here  # Optional, for image/PDF processing
   DATABASE_URL=postgresql://...  # Optional, for database storage
   ```

3. **Run the pipeline:**
   ```bash
   # Test with sample
   ./run_pipeline.sh --csv comments.csv --sample 100

   # Full run with database storage
   ./run_pipeline.sh --csv comments.csv --to-database
   ```

## What It Does

The pipeline processes regulation comments through these steps:

1. **Loads comments** from CSV file
2. **Downloads attachments** (PDFs, DOCX, images) and extracts text
3. **Analyzes each comment** individually with LLM for stance and themes
4. **Saves results** to JSON file and/or PostgreSQL database

## For Different Regulations

To analyze a different regulation:

1. **Replace your data:** Put your comments CSV file in the root directory
2. **Update the analyzer:** Edit `comment_analyzer.py` to customize:
   - Stance options (e.g., "Support", "Oppose", "Neutral")
   - Theme categories relevant to your regulation
   - System prompt with regulation-specific context

That's it! The pipeline handles everything else automatically.

## Files

- **`pipeline.py`** - Main processing script
- **`comment_analyzer.py`** - Regulation-specific LLM configuration
- **`run_pipeline.sh`** - Convenience script with sleep prevention
- **`schema.sql`** - PostgreSQL database schema
- **`requirements.txt`** - Python dependencies

## Options

```bash
./run_pipeline.sh --csv comments.csv [options]

Options:
  --sample N          Process only N random comments (for testing)
  --to-database       Store results in PostgreSQL 
  --model MODEL       Use different LLM model (default: gpt-4o-mini)
  --output FILE       JSON output file (default: analyzed_comments.json)
```

## Database Setup

If using PostgreSQL storage:

1. Create your database and run: `psql -f schema.sql`
2. Add `DATABASE_URL` to your `.env` file
3. Use `--to-database` flag when running

## Features

- **Attachment processing** - Handles PDFs (PyPDF2), DOCX files, and images (Gemini OCR)
- **No deduplication** - Each comment processed individually (accepts duplicates)
- **Sampling** - Test on subsets before full runs
- **Sleep prevention** - Uses `caffeinate` on macOS for long runs
- **Database integration** - Optional PostgreSQL storage
- **Generic design** - Easy to adapt for any regulation

## License

MIT