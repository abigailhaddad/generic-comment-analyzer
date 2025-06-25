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

3. **Set up for your regulation:**
   ```bash
   # Step 1: Detect CSV column structure
   python detect_columns.py

   # Step 2: Discover stances and themes from sample comments
   python discover_stances.py --sample 250

   # Step 3: Test with small sample
   python pipeline.py --csv comments.csv --sample 10
   ```

4. **Run full analysis:**
   ```bash
   # Full run with all options
   ./run_pipeline.sh --csv comments.csv --truncate 5000 --to-database
   ```

## How It Works

The system automatically discovers and adapts to your regulation:

1. **Column Detection** (`detect_columns.py`): Uses LLM to automatically map CSV columns to required fields (comment text, ID, date, submitter, attachments)

2. **Stance Discovery** (`discover_stances.py`): Analyzes sample comments to discover:
   - The main arguments/positions people are taking (not just pro/con)
   - Specific indicators that signal each stance
   - Recurring themes across comments
   - Generates complete analysis configuration

3. **Comment Analysis** (`pipeline.py`): For each comment, identifies:
   - **Stances**: Multiple arguments/positions (e.g., "board shouldn't be fired", "process lacks transparency")
   - **Themes**: Topics covered (e.g., "scientific integrity", "vaccine safety")
   - **Key Quote**: Most important excerpt
   - **Rationale**: Why those stances were selected

4. **Results**: Saves to JSON, optional PostgreSQL database, and automatically generates interactive HTML report

## For Different Regulations

The system is fully generic! Just:

1. Put your `comments.csv` file in the directory
2. Run the 3 setup commands above
3. The system discovers everything automatically

No manual configuration needed - it learns your regulation's specific arguments and themes.

## Files

- **`detect_columns.py`** - Auto-detects CSV column structure → `column_mapping.json`
- **`discover_stances.py`** - Discovers stances and themes → `analyzer_config.json`  
- **`pipeline.py`** - Main processing script
- **`comment_analyzer.py`** - Configurable LLM analyzer (uses `analyzer_config.json`)
- **`generate_report.py`** - HTML report generator (runs automatically)
- **`run_pipeline.sh`** - Convenience script with sleep prevention
- **`schema.sql`** - PostgreSQL database schema

## Options

```bash
./run_pipeline.sh --csv comments.csv [options]

Options:
  --sample N          Process only N random comments (for testing)
  --truncate N        Truncate comment text to N characters for LLM analysis (saves costs)
  --to-database       Store results in PostgreSQL 
  --model MODEL       Use different LLM model (default: gpt-4o-mini)
  --output FILE       JSON output file (default: analyzed_comments.json)
```

### HTML Report

The pipeline automatically generates an interactive HTML report (`index.html`) with:
- Summary statistics and distribution charts for all discovered stances/themes
- Checkbox filtering for stances, themes, and attachments  
- Text search for IDs, dates, quotes, and content
- Adaptive interface that adjusts to your specific regulation's fields
- Clean, simple design for easy analysis

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
- **Automatic HTML reports** - Interactive filtering and statistics
- **Generic design** - Easy to adapt for any regulation

## License

MIT