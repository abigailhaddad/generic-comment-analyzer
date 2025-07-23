# Generic Regulation Comment Analysis

Analyzes public comments on federal regulations using LLMs to identify stances and key quotes.

## Data Source

Works with comment data from [regulations.gov bulk download](https://www.regulations.gov/bulkdownload):

1. Request bulk data for your regulation docket from regulations.gov
2. You'll receive an email with download links when ready
3. Download and rename the comments file to `comments.csv` 
4. Place `comments.csv` in the root of this repository

**Note**: The pipeline does not resume - it processes all comments from scratch each time. Use `--sample` to test before running the full dataset.

## Quick Start

1. Set up environment:
   ```bash
   python3 -m venv myenv
   source myenv/bin/activate
   pip install -r requirements.txt
   ```

2. Add your API keys to `.env`:
   ```bash
   OPENAI_API_KEY=your_key_here
   GEMINI_API_KEY=your_key_here  # for image/PDF processing
   DATABASE_URL=postgresql://...  # Optional, for database storage
   ```

3. Configure for your regulation:
   ```bash
   python detect_columns.py                        # Detect CSV structure
   python discover_stances.py --sample 250         # Find stances
   python pipeline.py --csv comments.csv --sample 10  # Test run
   ```

4. Run full analysis:
   ```bash
   ./run_pipeline.sh --csv comments.csv --workers 8 --batch-size 100
   ```

## Web Interface

A web interface is available for discovering stances and running the pipeline:

1. First run `python detect_columns.py` to detect CSV structure
2. Launch the web interface:
   ```bash
   cd frontend
   python app.py
   ```
3. Open http://localhost:5000 in your browser to:
   - Discover stances from sample comments
   - Edit and refine discovered themes
   - Run the analysis pipeline with visual progress tracking
   - View the generated report

## How It Works

1. **Column Detection** (`detect_columns.py`): Maps CSV columns to required fields (comment text, ID, date, submitter, attachments) and extracts regulation metadata for report titles.

2. **Stance Discovery** (`discover_stances.py`): Analyzes sample comments to find:
   - Main arguments/positions people are taking
   - Updates `comment_analyzer.py` with enum constraints to prevent invalid values

3. **Comment Analysis** (`pipeline.py`): For each comment, identifies:
   - Stances: Arguments/positions expressed
   - Key Quote: Most important excerpt
   - Rationale: Why those stances were selected

4. **Output**: Saves to Parquet format and generates interactive HTML report

## Manual Customization (Advanced)

If you prefer to write your own analysis prompt instead of using automatic discovery, you need to manually edit **`comment_analyzer.py`**:

### 1. Edit the Enums in `comment_analyzer.py`

Open `comment_analyzer.py` and replace the auto-generated enums with your custom values:

```python
class Stance(str, Enum):
    SUPPORT_POLICY = "Support for Policy"
    OPPOSE_POLICY = "Opposition to Policy" 
    CONCERNS_PROCESS = "Concerns About Process"
    # Add your custom stances here

```

### 2. Edit the `create_regulation_analyzer` Function in `comment_analyzer.py`

Still in `comment_analyzer.py`, find the `create_regulation_analyzer` function and replace the auto-generated lists and prompt:

```python
def create_regulation_analyzer(model=None, timeout_seconds=None):
    # Hard-code your stance options
    stance_options = [
        "Support for Policy",
        "Opposition to Policy", 
        "Concerns About Process"
    ]
    
    
    # Write your custom system prompt
    system_prompt = """You are analyzing public comments about [YOUR REGULATION].

[Your regulation description here]

For each comment, identify:

1. Stances: Which of these positions does the commenter express?
- Support for Policy: [describe indicators]
- Opposition to Policy: [describe indicators] 
- Concerns About Process: [describe indicators]

2. Key Quote: Most important quote (max 100 words, verbatim from text)

3. Rationale: Brief explanation (1-2 sentences) of stance selection

Instructions:
- Select all applicable stances
- Be objective and avoid personal bias"""
    
    # Rest of function stays the same...
```

### 3. Save and Skip the Discovery Step

After editing `comment_analyzer.py`:

1. **Save the file** with your changes
2. **Skip**: `python discover_stances.py` (since you've manually configured everything)
3. **Run directly**: `python pipeline.py --csv comments.csv`

**Summary**: You're editing **two parts** of `comment_analyzer.py`:
- The `Stance` enum class at the top
- The `create_regulation_analyzer` function at the bottom

This gives you complete control over the analysis categories and prompt while maintaining enum validation.

## Files

- **`detect_columns.py`** - Auto-detects CSV column structure → `column_mapping.json` + `regulation_metadata.json`
- **`discover_stances.py`** - Discovers stances → `analyzer_config.json` + updates enum constraints
- **`pipeline.py`** - Main processing script (outputs Parquet format)
- **`comment_analyzer.py`** - Configurable LLM analyzer with enum validation (auto-updated by discover_stances.py)
- **`generate_report.py`** - HTML report generator (runs automatically)
- **`run_pipeline.sh`** - Convenience script with sleep prevention
- **`schema.sql`** - PostgreSQL database schema

## Options

```bash
./run_pipeline.sh --csv comments.csv [options]

Options:
  --sample N          Process only N random comments (for testing)
  --truncate N        Truncate comment text to N characters for LLM analysis (saves costs)
  --to-database       Store results in PostgreSQL database
  --workers N         Number of parallel workers for faster processing (default: 8)
  --batch-size N      Batch size for parallel processing (default: 50)
  --no-parallel       Disable parallel processing (slower but more stable)
  --model MODEL       Use different LLM model (default: gpt-4o-mini)
  --output FILE       Parquet output file (default: analyzed_comments.parquet)
```

### HTML Report

Generates an interactive HTML report (`index.html`) with:
- Regulation title and link to regulations.gov
- Summary statistics and distribution charts
- Checkbox filtering for stances and attachments  
- Text search for IDs, dates, quotes, and content

## Database Setup

If using PostgreSQL storage:

1. Create your database and run: `psql -f schema.sql`
2. Add `DATABASE_URL` to your `.env` file
3. Use `--to-database` flag when running

## Features

- Parallel processing for faster analysis
- Automatic regulation metadata extraction
- PDF, DOCX, and image attachment processing
- Smart deduplication
- Enum validation to prevent invalid values
- Parquet format output
- PostgreSQL database integration
- Interactive HTML reports

## Output

Saves results to `analyzed_comments.parquet` (compressed, efficient format) and generates `index.html` report. Optionally stores in PostgreSQL database with `--to-database` flag.

## License

MIT