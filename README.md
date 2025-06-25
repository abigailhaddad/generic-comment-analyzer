# Generic Regulation Comment Analysis

A simple, flexible system for analyzing public comments on federal regulations using LLMs.

## Data Source

This system is designed to work with comment data from **[regulations.gov bulk download](https://www.regulations.gov/bulkdownload)**:

1. **Request bulk data** for your regulation docket from regulations.gov
2. **You'll receive an email** with download links when ready
3. **Download and rename** the comments file to `comments.csv` 
4. **Place `comments.csv`** in the root of this repository

**Important**: The pipeline **does not resume** - it processes all comments from scratch each time. Use `--sample` to test before running the full dataset.

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
   # Step 1: Detect CSV column structure and extract regulation metadata
   python detect_columns.py

   # Step 2: Discover stances and themes from sample comments
   python discover_stances.py --sample 250

   # Step 3: Test with small sample to verify everything works
   python pipeline.py --csv comments.csv --sample 10
   ```

4. **Run full analysis:**
   ```bash
   # IMPORTANT: Test first, then run full dataset
   # The pipeline does not resume - it starts from scratch each time
   
   # Full run with parallel processing (recommended)
   ./run_pipeline.sh --csv comments.csv --workers 8 --batch-size 100
   
   # Full run with database storage
   ./run_pipeline.sh --csv comments.csv --workers 4 --batch-size 50 --to-database
   
   # Conservative run (if you hit API rate limits)
   ./run_pipeline.sh --csv comments.csv --workers 4 --batch-size 25
   ```

## How It Works

The system automatically discovers and adapts to your regulation with **built-in validation**:

1. **Column Detection** (`detect_columns.py`): Uses LLM to automatically map CSV columns to required fields (comment text, ID, date, submitter, attachments) and extract regulation metadata for professional report titles. Alternatively, manually edit `column_mapping.json` if you know your CSV structure.

2. **Stance Discovery** (`discover_stances.py`): Analyzes sample comments to discover:
   - The main arguments/positions people are taking (not just pro/con)
   - Specific indicators that signal each stance
   - Recurring themes across comments
   - Generates complete analysis configuration
   - **Automatically updates `comment_analyzer.py` with enum constraints** to prevent invalid values

3. **Comment Analysis** (`pipeline.py`): For each comment, identifies:
   - **Stances**: Multiple arguments/positions (e.g., "board shouldn't be fired", "process lacks transparency")
   - **Themes**: Topics covered (e.g., "scientific integrity", "vaccine safety")
   - **Key Quote**: Most important excerpt
   - **Rationale**: Why those stances were selected

4. **Results**: Saves to **Parquet format** (efficient compressed storage), optional PostgreSQL database, and automatically generates interactive HTML report

## For Different Regulations

The system is fully generic! Just:

1. Put your `comments.csv` file in the directory
2. Run the 3 setup commands above
3. The system discovers everything automatically

No manual configuration needed - it learns your regulation's specific arguments and themes.

## Built-in Validation System

The system includes **automatic enum validation** to ensure data quality:

1. **`discover_stances.py`** analyzes your comments and creates specific stance/theme lists
2. **Automatically updates `comment_analyzer.py`** with Pydantic enum constraints  
3. **Prevents invalid values** during analysis - only discovered stances/themes are allowed
4. **Type safety** - eliminates typos and inconsistent categorization

**Example**: If your regulation has stances like "Support for Transparency" and "Opposition to Changes", only these exact values can be assigned - no variations or typos allowed.

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

class Theme(str, Enum):
    ECONOMIC_IMPACT = "Economic Impact"
    ENVIRONMENTAL_CONCERNS = "Environmental Concerns"
    PUBLIC_SAFETY = "Public Safety"
    # Add your custom themes here
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
    
    # Hard-code your theme options  
    theme_options = [
        "Economic Impact",
        "Environmental Concerns",
        "Public Safety"
    ]
    
    # Write your custom system prompt
    system_prompt = """You are analyzing public comments about [YOUR REGULATION].

[Your regulation description here]

For each comment, identify:

1. Stances: Which of these positions does the commenter express?
- Support for Policy: [describe indicators]
- Opposition to Policy: [describe indicators] 
- Concerns About Process: [describe indicators]

2. Themes: Which topics are present?
- Economic Impact
- Environmental Concerns  
- Public Safety

3. Key Quote: Most important quote (max 100 words, verbatim from text)

4. Rationale: Brief explanation (1-2 sentences) of stance selection

Instructions:
- Select all applicable stances and themes
- Be objective and avoid personal bias"""
    
    # Rest of function stays the same...
```

### 3. Save and Skip the Discovery Step

After editing `comment_analyzer.py`:

1. **Save the file** with your changes
2. **Skip**: `python discover_stances.py` (since you've manually configured everything)
3. **Run directly**: `python pipeline.py --csv comments.csv`

**Summary**: You're editing **two parts** of `comment_analyzer.py`:
- The `Stance` and `Theme` enum classes at the top
- The `create_regulation_analyzer` function at the bottom

This gives you complete control over the analysis categories and prompt while maintaining enum validation.

## Files

- **`detect_columns.py`** - Auto-detects CSV column structure → `column_mapping.json` + `regulation_metadata.json`
- **`discover_stances.py`** - Discovers stances and themes → `analyzer_config.json` + updates enum constraints
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

The pipeline automatically generates an interactive HTML report (`index.html`) with:
- **Professional titles**: Uses extracted regulation metadata for proper report titles
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

- **Parallel processing** - 4-8x faster analysis with configurable worker threads
- **Professional reporting** - Automatic regulation metadata extraction for proper titles
- **Attachment processing** - Handles PDFs (PyPDF2), DOCX files, and images (Gemini OCR)
- **Smart deduplication** - Analyzes unique content only, then maps results back to all duplicates
- **Enum validation** - Prevents invalid stance/theme values through auto-generated Pydantic enums
- **Efficient storage** - Parquet format provides ~10x compression vs JSON
- **Sampling** - Test on subsets before full runs
- **Sleep prevention** - Uses `caffeinate` on macOS for long runs
- **Database integration** - Optional PostgreSQL storage (reads from Parquet)
- **Automatic HTML reports** - Interactive filtering and statistics
- **Generic design** - Easy to adapt for any regulation

## Data Storage

### Parquet Format (Default)

The pipeline now outputs **Parquet format by default** for efficient storage:

- **Compressed**: ~10x smaller than JSON format
- **Fast**: Optimized for data analysis and filtering
- **Compatible**: Works with pandas, SQL databases, and analytics tools
- **Automatic**: No conversion step needed

**Output**: `analyzed_comments.parquet` (instead of JSON)

### Database Integration

PostgreSQL upload now reads directly from Parquet files:
```bash
python pipeline.py --csv comments.csv --to-database
# Automatically uploads from analyzed_comments.parquet
```

## License

MIT