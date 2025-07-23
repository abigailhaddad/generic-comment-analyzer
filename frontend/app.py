from flask import Flask, render_template, request, jsonify, Response
import json
import os
import sys
from typing import Dict, List, Any
import logging
import subprocess
import threading
import queue
import time
import csv
import shutil
from werkzeug.utils import secure_filename
import glob

# Add parent directory to path to import discover_stances
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from discover_stances import discover_themes_experimental, load_comments_sample, generate_analyzer_config, save_config, update_comment_analyzer, DiscoveredThemes

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Model configurations
MODELS = {
    "gpt-4o": "GPT-4o",
    "gpt-4o-mini": "GPT-4o Mini"
}

def load_stances():
    """Load existing stances from stances.json"""
    stances_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'stances.json')
    if os.path.exists(stances_path):
        with open(stances_path, 'r') as f:
            return json.load(f)
    return []

def save_stances(stances):
    """Save stances to stances.json"""
    stances_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'stances.json')
    with open(stances_path, 'w') as f:
        json.dump(stances, f, indent=2)

@app.route('/')
def index():
    """Main page - start with file upload"""
    return render_template('upload.html')

@app.route('/setup')
def setup():
    """Setup page for column detection"""
    return render_template('column_detection.html')

@app.route('/stance_discovery')
def stance_discovery():
    """Stance discovery page"""
    return render_template('index.html', models=MODELS)

@app.route('/pipeline')
def pipeline():
    """Pipeline execution page"""
    return render_template('pipeline.html', models=MODELS)

@app.route('/discover_stances', methods=['POST'])
def discover_stances_endpoint():
    """Run stance discovery"""
    try:
        data = request.json
        model = data.get('model', 'gpt-4o')
        num_comments = data.get('num_comments', 50)
        
        logger.info(f"Discovering stances with model={model}, num_comments={num_comments}")
        
        # Find the CSV file
        base_dir = os.path.dirname(os.path.dirname(__file__))
        csv_files = glob.glob(os.path.join(base_dir, '*.csv'))
        if not csv_files:
            return jsonify({
                'success': False,
                'error': 'No CSV file found. Please upload a CSV file first.'
            })
        
        csv_file = csv_files[0]
        
        # Load comments sample
        comments = load_comments_sample(csv_file, num_comments)
        
        if not comments:
            return jsonify({
                'success': False,
                'error': 'No comments with text content found'
            }), 400
        
        # Run discovery with mutually_exclusive strategy and 5 themes
        result = discover_themes_experimental(
            comments=comments,
            model=model,
            target_count=5,
            prompt_strategy='mutually_exclusive'
        )
        
        if 'error' in result:
            return jsonify({
                'success': False,
                'error': result['error']
            }), 500
        
        # Extract stances from the result
        stances = result.get('themes', [])
        
        logger.info(f"Discovered themes structure: {json.dumps(stances, indent=2)}")
        
        # Save the discovered stances
        save_stances(stances)
        
        # Update comment_analyzer.py with the new stances
        try:
            # Convert to DiscoveredThemes object
            discovered = DiscoveredThemes(
                themes=result['themes'],
                regulation_name=result['regulation_name'],
                regulation_description=result['regulation_description']
            )
            
            # Generate configuration with theme:position format
            config = generate_analyzer_config(discovered)
            
            # Update to use theme:position format
            theme_position_options = []
            theme_position_indicators = {}
            
            for theme in discovered.themes:
                theme_name = theme['name']
                for position in theme.get('positions', []):
                    position_name = position['name']
                    formatted_name = f"{theme_name}: {position_name}"
                    theme_position_options.append(formatted_name)
                    
                    indicators = position['indicators']
                    if isinstance(indicators, list):
                        theme_position_indicators[formatted_name] = "; ".join(indicators)
                    else:
                        theme_position_indicators[formatted_name] = indicators
            
            config.stance_options = theme_position_options
            config.stance_indicators = theme_position_indicators
            
            # Update system prompt for theme:position format
            stance_list = "\n".join([f"- {name}: {indicators}" for name, indicators in theme_position_indicators.items()])
            config.system_prompt = f"""You are analyzing public comments about {discovered.regulation_name}.

{discovered.regulation_description}

For each comment, identify:

1. Stances: Which of these theme:position combinations does the commenter express? Look for the indicators listed below. (Select ALL that apply, or none if none apply)
{stance_list}

2. Key Quote: Select the most important quote (max 100 words) that best captures the essence of the comment. Must be verbatim from the text.

3. Rationale: Briefly explain (1-2 sentences) why you selected these theme:position combinations.

Instructions:
- A comment may express multiple stances or no clear stance
- Only select stances that are clearly expressed in the comment
- Be objective and avoid inserting personal opinions"""
            
            # Save configuration
            save_config(config)
            
            # Update comment_analyzer.py
            update_comment_analyzer(config)
            
            logger.info("Successfully updated comment_analyzer.py with new stances")
            
        except Exception as e:
            logger.error(f"Error updating comment_analyzer.py: {e}", exc_info=True)
            # Don't fail the whole request if this fails
        
        return jsonify({
            'success': True,
            'stances': stances,
            'count': len(stances)
        })
        
    except Exception as e:
        logger.error(f"Error discovering stances: {e}", exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/get_stances', methods=['GET'])
def get_stances():
    """Get current stances"""
    try:
        stances = load_stances()
        return jsonify({
            'success': True,
            'stances': stances
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/update_stances', methods=['POST'])
def update_stances():
    """Update stances with edits"""
    try:
        data = request.json
        stances = data.get('stances', [])
        
        # Save updated stances
        save_stances(stances)
        
        return jsonify({
            'success': True,
            'message': 'Stances updated successfully'
        })
        
    except Exception as e:
        logger.error(f"Error updating stances: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/run_pipeline', methods=['POST'])
def run_pipeline():
    """Run the analysis pipeline with streaming progress"""
    data = request.json
    model = data.get('model', 'gpt-4o-mini')
    num_comments = data.get('num_comments', 100)
    
    def generate():
        # Find the CSV file
        base_dir = os.path.dirname(os.path.dirname(__file__))
        csv_files = glob.glob(os.path.join(base_dir, '*.csv'))
        if not csv_files:
            yield f"data: {json.dumps({'status': 'error', 'message': 'No CSV file found'})}\n\n"
            return
        
        csv_filename = os.path.basename(csv_files[0])
        
        # Build command
        cmd = [
            sys.executable,
            os.path.join(base_dir, 'pipeline.py'),
            '--csv', csv_filename,
            '--sample', str(num_comments),
            '--model', model,
            '--output', 'analyzed_comments.parquet'
        ]
        
        # Run the pipeline
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1,
            cwd=os.path.dirname(os.path.dirname(__file__))
        )
        
        # Stream output
        for line in iter(process.stdout.readline, ''):
            if line:
                # Parse progress from tqdm output or regular logs
                yield f"data: {json.dumps({'message': line.rstrip()})}\n\n"
        
        # Wait for process to complete
        process.wait()
        
        if process.returncode == 0:
            yield f"data: {json.dumps({'status': 'complete', 'message': 'Pipeline completed successfully!'})}\n\n"
        else:
            yield f"data: {json.dumps({'status': 'error', 'message': 'Pipeline failed'})}\n\n"
    
    return Response(generate(), mimetype='text/event-stream')

@app.route('/check_output')
def check_output():
    """Check if output files exist"""
    parquet_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'analyzed_comments.parquet')
    html_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
    
    return jsonify({
        'parquet_exists': os.path.exists(parquet_path),
        'html_exists': os.path.exists(html_path)
    })

@app.route('/view_report')
def view_report():
    """Serve the generated report"""
    report_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'index.html')
    if os.path.exists(report_path):
        with open(report_path, 'r', encoding='utf-8') as f:
            return f.read()
    else:
        return "Report not found. Please run the analysis pipeline first.", 404

@app.route('/check_csv_file')
def check_csv_file():
    """Check if a CSV file exists in the project directory"""
    base_dir = os.path.dirname(os.path.dirname(__file__))
    
    # Look for any CSV file
    csv_files = glob.glob(os.path.join(base_dir, '*.csv'))
    
    if csv_files:
        # Use the first CSV file found
        csv_path = csv_files[0]
        filename = os.path.basename(csv_path)
        
        # Get file info
        try:
            size = os.path.getsize(csv_path)
            row_count = 0
            with open(csv_path, 'r', encoding='utf-8') as f:
                # Use CSV reader to properly handle multi-line fields
                reader = csv.DictReader(f)
                row_count = sum(1 for row in reader)
            
            return jsonify({
                'exists': True,
                'filename': filename,
                'size': size,
                'row_count': row_count
            })
        except Exception as e:
            logger.error(f"Error reading CSV file: {e}")
            return jsonify({'exists': False})
    
    return jsonify({'exists': False})

@app.route('/upload_csv', methods=['POST'])
def upload_csv():
    """Handle CSV file upload"""
    try:
        if 'file' not in request.files:
            return jsonify({'success': False, 'error': 'No file provided'})
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'success': False, 'error': 'No file selected'})
        
        if not file.filename.endswith('.csv'):
            return jsonify({'success': False, 'error': 'File must be a CSV'})
        
        # Save the file
        base_dir = os.path.dirname(os.path.dirname(__file__))
        
        # Remove any existing CSV files first
        existing_csvs = glob.glob(os.path.join(base_dir, '*.csv'))
        for existing in existing_csvs:
            try:
                os.remove(existing)
            except Exception as e:
                logger.error(f"Error removing existing CSV: {e}")
        
        # Save new file with secure filename
        filename = secure_filename(file.filename)
        csv_path = os.path.join(base_dir, filename)
        file.save(csv_path)
        
        # Get row count
        row_count = 0
        with open(csv_path, 'r', encoding='utf-8') as f:
            # Use CSV reader to properly handle multi-line fields
            reader = csv.DictReader(f)
            row_count = sum(1 for row in reader)
        
        return jsonify({
            'success': True,
            'filename': filename,
            'row_count': row_count
        })
        
    except Exception as e:
        logger.error(f"Error uploading file: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/check_detection_status')
def check_detection_status():
    """Check if column detection has been done"""
    base_dir = os.path.dirname(os.path.dirname(__file__))
    
    # Find the CSV file
    csv_files = glob.glob(os.path.join(base_dir, '*.csv'))
    csv_path = csv_files[0] if csv_files else None
    
    mapping_path = os.path.join(base_dir, 'column_mapping.json')
    metadata_path = os.path.join(base_dir, 'regulation_metadata.json')
    
    result = {
        'csv_exists': bool(csv_path and os.path.exists(csv_path)),
        'mapping_exists': os.path.exists(mapping_path),
        'metadata_exists': os.path.exists(metadata_path),
        'csv_columns': [],
        'mapping': {},
        'metadata': {}
    }
    
    # Get CSV columns if file exists
    if result['csv_exists']:
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                result['csv_columns'] = reader.fieldnames or []
        except Exception as e:
            logger.error(f"Error reading CSV: {e}")
    
    # Load existing mapping if exists
    if result['mapping_exists']:
        try:
            with open(mapping_path, 'r') as f:
                result['mapping'] = json.load(f)
        except Exception as e:
            logger.error(f"Error reading mapping: {e}")
    
    # Load existing metadata if exists
    if result['metadata_exists']:
        try:
            with open(metadata_path, 'r') as f:
                result['metadata'] = json.load(f)
        except Exception as e:
            logger.error(f"Error reading metadata: {e}")
    
    return jsonify(result)

@app.route('/run_column_detection', methods=['POST'])
def run_column_detection():
    """Run the column detection script"""
    try:
        base_dir = os.path.dirname(os.path.dirname(__file__))
        detect_script = os.path.join(base_dir, 'detect_columns.py')
        
        # Find the CSV file
        csv_files = glob.glob(os.path.join(base_dir, '*.csv'))
        if not csv_files:
            return jsonify({
                'success': False,
                'error': 'No CSV file found. Please upload a CSV file first.'
            })
        
        csv_filename = os.path.basename(csv_files[0])
        
        # Run the detection script with the CSV filename
        result = subprocess.run(
            [sys.executable, detect_script, '--csv', csv_filename],
            cwd=base_dir,
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            return jsonify({
                'success': False,
                'error': result.stderr or 'Detection failed'
            })
        
        # Load the results
        mapping_path = os.path.join(base_dir, 'column_mapping.json')
        metadata_path = os.path.join(base_dir, 'regulation_metadata.json')
        
        mapping = {}
        metadata = {}
        
        if os.path.exists(mapping_path):
            with open(mapping_path, 'r') as f:
                mapping = json.load(f)
        
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
        
        # Get CSV columns
        csv_files = glob.glob(os.path.join(base_dir, '*.csv'))
        csv_path = csv_files[0] if csv_files else None
        csv_columns = []
        if csv_path:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                csv_columns = reader.fieldnames or []
        
        return jsonify({
            'success': True,
            'mapping': mapping,
            'metadata': metadata,
            'csv_columns': csv_columns
        })
        
    except Exception as e:
        logger.error(f"Error running detection: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        })

@app.route('/get_column_sample')
def get_column_sample():
    """Get a sample value from a specific column"""
    column = request.args.get('column')
    if not column:
        return jsonify({'sample': None})
    
    try:
        base_dir = os.path.dirname(os.path.dirname(__file__))
        csv_files = glob.glob(os.path.join(base_dir, '*.csv'))
        if not csv_files:
            return jsonify({'sample': 'No CSV file found'})
        csv_path = csv_files[0]
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                value = row.get(column, '').strip()
                if value:
                    # Truncate long values
                    if len(value) > 200:
                        value = value[:200] + '...'
                    return jsonify({'sample': value})
        return jsonify({'sample': 'No data found'})
    except Exception as e:
        return jsonify({'sample': f'Error: {str(e)}'})

@app.route('/get_column_samples')
def get_column_samples():
    """Get multiple sample values from a specific column"""
    column = request.args.get('column')
    if not column:
        return jsonify({'samples': [], 'stats': None})
    
    try:
        base_dir = os.path.dirname(os.path.dirname(__file__))
        csv_files = glob.glob(os.path.join(base_dir, '*.csv'))
        if not csv_files:
            return jsonify({'samples': [], 'stats': None})
        csv_path = csv_files[0]
        samples = []
        total_rows = 0
        non_empty_count = 0
        
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            # Skip first row (header row was already consumed by DictReader)
            try:
                next(reader)  # Skip first data row
            except StopIteration:
                pass
                
            for i, row in enumerate(reader):
                if i >= 100:  # Sample first 100 rows (after skipping first)
                    break
                total_rows += 1
                value = row.get(column, '').strip()
                if value:
                    non_empty_count += 1
                    # Truncate to 80 characters
                    if len(value) > 80:
                        value = value[:80] + '...'
                    # Only add unique samples up to 3
                    if value not in samples and len(samples) < 3:
                        samples.append(value)
        
        return jsonify({
            'samples': samples,
            'stats': {
                'total': total_rows,
                'non_empty': non_empty_count
            }
        })
    except Exception as e:
        logger.error(f"Error getting column samples: {e}")
        return jsonify({'samples': [], 'error': str(e)})

@app.route('/save_column_mapping', methods=['POST'])
def save_column_mapping():
    """Save column mapping"""
    try:
        data = request.json
        mapping = data.get('mapping', {})
        
        # Validate required fields
        if 'text' not in mapping or 'id' not in mapping:
            return jsonify({
                'success': False,
                'error': 'Comment text and Document ID mappings are required'
            })
        
        mapping_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'column_mapping.json')
        with open(mapping_path, 'w') as f:
            json.dump(mapping, f, indent=2)
        
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

@app.route('/save_regulation_metadata', methods=['POST'])
def save_regulation_metadata():
    """Save regulation metadata"""
    try:
        data = request.json
        metadata = data.get('metadata', {})
        
        # Validate required fields
        if not all(k in metadata for k in ['regulation_name', 'docket_id', 'agency']):
            return jsonify({
                'success': False,
                'error': 'Regulation name, docket ID, and agency are required'
            })
        
        metadata_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'regulation_metadata.json')
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

if __name__ == '__main__':
    app.run(debug=True, port=5000)