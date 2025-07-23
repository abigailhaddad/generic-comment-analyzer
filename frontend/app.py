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

# Add parent directory to path to import discover_stances
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from discover_stances import discover_themes_experimental, load_comments_sample, generate_analyzer_config, save_config

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
    """Main page - always start with column detection"""
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
        
        # Run stance discovery
        csv_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'comments.csv')
        
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
        # Build command
        cmd = [
            sys.executable,
            os.path.join(os.path.dirname(os.path.dirname(__file__)), 'pipeline.py'),
            '--csv', 'comments.csv',
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

@app.route('/check_detection_status')
def check_detection_status():
    """Check if column detection has been done"""
    base_dir = os.path.dirname(os.path.dirname(__file__))
    csv_path = os.path.join(base_dir, 'comments.csv')
    mapping_path = os.path.join(base_dir, 'column_mapping.json')
    metadata_path = os.path.join(base_dir, 'regulation_metadata.json')
    
    result = {
        'csv_exists': os.path.exists(csv_path),
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
        
        # Run the detection script
        result = subprocess.run(
            [sys.executable, detect_script],
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
        csv_path = os.path.join(base_dir, 'comments.csv')
        csv_columns = []
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
        csv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'comments.csv')
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
        csv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'comments.csv')
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