from flask import Flask, render_template, request, jsonify
import json
import os
import sys
from typing import Dict, List, Any
import logging

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
    """Main page"""
    return render_template('index.html', models=MODELS)

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

if __name__ == '__main__':
    app.run(debug=True, port=5000)