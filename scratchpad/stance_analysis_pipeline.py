#!/usr/bin/env python3
"""
Stance Analysis Pipeline

Processes analyzed comments to calculate statistics, co-occurrences, and unusual combinations,
then generates an HTML report.
"""

import argparse
import json
import sys
import os
from typing import List, Dict, Any
from datetime import datetime

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from generate_report import (
    analyze_field_types, 
    calculate_stats, 
    calculate_stance_cooccurrence,
    identify_unusual_combinations,
    load_regulation_metadata
)


def process_stance_analysis(comments: List[Dict[str, Any]], output_json: str) -> Dict[str, Any]:
    """Process comments to calculate all statistics and analysis needed for report.
    
    Args:
        comments: List of analyzed comments
        output_json: Path to save the processed data
        
    Returns:
        Dictionary containing all processed data
    """
    print("Analyzing field types...")
    field_analysis = analyze_field_types(comments)
    
    # Always ensure new_stances is in field_analysis
    if 'new_stances' not in field_analysis:
        field_analysis['new_stances'] = {
            'type': 'checkbox',
            'is_list': True,
            'unique_values': [],
            'num_unique': 0,
            'total_occurrences': 0
        }
    
    print("Calculating statistics...")
    stats = calculate_stats(comments, field_analysis)
    
    print("Processing unusual stance combinations...")
    # Add unusual combination flag to each comment
    total_comments = stats['total_comments']
    stance_cooccurrence = stats.get('stance_cooccurrence', {})
    
    for comment in comments:
        analysis = comment.get('analysis', {})
        if analysis:
            stances = analysis.get('stances', [])
            if isinstance(stances, list):
                # Check if this comment has unusual combinations
                has_unusual = identify_unusual_combinations(
                    stances, 
                    stance_cooccurrence, 
                    total_comments,
                    threshold=0.02  # Flag combinations that appear in <2% of comments
                )
                analysis['has_unusual_combination'] = has_unusual
            else:
                analysis['has_unusual_combination'] = False
    
    # Count unusual combinations for stats
    unusual_count = sum(1 for c in comments 
                       if c.get('analysis', {}).get('has_unusual_combination', False))
    stats['unusual_combinations_count'] = unusual_count
    
    print(f"Found {unusual_count} comments with unusual stance combinations")
    
    # Load regulation metadata
    regulation_metadata = load_regulation_metadata()
    
    # Prepare final data structure
    processed_data = {
        'comments': comments,
        'field_analysis': field_analysis,
        'stats': stats,
        'regulation_metadata': regulation_metadata,
        'processing_timestamp': datetime.now().isoformat(),
        'total_comments': len(comments),
        'unusual_combinations_count': unusual_count
    }
    
    # Save to JSON
    print(f"Saving processed data to {output_json}...")
    with open(output_json, 'w', encoding='utf-8') as f:
        json.dump(processed_data, f, indent=2, ensure_ascii=False)
    
    return processed_data


def generate_report_from_json(json_file: str, output_html: str):
    """Generate HTML report from processed JSON data.
    
    Args:
        json_file: Path to processed data JSON
        output_html: Path for output HTML file
    """
    print(f"Loading processed data from {json_file}...")
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Import our custom generate_html that supports new_stances and unusual combinations
    from generate_report import generate_html
    
    print(f"Generating HTML report to {output_html}...")
    generate_html(
        data['comments'],
        data['stats'],
        data['field_analysis'],
        output_html
    )
    
    print(f"✅ Report generated: {output_html}")


def main():
    parser = argparse.ArgumentParser(description='Process stance analysis and generate report')
    parser.add_argument('--input', type=str, required=True, 
                       help='Input JSON file with analyzed comments')
    parser.add_argument('--output-json', type=str, default='processed_stance_data.json',
                       help='Output JSON file for processed data')
    parser.add_argument('--output-html', type=str, default='stance_analysis_report.html',
                       help='Output HTML report file')
    parser.add_argument('--skip-processing', action='store_true',
                       help='Skip processing and generate HTML from existing JSON')
    
    args = parser.parse_args()
    
    try:
        if not args.skip_processing:
            # Load input comments
            print(f"Loading comments from {args.input}...")
            with open(args.input, 'r', encoding='utf-8') as f:
                comments = json.load(f)
            
            print(f"Loaded {len(comments)} comments")
            
            # Process the data
            processed_data = process_stance_analysis(comments, args.output_json)
            
            print("\n" + "="*60)
            print("PROCESSING SUMMARY")
            print("="*60)
            print(f"Total comments: {processed_data['total_comments']}")
            print(f"Comments with unusual combinations: {processed_data['unusual_combinations_count']}")
            print(f"Unique stances: {len(processed_data['field_analysis']['stances']['unique_values'])}")
            print(f"Unique new stances: {len(processed_data['field_analysis']['new_stances']['unique_values'])}")
            print("="*60 + "\n")
        
        # Generate HTML report
        generate_report_from_json(args.output_json, args.output_html)
        
    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()