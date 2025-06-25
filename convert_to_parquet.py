#!/usr/bin/env python3
"""
Convert analyzed_comments.json to Parquet format
"""

import json
import pandas as pd
import os
from pathlib import Path

def convert_json_to_parquet(json_file='analyzed_comments.json', parquet_file='analyzed_comments.parquet'):
    """Convert JSON file to Parquet format and compare sizes."""
    
    # Check if JSON file exists
    if not os.path.exists(json_file):
        print(f"Error: {json_file} not found")
        return
    
    # Load JSON data
    print(f"Loading {json_file}...")
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Convert to DataFrame
    # Flatten nested analysis fields
    df_data = []
    for comment in data:
        flat_comment = {
            'id': comment.get('id'),
            'text': comment.get('text'),
            'comment_text': comment.get('comment_text'),
            'attachment_text': comment.get('attachment_text'),
            'submitter': comment.get('submitter'),
            'organization': comment.get('organization'),
            'date': comment.get('date'),
            'total_count': comment.get('total_count'),
            'is_unique': comment.get('is_unique'),
            'duplication_count': comment.get('duplication_count'),
            'duplication_ratio': comment.get('duplication_ratio'),
            'analysis_error': comment.get('analysis_error')
        }
        
        # Handle analysis fields
        if comment.get('analysis'):
            analysis = comment['analysis']
            flat_comment['stances'] = json.dumps(analysis.get('stances', []))
            flat_comment['themes'] = json.dumps(analysis.get('themes', []))
            flat_comment['key_quote'] = analysis.get('key_quote', '')
            flat_comment['rationale'] = analysis.get('rationale', '')
        else:
            flat_comment['stances'] = '[]'
            flat_comment['themes'] = '[]'
            flat_comment['key_quote'] = ''
            flat_comment['rationale'] = ''
        
        # Handle duplicate_ids
        flat_comment['duplicate_ids'] = json.dumps(comment.get('duplicate_ids', []))
        
        df_data.append(flat_comment)
    
    df = pd.DataFrame(df_data)
    
    # Save as Parquet
    print(f"\nSaving as {parquet_file}...")
    df.to_parquet(parquet_file, compression='snappy', index=False)
    
    # Compare file sizes
    json_size = os.path.getsize(json_file)
    parquet_size = os.path.getsize(parquet_file)
    
    print(f"\nFile size comparison:")
    print(f"JSON:    {json_size:,} bytes ({json_size/1024/1024:.2f} MB)")
    print(f"Parquet: {parquet_size:,} bytes ({parquet_size/1024/1024:.2f} MB)")
    print(f"Compression ratio: {json_size/parquet_size:.2f}x")
    print(f"Space saved: {(json_size-parquet_size)/1024/1024:.2f} MB ({(1-parquet_size/json_size)*100:.1f}%)")
    
    # Show DataFrame info
    print(f"\nDataFrame info:")
    print(f"Rows: {len(df):,}")
    print(f"Columns: {len(df.columns)}")
    print(f"\nColumn names:")
    for col in df.columns:
        print(f"  - {col}")

if __name__ == "__main__":
    convert_json_to_parquet()