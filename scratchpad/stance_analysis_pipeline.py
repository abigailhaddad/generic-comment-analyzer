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
import pandas as pd
import requests
import base64
import logging
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

# Set up logging
logger = logging.getLogger(__name__)

# Try to import text extraction libraries
try:
    from PyPDF2 import PdfReader
except ImportError:
    PdfReader = None

try:
    import docx
except ImportError:
    docx = None


def download_attachment(attachment_url: str, output_path: str) -> bool:
    """Download an attachment file."""
    try:
        response = requests.get(attachment_url, stream=True, timeout=30)
        response.raise_for_status()
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return True
    except Exception as e:
        logger.error(f"Failed to download {attachment_url}: {e}")
        return False


def extract_text_local(file_path: str) -> str:
    """Extract text using local libraries (PDF, DOCX, TXT)."""
    ext = file_path.lower().split('.')[-1]
    try:
        if ext == 'pdf' and PdfReader:
            reader = PdfReader(file_path)
            return "\n".join(page.extract_text() or '' for page in reader.pages)
        elif ext == 'docx' and docx:
            doc = docx.Document(file_path)
            return "\n".join(para.text for para in doc.paragraphs)
        elif ext == 'txt':
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                return f.read()
        else:
            return ""
    except Exception as e:
        logger.warning(f"Local extraction failed for {file_path}: {e}")
        return ""


def extract_text_with_gemini(file_path: str) -> str:
    """Extract text using Gemini API for images and complex PDFs."""
    gemini_api_key = os.getenv("GEMINI_API_KEY")
    if not gemini_api_key:
        logger.warning("GEMINI_API_KEY not found, skipping Gemini extraction")
        return ""
    
    # Check file size (skip large files)
    file_size = os.path.getsize(file_path)
    if file_size > 5 * 1024 * 1024:  # 5MB limit
        logger.warning(f"File too large for Gemini: {file_path}")
        return ""
    
    try:
        # Read and encode file
        with open(file_path, "rb") as f:
            encoded_data = base64.b64encode(f.read()).decode("utf-8")
        
        # Determine MIME type
        ext = file_path.lower().split('.')[-1]
        mime_types = {
            'pdf': 'application/pdf',
            'png': 'image/png', 
            'jpg': 'image/jpeg',
            'jpeg': 'image/jpeg',
            'gif': 'image/gif',
            'bmp': 'image/bmp',
        }
        mime_type = mime_types.get(ext, 'application/pdf')
        
        # Call Gemini API
        url = f"https://generativelanguage.googleapis.com/v1/models/gemini-1.5-flash:generateContent?key={gemini_api_key}"
        payload = {
            "contents": [{
                "parts": [
                    {"text": "Extract all text from this document. Return only the raw text content."},
                    {"inlineData": {"mimeType": mime_type, "data": encoded_data}}
                ]
            }],
            "generationConfig": {"temperature": 0.1, "maxOutputTokens": 8192}
        }
        
        response = requests.post(url, json=payload, timeout=60)
        response.raise_for_status()
        
        result = response.json()
        text = result['candidates'][0]['content']['parts'][0]['text']
        return text.strip()
        
    except Exception as e:
        logger.error(f"Gemini extraction failed for {file_path}: {e}")
        return ""


def process_attachments(comment_data: Dict[str, Any], attachments_dir: str, attachment_col: str = 'Attachment Files') -> str:
    """Download and process attachments for a comment, return combined text."""
    if attachment_col not in comment_data or not comment_data[attachment_col]:
        return ""
    
    attachment_urls = comment_data[attachment_col].split(',')
    combined_attachment_text = []
    
    # Get comment ID using multiple possible field names
    comment_id = (comment_data.get('Document ID') or 
                 comment_data.get('id') or 
                 comment_data.get('Comment ID') or 
                 'unknown_comment')
    comment_attachment_dir = os.path.join(attachments_dir, comment_id)
    
    for i, url in enumerate(attachment_urls):
        url = url.strip()
        if not url:
            continue
            
        # Generate filename from URL
        filename = f"attachment_{i+1}_{url.split('/')[-1]}"
        if '.' not in filename:
            filename += '.pdf'  # Default extension
            
        file_path = os.path.join(comment_attachment_dir, filename)
        
        # Download attachment
        logger.info(f"  Downloading attachment: {filename}")
        if download_attachment(url, file_path):
            # Extract text locally first
            text = extract_text_local(file_path)
            
            # If local extraction yields minimal text, try Gemini
            if len(text.strip()) < 50:
                logger.info(f"  Minimal text from local extraction, trying Gemini...")
                gemini_text = extract_text_with_gemini(file_path)
                if len(gemini_text.strip()) > len(text.strip()):
                    text = gemini_text
            
            if text.strip():
                combined_attachment_text.append(text.strip())
                logger.info(f"  Extracted {len(text)} characters from {filename}")
            else:
                logger.warning(f"  No text extracted from {filename}")
    
    return "\n\n--- ATTACHMENT ---\n\n".join(combined_attachment_text)


def load_column_mappings() -> Dict[str, str]:
    """Load column mappings from JSON file."""
    config_files = ['../column_mapping.json', 'column_mapping.json']
    for config_file in config_files:
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r') as f:
                    return json.load(f)
            except:
                pass
    
    # Fallback defaults
    return {
        'text': 'Comment',
        'id': 'Document ID', 
        'date': 'Posted Date',
        'submitter': 'submitter',
        'organization': 'organization'
    }


def load_and_standardize_csv(csv_file: str, process_attachments_flag: bool = False, attachments_dir: str = "attachments") -> List[Dict[str, Any]]:
    """Load CSV and create standardized field names based on column mapping.
    
    Args:
        csv_file: Path to CSV file
        process_attachments_flag: Whether to download and process attachments
        attachments_dir: Directory to store downloaded attachments
        
    Returns:
        List of comment dictionaries with standardized field names
    """
    print(f"Loading CSV data from {csv_file}...")
    
    # Load column mappings
    column_mappings = load_column_mappings()
    print(f"Using column mappings: {column_mappings}")
    
    # Read CSV
    df = pd.read_csv(csv_file, low_memory=False)
    print(f"Loaded {len(df)} rows from CSV")
    
    comments = []
    attachments_processed = 0
    
    for _, row in df.iterrows():
        comment = {}
        
        # Map CSV columns to standardized field names
        for standard_field, csv_column in column_mappings.items():
            if csv_column in df.columns:
                comment[standard_field] = str(row[csv_column]) if pd.notna(row[csv_column]) else ''
            else:
                comment[standard_field] = ''
                if csv_column != 'attachment_files':  # Don't warn about optional fields
                    print(f"Warning: Column '{csv_column}' not found in CSV for field '{standard_field}'")
        
        # Ensure required fields exist
        if not comment.get('id'):
            comment['id'] = f"row_{len(comments)}"
        if not comment.get('text'):
            continue  # Skip rows without text
        
        # Process attachments if requested
        attachment_text = ""
        if process_attachments_flag and comment.get('attachment_files'):
            print(f"Processing attachments for comment {comment['id']}")
            # Create a temporary dict with the original CSV column names for attachment processing
            temp_row_dict = dict(row)
            temp_row_dict['id'] = comment['id']  # Ensure ID is available
            attachment_text = process_attachments(temp_row_dict, attachments_dir)
            if attachment_text:
                attachments_processed += 1
        
        # Add attachment_text field
        comment['attachment_text'] = attachment_text
        
        # Combine comment text with attachment text for full_text
        full_text = comment['text']
        if attachment_text:
            if full_text:
                full_text += f"\n\n--- ATTACHMENT CONTENT ---\n{attachment_text}"
            else:
                full_text = attachment_text
        comment['text'] = full_text  # Update text field with combined content
        comment['comment_text'] = comment.get('text', '')  # Store original comment text
            
        comments.append(comment)
    
    print(f"Processed {len(comments)} comments with text content")
    if process_attachments_flag:
        print(f"Successfully processed attachments for {attachments_processed} comments")
    return comments


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
    
    # Always ensure new_stances is in field_analysis and extract from comments
    all_new_stances = set()
    for comment in comments:
        new_stances = comment.get('analysis', {}).get('new_stances', [])
        if isinstance(new_stances, list):
            all_new_stances.update(new_stances)
    
    field_analysis['new_stances'] = {
        'type': 'checkbox',
        'is_list': True,
        'unique_values': sorted(list(all_new_stances)),
        'num_unique': len(all_new_stances),
        'total_occurrences': len([c for c in comments if c.get('analysis', {}).get('new_stances', [])])
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
                       help='Input file (JSON with analyzed comments or CSV)')
    parser.add_argument('--input-type', type=str, choices=['json', 'csv'], default='json',
                       help='Type of input file (json or csv)')
    parser.add_argument('--output-json', type=str, default='processed_stance_data.json',
                       help='Output JSON file for processed data')
    parser.add_argument('--output-html', type=str, default='stance_analysis_report.html',
                       help='Output HTML report file')
    parser.add_argument('--skip-processing', action='store_true',
                       help='Skip processing and generate HTML from existing JSON')
    parser.add_argument('--process-attachments', action='store_true',
                       help='Download and process attachments (for CSV input only)')
    parser.add_argument('--attachments-dir', type=str, default='attachments',
                       help='Directory to store downloaded attachments')
    
    args = parser.parse_args()
    
    try:
        if not args.skip_processing:
            # Load input comments
            if args.input_type == 'csv':
                print(f"Loading and standardizing CSV data from {args.input}...")
                if args.process_attachments:
                    print("Attachment processing enabled - this may take a while...")
                comments = load_and_standardize_csv(args.input, args.process_attachments, args.attachments_dir)
            else:
                print(f"Loading analyzed comments from {args.input}...")
                with open(args.input, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # If it's a processed JSON file, extract the comments array
                    if isinstance(data, dict) and 'comments' in data:
                        comments = data['comments']
                    else:
                        comments = data
            
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