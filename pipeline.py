#!/usr/bin/env python3
"""
Generic Regulation Comment Analysis Pipeline

A simple pipeline for analyzing public comments on federal regulations.
Fetches comments, analyzes them with LLM, and stores results in PostgreSQL.

Usage: python pipeline.py --csv comments.csv [--sample N] [--model gpt-4o-mini]
"""

import argparse
import json
import os
import csv
import logging
import requests
import base64
import time
from pathlib import Path
from typing import List, Dict, Any, Optional
import random
from dotenv import load_dotenv
from PyPDF2 import PdfReader
import docx
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from tqdm import tqdm
import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Load environment variables
load_dotenv()

# Import the generic comment analyzer
from comment_analyzer import CommentAnalyzer

# Simple logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
        if ext == 'pdf':
            reader = PdfReader(file_path)
            return "\n".join(page.extract_text() or '' for page in reader.pages)
        elif ext == 'docx':
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

def load_column_mapping() -> Dict[str, str]:
    """Load column mappings from config file."""
    try:
        if os.path.exists('column_mapping.json'):
            with open('column_mapping.json', 'r', encoding='utf-8') as f:
                return json.load(f)
        else:
            logger.warning("No column_mapping.json found, using default mappings")
            # Fallback to common column names
            return {
                'text': 'Comment',
                'id': 'Document ID', 
                'date': 'Posted Date',
                'first_name': 'First Name',
                'last_name': 'Last Name',
                'organization': 'Organization Name',
                'attachment_files': 'Attachment Files'
            }
    except Exception as e:
        logger.error(f"Failed to load column mapping: {e}")
        return {}

def read_comments_from_csv(csv_file: str, limit: Optional[int] = None, sample_size: Optional[int] = None) -> List[Dict[str, Any]]:
    """Read comments from CSV file and return as list of dicts."""
    logger.info(f"Reading comments from {csv_file}")
    
    # Load column mappings
    column_mapping = load_column_mapping()
    if not column_mapping:
        logger.error("No column mappings available")
        return []
    
    # Create attachments directory
    attachments_dir = "attachments"
    os.makedirs(attachments_dir, exist_ok=True)
    
    # First pass: collect basic comment info without processing attachments
    all_rows = []
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if limit and i >= limit:
                break
            all_rows.append(row)
    
    # Apply sampling if requested
    if sample_size and len(all_rows) > sample_size:
        logger.info(f"Sampling {sample_size} comments from {len(all_rows)} total before processing attachments")
        all_rows = random.sample(all_rows, sample_size)
    
    # Second pass: process the selected comments with attachments
    comments = []
    for i, row in enumerate(all_rows):
        # Extract comment ID and text using column mappings
        comment_id = (row.get(column_mapping.get('id', '')) or 
                     row.get('Document ID') or 
                     row.get('id') or 
                     f"comment_{i}")
        
        comment_text = (row.get(column_mapping.get('text', '')) or 
                       row.get('Comment', '')).strip()
        
        # Check for attachments using column mapping
        attachment_col = column_mapping.get('attachment_files', 'Attachment Files')
        has_attachments = row.get(attachment_col, '').strip()
        
        # Skip empty comments without attachments
        if not comment_text and not has_attachments:
            continue
        
        # Process attachments
        attachment_text = ""
        if has_attachments:
            logger.info(f"Processing attachments for comment {comment_id}")
            attachment_text = process_attachments(row, attachments_dir, attachment_col)
        
        # Combine comment text and attachment text
        full_text = comment_text
        if attachment_text:
            if full_text:
                full_text += f"\n\n--- ATTACHMENT CONTENT ---\n{attachment_text}"
            else:
                full_text = attachment_text
        
        # Skip if still no text
        if not full_text.strip():
            continue
        
        # Build submitter name from first/last name or use combined field
        submitter = ""
        first_name_col = column_mapping.get('first_name', 'First Name')
        last_name_col = column_mapping.get('last_name', 'Last Name')
        
        first_name = row.get(first_name_col, '').strip()
        last_name = row.get(last_name_col, '').strip()
        
        if first_name or last_name:
            submitter = f"{first_name} {last_name}".strip()
        else:
            # Try other common submitter fields
            submitter = (row.get('Submitter Name', '') or 
                        row.get('submitter', '') or 
                        row.get('Author', ''))
        
        comment_data = {
            'id': comment_id,
            'text': full_text,
            'comment_text': comment_text,
            'attachment_text': attachment_text,
            'submitter': submitter,
            'organization': row.get(column_mapping.get('organization', 'Organization Name'), ''),
            'date': row.get(column_mapping.get('date', 'Posted Date'), ''),
        }
        
        comments.append(comment_data)
    
    logger.info(f"Loaded {len(comments)} comments")
    return comments

def create_dedup_table(comments: List[Dict[str, Any]]) -> tuple[List[Dict[str, Any]], Dict[str, List[Dict[str, Any]]]]:
    """Create deduplication table and return unique comments with mapping."""
    logger.info("Creating deduplication table...")
    
    # Group by combined text content
    text_groups = {}
    for comment in comments:
        text_key = comment['text'].strip().lower()
        if text_key not in text_groups:
            text_groups[text_key] = []
        text_groups[text_key].append(comment)
    
    # Create unique comments list with duplication stats
    unique_comments = []
    duplicate_mapping = {}
    
    for text_key, group in text_groups.items():
        # Use the first comment as the representative
        representative = group[0].copy()
        
        # Add duplication tracking fields
        representative['total_count'] = len(group)
        representative['is_unique'] = len(group) == 1
        representative['duplication_count'] = len(group)  # Raw count of duplicates
        representative['duplication_ratio'] = len(group)  # Will be updated later with correct ratio
        
        # Store all IDs that have this content
        representative['duplicate_ids'] = [c['id'] for c in group]
        
        unique_comments.append(representative)
        
        # Map text key to full group for later merging
        duplicate_mapping[text_key] = group
    
    total_comments = len(comments)
    unique_count = len(unique_comments)
    
    # Update each unique comment with the correct ratio based on total dataset
    for unique_comment in unique_comments:
        group_size = unique_comment['duplication_count']
        # Calculate the fraction: if half the comments are this duplicate, it's 1/2
        from fractions import Fraction
        fraction = Fraction(group_size, total_comments)
        unique_comment['duplication_ratio'] = f"1/{total_comments//group_size}"
    
    logger.info(f"Deduplication complete:")
    logger.info(f"  Total comments: {total_comments}")
    logger.info(f"  Unique content: {unique_count}")
    logger.info(f"  Duplication ratio: {total_comments/unique_count:.1f}x average")
    logger.info(f"  Will analyze {unique_count} unique pieces of content")
    
    return unique_comments, duplicate_mapping

def merge_analysis_results(unique_analyzed_comments: List[Dict[str, Any]], duplicate_mapping: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """Merge analysis results back to all original comments."""
    logger.info("Merging analysis results back to full dataset...")
    
    all_analyzed_comments = []
    
    for unique_comment in unique_analyzed_comments:
        text_key = unique_comment['text'].strip().lower()
        
        if text_key in duplicate_mapping:
            # Apply the analysis to all comments with this text
            for original_comment in duplicate_mapping[text_key]:
                merged_comment = original_comment.copy()
                
                # Add the analysis result
                merged_comment['analysis'] = unique_comment.get('analysis')
                merged_comment['analysis_error'] = unique_comment.get('analysis_error')
                
                # Add duplication tracking info
                merged_comment['total_count'] = unique_comment['total_count']
                merged_comment['is_unique'] = unique_comment['is_unique'] 
                merged_comment['duplication_count'] = unique_comment['duplication_count']
                merged_comment['duplication_ratio'] = unique_comment['duplication_ratio']
                merged_comment['duplicate_ids'] = unique_comment['duplicate_ids']
                
                all_analyzed_comments.append(merged_comment)
    
    logger.info(f"Merged analysis results to {len(all_analyzed_comments)} total comments")
    return all_analyzed_comments

def analyze_single_comment(analyzer, comment, truncate_chars=None):
    """Analyze a single comment (for use in parallel processing)."""
    try:
        # Prepare text for analysis (truncate if requested)
        analysis_text = comment['text']
        if truncate_chars and len(analysis_text) > truncate_chars:
            analysis_text = analysis_text[:truncate_chars]
        
        analysis_result = analyzer.analyze(analysis_text, comment_id=comment['id'])
        
        return {
            **comment,
            'analysis': analysis_result
        }
        
    except Exception as e:
        logger.error(f"Failed to analyze comment {comment['id']}: {e}")
        return {
            **comment,
            'analysis': None,
            'analysis_error': str(e)
        }

def analyze_comments_parallel(comments: List[Dict[str, Any]], model: str = "gpt-4o-mini", truncate_chars: Optional[int] = None, max_workers: int = 8, batch_size: int = 50) -> List[Dict[str, Any]]:
    """Analyze comments using parallel processing for much faster LLM calls."""
    logger.info(f"Analyzing {len(comments)} comments with {model}")
    logger.info(f"Using {max_workers} parallel workers, batch size {batch_size}")
    if truncate_chars:
        logger.info(f"Truncating text to {truncate_chars} characters for LLM analysis")
    
    analyzed_comments = []
    
    # Process in batches to avoid overwhelming the API
    for batch_start in tqdm(range(0, len(comments), batch_size), desc="Processing batches", unit="batch"):
        batch_end = min(batch_start + batch_size, len(comments))
        batch_comments = comments[batch_start:batch_end]
        
        # Use ThreadPoolExecutor for parallel API calls
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Create analyzer for each worker (thread-safe)
            def create_analyzer():
                return CommentAnalyzer(model=model)
            
            # Submit all comments in this batch
            future_to_comment = {}
            for comment in batch_comments:
                analyzer = create_analyzer()
                future = executor.submit(analyze_single_comment, analyzer, comment, truncate_chars)
                future_to_comment[future] = comment
            
            # Collect results as they complete
            batch_results = []
            with tqdm(total=len(batch_comments), desc=f"Batch {batch_start//batch_size + 1}", leave=False, unit="comment") as pbar:
                for future in as_completed(future_to_comment):
                    result = future.result()
                    batch_results.append(result)
                    pbar.update(1)
        
        # Maintain original order within batch
        comment_id_to_result = {result['id']: result for result in batch_results}
        ordered_results = [comment_id_to_result[comment['id']] for comment in batch_comments]
        analyzed_comments.extend(ordered_results)
        
        # Brief pause between batches to be respectful to API
        if batch_end < len(comments):
            time.sleep(0.1)
    
    logger.info(f"✅ Completed analysis of {len(analyzed_comments)} comments")
    return analyzed_comments

def analyze_comments(comments: List[Dict[str, Any]], model: str = "gpt-4o-mini", truncate_chars: Optional[int] = None, parallel: bool = True) -> List[Dict[str, Any]]:
    """Analyze comments using the LLM with optional parallel processing."""
    if parallel and len(comments) > 5:
        # Use parallel processing for better performance
        return analyze_comments_parallel(comments, model, truncate_chars)
    else:
        # Fall back to sequential processing for small batches or if parallel is disabled
        logger.info(f"Analyzing {len(comments)} comments with {model} (sequential)")
        if truncate_chars:
            logger.info(f"Truncating text to {truncate_chars} characters for LLM analysis")
        
        # Initialize analyzer using configuration file
        analyzer = CommentAnalyzer(model=model)
        
        analyzed_comments = []
        
        # Use tqdm for progress bar
        for comment in tqdm(comments, desc="Analyzing comments", unit="comment"):
            result = analyze_single_comment(analyzer, comment, truncate_chars)
            analyzed_comments.append(result)
        
        return analyzed_comments

def save_results(analyzed_comments: List[Dict[str, Any]], output_file: str):
    """Save analyzed comments to Parquet file."""
    logger.info(f"Saving {len(analyzed_comments)} analyzed comments to {output_file}")
    
    # Convert to DataFrame and save as Parquet
    df = pd.DataFrame(analyzed_comments)
    df.to_parquet(output_file, index=False)
    logger.info(f"✅ Saved results to {output_file}")

def get_db_connection():
    """Get PostgreSQL database connection."""
    db_url = os.getenv('DATABASE_URL')
    if not db_url:
        logger.warning("DATABASE_URL not found in environment")
        return None
    
    try:
        conn = psycopg2.connect(db_url, cursor_factory=RealDictCursor)
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        return None

def check_database_status(regulation_name: str = "Schedule F Civil Service Rule"):
    """Check database status and get user confirmation for deletion if needed."""
    conn = get_db_connection()
    if not conn:
        return True  # If no database, proceed without checking
    
    try:
        cursor = conn.cursor()
        
        # Try to query the comments table directly
        try:
            cursor.execute("SELECT COUNT(*) FROM comments WHERE regulation_name = %s", (regulation_name,))
            result = cursor.fetchone()
            existing_count = result['count'] if result else 0
            
            if existing_count > 0:
                logger.info(f"🗄️  Found {existing_count} existing records for regulation: {regulation_name}")
                response = input(f"Delete {existing_count} existing records and proceed? (y/N): ")
                if not response.lower().startswith('y'):
                    logger.info("❌ Cancelled - database storage aborted")
                    return False
                logger.info(f"✅ Confirmed deletion of {existing_count} records")
            else:
                logger.info(f"🗄️  No existing records found for regulation: {regulation_name}")
                
        except Exception as table_error:
            # Table probably doesn't exist
            if "does not exist" in str(table_error):
                logger.info("🗄️  Comments table does not exist")
                response = input("Create the comments table? (y/N): ")
                if not response.lower().startswith('y'):
                    logger.info("❌ Cancelled - table creation aborted")
                    return False
                
                # Read and execute schema
                try:
                    # Rollback any existing transaction
                    conn.rollback()
                    
                    with open('schema.sql', 'r') as f:
                        schema_sql = f.read()
                    cursor.execute(schema_sql)
                    conn.commit()
                    logger.info("✅ Comments table created successfully")
                except Exception as e:
                    logger.error(f"Failed to create table: {e}")
                    conn.rollback()
                    return False
            else:
                # Some other database error
                raise table_error
        
        return True
        
    except Exception as e:
        logger.error(f"Database check failed: {e}")
        return False
    finally:
        conn.close()

def store_in_postgres_from_parquet(parquet_file: str, regulation_name: str = "Schedule F Civil Service Rule", docket_id: str = "OPM-2025-0004"):
    """Store analyzed comments in PostgreSQL database from Parquet file."""
    conn = get_db_connection()
    if not conn:
        logger.warning("⚠️  Database connection failed, skipping PostgreSQL storage")
        return
    
    # Load data from Parquet
    logger.info(f"Loading data from {parquet_file}")
    df = pd.read_parquet(parquet_file)
    analyzed_comments = df.to_dict('records')
    
    try:
        cursor = conn.cursor()
        
        # Clear existing data for this regulation (already confirmed in main)
        logger.info(f"Clearing existing data for regulation: {regulation_name}")
        cursor.execute("DELETE FROM comments WHERE regulation_name = %s", (regulation_name,))
        deleted_count = cursor.rowcount
        if deleted_count > 0:
            logger.info(f"✅ Deleted {deleted_count} existing records")
        else:
            logger.info("No existing records found to delete")
        
        # Prepare batch insert data
        batch_data = []
        for comment in analyzed_comments:
            analysis = comment.get('analysis', {})
            
            # Parse date if it exists
            submission_date = None
            if comment.get('date'):
                try:
                    from datetime import datetime
                    submission_date = datetime.fromisoformat(comment['date'].replace('Z', '+00:00'))
                except:
                    submission_date = None
            
            batch_data.append((
                comment['id'],
                comment.get('submitter', ''),
                comment.get('organization', ''),
                submission_date,
                comment.get('comment_text', ''),
                comment.get('attachment_text', ''),
                comment.get('text', ''),
                analysis.get('stance'),
                analysis.get('key_quote'),
                analysis.get('rationale'),
                bool(comment.get('attachment_text', '').strip()),
                'gpt-4o-mini',  # TODO: get from args
                regulation_name,
                docket_id
            ))
        
        # Process in batches of 1000 records at a time
        batch_size = 1000
        total_batches = (len(batch_data) + batch_size - 1) // batch_size
        
        for i in range(0, len(batch_data), batch_size):
            batch_chunk = batch_data[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            logger.info(f"Inserting batch {batch_num}/{total_batches} ({len(batch_chunk)} records)")
            
            cursor.executemany("""
                INSERT INTO comments (
                    comment_id, submitter_name, organization, submission_date,
                    comment_text, attachment_text, combined_text,
                    stance, key_quote, rationale,
                    has_attachments, model_used, regulation_name, docket_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, batch_chunk)
        
        conn.commit()
        logger.info(f"✅ Stored {len(batch_data)} comments in PostgreSQL database (batch insert)")
        
    except Exception as e:
        logger.error(f"Database storage failed: {e}")
        conn.rollback()
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(description='Generic regulation comment analysis pipeline')
    parser.add_argument('--csv', type=str, required=True, help='Path to comments CSV file')
    parser.add_argument('--output', type=str, default='analyzed_comments.parquet', help='Output Parquet file')
    parser.add_argument('--sample', type=int, help='Process only N random comments for testing')
    parser.add_argument('--model', type=str, default='gpt-4o-mini', help='LLM model to use')
    parser.add_argument('--truncate', type=int, help='Truncate comment text to N characters before LLM analysis (saves costs)')
    parser.add_argument('--to-database', action='store_true', help='Store results in PostgreSQL database (requires DATABASE_URL in .env)')
    parser.add_argument('--workers', type=int, default=8, help='Number of parallel workers for LLM calls (default: 8)')
    parser.add_argument('--batch-size', type=int, default=50, help='Batch size for parallel processing (default: 50)')
    parser.add_argument('--no-parallel', action='store_true', help='Disable parallel processing (use sequential)')
    
    args = parser.parse_args()
    
    try:
        # Check database status early if database storage is requested
        if args.to_database:
            logger.info("=== DATABASE CHECK ===")
            if not check_database_status():
                logger.info("Exiting due to database check cancellation")
                return
        
        # Step 1: Read comments from CSV with attachments (sampling applied inside)
        logger.info("=== STEP 1: Loading Comments ===")
        comments = read_comments_from_csv(args.csv, sample_size=args.sample)
        
        # Step 2: Create deduplication table
        logger.info("=== STEP 2: Creating Deduplication Table ===")
        unique_comments, duplicate_mapping = create_dedup_table(comments)
        
        # Step 3: Analyze only unique comments
        logger.info("=== STEP 3: Analyzing Unique Comments ===")
        if args.no_parallel:
            unique_analyzed_comments = analyze_comments(unique_comments, args.model, args.truncate, parallel=False)
        else:
            # Update the parallel function call to use the new parameters
            unique_analyzed_comments = analyze_comments_parallel(unique_comments, args.model, args.truncate, args.workers, args.batch_size)
        
        # Step 4: Merge analysis results back to full dataset
        logger.info("=== STEP 4: Merging Results ===")
        analyzed_comments = merge_analysis_results(unique_analyzed_comments, duplicate_mapping)
        
        # Step 5: Save results locally
        logger.info("=== STEP 5: Saving Results ===")
        save_results(analyzed_comments, args.output)
        
        # Step 6: Store in PostgreSQL
        if args.to_database:
            logger.info("=== STEP 6: Database Storage ===")
            store_in_postgres_from_parquet(args.output)
        else:
            logger.info("=== STEP 6: Skipping Database Storage ===")
            logger.info("Use --to-database flag to store in PostgreSQL")
        
        # Generate HTML report
        logger.info("=== STEP 7: Generating HTML Report ===")
        try:
            from generate_report import load_results_parquet, calculate_stats, generate_html, analyze_field_types
            
            html_output = "index.html"
            logger.info(f"Loading results from {args.output}...")
            comments = load_results_parquet(args.output)
            
            logger.info("Analyzing field types...")
            field_analysis = analyze_field_types(comments)
            
            logger.info("Calculating statistics...")
            stats = calculate_stats(comments, field_analysis)
            
            logger.info(f"Generating HTML report: {html_output}")
            generate_html(comments, stats, field_analysis, html_output)
            
            logger.info(f"✅ HTML report generated: {html_output}")
            
        except Exception as e:
            logger.error(f"HTML report generation failed: {e}")
            logger.info("Pipeline completed but without HTML report")
        
        # Summary
        logger.info("=== PIPELINE COMPLETE ===")
        logger.info(f"Processed {len(analyzed_comments)} comments")
        logger.info(f"Results saved to: {args.output} (Parquet format)")
        logger.info(f"HTML report: index.html")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()