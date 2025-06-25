#!/usr/bin/env python3
"""
CSV Column Detection Script

Uses LLM to automatically detect which columns in your CSV file contain:
- Comment text
- Comment ID
- Date
- Submitter name
- Organization
- Attachment files

Saves the mappings to column_mapping.json for use by the pipeline.
"""

import argparse
import csv
import json
import random
import logging
from typing import List, Dict, Any
from pydantic import BaseModel, Field
import litellm

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Removed theme discovery - now just doing CSV column detection

def extract_regulation_metadata(csv_file: str, model: str = "gpt-4o-mini") -> Dict[str, str]:
    """Use LLM to extract regulation metadata from CSV data."""
    logger.info(f"Extracting regulation metadata from {csv_file} using {model}")
    
    # Read first few rows including the header row
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        columns = reader.fieldnames or []
        sample_rows = []
        for i, row in enumerate(reader):
            if i >= 3:  # Just need a few rows for metadata
                break
            sample_rows.append(row)
    
    system_prompt = """You are analyzing a CSV file containing public comments on a government regulation. 
    
Extract metadata about this regulation to create a descriptive title for an analysis report.

Return a JSON object with these fields:
- regulation_name: A clear, descriptive name for this regulation/rulemaking (e.g., "ACIP Vaccine Recommendations", "EPA Clean Air Standards")
- docket_id: The regulatory docket ID if present (e.g., "CDC-2025-0024", "EPA-HQ-OAR-2023-0001")  
- agency: The government agency (e.g., "CDC", "EPA", "FDA")
- brief_description: A brief 1-2 sentence description of what this regulation is about

Example output:
{
  "regulation_name": "ACIP Vaccine Recommendations Meeting",
  "docket_id": "CDC-2025-0024", 
  "agency": "CDC",
  "brief_description": "Public comments on ACIP committee recommendations for vaccine policy."
}"""

    user_prompt = f"""Analyze this regulatory comment data and extract metadata:

Columns: {', '.join(columns)}

Sample data (first 3 rows):
{json.dumps(sample_rows, indent=2)}

Extract the regulation name, docket ID, agency, and brief description. Focus on creating a clear, descriptive regulation name that would make sense as a report title.

Return only the JSON metadata."""

    try:
        response = litellm.completion(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.1
        )
        
        result_text = response.choices[0].message.content.strip()
        
        # Clean up the response (remove markdown formatting)
        if result_text.startswith('```json'):
            result_text = result_text.replace('```json', '').replace('```', '').strip()
        elif result_text.startswith('```'):
            result_text = result_text.replace('```', '').strip()
        
        metadata = json.loads(result_text)
        logger.info(f"Extracted metadata: {metadata['regulation_name']}")
        return metadata
        
    except Exception as e:
        logger.error(f"Failed to extract regulation metadata: {e}")
        return {
            "regulation_name": "Regulation Comments Analysis", 
            "docket_id": "",
            "agency": "",
            "brief_description": ""
        }

def detect_csv_columns(csv_file: str, model: str = "gpt-4o-mini") -> Dict[str, str]:
    """Use LLM to detect CSV column mappings."""
    logger.info(f"Detecting CSV column structure in {csv_file} using {model}")
    
    # Read first 5 rows of data
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        columns = reader.fieldnames or []
        sample_rows = []
        for i, row in enumerate(reader):
            if i >= 5:
                break
            sample_rows.append(row)
    
    logger.info(f"Found {len(columns)} columns, analyzing first {len(sample_rows)} rows")
    
    # Create sample data for LLM analysis
    sample_data = {
        "columns": columns,
        "sample_rows": sample_rows
    }
    
    system_prompt = """You are analyzing a CSV file to identify which columns contain specific types of data.

You need to identify columns for these required fields:
- text: The main comment/text content (required)
- id: Unique identifier for each comment (required) 
- date: Date/timestamp when comment was submitted
- first_name: Submitter's first name (or combined name field)
- last_name: Submitter's last name
- organization: Organization/company name
- attachment_files: Files attached to comments (often contains URLs or file paths)

Return a JSON object mapping field names to column names. Only include mappings for columns that actually exist. If you can't find a good match for a field, omit it from the response.

Example output:
{
  "text": "Comment",
  "id": "Document ID", 
  "date": "Posted Date",
  "first_name": "First Name",
  "last_name": "Last Name",
  "organization": "Organization Name",
  "attachment_files": "Content Files"
}"""

    user_prompt = f"""Analyze this CSV data structure and identify the column mappings:

Columns: {', '.join(columns)}

Sample data (first 5 rows):
{json.dumps(sample_data['sample_rows'], indent=2)}

Identify which columns contain:
1. text: Main comment/text content (REQUIRED - this is the most important field)
2. id: Unique identifier (REQUIRED)
3. date: Submission date/timestamp
4. first_name: Submitter first name
5. last_name: Submitter last name  
6. organization: Organization/company name
7. attachment_files: File attachments (URLs, file paths, etc.)

Return only the JSON mapping."""

    try:
        response = litellm.completion(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            response_format={"type": "json_object"},
            timeout=60
        )
        
        result = json.loads(response.choices[0].message.content)
        
        # Validate that we found the required fields
        if 'text' not in result:
            logger.error("LLM failed to identify text column")
            return {}
        
        if 'id' not in result:
            logger.error("LLM failed to identify ID column")
            return {}
            
        logger.info(f"✅ LLM detected column mappings: {result}")
        return result
        
    except Exception as e:
        logger.error(f"LLM column detection failed: {e}")
        # Fallback to rule-based detection
        return _fallback_column_detection(columns)

def _fallback_column_detection(columns: List[str]) -> Dict[str, str]:
    """Fallback rule-based column detection."""
    logger.info("Using fallback rule-based column detection")
    
    column_mapping = {}
    
    # Comment text field (most important)
    text_candidates = ['Comment', 'comment', 'comment_text', 'text', 'Text', 'Comment Text', 'content', 'Content']
    text_col = next((col for col in columns if col in text_candidates), None)
    if text_col:
        column_mapping['text'] = text_col
        logger.info(f"Found text column: '{text_col}'")
    else:
        # Fuzzy match for text-like columns
        text_col = next((col for col in columns if any(keyword in col.lower() for keyword in ['comment', 'text', 'content'])), None)
        if text_col:
            column_mapping['text'] = text_col
            logger.info(f"Found text column (fuzzy match): '{text_col}'")
    
    # ID field
    id_candidates = ['Document ID', 'document_id', 'id', 'ID', 'comment_id', 'Comment ID', 'tracking_number', 'Tracking Number']
    id_col = next((col for col in columns if col in id_candidates), None)
    if id_col:
        column_mapping['id'] = id_col
        logger.info(f"Found ID column: '{id_col}'")
    
    # Date field
    date_candidates = ['Posted Date', 'posted_date', 'date', 'Date', 'submission_date', 'Received Date', 'received_date']
    date_col = next((col for col in columns if col in date_candidates), None)
    if date_col:
        column_mapping['date'] = date_col
        logger.info(f"Found date column: '{date_col}'")
    
    # Submitter name fields
    first_name_candidates = ['First Name', 'first_name', 'firstName', 'submitter_first']
    last_name_candidates = ['Last Name', 'last_name', 'lastName', 'submitter_last']
    
    first_name_col = next((col for col in columns if col in first_name_candidates), None)
    last_name_col = next((col for col in columns if col in last_name_candidates), None)
    
    if first_name_col:
        column_mapping['first_name'] = first_name_col
        logger.info(f"Found first name column: '{first_name_col}'")
    if last_name_col:
        column_mapping['last_name'] = last_name_col
        logger.info(f"Found last name column: '{last_name_col}'")
    
    # Organization field
    org_candidates = ['Organization Name', 'organization', 'Organization', 'org', 'company', 'Company']
    org_col = next((col for col in columns if col in org_candidates), None)
    if org_col:
        column_mapping['organization'] = org_col
        logger.info(f"Found organization column: '{org_col}'")
    
    # Attachment files
    attachment_candidates = ['Content Files', 'Attachment Files', 'attachments', 'files', 'attachment_files']
    attachment_col = next((col for col in columns if col in attachment_candidates), None)
    if attachment_col:
        column_mapping['attachment_files'] = attachment_col
        logger.info(f"Found attachment column: '{attachment_col}'")
    
    if not column_mapping.get('text'):
        logger.error("Critical: No text/comment column found!")
        logger.info(f"Available columns: {', '.join(columns)}")
    
    return column_mapping

# Theme discovery functions removed - now manually create analyzer_config.json

def main():
    parser = argparse.ArgumentParser(description='CSV column detection tool')
    parser.add_argument('--model', type=str, default='gpt-4o-mini', help='LLM model to use')
    
    args = parser.parse_args()
    
    try:
        csv_file = 'comments.csv'
        
        # Detect CSV column mappings
        logger.info("🔍 Detecting CSV column mappings...")
        column_mapping = detect_csv_columns(csv_file, args.model)
        
        # Extract regulation metadata
        logger.info("📋 Extracting regulation metadata...")
        regulation_metadata = extract_regulation_metadata(csv_file, args.model)
        
        if column_mapping:
            print("\n" + "="*60)
            print("CSV COLUMN MAPPINGS DETECTED")
            print("="*60)
            for field, column in column_mapping.items():
                print(f"  {field:15} -> '{column}'")
            print("="*60)
            
            print("\n" + "="*60)
            print("REGULATION METADATA DETECTED")
            print("="*60)
            print(f"  Name: {regulation_metadata['regulation_name']}")
            print(f"  Docket: {regulation_metadata['docket_id']}")
            print(f"  Agency: {regulation_metadata['agency']}")
            print(f"  Description: {regulation_metadata['brief_description']}")
            print("="*60)
            
            # Save column mapping to a separate file
            with open('column_mapping.json', 'w', encoding='utf-8') as f:
                json.dump(column_mapping, f, indent=2, ensure_ascii=False)
            logger.info("✅ Column mappings saved to column_mapping.json")
            
            # Save regulation metadata 
            with open('regulation_metadata.json', 'w', encoding='utf-8') as f:
                json.dump(regulation_metadata, f, indent=2, ensure_ascii=False)
            logger.info("✅ Regulation metadata saved to regulation_metadata.json")
            
            print("\nNext steps:")
            print("1. Review and edit column_mapping.json if needed")
            print("2. Review regulation_metadata.json for report titles")
            print("3. Create analyzer_config.json with your regulation-specific configuration")
            print("4. Run the pipeline: python pipeline.py --csv comments.csv")
        else:
            logger.error("❌ Failed to detect column mappings")
            return
        
        logger.info("✅ Column detection finished!")
        
    except Exception as e:
        logger.error(f"Column detection failed: {e}")
        raise

if __name__ == "__main__":
    main()