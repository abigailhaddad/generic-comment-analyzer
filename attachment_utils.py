#!/usr/bin/env python3
"""
Utility functions for handling attachments in comments.
Used by both pipeline.py and discover_stances.py
"""

import os
import logging
import requests
import base64
from typing import Dict, Any, Tuple, Optional

logger = logging.getLogger(__name__)

def extract_text_with_gemini(file_path: str) -> str:
    """Extract text using Gemini API for images and complex PDFs."""
    gemini_api_key = os.getenv("GEMINI_API_KEY")
    if not gemini_api_key:
        logger.debug("GEMINI_API_KEY not found, skipping Gemini extraction")
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
        logger.debug(f"Gemini extraction failed for {file_path}: {e}")
        return ""

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

def extract_text_from_file(file_path: str, use_gemini: bool = False) -> str:
    """Extract text from various file types."""
    import PyPDF2
    import docx
    
    # Try Gemini first if enabled and available
    if use_gemini:
        gemini_text = extract_text_with_gemini(file_path)
        if gemini_text:
            return gemini_text
    
    if file_path.lower().endswith('.pdf'):
        try:
            text = []
            with open(file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                for page_num in range(len(pdf_reader.pages)):
                    page = pdf_reader.pages[page_num]
                    text.append(page.extract_text())
            return '\n'.join(text)
        except Exception as e:
            logger.error(f"Failed to extract text from PDF {file_path}: {e}")
            return ""
    
    elif file_path.lower().endswith(('.doc', '.docx')):
        try:
            doc = docx.Document(file_path)
            return '\n'.join([paragraph.text for paragraph in doc.paragraphs])
        except Exception as e:
            logger.error(f"Failed to extract text from DOC {file_path}: {e}")
            return ""
    
    elif file_path.lower().endswith('.txt'):
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                return file.read()
        except Exception as e:
            logger.error(f"Failed to read text file {file_path}: {e}")
            return ""
    
    elif file_path.lower().endswith(('.html', '.htm')):
        try:
            from bs4 import BeautifulSoup
            with open(file_path, 'r', encoding='utf-8') as file:
                soup = BeautifulSoup(file.read(), 'html.parser')
                # Remove script and style elements
                for script in soup(["script", "style"]):
                    script.decompose()
                return soup.get_text()
        except Exception as e:
            logger.error(f"Failed to extract text from HTML {file_path}: {e}")
            return ""
    
    else:
        logger.warning(f"Unsupported file type: {file_path}")
        return ""

def process_attachments(comment_data: Dict[str, Any], attachments_dir: str, 
                       attachment_col: str = 'Attachment Files',
                       download_missing: bool = True,
                       use_gemini: bool = False) -> Tuple[str, Dict[str, Any]]:
    """
    Download and process attachments for a comment, return combined text and processing status.
    
    Args:
        comment_data: Dictionary containing comment data
        attachments_dir: Base directory for storing attachments
        attachment_col: Name of the column containing attachment URLs
        download_missing: Whether to download attachments that don't exist locally
        use_gemini: Whether to use Gemini API for text extraction (requires GEMINI_API_KEY)
    
    Returns:
        Tuple of (combined_text, processing_status)
    """
    comment_id = comment_data.get('Document ID', 'Unknown')
    logger.info(f"=== PROCESSING ATTACHMENTS FOR {comment_id} ===")
    
    if attachment_col not in comment_data or not comment_data[attachment_col]:
        logger.info(f"  No attachments found for {comment_id}")
        return "", {"total": 0, "processed": 0, "failed": 0, "failures": []}
    
    attachment_urls = comment_data[attachment_col].split(',')
    logger.info(f"  Found {len(attachment_urls)} attachment URLs")
    combined_attachment_text = []
    processing_status = {
        "total": len([url for url in attachment_urls if url.strip()]),
        "processed": 0,
        "failed": 0,
        "failures": []
    }
    
    # Create directory for this comment's attachments
    comment_id = (comment_data.get('Document ID') or 
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
        text_cache_path = os.path.join(comment_attachment_dir, f"{filename}.extracted.txt")
        
        # Check if we already have extracted text
        if os.path.exists(text_cache_path):
            try:
                logger.info(f"  Found cached text for {filename}, loading...")
                with open(text_cache_path, 'r', encoding='utf-8') as f:
                    text = f.read()
                if text.strip():
                    combined_attachment_text.append(text.strip())
                    logger.info(f"  SUCCESS: Loaded {len(text)} characters from cache")
                    processing_status["processed"] += 1
                continue
            except Exception as e:
                logger.warning(f"  Failed to load cached text: {e}")
        
        # Check if attachment file already exists
        if os.path.exists(file_path):
            logger.info(f"  Attachment {filename} already exists, skipping download")
        else:
            if not download_missing:
                logger.info(f"  Skipping download of {filename} (download_missing=False)")
                processing_status["failed"] += 1
                processing_status["failures"].append({"filename": filename, "reason": "skipped_download"})
                continue
                
            # Download attachment
            logger.info(f"  Downloading attachment: {filename}")
            if not download_attachment(url, file_path):
                processing_status["failed"] += 1
                processing_status["failures"].append({"filename": filename, "reason": "download_failed"})
                continue
        
        # Extract text from file
        logger.info(f"  Extracting text from {filename}...")
        text = extract_text_from_file(file_path, use_gemini=use_gemini)
        
        if not text or not text.strip():
            logger.warning(f"  No text extracted from {filename}")
            processing_status["failed"] += 1
            processing_status["failures"].append({"filename": filename, "reason": "no_text_extracted"})
            continue
        
        # Save extracted text for future use
        os.makedirs(os.path.dirname(text_cache_path), exist_ok=True)
        try:
            with open(text_cache_path, 'w', encoding='utf-8') as f:
                f.write(text)
            logger.info(f"  Cached extracted text to {text_cache_path}")
        except Exception as e:
            logger.warning(f"  Failed to save text cache: {e}")
        
        combined_attachment_text.append(text.strip())
        logger.info(f"  SUCCESS: Extracted {len(text)} characters from {filename}")
        logger.info(f"  First 100 chars: {text.strip()[:100]}...")
        processing_status["processed"] += 1
    
    logger.info(f"=== ATTACHMENT PROCESSING COMPLETE FOR {comment_id} ===")
    logger.info(f"  Status: {processing_status}")
    logger.info(f"  Total text extracted: {len(''.join(combined_attachment_text))} characters")
    
    return "\n\n--- ATTACHMENT ---\n\n".join(combined_attachment_text), processing_status