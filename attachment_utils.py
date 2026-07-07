#!/usr/bin/env python3
"""
Utility functions for handling attachments in comments.
Used by both pipeline.py and discover_stances.py
"""

import os
import re
import base64
import mimetypes
import logging
import requests
from typing import Dict, Any, Tuple, Optional
from dotenv import load_dotenv
import litellm
litellm.drop_params = True  # drop params a model does not support (e.g. temperature on GPT-5 reasoning models)

load_dotenv()

logger = logging.getLogger(__name__)


def is_gibberish(text: str, min_words: int = 3, min_english_ratio: float = 0.10) -> bool:
    """Detect if extracted text is gibberish/unreadable.

    Checks:
    1. Too few real words
    2. Low ratio of common English words
    3. High ratio of non-ASCII characters
    """
    if not text or len(text.strip()) < 10:
        return True

    # Check non-ASCII ratio
    ascii_chars = sum(1 for c in text if ord(c) < 128)
    if len(text) > 0 and ascii_chars / len(text) < 0.5:
        logger.warning(f"Gibberish detected: {ascii_chars / len(text):.0%} ASCII in '{text[:60]}...'")
        return True

    # Split into words (alphabetic sequences of 2+ chars)
    words = re.findall(r'[a-zA-Z]{2,}', text)
    if len(words) < min_words:
        logger.warning(f"Gibberish detected: only {len(words)} words in '{text[:60]}...'")
        return True

    # Check against common English words (top ~100 + domain terms)
    common = {
        'the', 'be', 'to', 'of', 'and', 'a', 'in', 'that', 'have', 'it',
        'for', 'not', 'on', 'with', 'he', 'as', 'you', 'do', 'at', 'this',
        'but', 'his', 'by', 'from', 'they', 'we', 'her', 'she', 'or', 'an',
        'will', 'my', 'one', 'all', 'would', 'there', 'their', 'what', 'so',
        'if', 'about', 'who', 'which', 'when', 'can', 'no', 'was', 'are',
        'is', 'am', 'has', 'been', 'were', 'had', 'did', 'does', 'its',
        'also', 'than', 'other', 'into', 'could', 'may', 'after', 'use',
        'two', 'how', 'our', 'work', 'first', 'well', 'way', 'even', 'new',
        'because', 'any', 'these', 'give', 'most', 'us', 'should', 'need',
        'said', 'each', 'tell', 'does', 'set', 'three', 'want', 'still',
        'own', 'make', 'made', 'just', 'over', 'such', 'take', 'only',
        'some', 'very', 'then', 'them', 'same', 'being', 'many', 'those',
        'must', 'before', 'between', 'more', 'through', 'under', 'against',
        'law', 'rule', 'state', 'federal', 'attorney', 'bar', 'comment',
        'proposed', 'public', 'department', 'justice', 'oppose', 'support',
        'cases', 'special', 'agent', 'reported', 'misconduct', 'actions',
        'court', 'legal', 'government', 'investigation', 'oversight',
        'amendment', 'section', 'act', 'authority', 'conduct', 'rights',
        'general', 'ethical', 'professional', 'independent', 'system',
        'united', 'states', 'people', 'country', 'citizens', 'american',
    }
    lower_words = [w.lower() for w in words]
    english_count = sum(1 for w in lower_words if w in common)
    ratio = english_count / len(lower_words) if lower_words else 0

    if ratio < min_english_ratio:
        logger.warning(f"Gibberish detected: {ratio:.0%} common words in '{text[:60]}...'")
        return True

    return False


def extract_text_with_gemini(file_path: str) -> str:
    """Extract text from images using OpenAI vision via LiteLLM.

    NOTE: The function name is legacy (formerly used Google Gemini multimodal).
    The implementation now uses OpenAI's gpt-5.4-mini vision through LiteLLM.
    Only image files are OCR'd here; PDFs and other types are skipped
    (text-based PDFs are handled by the PyPDF2 path in extract_text_from_file).
    """
    openai_api_key = os.getenv("OPENAI_API_KEY")
    if not openai_api_key:
        logger.debug("OPENAI_API_KEY not found, skipping vision extraction")
        return ""

    # Check file size (skip large files)
    file_size = os.path.getsize(file_path)
    if file_size > 5 * 1024 * 1024:  # 5MB limit
        logger.warning(f"File too large for vision extraction: {file_path}")
        return ""

    # Determine MIME type; only images are supported by this path
    mime, _ = mimetypes.guess_type(file_path)
    image_mimes = {
        "image/png", "image/jpeg", "image/gif", "image/webp",
    }
    if mime not in image_mimes:
        logger.debug(
            f"Skipping vision extraction for non-image file {file_path} "
            f"(mime={mime}); PDFs/text handled by other extraction paths"
        )
        return ""

    try:
        with open(file_path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("utf-8")

        resp = litellm.completion(
            model="gpt-5.4-mini",
            messages=[{"role": "user", "content": [
                {"type": "text", "text": "Extract all text from this document. Return only the raw text content. If there is no readable text, return exactly the word EMPTY and nothing else."},
                {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{b64}"}},
            ]}],
            temperature=0.0,
        )
        text = resp.choices[0].message.content or ""
        text = text.strip()

        # Check for the EMPTY sentinel we asked for (or empty response)
        if not text or text.upper() == 'EMPTY':
            logger.info(f"Vision extraction found no text in {file_path}")
            return ""

        if is_gibberish(text):
            logger.warning(f"Vision extraction returned gibberish for {file_path}, discarding")
            return ""
        return text

    except Exception as e:
        logger.warning(f"Vision extraction failed for {file_path}: {e}")
        return ""

def download_attachment(attachment_url: str, output_path: str) -> bool:
    """Download an attachment file."""
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'}
        response = requests.get(attachment_url, stream=True, timeout=30, headers=headers)
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
    
    text = ""
    if file_path.lower().endswith('.pdf'):
        try:
            parts = []
            with open(file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                for page_num in range(len(pdf_reader.pages)):
                    page = pdf_reader.pages[page_num]
                    parts.append(page.extract_text())
            text = '\n'.join(parts)
        except Exception as e:
            logger.error(f"Failed to extract text from PDF {file_path}: {e}")
            return ""

    elif file_path.lower().endswith(('.doc', '.docx')):
        try:
            doc = docx.Document(file_path)
            text = '\n'.join([paragraph.text for paragraph in doc.paragraphs])
        except Exception as e:
            logger.error(f"Failed to extract text from DOC {file_path}: {e}")
            return ""
    
    elif file_path.lower().endswith('.txt'):
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                text = file.read()
        except Exception as e:
            logger.error(f"Failed to read text file {file_path}: {e}")
            return ""

    elif file_path.lower().endswith(('.html', '.htm')):
        try:
            from bs4 import BeautifulSoup
            with open(file_path, 'r', encoding='utf-8') as file:
                soup = BeautifulSoup(file.read(), 'html.parser')
                for script in soup(["script", "style"]):
                    script.decompose()
                text = soup.get_text()
        except Exception as e:
            logger.error(f"Failed to extract text from HTML {file_path}: {e}")
            return ""

    else:
        logger.warning(f"Unsupported file type: {file_path}")
        return ""

    # Check for gibberish before returning
    if is_gibberish(text):
        logger.warning(f"Gibberish detected in local extraction of {file_path}, discarding")
        return ""
    return text

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
        
        # Save extracted text for future use (even if empty, to avoid re-processing)
        os.makedirs(os.path.dirname(text_cache_path), exist_ok=True)
        try:
            with open(text_cache_path, 'w', encoding='utf-8') as f:
                f.write(text or "")
            logger.info(f"  Cached extracted text to {text_cache_path}")
        except Exception as e:
            logger.warning(f"  Failed to save text cache: {e}")

        if not text or not text.strip():
            logger.warning(f"  No text extracted from {filename}")
            processing_status["failed"] += 1
            processing_status["failures"].append({"filename": filename, "reason": "no_text_extracted"})
            continue
        
        combined_attachment_text.append(text.strip())
        logger.info(f"  SUCCESS: Extracted {len(text)} characters from {filename}")
        logger.info(f"  First 100 chars: {text.strip()[:100]}...")
        processing_status["processed"] += 1
    
    logger.info(f"=== ATTACHMENT PROCESSING COMPLETE FOR {comment_id} ===")
    logger.info(f"  Status: {processing_status}")
    logger.info(f"  Total text extracted: {len(''.join(combined_attachment_text))} characters")
    
    return "\n\n--- ATTACHMENT ---\n\n".join(combined_attachment_text), processing_status