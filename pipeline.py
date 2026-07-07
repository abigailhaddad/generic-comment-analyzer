#!/usr/bin/env python3
"""
Generic Regulation Comment Analysis Pipeline

A simple pipeline for analyzing public comments on federal regulations.
Fetches comments, analyzes them with LLM, and stores results in PostgreSQL.

Usage: python pipeline.py --csv comments.csv [--sample N] [--model gemini-2.0-flash]
"""

import argparse
import json
import os
import csv
import re
import logging
import base64
import time
from pathlib import Path
from typing import List, Dict, Any, Optional

# Import attachment utilities
from attachment_utils import download_attachment, extract_text_from_file, process_attachments
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

# Load environment variables from the .env next to this script (robust to chdir)
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))

# Import the generic comment analyzer
from comment_analyzer import CommentAnalyzer

# Simple logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('pipeline.log', mode='w'),
    ],
)
logger = logging.getLogger(__name__)

def load_yaml_config():
    """Load full analyzer config from analyzer_config.yaml (or .json fallback)."""
    import yaml

    for config_file, loader in [('analyzer_config.yaml', yaml.safe_load), ('analyzer_config.json', json.load)]:
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r') as f:
                    config = loader(f)
                    logger.info(f"Loaded config from {config_file}")
                    return config
            except Exception as e:
                logger.warning(f"Failed to load config from {config_file}: {e}")

    logger.warning("No analyzer_config found, using defaults")
    return {}


def load_regulation_info():
    """Load regulation name and docket ID from analyzer config."""
    config = load_yaml_config()
    regulation_name = config.get('regulation_name', 'Unknown Regulation')
    docket_id = 'REG-2025-001'
    logger.info(f"Regulation: {regulation_name}")
    return regulation_name, docket_id


def load_regex_flags():
    """Load regex flag definitions from analyzer config.

    Returns a dict of {flag_name: [compiled_regex, ...]}.
    """
    config = load_yaml_config()
    regex_flags = config.get('regex_flags', {})
    compiled = {}
    for flag_name, flag_def in regex_flags.items():
        patterns = flag_def.get('patterns', []) if isinstance(flag_def, dict) else []
        compiled[flag_name] = [re.compile(p, re.IGNORECASE) for p in patterns]
    return compiled



def load_column_mapping() -> Dict[str, str]:
    """Load column mappings from config file."""
    # column_mapping.json is a shared regulations.gov schema — it lives next to the
    # code at the repo root, not in the per-regulation dir we chdir into.
    shared_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'column_mapping.json')
    try:
        if os.path.exists(shared_path):
            with open(shared_path, 'r', encoding='utf-8') as f:
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

def read_comments_from_csv(csv_file: str, limit: Optional[int] = None, sample_size: Optional[int] = None, random_seed: int = 42, use_gemini: bool = False) -> List[Dict[str, Any]]:
    """Read comments from CSV file and return as list of dicts."""
    logger.info(f"Reading comments from {csv_file}")
    
    # Set random seed for reproducibility
    random.seed(random_seed)
    logger.info(f"Using random seed: {random_seed} for reproducible sampling")
    
    # Load column mappings
    column_mapping = load_column_mapping()
    if not column_mapping:
        logger.error("No column mappings available")
        return []

    # Load regex flags from config
    regex_flags = load_regex_flags()
    
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
        logger.info(f"Sampling {sample_size} comments from {len(all_rows)} total")
        all_rows = random.sample(all_rows, sample_size)
    
    # Second pass: process the selected comments with attachments
    logger.info("Processing comments and downloading attachments...")
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
        attachment_status = None
        if has_attachments:
            logger.info(f"Processing attachments for comment {comment_id}")
            attachment_text, attachment_status = process_attachments(row, attachments_dir, attachment_col, use_gemini=use_gemini)
        
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
        
        # First check if there's a mapped submitter field
        if 'submitter' in column_mapping:
            submitter = row.get(column_mapping['submitter'], '').strip()
        
        # If no submitter found, try first/last name fields
        if not submitter:
            first_name_col = column_mapping.get('first_name', 'First Name')
            last_name_col = column_mapping.get('last_name', 'Last Name')
            
            first_name = row.get(first_name_col, '').strip()
            last_name = row.get(last_name_col, '').strip()
            
            if first_name or last_name:
                submitter = f"{first_name} {last_name}".strip()
            else:
                # Try other common submitter fields as fallback
                submitter = (row.get('Submitter Name', '') or 
                            row.get('submitter', '') or 
                            row.get('Author', ''))
        
        comment_data = {
            'id': comment_id,
            'text': full_text,
            'comment_text': comment_text,
            'attachment_text': attachment_text,
            'attachment_status': attachment_status,
            'submitter': submitter,
            'organization': row.get(column_mapping.get('organization', 'Organization Name'), ''),
            'date': row.get(column_mapping.get('date', 'Posted Date'), ''),
        }

        # Apply regex-based flags from config (no LLM needed)
        for flag_name, patterns in regex_flags.items():
            comment_data[flag_name] = any(p.search(full_text) for p in patterns)
        
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

def validate_extracted_quote(quote: str, source_text: str, threshold: float = 0.7) -> dict:
    """Check if an extracted quote actually appears in the source text.

    Returns dict with 'valid' bool and 'match_score' float.
    Uses longest common substring ratio as the match metric.
    """
    if not quote or not source_text:
        return {'valid': not bool(quote), 'match_score': 0.0}

    q = quote.lower().strip()
    s = source_text.lower()

    # Exact substring match
    if q in s:
        return {'valid': True, 'match_score': 1.0}

    # Longest common substring ratio
    m, n = len(q), len(s)
    if m == 0:
        return {'valid': True, 'match_score': 0.0}

    # Optimize: only search windows roughly the size of the quote
    best_len = 0
    for i in range(m):
        for j in range(n):
            length = 0
            while i + length < m and j + length < n and q[i + length] == s[j + length]:
                length += 1
            best_len = max(best_len, length)

    score = best_len / m
    return {'valid': score >= threshold, 'match_score': round(score, 3)}


def validate_analysis(analysis: dict, comment_text: str, submitter: str = '', organization: str = '') -> dict:
    """Validate extracted quotes in analysis results against source text + metadata."""
    if not analysis:
        return analysis

    # Validate political affiliation is an actual party
    VALID_PARTIES = {'Republican', 'Democrat', 'Independent', 'Libertarian', 'Green'}
    pol = analysis.get('political_affiliation', '')
    if pol and pol not in VALID_PARTIES:
        logger.warning(f"Invalid political affiliation '{pol}', clearing")
        analysis['political_affiliation'] = ''
        analysis['political_affiliation_quote'] = ''

    # Validate state is a real US state/DC abbreviation
    VALID_STATES = {'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC'}
    state = analysis.get('state_identified', '')
    if state and state not in VALID_STATES:
        logger.warning(f"Invalid state '{state}', clearing")
        analysis['state_identified'] = ''
        analysis['state_quote'] = ''

    combined_source = f"{submitter} {organization} {comment_text}"

    # Validate all quote fields against the combined text
    quote_fields = ['entity_name', 'state_quote', 'political_affiliation_quote', 'key_quote']
    for field in quote_fields:
        quote = analysis.get(field, '')
        if quote:
            result = validate_extracted_quote(quote, combined_source)
            analysis[f'{field}_match_score'] = result['match_score']
            if not result['valid']:
                logger.warning(f"Low match for {field}: score={result['match_score']:.2f}, quote='{quote[:80]}...'")

    return analysis


FALLBACK_MODEL = 'gpt-5.4-mini'  # stronger OpenAI model retried when the primary model errors


def analyze_single_comment(analyzer, comment, truncate_chars=None):
    """Analyze a single comment (for use in parallel processing).

    On failure, retries once with the stronger fallback model.
    """
    analysis_text = comment['text']
    if truncate_chars and len(analysis_text) > truncate_chars:
        analysis_text = analysis_text[:truncate_chars]

    organization = comment.get('organization', '')
    submitter = comment.get('submitter', '')

    try:
        analysis_result = analyzer.analyze(analysis_text,
                                         comment_id=comment['id'],
                                         organization=organization,
                                         submitter=submitter)
        analysis_result = validate_analysis(analysis_result, comment['text'],
                                           submitter=submitter, organization=organization)
        return {**comment, 'analysis': analysis_result, 'model_used': analyzer.model}

    except Exception as e:
        logger.warning(f"Primary model failed for {comment['id']}: {e}. Trying {FALLBACK_MODEL}...")

    # One retry with stronger model
    try:
        fallback = CommentAnalyzer(model=FALLBACK_MODEL, config_file='analyzer_config.yaml')
        analysis_result = fallback.analyze(analysis_text,
                                          comment_id=comment['id'],
                                          organization=organization,
                                          submitter=submitter)
        analysis_result = validate_analysis(analysis_result, comment['text'],
                                           submitter=submitter, organization=organization)
        logger.info(f"Fallback model succeeded for {comment['id']}")
        return {**comment, 'analysis': analysis_result, 'model_used': FALLBACK_MODEL}

    except Exception as e2:
        logger.error(f"Fallback model also failed for {comment['id']}: {e2}")
        return {**comment, 'analysis': None, 'analysis_error': str(e2), 'model_used': analyzer.model}

CHECKPOINT_FILE = '.analysis_checkpoint.jsonl'


def _checkpoint_key(comment: Dict[str, Any]) -> str:
    """Normalized comment text — the stable recovery key. Keying on text (not the
    dedup representative id) makes resume robust: the same comment content recovers
    regardless of which duplicate happened to be chosen as representative this run."""
    return (comment.get('text') or '').strip().lower()


def _load_checkpoint() -> Dict[str, Dict[str, Any]]:
    """Load previously checkpointed analysis results, keyed by normalized text."""
    results = {}
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            for line in f:
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                key = entry.get('text_key')
                if key:  # skip legacy id-only entries; the parquet snapshot covers those
                    results[key] = entry
        logger.info(f"Loaded {len(results)} results from checkpoint (keyed by text)")
    return results


def _append_checkpoint(results: List[Dict[str, Any]]):
    """Append batch results to checkpoint file, keyed by normalized text."""
    with open(CHECKPOINT_FILE, 'a') as f:
        for r in results:
            f.write(json.dumps({
                'text_key': _checkpoint_key(r),
                'id': r['id'],
                'analysis': r.get('analysis'),
                'analysis_error': r.get('analysis_error'),
            }) + '\n')


def analyze_comments_parallel(comments: List[Dict[str, Any]], model: str = "gemini-2.0-flash", truncate_chars: Optional[int] = None, max_workers: int = 8, batch_size: int = 50, output_file: Optional[str] = None, snapshot_every: int = 5) -> List[Dict[str, Any]]:
    """Analyze comments using parallel processing for much faster LLM calls."""
    logger.info(f"Analyzing {len(comments)} comments with {model}")
    logger.info(f"Using {max_workers} parallel workers, batch size {batch_size}")
    if truncate_chars:
        logger.info(f"Truncating text to {truncate_chars} characters for LLM analysis")

    # Load checkpoint to skip already-analyzed comments
    checkpoint = _load_checkpoint()
    already_done = []
    still_needed = []
    for comment in comments:
        key = _checkpoint_key(comment)
        if key in checkpoint:
            cp = checkpoint[key]
            comment['analysis'] = cp.get('analysis')
            if cp.get('analysis_error'):
                comment['analysis_error'] = cp['analysis_error']
            already_done.append(comment)
        else:
            still_needed.append(comment)

    if already_done:
        logger.info(f"Recovered {len(already_done)} results from checkpoint, {len(still_needed)} remaining")

    analyzed_comments = list(already_done)
    total_comments = len(still_needed)

    if total_comments == 0:
        logger.info("All comments already analyzed (from checkpoint)")
        return analyzed_comments

    # Create overall progress bar
    with tqdm(total=total_comments, desc="Analyzing comments", unit="comment") as overall_pbar:
        # Process in batches to avoid overwhelming the API
        for batch_start in range(0, len(still_needed), batch_size):
            batch_end = min(batch_start + batch_size, len(still_needed))
            batch_comments = still_needed[batch_start:batch_end]

            # Use ThreadPoolExecutor for parallel API calls
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Create analyzer for each worker (thread-safe)
                def create_analyzer():
                    # Use analyzer_config.json from current directory
                    return CommentAnalyzer(model=model, config_file='analyzer_config.yaml')

                # Submit all comments in this batch
                future_to_comment = {}
                for comment in batch_comments:
                    analyzer = create_analyzer()
                    future = executor.submit(analyze_single_comment, analyzer, comment, truncate_chars)
                    future_to_comment[future] = comment

                # Collect results as they complete
                batch_results = []
                for future in as_completed(future_to_comment):
                    result = future.result()
                    batch_results.append(result)
                    overall_pbar.update(1)  # Update overall progress bar

                # Maintain original order within batch
                comment_id_to_result = {result['id']: result for result in batch_results}
                ordered_results = [comment_id_to_result[comment['id']] for comment in batch_comments]
                analyzed_comments.extend(ordered_results)

                # Save checkpoint after each batch
                _append_checkpoint(ordered_results)

                # Log running progress and periodically write an inspectable parquet
                # snapshot so results can be viewed / the report regenerated mid-run.
                batch_num = batch_start // batch_size + 1
                done = len(analyzed_comments)
                logger.info(f"Batch {batch_num}: {done}/{len(comments)} analyzed so far")
                # Write to a SEPARATE inspection file — never the output parquet, which
                # is also the cross-run reuse source and must not be clobbered mid-run.
                if output_file and batch_num % snapshot_every == 0:
                    snapshot_file = output_file.replace('.parquet', '.inprogress.parquet')
                    try:
                        save_results(analyzed_comments, snapshot_file)
                        logger.info(f"Snapshot written: {done} comments -> {snapshot_file}")
                    except Exception as e:
                        logger.warning(f"Snapshot write failed: {e}")

                # Brief pause between batches to be respectful to API
                if batch_end < len(still_needed):
                    time.sleep(0.1)

    logger.info(f"Completed analysis of {len(analyzed_comments)} comments")
    return analyzed_comments

def analyze_comments(comments: List[Dict[str, Any]], model: str = "gemini-2.0-flash", truncate_chars: Optional[int] = None, parallel: bool = True) -> List[Dict[str, Any]]:
    """Analyze comments using the LLM with optional parallel processing."""
    if parallel and len(comments) > 5:
        # Use parallel processing for better performance
        return analyze_comments_parallel(comments, model, truncate_chars)
    else:
        # Fall back to sequential processing for small batches or if parallel is disabled
        logger.info(f"Analyzing {len(comments)} comments with {model} (sequential)")
        if truncate_chars:
            logger.info(f"Truncating text to {truncate_chars} characters for LLM analysis")
        
        # Initialize analyzer using configuration file from current directory
        analyzer = CommentAnalyzer(model=model, config_file='analyzer_config.yaml')
        
        analyzed_comments = []
        
        # Use tqdm for progress bar
        for comment in tqdm(comments, desc="Analyzing comments", unit="comment"):
            result = analyze_single_comment(analyzer, comment, truncate_chars)
            analyzed_comments.append(result)
        
        return analyzed_comments

def detect_campaigns(comments: List[Dict[str, Any]], threshold: float = 0.45, min_campaign_size: int = 5) -> List[Dict[str, Any]]:
    """Detect form letter campaigns using MinHash LSH on 5-gram Jaccard similarity.

    Assigns campaign_id and campaign_size to each comment. Comments not in any
    campaign get campaign_id=None.
    """
    from datasketch import MinHash, MinHashLSH

    logger.info(f"Detecting form letter campaigns (threshold={threshold}, min_size={min_campaign_size})")

    NUM_PERM = 128
    lsh = MinHashLSH(threshold=threshold, num_perm=NUM_PERM)
    minhashes = {}
    idx_to_comment = {}

    def normalize(text):
        text = re.sub(r'[^a-z0-9 ]', '', text.lower())
        return re.sub(r'\s+', ' ', text).strip()

    # Build MinHash signatures from 5-grams
    for i, comment in enumerate(comments):
        text = normalize(comment.get('text', '') or '')
        words = text.split()
        if len(words) < 5:
            continue

        shingles = set(tuple(words[j:j+5]) for j in range(len(words) - 4))
        m = MinHash(num_perm=NUM_PERM)
        for s in shingles:
            m.update(' '.join(s).encode('utf-8'))

        minhashes[i] = m
        idx_to_comment[i] = comment
        try:
            lsh.insert(str(i), m)
        except ValueError:
            pass  # duplicate minhash

    logger.info(f"Built MinHash signatures for {len(minhashes)} comments")

    # Query to find clusters
    visited = set()
    campaigns = []

    for idx, m in minhashes.items():
        if idx in visited:
            continue
        result = lsh.query(m)
        cluster = [int(r) for r in result]
        for c in cluster:
            visited.add(c)
        if len(cluster) >= min_campaign_size:
            campaigns.append(cluster)

    campaigns.sort(key=len, reverse=True)
    logger.info(f"Found {len(campaigns)} campaigns with {min_campaign_size}+ members")

    # Build lookup: comment index -> campaign info
    idx_to_campaign = {}
    for campaign_id, cluster in enumerate(campaigns):
        # Find the most common text in this cluster (the "canonical" version)
        from collections import Counter
        text_counts = Counter()
        for idx in cluster:
            ct = (comments[idx].get('comment_text', '') or '').strip()
            text_counts[ct] += 1
        canonical_text = text_counts.most_common(1)[0][0] if text_counts else ''

        for idx in cluster:
            idx_to_campaign[idx] = {
                'campaign_id': campaign_id,
                'campaign_size': len(cluster),
                'campaign_canonical': canonical_text,
            }

    # Apply to comments
    total_in_campaigns = 0
    for i, comment in enumerate(comments):
        if i in idx_to_campaign:
            comment['campaign_id'] = idx_to_campaign[i]['campaign_id']
            comment['campaign_size'] = idx_to_campaign[i]['campaign_size']
            comment['campaign_canonical'] = idx_to_campaign[i]['campaign_canonical']
            total_in_campaigns += 1
        else:
            comment['campaign_id'] = None
            comment['campaign_size'] = None
            comment['campaign_canonical'] = None

    logger.info(f"Tagged {total_in_campaigns} comments across {len(campaigns)} campaigns")
    logger.info(f"Remaining unique comments: {len(comments) - total_in_campaigns}")

    return comments


def cluster_families(comments: List[Dict[str, Any]], threshold: float = 0.3) -> List[Dict[str, Any]]:
    """Cluster campaigns into letter families using MinHash LSH on 5-gram Jaccard similarity.

    Re-clusters from scratch each run so new campaigns are absorbed or form new families.
    Family ID = campaign_id of the largest campaign in the family (stable representative).
    """
    from datasketch import MinHash, MinHashLSH

    NUM_PERM = 128

    # Build per-campaign data
    campaign_data = {}
    for c in comments:
        cid = c.get('campaign_id')
        if cid is None:
            continue
        if cid not in campaign_data:
            canonical = c.get('campaign_canonical')
            campaign_data[cid] = {'size': 0, 'canonical': canonical if isinstance(canonical, str) else ''}
        campaign_data[cid]['size'] += 1

    if not campaign_data:
        for c in comments:
            c['family_id'] = None
            c['family_label'] = None
        return comments

    def normalize(text):
        return re.sub(r'[^a-z0-9 ]', '', text.lower()).strip()

    def make_minhash(text):
        words = normalize(text).split()
        m = MinHash(num_perm=NUM_PERM)
        for shingle in (tuple(words[j:j+5]) for j in range(max(1, len(words) - 4))):
            m.update(' '.join(shingle).encode('utf-8'))
        return m

    cids = list(campaign_data.keys())
    minhashes = {cid: make_minhash(campaign_data[cid]['canonical']) for cid in cids}

    lsh = MinHashLSH(threshold=threshold, num_perm=NUM_PERM)
    for cid, m in minhashes.items():
        try:
            lsh.insert(str(cid), m)
        except ValueError:
            pass

    # Union-find using LSH queries
    parent = {cid: cid for cid in cids}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
        px, py = find(x), find(y)
        if px != py:
            parent[px] = py

    for cid, m in minhashes.items():
        for match in lsh.query(m):
            union(cid, float(match))

    # Build families: family_id = campaign_id of largest member
    families = {}
    for cid in cids:
        root = find(cid)
        if root not in families:
            families[root] = []
        families[root].append(cid)

    family_rep = {}
    for root, members in families.items():
        rep = max(members, key=lambda cid: campaign_data[cid]['size'])
        label_words = normalize(campaign_data[rep]['canonical']).split()
        label = ' '.join(label_words[:8]) if label_words else f'campaign-{rep}'
        family_rep[root] = {'family_id': rep, 'family_label': label}

    cid_to_family = {cid: family_rep[find(cid)] for cid in cids}

    logger.info(f"Clustered {len(cids)} campaigns into {len(families)} letter families")

    for c in comments:
        cid = c.get('campaign_id')
        if cid is not None and cid in cid_to_family:
            c['family_id'] = cid_to_family[cid]['family_id']
            c['family_label'] = cid_to_family[cid]['family_label']
        else:
            c['family_id'] = None
            c['family_label'] = None

    return comments


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

def check_database_status(regulation_name: str):
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

def store_in_postgres_from_parquet(parquet_file: str, regulation_name: str, docket_id: str):
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
                'gemini-2.0-flash',  # TODO: get from args
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
    parser.add_argument('--regulation', type=str, help='Regulation slug under regulations/<slug>/. The pipeline chdirs into it so config, source CSV, attachments, and outputs all resolve there.')
    parser.add_argument('--csv', type=str, default=None, help='Path to comments CSV file (default: source.csv in the regulation dir)')
    parser.add_argument('--output', type=str, default=None, help='Output Parquet file (default: full_run.parquet in the regulation dir)')
    parser.add_argument('--sample', type=int, help='Process only N random comments for testing')
    parser.add_argument('--model', type=str, default='gpt-5.4-nano', help='LLM model to use (LiteLLM model string, e.g. gpt-4o-mini)')
    parser.add_argument('--truncate', type=int, default=50000, help='Truncate comment text to N characters before LLM analysis (default: 50000)')
    parser.add_argument('--to-database', action='store_true', help='Store results in PostgreSQL database (requires DATABASE_URL in .env)')
    parser.add_argument('--workers', type=int, default=8, help='Number of parallel workers for LLM calls (default: 8)')
    parser.add_argument('--batch-size', type=int, default=50, help='Batch size for parallel processing (default: 50)')
    parser.add_argument('--no-parallel', action='store_true', help='Disable parallel processing (use sequential)')
    parser.add_argument('--use-gemini', action='store_true', help='Use a vision LLM (OpenAI) for attachment image OCR (requires OPENAI_API_KEY)')
    parser.add_argument('--no-verify', action='store_true', help='Skip the second-pass stance/entity verification step')
    parser.add_argument('--reprocess', action='store_true', help='Reprocess all comments even if output file exists (default: incremental)')

    args = parser.parse_args()

    # Resolve the regulation working directory. All config/data/output paths are
    # relative to it, so we chdir in and let the bare-relative reads/writes land there.
    if args.regulation:
        reg_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'regulations', args.regulation)
        if not os.path.isdir(reg_dir):
            raise SystemExit(f"Regulation directory not found: {reg_dir}")
        os.chdir(reg_dir)
        logger.info(f"Working in regulation directory: {reg_dir}")
    if args.csv is None:
        args.csv = 'source.csv'
    if args.output is None:
        args.output = 'full_run.parquet'

    try:
        # Load regulation info from config
        regulation_name, docket_id = load_regulation_info()
        
        # Check database status early if database storage is requested
        if args.to_database:
            logger.info("=== DATABASE CHECK ===")
            if not check_database_status(regulation_name):
                logger.info("Exiting due to database check cancellation")
                return
        
        # Step 1: Read comments from CSV with attachments (sampling applied inside)
        logger.info("=== STEP 1: Loading Comments ===")
        comments = read_comments_from_csv(args.csv, sample_size=args.sample, use_gemini=args.use_gemini)
        
        # Step 2: Create deduplication table
        logger.info("=== STEP 2: Creating Deduplication Table ===")
        unique_comments, duplicate_mapping = create_dedup_table(comments)
        
        # Step 3: Analyze only unique comments (incremental if output exists)
        logger.info("=== STEP 3: Analyzing Unique Comments ===")

        # Load previous results for incremental mode
        previous_results = {}
        if not args.reprocess and os.path.exists(args.output):
            try:
                prev_df = pd.read_parquet(args.output)
                for _, row in prev_df.iterrows():
                    text_key = (row.get('text', '') or '').strip().lower()
                    previous_results[text_key] = row.to_dict()
                logger.info(f"Loaded {len(previous_results)} previously analyzed results from {args.output}")
            except Exception as e:
                logger.warning(f"Could not load previous results: {e}")

        if previous_results:
            # Split unique comments into already-analyzed and new
            new_comments = []
            reused_comments = []
            for comment in unique_comments:
                text_key = comment['text'].strip().lower()
                if text_key in previous_results:
                    prev = previous_results[text_key]
                    comment['analysis'] = prev.get('analysis')
                    reused_comments.append(comment)
                else:
                    new_comments.append(comment)

            logger.info(f"Reusing {len(reused_comments)} previously analyzed comments")
            logger.info(f"Analyzing {len(new_comments)} new comments")

            if new_comments:
                if args.no_parallel:
                    new_analyzed = analyze_comments(new_comments, args.model, args.truncate, parallel=False)
                else:
                    new_analyzed = analyze_comments_parallel(new_comments, args.model, args.truncate, args.workers, args.batch_size, output_file=args.output)
            else:
                new_analyzed = []

            unique_analyzed_comments = reused_comments + new_analyzed
        else:
            if args.no_parallel:
                unique_analyzed_comments = analyze_comments(unique_comments, args.model, args.truncate, parallel=False)
            else:
                unique_analyzed_comments = analyze_comments_parallel(unique_comments, args.model, args.truncate, args.workers, args.batch_size, output_file=args.output)

        # Step 4: Merge analysis results back to full dataset
        logger.info("=== STEP 4: Merging Results ===")
        analyzed_comments = merge_analysis_results(unique_analyzed_comments, duplicate_mapping)

        # Save after merge so LLM work is never lost
        logger.info("=== Saving intermediate results ===")
        save_results(analyzed_comments, args.output)
        # Clean up checkpoint now that results are saved
        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
            logger.info("Cleaned up analysis checkpoint")

        # Step 5: Verify ambiguous stance classifications with stronger model
        if args.no_verify:
            logger.info("=== STEP 5: Stance Verification (skipped: --no-verify) ===")
        else:
            logger.info("=== STEP 5: Stance Verification ===")
            from verify_stances import verify_stances
            analyzed_comments = verify_stances(analyzed_comments)

        # Step 6: Detect form letter campaigns
        logger.info("=== STEP 6: Campaign Detection ===")
        analyzed_comments = detect_campaigns(analyzed_comments)

        # Step 6b: Cluster campaigns into letter families
        analyzed_comments = cluster_families(analyzed_comments)

        # Step 7: Save final results
        logger.info("=== STEP 7: Saving Final Results ===")
        save_results(analyzed_comments, args.output)
        
        # Step 7: Store in PostgreSQL
        if args.to_database:
            logger.info("=== STEP 7: Database Storage ===")
            store_in_postgres_from_parquet(args.output, regulation_name, docket_id)
        else:
            logger.info("=== STEP 7: Skipping Database Storage ===")
            logger.info("Use --to-database flag to store in PostgreSQL")

        # Generate HTML report
        logger.info("=== STEP 8: Generating HTML Report ===")
        try:
            from generate_report import load_results_parquet, generate_html

            html_output = "index.html"
            logger.info(f"Loading results from {args.output}...")
            comments = load_results_parquet(args.output)

            logger.info(f"Generating HTML report: {html_output}")
            generate_html(comments, {}, {}, html_output)
            
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