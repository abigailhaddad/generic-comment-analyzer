#!/usr/bin/env python3
"""
Test Discovered Stances Script

Runs a single stance discovery configuration and then tests it on a sample of comments,
generating a report showing how well the discovered stances work.
"""

import argparse
import json
import sys
import os
import logging
import time
from typing import List, Dict, Any
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Add parent directory to path to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from discover_stances_experiment import discover_stances_experimental
from comment_analyzer import CommentAnalyzer
from generate_report import generate_html, analyze_field_types, calculate_stats

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def create_analyzer_from_discovered_stances(discovered_stances: Dict[str, Any]) -> CommentAnalyzer:
    """Create a CommentAnalyzer instance from discovered stances."""
    
    logger.info("Creating analyzer with discovered stances...")
    logger.info(f"Creating analyzer for regulation: {discovered_stances['regulation_name']}")
    
    # Build stance options list
    stance_options = [stance["name"] for stance in discovered_stances["stances"]]
    logger.info(f"Stance options being configured: {stance_options}")
    
    # Build stance indicators text for system prompt
    stance_indicators_text = []
    for stance in discovered_stances["stances"]:
        indicators = stance["indicators"]
        if isinstance(indicators, list):
            indicators_str = "; ".join(indicators)
        else:
            indicators_str = indicators
        stance_indicators_text.append(f"- {stance['name']}: {indicators_str}")
    
    # Create system prompt
    system_prompt = f"""You are analyzing public comments about {discovered_stances['regulation_name']}.

{discovered_stances['regulation_description']}

For each comment, identify:

1. Stances: Which of these positions/arguments does the commenter express? Look for the indicators listed below. (Select ALL that apply, or none if none apply)
{chr(10).join(stance_indicators_text)}

2. Key Quote: Select the most important quote (max 100 words) that best captures the essence of the comment. Must be verbatim from the text.

3. Rationale: Briefly explain (1-2 sentences) why you selected these stances.

Instructions:
- A comment may express multiple stances or no clear stance
- Only select stances that are clearly expressed in the comment
- Be objective and avoid inserting personal opinions"""
    
    logger.info("System prompt created (first 200 chars):")
    logger.info(system_prompt[:200] + "...")

    # Create temporary config
    import tempfile
    temp_config = {
        'regulation_name': discovered_stances['regulation_name'],
        'regulation_description': discovered_stances['regulation_description'],
        'stance_options': stance_options,
        'system_prompt': system_prompt
    }
    
    # Write temporary config file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(temp_config, f)
        temp_config_file = f.name
    
    # Change to temp directory to avoid loading parent analyzer_config.json
    original_cwd = os.getcwd()
    os.chdir('/tmp')
    
    try:
        # Create analyzer WITHOUT config file to avoid hardcoded enum constraints
        analyzer = CommentAnalyzer(
            model=os.getenv('LLM_MODEL', 'gpt-4o-mini'),
            timeout_seconds=120
        )
        
        # Manually override the configuration to bypass hardcoded enums
        analyzer.stance_options = stance_options
        analyzer.system_prompt = system_prompt
        analyzer.regulation_name = discovered_stances['regulation_name']
        analyzer.regulation_description = discovered_stances['regulation_description']
        
        # Store config data for parallel processing
        analyzer._config_data = temp_config
        
        logger.info(f"FIXED: Manually configured analyzer with stance options: {stance_options}")
        
        return analyzer
    finally:
        os.chdir(original_cwd)
        # Clean up temp file
        os.unlink(temp_config_file)


def analyze_single_comment_custom(stance_options: List[str], system_prompt: str, comment: Dict[str, Any], comment_index: int, truncate_chars: int = 2000) -> Dict[str, Any]:
    """Analyze a single comment using direct LLM call to bypass hardcoded enums."""
    import litellm
    
    try:
        # Truncate comment text if too long
        comment_text = comment['text']
        if len(comment_text) > truncate_chars:
            comment_text = comment_text[:truncate_chars]
            logger.debug(f"Truncated comment {comment_index} from {len(comment['text'])} to {truncate_chars} characters")
        
        # Direct LLM call without constrained response format
        user_prompt = f"""Analyze this comment and identify stances/positions:

{comment_text}

Respond in JSON format with:
- stances: list of stance names that apply (from the stance options in the system prompt)
- key_quote: most important quote from the comment (max 100 words)
- rationale: brief explanation of why these stances were selected
"""

        response = litellm.completion(
            model=os.getenv('LLM_MODEL', 'gpt-4o-mini'),
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            response_format={"type": "json_object"},
            timeout=120
        )
        
        # Parse the response
        import json
        result = json.loads(response.choices[0].message.content)
        
        analysis_result = {
            'stances': result.get('stances', []),
            'key_quote': result.get('key_quote', ''),
            'rationale': result.get('rationale', '')
        }
        
        # Create comment record preserving original data
        analyzed_comment = {
            'id': comment.get('id', f'test-{comment_index}'),
            'text': comment['text'],
            'comment_text': comment['text'],
            'date': comment.get('date', datetime.now().isoformat()),
            'submitter': comment.get('submitter', ''),  # Preserve original submitter data
            'organization': comment.get('organization', ''),  # Preserve original organization data
            'attachment_text': comment.get('attachment_text', ''),
            'duplication_count': comment.get('duplication_count', 1),
            'duplication_ratio': comment.get('duplication_ratio', 1),
            'analysis': analysis_result
        }
        
        return analyzed_comment
        
    except Exception as e:
        logger.error(f"Failed to analyze comment {comment_index}: {e}")
        return None

def analyze_comments_sample(comments: List[Dict[str, Any]], discovered_stances: Dict[str, Any], sample_size: int = 50, max_workers: int = 2, batch_size: int = 10) -> List[Dict[str, Any]]:
    """Analyze a sample of comments using parallel processing."""
    import random
    
    # Sample comments if needed
    if len(comments) > sample_size:
        sampled_comments = random.sample(comments, sample_size)
    else:
        sampled_comments = comments
    
    logger.info(f"Analyzing {len(sampled_comments)} comments with {max_workers} workers in batches of {batch_size}...")
    
    analyzed_comments = []
    
    # Process in batches to avoid overwhelming the API
    for batch_start in tqdm(range(0, len(sampled_comments), batch_size), desc="Processing batches", unit="batch"):
        batch_end = min(batch_start + batch_size, len(sampled_comments))
        batch_comments = sampled_comments[batch_start:batch_end]
        
        batch_results = []
        
        # Use ThreadPoolExecutor for parallel API calls
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Get stance configuration
            stance_options = [stance['name'] for stance in discovered_stances['stances']]
            
            # Build system prompt
            stance_indicators_text = []
            for stance in discovered_stances['stances']:
                indicators = stance['indicators']
                if isinstance(indicators, list):
                    indicators_str = "; ".join(indicators)
                else:
                    indicators_str = indicators
                stance_indicators_text.append(f"- {stance['name']}: {indicators_str}")
            
            system_prompt = f"""You are analyzing public comments about {discovered_stances['regulation_name']}.

{discovered_stances['regulation_description']}

For each comment, identify:

1. Stances: Which of these positions/arguments does the commenter express? Look for the indicators listed below. (Select ALL that apply, or none if none apply)
{chr(10).join(stance_indicators_text)}

2. Key Quote: Select the most important quote (max 100 words) that best captures the essence of the comment. Must be verbatim from the text.

3. Rationale: Briefly explain (1-2 sentences) why you selected these stances.

Instructions:
- A comment may express multiple stances or no clear stance
- Only select stances that are clearly expressed in the comment
- Be objective and avoid inserting personal opinions"""
            
            # Function to create analyzer for each worker (thread-safe)
            def create_analyzer():
                if config_data:
                    # Create analyzer without config file to avoid hardcoded enum constraints
                    original_cwd = os.getcwd()
                    os.chdir('/tmp')
                    
                    try:
                        analyzer = CommentAnalyzer(
                            model=os.getenv('LLM_MODEL', 'gpt-4o-mini'),
                            timeout_seconds=120
                        )
                        
                        # Manually configure to bypass hardcoded enums
                        analyzer.stance_options = config_data['stance_options']
                        analyzer.system_prompt = config_data['system_prompt']
                        analyzer.regulation_name = config_data['regulation_name']
                        analyzer.regulation_description = config_data['regulation_description']
                        
                        return analyzer
                    finally:
                        os.chdir(original_cwd)
                else:
                    return CommentAnalyzer(
                        model=os.getenv('LLM_MODEL', 'gpt-4o-mini'),
                        timeout_seconds=120
                    )
            
            # Submit all comments in this batch
            future_to_comment = {}
            for i, comment in enumerate(batch_comments):
                comment_index = batch_start + i
                future = executor.submit(analyze_single_comment_custom, stance_options, system_prompt, comment, comment_index)
                future_to_comment[future] = comment
            
            # Collect results as they complete
            with tqdm(total=len(batch_comments), desc=f"Batch {batch_start//batch_size + 1}", leave=False) as pbar:
                for future in as_completed(future_to_comment, timeout=180):  # 3 minute timeout per comment
                    try:
                        result = future.result(timeout=60)  # 1 minute timeout to get result
                        if result is not None:
                            batch_results.append(result)
                        pbar.update(1)
                    except Exception as e:
                        logger.error(f"Comment analysis failed or timed out: {e}")
                        pbar.update(1)
        
        analyzed_comments.extend(batch_results)
        
        # Brief pause between batches to be courteous to the API
        if batch_end < len(sampled_comments):
            time.sleep(2)  # Longer pause to avoid rate limits
    
    logger.info(f"Successfully analyzed {len(analyzed_comments)} comments")
    return analyzed_comments


def find_latest_experiment_json():
    """Find the most recent stance discovery experiment JSON file."""
    import glob
    
    json_files = glob.glob("stance_discovery_experiment_*.json")
    if not json_files:
        return None
    
    # Sort by modification time, most recent first
    json_files.sort(key=os.path.getmtime, reverse=True)
    return json_files[0]

def extract_discovered_stances_from_experiment(experiment_file: str, strategy: str, count: int) -> Dict[str, Any]:
    """Extract specific discovered stances from experiment results."""
    with open(experiment_file, 'r', encoding='utf-8') as f:
        all_results = json.load(f)
    
    # Find matching result
    for result in all_results:
        if result.get('strategy') == strategy and result.get('target_count') == count:
            if 'error' not in result:
                return result
            else:
                raise ValueError(f"Experiment with {strategy}/{count} had error: {result['error']}")
    
    raise ValueError(f"No successful experiment found for {strategy}/{count}")

def main():
    parser = argparse.ArgumentParser(description='Test discovered stances on comment sample')
    parser.add_argument('--strategy', type=str, default='mutually_exclusive', 
                       choices=['original', 'mutually_exclusive'],
                       help='Prompting strategy to use')
    parser.add_argument('--count', type=int, default=8, 
                       help='Target number of stances to discover')
    parser.add_argument('--analyze-sample', type=int, default=50,
                       help='Number of comments to analyze with discovered stances')
    parser.add_argument('--json-file', type=str, default=None,
                       help='Specific JSON file to use (defaults to most recent)')
    parser.add_argument('--csv-file', type=str, default='../comments.csv',
                       help='CSV file to use for loading comments (default: ../comments.csv)')
    
    args = parser.parse_args()
    
    try:
        # Find or use specified JSON file
        if args.json_file:
            experiment_file = args.json_file
        else:
            experiment_file = find_latest_experiment_json()
            if not experiment_file:
                logger.error("No experiment JSON files found. Run discover_stances_experiment.py first.")
                return
        
        logger.info(f"Using experiment results from: {experiment_file}")
        
        # Extract discovered stances for specified strategy/count
        logger.info(f"Looking for {args.strategy} strategy with {args.count} target stances...")
        discovered = extract_discovered_stances_from_experiment(experiment_file, args.strategy, args.count)
        
        logger.info(f"Found experiment: {discovered['actual_count']} stances discovered")
        logger.info(f"Regulation: {discovered['regulation_name']}")
        logger.info("Using the following stances:")
        for i, stance in enumerate(discovered['stances'], 1):
            logger.info(f"  {i}. {stance['name']}")
        logger.info(f"Regulation description: {discovered['regulation_description']}")
        
        # Load comments for testing with proper column mapping
        csv_file = args.csv_file
        logger.info(f"Loading comments for testing from {csv_file}...")
        from stance_analysis_pipeline import load_and_standardize_csv
        
        # Process attachments for a small sample to avoid long download times
        process_attachments = args.analyze_sample <= 10  # Only process attachments for small samples
        if process_attachments:
            logger.info("Small sample detected - will process attachments")
        
        all_comments = load_and_standardize_csv(csv_file, process_attachments, 'test_attachments')
        
        # Sample comments if needed (same logic as load_comments_sample)
        import random
        if len(all_comments) > args.analyze_sample * 2:
            comments = random.sample(all_comments, args.analyze_sample * 2)
        else:
            comments = all_comments
        
        if not comments:
            logger.error("No comments found")
            return
        
        # Create analyzer with discovered stances
        logger.info("Creating analyzer with discovered stances...")
        
        # Analyze test comments using updated function that preserves real data
        logger.info(f"Analyzing {args.analyze_sample} test comments...")
        analyzed_comments = analyze_comments_sample(comments, discovered, args.analyze_sample)
        
        if not analyzed_comments:
            logger.error("No comments were successfully analyzed")
            return
        
        logger.info(f"Successfully analyzed {len(analyzed_comments)} comments")
        
        # Generate report
        logger.info("Generating report...")
        
        # Analyze field types
        field_analysis = analyze_field_types(analyzed_comments)
        
        # Override stance information with discovered stances (fix for correct stance display)
        discovered_stance_names = [stance['name'] for stance in discovered['stances']]
        logger.info(f"DEBUG: Original field analysis stances: {field_analysis.get('stances', {}).get('unique_values', [])}")
        logger.info(f"DEBUG: Overriding field analysis with discovered stance names: {discovered_stance_names}")
        
        field_analysis['stances'] = {
            'type': 'checkbox',
            'is_list': True,
            'unique_values': discovered_stance_names,
            'count': len(discovered_stance_names)
        }
        
        # Always add new_stances to field_analysis (even if empty)
        all_new_stances = set()
        for comment in analyzed_comments:
            new_stances = comment.get('analysis', {}).get('new_stances', [])
            all_new_stances.update(new_stances)
        
        field_analysis['new_stances'] = {
            'type': 'checkbox',
            'is_list': True,
            'unique_values': list(all_new_stances),
            'count': len(all_new_stances)
        }
        logger.info(f"DEBUG: Found {len(all_new_stances)} new stances: {list(all_new_stances)}")
        
        logger.info(f"DEBUG: Final field analysis stances: {field_analysis['stances']['unique_values']}")
        
        # Also log what stances are actually in the analyzed comments
        logger.info("DEBUG: Checking stances found in analyzed comments:")
        for i, comment in enumerate(analyzed_comments[:10]):  # First 10 comments
            stances = comment.get('analysis', {}).get('stances', [])
            new_stances = comment.get('analysis', {}).get('new_stances', [])
            logger.info(f"  Comment {i+1} stances: {stances}")
            if new_stances:
                logger.info(f"  Comment {i+1} NEW stances: {new_stances}")
        
        # Also show unique values for key fields to help debug column names
        logger.info("DEBUG: Showing unique values from comments to identify correct field names:")
        
        # Get all unique submitter-like field values
        submitter_fields = ['submitter', 'first_name', 'last_name', 'author', 'name']
        for field in submitter_fields:
            unique_values = set()
            for comment in analyzed_comments:
                value = comment.get(field, '')
                if value and value.strip():
                    unique_values.add(value.strip())
            if unique_values:
                logger.info(f"  {field}: {list(unique_values)[:10]} (showing first 10)")
        
        # Get all unique organization field values
        org_fields = ['organization', 'organization_name', 'org', 'company']
        for field in org_fields:
            unique_values = set()
            for comment in analyzed_comments:
                value = comment.get(field, '')
                if value and value.strip():
                    unique_values.add(value.strip())
            if unique_values:
                logger.info(f"  {field}: {list(unique_values)[:10]} (showing first 10)")
        
        # Save analyzed comments to JSON for pipeline processing
        temp_json = f"temp_analyzed_comments_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(temp_json, 'w', encoding='utf-8') as f:
            json.dump(analyzed_comments, f, indent=2, ensure_ascii=False)
        
        # Use the pipeline to process data and generate report
        output_file = f"stance_test_report_{args.strategy}_{args.count}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        processed_json = f"processed_stance_data_{args.strategy}_{args.count}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # Run the pipeline
        from stance_analysis_pipeline import process_stance_analysis, generate_report_from_json
        
        # Process the data (calculates all statistics, co-occurrences, unusual combinations)
        processed_data = process_stance_analysis(analyzed_comments, processed_json)
        
        # Generate HTML from processed data
        generate_report_from_json(processed_json, output_file)
        
        # Clean up temp file
        os.unlink(temp_json)
        
        logger.info(f"✅ Report generated: {output_file}")
        
        # Print summary
        print("\n" + "="*80)
        print("STANCE DISCOVERY TEST SUMMARY")
        print("="*80)
        print(f"Strategy: {args.strategy}")
        print(f"Target stances: {args.count}")
        print(f"Actual stances discovered: {discovered['actual_count']}")
        print(f"Comments analyzed: {len(analyzed_comments)}")
        print(f"Report saved to: {output_file}")
        print("\nDiscovered stances:")
        for i, stance in enumerate(discovered['stances'], 1):
            print(f"  {i}. {stance['name']}")
        print("="*80)
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        raise


if __name__ == "__main__":
    main()