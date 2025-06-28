#!/usr/bin/env python3
"""
Stance Discovery Experiment Script

Tests different prompting strategies to discover stances/arguments.
Compares results across different target numbers and prompt approaches.
"""

import argparse
import csv
import json
import random
import logging
import os
from typing import List, Dict, Any
from pydantic import BaseModel, Field
import litellm
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DiscoveredStances(BaseModel):
    """Model for discovered positions/stances in comments."""
    stances: List[Dict[str, Any]] = Field(
        description="List of 5-7 main POSITIONS people are taking (supporting or opposing specific aspects). Each should have 'name' (clear support/opposition label) and 'indicators' (specific phrases, keywords, or arguments that signal someone holds this position)"
    )
    regulation_name: str = Field(
        description="A concise name for what these comments are about"
    )
    regulation_description: str = Field(
        description="Brief 1-2 sentence description of the issue/regulation"
    )

class CommentAnalyzerConfig(BaseModel):
    """Complete configuration for comment analyzer."""
    regulation_name: str
    regulation_description: str
    stance_options: List[str] = Field(
        description="List of stance names that can be selected"
    )
    stance_indicators: Dict[str, str] = Field(
        description="Mapping of stance names to their detection indicators"
    )
    system_prompt: str

def load_comments_sample(csv_file: str, sample_size: int = 500) -> List[Dict[str, Any]]:
    """Load a random sample of comments from CSV file."""
    logger.info(f"Loading comments from {csv_file}")
    
    # Try to load column mappings if available
    column_mapping = {}
    try:
        if os.path.exists('../column_mapping.json'):
            with open('../column_mapping.json', 'r', encoding='utf-8') as f:
                column_mapping = json.load(f)
            logger.info("Using column mappings from column_mapping.json")
    except:
        pass
    
    comments = []
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        all_comments = list(reader)
    
    # Take random sample
    if len(all_comments) > sample_size:
        sampled_comments = random.sample(all_comments, sample_size)
        logger.info(f"Sampled {sample_size} comments from {len(all_comments)} total")
    else:
        sampled_comments = all_comments
        logger.info(f"Using all {len(all_comments)} comments (less than sample size of {sample_size})")
    
    # Convert to our format using column mappings if available
    for comment in sampled_comments:
        # Get text field using column mapping or fallback
        text_col = column_mapping.get('text', 'Comment')
        text = comment.get(text_col, '') or comment.get('Comment', '')
        
        if text.strip():
            comments.append({
                'text': text,
                'id': comment.get(column_mapping.get('id', 'Document ID'), '')
            })
    
    logger.info(f"Loaded {len(comments)} comments with text content")
    return comments

def discover_stances_experimental(comments: List[Dict[str, Any]], model: str = "gpt-4.1", target_count: int = 6, prompt_strategy: str = "original") -> Dict[str, Any]:
    """Use LLM to discover main stances/arguments from comments with experimental prompting."""
    logger.info(f"Analyzing {len(comments)} comments using {prompt_strategy} strategy, targeting {target_count} stances")
    
    # Prepare text sample for analysis
    comment_texts = []
    for i, comment in enumerate(comments[:150]):  # First 150 for better position discovery
        text = comment['text'][:500]  # Truncate very long comments
        comment_texts.append(f"Comment {i+1}: {text}")
    
    combined_text = "\n\n".join(comment_texts)
    
    # Choose system prompt based on strategy
    if prompt_strategy == "original":
        system_prompt = f"""You are analyzing public comments to identify the main POSITIONS people are taking - specifically whether they SUPPORT or OPPOSE different aspects of the regulation/policy.

Your task is to:
1. Identify exactly {target_count} distinct POSITIONS that people are taking (support/opposition stances)
2. For each position, provide specific indicators that help identify when someone holds that stance
3. Determine what regulation/issue is being discussed

Focus on POSITIONS like:
- "Support for [specific policy aspect]" with indicators like: endorses the approach, calls for implementation, praises benefits
- "Opposition to [specific policy aspect]" with indicators like: criticizes the approach, calls for removal/changes, highlights problems
- "Support for stronger measures" with indicators like: current proposal insufficient, calls for more robust action
- "Opposition to regulatory overreach" with indicators like: government interference, burden on business, individual rights

Each position should:
- Be clearly framed as SUPPORT or OPPOSITION to something specific
- Have specific indicators: phrases, keywords, concepts, or argument patterns that signal this position
- Be distinct from other positions
- Be actually present in multiple comments
- Focus on what people are FOR or AGAINST, not just topics they mention

Output as structured JSON."""

    elif prompt_strategy == "mutually_exclusive":
        system_prompt = f"""You are analyzing public comments to identify the main POSITIONS people are taking - specifically whether they SUPPORT or OPPOSE different aspects of the regulation/policy.

IMPORTANT: Focus on identifying positions that are MUTUALLY EXCLUSIVE or at least distinct enough that individual commenters would typically hold only one of these positions. People who strongly support one position are unlikely to also strongly support an opposing position.

Your task is to:
1. Identify exactly {target_count} distinct POSITIONS that people are taking (support/opposition stances)
2. Ensure these positions represent different sides of debates or different priorities that don't typically overlap
3. For each position, provide specific indicators that help identify when someone holds that stance
4. Determine what regulation/issue is being discussed

Think of positions as representing different "camps" or "sides" in the debate:
- People in Camp A are unlikely to also be in Camp B if they're opposing positions
- People might have nuanced views, but typically align more strongly with one position
- Look for natural divisions and conflicts in the comments

Examples of mutually exclusive positions:
- "Support for immediate vaccine removal" vs "Support for continued vaccine access"
- "Trust in current committee" vs "Distrust in current committee leadership"
- "Prioritize individual choice" vs "Prioritize public health mandates"

Each position should:
- Represent a distinct viewpoint that commenters typically hold exclusively
- Be clearly framed as SUPPORT or OPPOSITION to something specific
- Have specific indicators: phrases, keywords, concepts, or argument patterns
- Be actually present in multiple comments
- Not significantly overlap with other identified positions

Output as structured JSON."""

    user_prompt = f"""Analyze these public comments and identify the main POSITIONS people are taking (what they SUPPORT or OPPOSE):

{combined_text}

For each position, provide:
1. Name: Clear support/opposition label (e.g., "Support for X", "Opposition to Y")
2. Indicators: Specific phrases, keywords, concepts, or argument patterns that signal someone holds this position

Also identify:
- What regulation/issue this is about

Focus on creating actionable indicators that an AI can use to detect these support/opposition positions in new comments."""

    try:
        response = litellm.completion(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "discovered_stances",
                    "schema": DiscoveredStances.model_json_schema()
                }
            },
            timeout=120
        )
        
        response_content = response.choices[0].message.content
        result_data = json.loads(response_content)
        
        # Check if the response is wrapped in a schema structure
        if 'properties' in result_data and 'description' in result_data:
            actual_data = result_data['properties']
            result_data = actual_data
        
        result = DiscoveredStances(**result_data)
        
        return {
            "strategy": prompt_strategy,
            "target_count": target_count,
            "actual_count": len(result.stances),
            "regulation_name": result.regulation_name,
            "regulation_description": result.regulation_description,
            "stances": [{"name": stance["name"], "indicators": stance["indicators"]} for stance in result.stances],
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Stance discovery failed for {prompt_strategy}/{target_count}: {e}")
        return {
            "strategy": prompt_strategy,
            "target_count": target_count,
            "actual_count": 0,
            "error": str(e),
            "timestamp": datetime.now().isoformat()
        }

def generate_analyzer_config(discovered: DiscoveredStances) -> CommentAnalyzerConfig:
    """Generate complete analyzer configuration from discovered stances."""
    
    # Extract stance names and create indicators
    stance_options = [stance['name'] for stance in discovered.stances]
    stance_indicators = {}
    
    # Convert indicators to strings if they're lists
    for stance in discovered.stances:
        name = stance['name']
        indicators = stance['indicators']
        if isinstance(indicators, list):
            # Join list items with semicolons
            stance_indicators[name] = "; ".join(indicators)
        else:
            stance_indicators[name] = indicators
    
    # Create system prompt
    stance_list = "\n".join([f"- {name}: {indicators}" for name, indicators in stance_indicators.items()])
    
    system_prompt = f"""You are analyzing public comments about {discovered.regulation_name}.

{discovered.regulation_description}

For each comment, identify:

1. Stances: Which of these positions/arguments does the commenter express? Look for the indicators listed below. (Select ALL that apply, or none if none apply)
{stance_list}

2. Key Quote: Select the most important quote (max 100 words) that best captures the essence of the comment. Must be verbatim from the text.

3. Rationale: Briefly explain (1-2 sentences) why you selected these stances.

Instructions:
- A comment may express multiple stances or no clear stance
- Only select stances that are clearly expressed in the comment
- Be objective and avoid inserting personal opinions"""

    return CommentAnalyzerConfig(
        regulation_name=discovered.regulation_name,
        regulation_description=discovered.regulation_description,
        stance_options=stance_options,
        stance_indicators=stance_indicators,
        system_prompt=system_prompt
    )

def save_config(config: CommentAnalyzerConfig, output_file: str = "analyzer_config.json"):
    """Save analyzer configuration to JSON file."""
    config_data = {
        "regulation_name": config.regulation_name,
        "regulation_description": config.regulation_description,
        "stance_options": config.stance_options,
        "stance_indicators": config.stance_indicators,
        "system_prompt": config.system_prompt,
        "generated_at": "2025-06-25"
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(config_data, f, indent=2, ensure_ascii=False)
    
    logger.info(f"✅ Configuration saved to {output_file}")

def update_comment_analyzer(config: CommentAnalyzerConfig):
    """Update comment_analyzer.py with discovered enums and lists."""
    import re
    
    # Read the current comment_analyzer.py
    with open('comment_analyzer.py', 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Create enum definitions
    stance_enum_items = []
    for i, stance in enumerate(config.stance_options):
        # Create valid enum names by replacing spaces and special chars
        enum_name = re.sub(r'[^A-Za-z0-9]+', '_', stance.upper()).strip('_')
        stance_enum_items.append(f'    {enum_name} = "{stance}"')
    
    # Build the new enum definitions
    stance_enum = "class Stance(str, Enum):\n" + "\n".join(stance_enum_items)
    
    # Replace existing enum definitions
    # First, find and replace Stance enum
    stance_pattern = r'class Stance\(str, Enum\):\n(?:    .*\n)*'
    if re.search(stance_pattern, content):
        content = re.sub(stance_pattern, stance_enum + "\n", content)
    else:
        # If no Stance enum exists, add it after imports
        import_end = content.find('\n\n', content.find('from typing import'))
        if import_end > 0:
            content = content[:import_end] + "\n\n" + stance_enum + "\n" + content[import_end:]
    
    
    # Now update the stance_options list in create_regulation_analyzer
    # Build the new list definitions
    stance_list = '    stance_options = [\n' + ',\n'.join([f'        "{stance}"' for stance in config.stance_options]) + '\n    ]'
    
    # Replace stance_options list
    stance_list_pattern = r'    stance_options = \[[\s\S]*?\]'
    if re.search(stance_list_pattern, content):
        content = re.sub(stance_list_pattern, stance_list, content, count=1)
    
    
    # Also update the system prompt in create_regulation_analyzer
    system_prompt_pattern = r'    system_prompt = """[\s\S]*?"""'
    if re.search(system_prompt_pattern, content):
        # Escape the system prompt for regex
        escaped_prompt = config.system_prompt.replace('\\', '\\\\').replace('"', '\\"')
        new_system_prompt = f'    system_prompt = """{config.system_prompt}"""'
        content = re.sub(system_prompt_pattern, new_system_prompt, content, count=1)
    
    # Write the updated content back
    with open('comment_analyzer.py', 'w', encoding='utf-8') as f:
        f.write(content)
    
    logger.info(f"✅ Updated comment_analyzer.py with {len(config.stance_options)} stances")

def print_results(discovered: DiscoveredStances, config: CommentAnalyzerConfig):
    """Print discovered positions and configuration."""
    print("\n" + "="*80)
    print("DISCOVERED POSITIONS AND STANCES")
    print("="*80)
    
    print(f"\nRegulation: {discovered.regulation_name}")
    print(f"Description: {discovered.regulation_description}")
    
    print(f"\nPositions/Stances ({len(discovered.stances)}):")
    for i, stance in enumerate(discovered.stances, 1):
        print(f"\n  {i}. {stance['name']}")
        indicators = stance['indicators']
        if isinstance(indicators, list):
            print(f"     Indicators:")
            for indicator in indicators:
                print(f"       • {indicator}")
        else:
            print(f"     Indicators: {indicators}")
    
    
    print("\n" + "="*80)
    print("CONFIGURATION GENERATED")
    print("="*80)
    print("✅ analyzer_config.json has been created")
    print("✅ comment_analyzer.py has been updated with enum definitions")
    print("✅ The comment analyzer will now identify support/opposition positions per comment")
    print("✅ Ready to run: python pipeline.py --csv comments.csv")

def generate_experiment_html(results: List[Dict[str, Any]], output_file: str = "stance_discovery_experiment.html"):
    """Generate HTML table showing experimental results."""
    
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Stance Discovery Experiment Results</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
            background: #f8f9fa;
        }}
        
        .header {{
            text-align: center;
            margin-bottom: 30px;
            padding: 20px;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        
        h1 {{
            color: #333;
            margin-bottom: 10px;
        }}
        
        .subtitle {{
            color: #666;
            font-size: 1.1em;
        }}
        
        .results-table {{
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            overflow: hidden;
            margin-bottom: 20px;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
        }}
        
        th {{
            background: #2c3e50;
            color: white;
            padding: 12px;
            text-align: left;
            font-weight: 600;
        }}
        
        td {{
            padding: 12px;
            border-bottom: 1px solid #dee2e6;
            vertical-align: top;
        }}
        
        tr:nth-child(even) {{
            background: #f8f9fa;
        }}
        
        .strategy-original {{
            background: #e3f2fd;
        }}
        
        .strategy-mutually_exclusive {{
            background: #f3e5f5;
        }}
        
        .stance-list {{
            font-size: 0.9em;
            line-height: 1.4;
        }}
        
        .stance-item {{
            margin-bottom: 8px;
            padding: 4px 8px;
            background: #ffffff;
            border-left: 3px solid #007bff;
            border-radius: 4px;
        }}
        
        .error {{
            color: #dc3545;
            font-style: italic;
        }}
        
        .timestamp {{
            font-size: 0.8em;
            color: #666;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>Stance Discovery Experiment Results</h1>
        <div class="subtitle">Comparing prompting strategies and target stance counts</div>
        <div class="timestamp">Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</div>
    </div>
    
    <div class="results-table">
        <table>
            <thead>
                <tr>
                    <th>Strategy</th>
                    <th>Target Count</th>
                    <th>Actual Count</th>
                    <th>Discovered Stances</th>
                </tr>
            </thead>
            <tbody>"""
    
    for result in results:
        strategy_class = f"strategy-{result['strategy']}"
        
        if 'error' in result:
            stances_html = f'<div class="error">Error: {result["error"]}</div>'
        else:
            stance_items = []
            for stance in result.get('stances', []):
                stance_items.append(f'<div class="stance-item">{stance["name"]}</div>')
            stances_html = f'<div class="stance-list">{"".join(stance_items)}</div>'
        
        html_content += f"""
                <tr class="{strategy_class}">
                    <td><strong>{result['strategy'].replace('_', ' ').title()}</strong></td>
                    <td>{result['target_count']}</td>
                    <td>{result['actual_count']}</td>
                    <td>{stances_html}</td>
                </tr>"""
    
    html_content += """
            </tbody>
        </table>
    </div>
</body>
</html>"""
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    logger.info(f"✅ HTML results saved to {output_file}")

def archive_old_files():
    """Move existing JSON and HTML files to archives subfolder."""
    import glob
    import shutil
    
    # Create archives directory if it doesn't exist
    archives_dir = "archives"
    os.makedirs(archives_dir, exist_ok=True)
    
    # Find files to archive
    json_files = glob.glob("stance_discovery_experiment_*.json")
    html_files = glob.glob("stance_discovery_experiment_*.html")
    
    files_moved = 0
    for file_path in json_files + html_files:
        try:
            shutil.move(file_path, os.path.join(archives_dir, file_path))
            files_moved += 1
            logger.info(f"Moved {file_path} to archives/")
        except Exception as e:
            logger.warning(f"Failed to move {file_path}: {e}")
    
    if files_moved > 0:
        logger.info(f"📁 Archived {files_moved} old experiment files")
    else:
        logger.info("📁 No old experiment files to archive")

def main():
    parser = argparse.ArgumentParser(description='Run stance discovery experiments')
    parser.add_argument('--sample', type=int, default=500, help='Number of comments to analyze (default: 500)')
    parser.add_argument('--model', type=str, default='gpt-4.1', help='LLM model to use (default: gpt-4.1)')
    parser.add_argument('--counts', nargs='+', type=int, default=[5, 6, 7, 8, 9, 10], 
                       help='Target stance counts to test (default: 5 6 7 8 9 10)')
    parser.add_argument('--strategies', nargs='+', choices=['original', 'mutually_exclusive'], 
                       default=['original', 'mutually_exclusive'],
                       help='Strategies to test (default: original mutually_exclusive)')
    
    args = parser.parse_args()
    
    try:
        # Archive old files before starting
        archive_old_files()
        csv_file = '../comments.csv'
        
        # Load comments sample
        logger.info("📊 Loading comment sample...")
        comments = load_comments_sample(csv_file, args.sample)
        
        if not comments:
            logger.error("No comments with text content found")
            return
        
        # Run experiments
        target_counts = args.counts
        strategies = args.strategies
        
        all_results = []
        
        logger.info("🧪 Starting experiments...")
        for strategy in strategies:
            for target_count in target_counts:
                logger.info(f"Running {strategy} strategy with target count {target_count}")
                
                result = discover_stances_experimental(
                    comments=comments,
                    model=args.model,
                    target_count=target_count,
                    prompt_strategy=strategy
                )
                
                all_results.append(result)
                
                # Log summary
                if 'error' in result:
                    logger.warning(f"❌ {strategy}/{target_count}: Error - {result['error']}")
                else:
                    logger.info(f"✅ {strategy}/{target_count}: Found {result['actual_count']} stances")
        
        # Save results to JSON
        json_output = f"stance_discovery_experiment_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(json_output, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        
        logger.info(f"✅ JSON results saved to {json_output}")
        
        # Generate HTML table
        html_output = f"stance_discovery_experiment_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        generate_experiment_html(all_results, html_output)
        
        # Print summary
        print("\n" + "="*80)
        print("EXPERIMENT SUMMARY")
        print("="*80)
        print(f"Total experiments run: {len(all_results)}")
        print(f"Strategies tested: {', '.join(strategies)}")
        print(f"Target counts tested: {', '.join(map(str, target_counts))}")
        print(f"Results saved to: {json_output}")
        print(f"HTML table saved to: {html_output}")
        
        successful_results = [r for r in all_results if 'error' not in r]
        if successful_results:
            avg_count = sum(r['actual_count'] for r in successful_results) / len(successful_results)
            print(f"Average stances discovered: {avg_count:.1f}")
        
        print("="*80)
        
    except Exception as e:
        logger.error(f"Experiment failed: {e}")
        raise

if __name__ == "__main__":
    main()