#!/usr/bin/env python3
"""
Stance Discovery Script

Analyzes a sample of comments to discover the main arguments/positions
people are taking, then generates a complete configuration for analysis.
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
        if os.path.exists('column_mapping.json'):
            with open('column_mapping.json', 'r', encoding='utf-8') as f:
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

def discover_stances(comments: List[Dict[str, Any]], model: str = "gpt-4.1") -> DiscoveredStances:
    """Use LLM to discover main stances/arguments from comments."""
    logger.info(f"Analyzing {len(comments)} comments to discover stances using {model}")
    
    # Prepare text sample for analysis
    comment_texts = []
    for i, comment in enumerate(comments[:150]):  # First 150 for better position discovery
        text = comment['text'][:500]  # Truncate very long comments
        comment_texts.append(f"Comment {i+1}: {text}")
    
    combined_text = "\n\n".join(comment_texts)
    
    system_prompt = """You are analyzing public comments to identify the main POSITIONS people are taking - specifically whether they SUPPORT or OPPOSE different aspects of the regulation/policy.

Your task is to:
1. Identify 5-7 distinct POSITIONS that people are taking (support/opposition stances)
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

    user_prompt = f"""Analyze these public comments and identify the main POSITIONS people are taking (what they SUPPORT or OPPOSE):

{combined_text}

For each position, provide:
1. Name: Clear support/opposition label (e.g., "Support for X", "Opposition to Y")
2. Indicators: Specific phrases, keywords, concepts, or argument patterns that signal someone holds this position

Also identify:
- What regulation/issue this is about

Focus on creating actionable indicators that an AI can use to detect these support/opposition positions in new comments. Each position should be clearly about being FOR or AGAINST something specific."""

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
        logger.info(f"Raw response: {response_content[:500]}...")
        
        result_data = json.loads(response_content)
        logger.info(f"Parsed data keys: {list(result_data.keys())}")
        
        # Check if the response is wrapped in a schema structure
        if 'properties' in result_data and 'description' in result_data:
            # Extract the actual data from the properties field
            actual_data = result_data['properties']
            logger.info(f"Found schema wrapper, extracting properties: {list(actual_data.keys())}")
            result_data = actual_data
        
        result = DiscoveredStances(**result_data)
        
        logger.info(f"✅ Discovered {len(result.stances)} stances")
        return result
        
    except Exception as e:
        logger.error(f"Stance discovery failed: {e}")
        if 'response_content' in locals():
            logger.error(f"Response content: {response_content}")
        raise

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
    
    # Remove Theme enum entirely
    theme_pattern = r'class Theme\(str, Enum\):\n(?:    .*\n)*'
    if re.search(theme_pattern, content):
        content = re.sub(theme_pattern, "", content)
    
    # Now update the stance_options list in create_regulation_analyzer
    # Build the new list definitions
    stance_list = '    stance_options = [\n' + ',\n'.join([f'        "{stance}"' for stance in config.stance_options]) + '\n    ]'
    
    # Replace stance_options list
    stance_list_pattern = r'    stance_options = \[[\s\S]*?\]'
    if re.search(stance_list_pattern, content):
        content = re.sub(stance_list_pattern, stance_list, content, count=1)
    
    # Remove theme_options list
    theme_list_pattern = r'    theme_options = \[[\s\S]*?\]'
    if re.search(theme_list_pattern, content):
        content = re.sub(theme_list_pattern, "", content)
    
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

def main():
    parser = argparse.ArgumentParser(description='Discover stances and generate analyzer configuration')
    parser.add_argument('--sample', type=int, default=500, help='Number of comments to analyze (default: 500)')
    parser.add_argument('--model', type=str, default='gpt-4.1', help='LLM model to use (default: gpt-4.1 for better position analysis)')
    parser.add_argument('--output', type=str, default='analyzer_config.json', help='Output configuration file')
    
    args = parser.parse_args()
    
    try:
        csv_file = 'comments.csv'
        
        # Load comments sample
        logger.info("📊 Loading comment sample...")
        comments = load_comments_sample(csv_file, args.sample)
        
        if not comments:
            logger.error("No comments with text content found")
            return
        
        # Discover stances
        logger.info("🔍 Discovering positions and stances...")
        discovered = discover_stances(comments, args.model)
        
        # Generate configuration
        logger.info("⚙️ Generating analyzer configuration...")
        config = generate_analyzer_config(discovered)
        
        # Save configuration
        save_config(config, args.output)
        
        # Update comment_analyzer.py with enums
        update_comment_analyzer(config)
        
        # Display results
        print_results(discovered, config)
        
        logger.info("✅ Stance discovery complete!")
        
    except Exception as e:
        logger.error(f"Stance discovery failed: {e}")
        raise

if __name__ == "__main__":
    main()