#!/usr/bin/env python3
"""
Theme-Based Stance Discovery Script

Analyzes a sample of comments to discover the main themes and positions
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
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DiscoveredThemes(BaseModel):
    """Model for discovered themes and positions in comments."""
    themes: List[Dict[str, Any]] = Field(
        description="List of main THEMES (topics) being discussed. Each theme should have 'name' (topic name), 'description' (brief description), and 'positions' (list of 2-3 positions/stances people take on this theme). Each position should have 'name' (clear support/opposition label) and 'indicators' (specific phrases, keywords, or arguments that signal someone holds this position)"
    )
    regulation_name: str = Field(
        description="A concise name for what these comments are about"
    )
    regulation_description: str = Field(
        description="Brief 1-2 sentence description of the issue/regulation"
    )
    entity_types: List[str] = Field(
        description="List of types of entities submitting comments (e.g., 'Individual', 'Healthcare Provider', 'Insurance Company', 'Technology Company', etc.)"
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
    entity_types: List[str] = Field(
        description="List of entity types that are submitting comments"
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
                'id': comment.get(column_mapping.get('id', 'Document ID'), ''),
                'submitter': comment.get(column_mapping.get('submitter', 'Title'), ''),
                'organization': comment.get(column_mapping.get('organization', 'Organization Name'), '')
            })
    
    logger.info(f"Loaded {len(comments)} comments with text content")
    return comments

def discover_themes_experimental(comments: List[Dict[str, Any]], model: str = "gpt-4.1", target_count: int = 6, prompt_strategy: str = "original") -> Dict[str, Any]:
    """Use LLM to discover main themes and positions from comments with experimental prompting."""
    logger.info(f"Analyzing {len(comments)} comments using {prompt_strategy} strategy, targeting {target_count} themes")
    
    # Prepare text sample for analysis
    comment_texts = []
    for i, comment in enumerate(comments[:150]):  # First 150 for better position discovery
        org = comment.get('organization', '').strip()
        submitter = comment.get('submitter', '').strip()
        
        # Build comment header with submitter info
        header_parts = [f"Comment {i+1}"]
        if org:
            header_parts.append(f"Organization: {org}")
        if submitter:
            header_parts.append(f"Submitter: {submitter}")
        
        header = " | ".join(header_parts)
        full_entry = f"{header}: {comment['text']}"
        
        # Truncate the full entry (header + text) to 1000 characters
        if len(full_entry) > 1000:
            full_entry = full_entry[:1000] + "..."
            
        comment_texts.append(full_entry)
    
    combined_text = "\n\n".join(comment_texts)
    
    # Choose system prompt based on strategy
    if prompt_strategy == "original":
        system_prompt = f"""You are analyzing public comments to identify the main THEMES (topics) being discussed and the POSITIONS people take on each theme.

Your task is to:
1. Identify exactly {target_count} distinct THEMES (topics/subjects) that people are discussing
2. For each theme, identify 2-3 distinct POSITIONS that people are taking (support/opposition stances)
3. For each position, provide specific indicators that help identify when someone holds that stance
4. Determine what regulation/issue is being discussed
5. Identify the types of entities submitting comments based on organization names, titles, AND content of comments (e.g., when they say "as a physician" or "our bank")

Themes are topics like:
- "Vaccine safety and efficacy"
- "Regulatory oversight and authority" 
- "Individual choice and rights"
- "Economic impact"

Positions are stances people take on themes like:
- "Support maintaining universal vaccine access for all age groups" with indicators: endorses safety data, supports availability, emphasizes public health benefits
- "Oppose vaccine mandates based on personal freedom concerns" with indicators: emphasizes individual choice, questions government authority, cites bodily autonomy
- "Support stricter safety monitoring and transparency requirements" with indicators: calls for more regulation, wants additional safeguards, demands full disclosure

Each theme should:
- Be a clear topic/subject area that people discuss
- Have 2-3 distinct positions that people actually take on this theme
- Be actually present in multiple comments

Each position should:
- Be DETAILED and SPECIFIC - not just "Support X" but "Support X because of Y reason/context"
- Include the core argument or reasoning in the position name itself
- Be 8-15 words long to capture the nuance of the position
- Examples of good detailed positions:
  - "Support removing COVID vaccines due to safety concerns and adverse event reports"
  - "Oppose political interference in ACIP's science-based decision making process"
  - "Support maintaining vaccine access to protect immunocompromised and vulnerable populations"
- Be clearly framed as SUPPORT or OPPOSITION to something specific within the theme
- NEVER have two "Support for" positions that are opposites - use "Support for X" vs "Oppose X" instead
- Have specific indicators: phrases, keywords, concepts, or argument patterns
- Be clearly distinct from other positions within the same theme

Output as structured JSON."""

    elif prompt_strategy == "mutually_exclusive":
        system_prompt = f"""You are analyzing public comments to identify the main THEMES (topics) being discussed and the POSITIONS people take on each theme.

IMPORTANT: For each theme, create genuinely OPPOSING positions that represent different viewpoints, even if one viewpoint is not well-represented in the sample.

Your task is to:
1. Identify UP TO {target_count} distinct THEMES (topics/subjects) that people are discussing
2. For each theme, create EXACTLY 2 positions (maximum 3 only if there's a genuine third viewpoint) that represent opposing viewpoints
3. For each position, provide specific indicators (these can be hypothetical for underrepresented positions)
4. Ensure positions are truly opposing, not just reworded versions of the same stance
5. Identify the types of entities submitting comments based on organization names, titles, AND content of comments (e.g., when they say "as a physician" or "our bank")

CRITICAL RULES for creating opposing positions:
1. Positions must represent fundamentally DIFFERENT viewpoints, not the same view with different emphasis
2. BAD example (these are the same position):
   - "Support protecting environmental regulations from industry influence"
   - "Oppose weakening environmental protections"
   These both support environmental protection - they're the same position!

3. GOOD example (these are genuinely opposing):
   - "Support stricter environmental regulations for public health"
   - "Support reducing regulations to promote business growth"
   These represent different priorities and viewpoints!

4. Another GOOD example:
   - "Support mandatory vaccination requirements for public safety"
   - "Support individual choice in medical decisions"

For each theme:
- Create positions that someone could reasonably hold
- Even if 99% of comments oppose something, create a position representing why someone might support it
- Base opposing positions on reasonable policy arguments, even if not in your sample

Each position should:
- Represent a distinct policy preference or priority
- Include plausible indicators (what someone with this view might say)
- Be clearly differentiated from other positions on the theme

Output as structured JSON."""

    user_prompt = f"""Analyze these public comments and identify the main THEMES (topics) and POSITIONS people are taking:

{combined_text}

For each theme, provide:
1. Name: Clear topic name (e.g., "Vaccine Safety", "Regulatory Authority")
2. Description: Brief description of what this theme covers
3. Positions: 2-3 distinct positions people take on this theme

For each position within a theme, provide:
1. Name: Clear support/opposition label (e.g., "Support for X", "Oppose X", "Agree with Y", "Disagree with Y")
   IMPORTANT: If you have opposing positions, make one "Support" and one "Oppose" - NEVER two "Support for" statements that contradict each other
2. Indicators: Specific phrases, keywords, concepts, or argument patterns that signal someone holds this position

Also identify:
- What regulation/issue this is about
- What types of entities are submitting comments (look for clues in organization names, submitter titles, AND the comment text itself)
  Examples: Individual/Citizen, Healthcare Provider, Insurance Company, Technology Company, 
  Professional Association, Advocacy Group, Government Agency, Academic Institution, etc.
  Look for phrases like "As a small business owner", "Our hospital", "I am a patient", "We are a trade association", etc.
  Be specific to the regulation domain (e.g., for health regulations: Hospital System, Medical Device Manufacturer, Pharmacy, etc.)

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
                    "name": "discovered_themes",
                    "schema": DiscoveredThemes.model_json_schema()
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
        
        # Log what we got for debugging
        logger.info(f"Response includes entity_types: {'entity_types' in result_data}")
        if 'entity_types' in result_data:
            logger.info(f"Entity types found: {result_data['entity_types']}")
        else:
            logger.warning("No entity_types in response! Keys found: " + str(list(result_data.keys())))
        
        result = DiscoveredThemes(**result_data)
        
        # Debug: Check if entity_types made it to the model
        logger.info(f"DiscoveredThemes object has {len(result.entity_types)} entity types")
        
        # Flatten themes and positions for backward compatibility
        all_positions = []
        for theme in result.themes:
            for position in theme.get('positions', []):
                all_positions.append({
                    "name": position["name"],
                    "indicators": position["indicators"],
                    "theme": theme["name"]
                })
        
        return {
            "strategy": prompt_strategy,
            "target_count": target_count,
            "actual_count": len(result.themes),
            "regulation_name": result.regulation_name,
            "regulation_description": result.regulation_description,
            "themes": result.themes,
            "entity_types": result.entity_types,
            "positions": all_positions,  # For backward compatibility
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

def generate_analyzer_config(discovered: DiscoveredThemes) -> CommentAnalyzerConfig:
    """Generate complete analyzer configuration from discovered stances."""
    
    # Extract position names and create indicators from all themes
    stance_options = []
    stance_indicators = {}
    
    # Flatten positions from all themes
    for theme in discovered.themes:
        for position in theme.get('positions', []):
            name = position['name']
            indicators = position['indicators']
            stance_options.append(name)
            
            if isinstance(indicators, list):
                # Join list items with semicolons
                stance_indicators[name] = "; ".join(indicators)
            else:
                stance_indicators[name] = indicators
    
    # Create system prompt
    stance_list = "\n".join([f"- {name}: {indicators}" for name, indicators in stance_indicators.items()])
    entity_list = "\n".join([f"- {entity}" for entity in discovered.entity_types])
    
    system_prompt = f"""You are analyzing public comments about {discovered.regulation_name}.

{discovered.regulation_description}

For each comment, identify:

1. Stances: Which of these positions/arguments does the commenter express? Look for the indicators listed below. (Select ALL that apply, or none if none apply)
{stance_list}

2. Entity Type: Identify what type of entity is submitting this comment. Look for clues in the organization name, submitter title, and the comment text itself (e.g., "As a physician", "Our hospital", "I am a patient"). Only select a specific entity type if there's clear evidence. If you cannot determine the entity type from the available information, select "Other/Unknown". Choose from:
{entity_list}

3. Key Quote: Select the most important quote (max 100 words) that best captures the essence of the comment. Must be verbatim from the text.

4. Rationale: Briefly explain (1-2 sentences) why you selected these stances.

Instructions:
- A comment may express multiple stances or no clear stance
- Only select stances that are clearly expressed in the comment
- Be objective and avoid inserting personal opinions"""

    return CommentAnalyzerConfig(
        regulation_name=discovered.regulation_name,
        regulation_description=discovered.regulation_description,
        stance_options=stance_options,
        stance_indicators=stance_indicators,
        entity_types=discovered.entity_types,
        system_prompt=system_prompt
    )

def save_config(config: CommentAnalyzerConfig, output_file: str = "analyzer_config.json"):
    """Save analyzer configuration to JSON file."""
    config_data = {
        "regulation_name": config.regulation_name,
        "regulation_description": config.regulation_description,
        "stance_options": config.stance_options,
        "stance_indicators": config.stance_indicators,
        "entity_types": config.entity_types,
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
    
    # Create EntityType enum definitions
    entity_enum_items = []
    # Ensure we have a full list including Other/Unknown
    entity_types_with_other = config.entity_types.copy()
    if "Other/Unknown" not in entity_types_with_other:
        entity_types_with_other.append("Other/Unknown")
    
    for entity in entity_types_with_other:
        # Create valid enum names by replacing spaces and special chars
        enum_name = re.sub(r'[^A-Za-z0-9]+', '_', entity.upper()).strip('_')
        entity_enum_items.append(f'    {enum_name} = "{entity}"')
    
    entity_enum = "class EntityType(str, Enum):\n" + "\n".join(entity_enum_items)
    
    # Replace existing enum definitions
    # First, find and replace EntityType enum
    entity_pattern = r'class EntityType\(str, Enum\):\n(?:    .*\n)*'
    if re.search(entity_pattern, content):
        content = re.sub(entity_pattern, entity_enum + "\n", content)
    else:
        # If no EntityType enum exists, add it before Stance enum
        stance_start = content.find('class Stance(str, Enum):')
        if stance_start > 0:
            content = content[:stance_start] + entity_enum + "\n\n\n" + content[stance_start:]
    
    # Then, find and replace Stance enum
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

def print_results(discovered: DiscoveredThemes, config: CommentAnalyzerConfig, csv_file: str = 'comments.csv'):
    """Print discovered positions and configuration."""
    print("\n" + "="*80)
    print("DISCOVERED POSITIONS AND STANCES")
    print("="*80)
    
    print(f"\nRegulation: {discovered.regulation_name}")
    print(f"Description: {discovered.regulation_description}")
    
    print(f"\nEntity Types ({len(discovered.entity_types)}):")
    for entity_type in discovered.entity_types:
        print(f"  • {entity_type}")
    
    print(f"\nThemes ({len(discovered.themes)}):")
    for i, theme in enumerate(discovered.themes, 1):
        print(f"\n  {i}. {theme['name']}")
        print(f"     Description: {theme.get('description', 'N/A')}")
        print(f"     Positions ({len(theme.get('positions', []))}):") 
        for j, position in enumerate(theme.get('positions', []), 1):
            print(f"       {j}. {position['name']}")
            indicators = position['indicators']
            if isinstance(indicators, list):
                print(f"          Indicators:")
                for indicator in indicators:
                    print(f"            • {indicator}")
            else:
                print(f"          Indicators: {indicators}")
    
    
    print("\n" + "="*80)
    print("CONFIGURATION GENERATED")
    print("="*80)
    print("✅ analyzer_config.json has been created")
    print("✅ comment_analyzer.py has been updated with enum definitions")
    print("✅ The comment analyzer will now identify support/opposition positions per comment")
    print(f"✅ Found {len(discovered.themes)} themes with {sum(len(theme.get('positions', [])) for theme in discovered.themes)} total positions")
    print(f"✅ Found {len(discovered.entity_types)} entity types: {', '.join(discovered.entity_types)}")
    print(f"✅ Ready to run: python pipeline.py --csv {csv_file}")


def main():
    parser = argparse.ArgumentParser(description='Discover themes and positions in public comments')
    parser.add_argument('--sample', type=int, default=250, help='Number of comments to analyze (default: 250)')
    parser.add_argument('--model', type=str, default='gpt-4o', help='LLM model to use (default: gpt-4o)')
    parser.add_argument('--csv', type=str, default='comments.csv', help='CSV file to analyze (default: comments.csv)')
    
    args = parser.parse_args()
    
    try:
        csv_file = args.csv
        
        # Load comments sample
        logger.info("Loading comment sample...")
        comments = load_comments_sample(csv_file, args.sample)
        
        if not comments:
            logger.error("No comments with text content found")
            return
        
        # Run discovery with mutually_exclusive strategy and 5 themes
        logger.info("Discovering themes and positions...")
        result = discover_themes_experimental(
            comments=comments,
            model=args.model,
            target_count=5,
            prompt_strategy='mutually_exclusive'
        )
        
        if 'error' in result:
            logger.error(f"Discovery failed: {result['error']}")
            return
        
        # Convert to DiscoveredThemes object
        discovered = DiscoveredThemes(
            themes=result['themes'],
            regulation_name=result['regulation_name'],
            regulation_description=result['regulation_description'],
            entity_types=result.get('entity_types', [])
        )
        
        # Generate configuration with theme:position format
        config = generate_analyzer_config(discovered)
        
        # Update to use theme:position format
        theme_position_options = []
        theme_position_indicators = {}
        
        for theme in discovered.themes:
            theme_name = theme['name']
            for position in theme.get('positions', []):
                position_name = position['name']
                formatted_name = f"{theme_name}: {position_name}"
                theme_position_options.append(formatted_name)
                
                indicators = position['indicators']
                if isinstance(indicators, list):
                    theme_position_indicators[formatted_name] = "; ".join(indicators)
                else:
                    theme_position_indicators[formatted_name] = indicators
        
        config.stance_options = theme_position_options
        config.stance_indicators = theme_position_indicators
        # Ensure Other/Unknown is in entity_types
        entity_types_with_other = discovered.entity_types.copy()
        if "Other/Unknown" not in entity_types_with_other:
            entity_types_with_other.append("Other/Unknown")
        config.entity_types = entity_types_with_other
        
        # Update system prompt for theme:position format
        stance_list = "\n".join([f"- {name}: {indicators}" for name, indicators in theme_position_indicators.items()])
        entity_list = "\n".join([f"- {entity}" for entity in config.entity_types])
        
        config.system_prompt = f"""You are analyzing public comments about {discovered.regulation_name}.

{discovered.regulation_description}

For each comment, identify:

1. Stances: Which of these theme:position combinations does the commenter express? Look for the indicators listed below. (Select ALL that apply, or none if none apply)
{stance_list}

2. Entity Type: Identify what type of entity is submitting this comment. Look for clues in the organization name, submitter title, and the comment text itself (e.g., "As a physician", "Our hospital", "I am a patient"). Only select a specific entity type if there's clear evidence. If you cannot determine the entity type from the available information, select "Other/Unknown". Choose from:
{entity_list}

3. Key Quote: Select the most important quote (max 100 words) that best captures the essence of the comment. Must be verbatim from the text.

4. Rationale: Briefly explain (1-2 sentences) why you selected these theme:position combinations.

Instructions:
- A comment may express multiple stances or no clear stance
- Only select stances that are clearly expressed in the comment
- Be objective and avoid inserting personal opinions"""
        
        # Save configuration
        save_config(config)
        
        # Update comment_analyzer.py
        update_comment_analyzer(config)
        
        # Print results
        print_results(discovered, config, csv_file)
        
    except Exception as e:
        logger.error(f"Discovery failed: {e}")
        raise

if __name__ == "__main__":
    main()