#!/usr/bin/env python3
"""
Generic Comment Analyzer

A regulation-agnostic analyzer for public comments using OpenAI via LiteLLM.
The analysis configuration (stances, prompts) should be defined
per regulation in a separate configuration.
"""

import os
import json
import threading
import logging
from enum import Enum
from dotenv import load_dotenv
import litellm
litellm.drop_params = True  # drop params a model does not support (e.g. temperature on GPT-5 reasoning models)
from pydantic import BaseModel, Field, create_model
from typing import List, Optional, Dict, Any

# Load environment variables
load_dotenv()

# Setup logging
logger = logging.getLogger(__name__)

class TimeoutError(Exception):
    """Custom timeout exception for API calls"""
    pass

class CommentAnalysisResult(BaseModel):
    """Standard model for comment analysis results"""
    stances: List[str] = Field(
        default_factory=list,
        description="List of stances/arguments expressed in the comment (0 or more); each item must be one of the provided stance option names"
    )
    entity_type: str = Field(
        description="Type of entity submitting the comment (must be one of the predefined entity types)"
    )
    entity_name: str = Field(
        default="",
        description="Verbatim quote from the text that justifies the entity type classification. Must be an exact substring from anywhere in the provided text. Examples: 'As an attorney and former government attorney', 'Retired Judge Smith', 'The Beverly Hills Bar Association opposes'. Leave empty if no identifying information is present."
    )
    state_identified: str = Field(
        default="",
        description="US state the submitter is from, if identifiable. Extract from any clue: city/state mentioned in text, submitter name/address, organization location, bar membership state, etc. Use the two-letter abbreviation (e.g. 'CA', 'TX', 'NY'). Leave empty if no state can be determined."
    )
    state_quote: str = Field(
        default="",
        description="Verbatim quote from the text that justifies the state identification. Must be an exact substring. Examples: 'licensed in the state of California', 'St Louis, MO', 'here in Austin, Texas'. Leave empty if no state identified."
    )
    political_affiliation: str = Field(
        default="",
        description="Political affiliation ONLY if the submitter explicitly self-identifies. Must be one of: 'Republican', 'Democrat', 'Independent', 'Libertarian', or empty. Do NOT infer from policy positions — only use explicit statements like 'as a Republican', 'registered Democrat', 'lifelong Republican voter'."
    )
    political_affiliation_quote: str = Field(
        default="",
        description="Verbatim quote from the text where the submitter self-identifies their political affiliation. Must be an exact substring. Leave empty if no explicit self-identification."
    )
    key_quote: str = Field(
        description="The most important quote that captures the essence of the comment (max 100 words)"
    )
    rationale: str = Field(
        description="Brief explanation of the stance selection (1-2 sentences)"
    )


def _build_result_model(stance_options: List[str], entity_types: List[str]):
    """Build a schema whose stances/entity_type are constrained to the config values.

    Using string enums forces the model (via OpenAI structured outputs) to emit only
    exact config values, instead of near-misses that then get filtered out.
    Falls back to the free-form model if the config is empty.
    """
    if not stance_options or not entity_types:
        return CommentAnalysisResult
    StanceEnum = Enum("StanceEnum", {f"S{i}": v for i, v in enumerate(stance_options)}, type=str)
    EntityEnum = Enum("EntityEnum", {f"E{i}": v for i, v in enumerate(entity_types)}, type=str)
    return create_model(
        "ConstrainedCommentAnalysisResult",
        __base__=CommentAnalysisResult,
        stances=(List[StanceEnum], Field(default_factory=list,
            description="All stances/concerns expressed in the comment; select 0 or more from the allowed values (an exact match required).")),
        entity_type=(EntityEnum, Field(
            description="Type of entity submitting the comment; must be exactly one of the allowed values.")),
    )


# ---------------------------------------------------------------------------
# `fields:`-driven schema + prompt (single source of truth per regulation).
# When analyzer_config.yaml declares a `fields:` block, every analysis field's
# name+type is declared once and drives BOTH the Pydantic schema and the system
# prompt. When absent, we fall back to the legacy stances/entity/additional_fields
# behavior above.
# ---------------------------------------------------------------------------

def _resolve_field_options(field: Dict[str, Any], stance_options: List[str], entity_types: List[str]) -> List[str]:
    """Resolve a field's enum options: inline `options`, or `options_from` a shared list."""
    src = field.get('options_from')
    if src == 'stances':
        return list(stance_options)
    if src == 'entity_types':
        return list(entity_types)
    return list(field.get('options', []) or [])


def _parse_fields(raw: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
    """Return the `fields:` list with options resolved, or None for legacy configs."""
    fields = raw.get('fields')
    if not fields:
        return None
    stance_options = [s['name'] for s in raw.get('stances', [])]
    entity_types = list(raw.get('entity_types', []))
    resolved = []
    for f in fields:
        f = dict(f)
        f['options'] = _resolve_field_options(f, stance_options, entity_types)
        resolved.append(f)
    return resolved


# Pydantic type per declared field type.
#   multi_enum   -> List[Enum(options)]   (0..N)
#   single_enum  -> Enum(options)          (exactly 1, required)
#   enum_or_empty-> str constrained to options-or-"" (kept as free str; validated downstream)
#   text/quote/short_text -> str
def _build_result_model_from_fields(fields: List[Dict[str, Any]], stance_options: List[str], entity_types: List[str]):
    """Dynamically build the Pydantic result model from the `fields:` declaration.

    Fields with `source: regex` are derived from the comment text at report time,
    not by the LLM, so they are excluded from the schema.
    """
    model_fields: Dict[str, Any] = {}
    for f in fields:
        if f.get('source') == 'regex':
            continue
        name = f['name']
        ftype = f.get('type', 'text')
        opts = _resolve_field_options(f, stance_options, entity_types)
        desc = (f.get('prompt') or f.get('label') or name).strip()
        if ftype == 'multi_enum':
            E = Enum(f"Enum_{name}", {f"V{i}": v for i, v in enumerate(opts)}, type=str)
            model_fields[name] = (List[E], Field(default_factory=list, description=desc))
        elif ftype == 'single_enum':
            E = Enum(f"Enum_{name}", {f"V{i}": v for i, v in enumerate(opts)}, type=str)
            model_fields[name] = (E, Field(description=desc))
        else:  # text, quote, short_text, enum_or_empty
            model_fields[name] = (str, Field(default="", description=desc))
    return create_model("ConfiguredCommentAnalysisResult", __base__=BaseModel, **model_fields)


def _build_prompt_from_fields(raw: Dict[str, Any], fields: List[Dict[str, Any]]) -> str:
    """Assemble the system prompt from each field's label + prompt (and enum options)."""
    stances = raw.get('stances', [])
    entity_types = list(raw.get('entity_types', []))
    entity_rules = (raw.get('entity_classification_rules') or '').strip()
    instructions = raw.get('instructions', [])

    parts = [
        f"You are analyzing public comments about a proposed rule: {raw.get('regulation_name', '')}.",
        (raw.get('regulation_description') or '').strip(),
        "",
        "For each comment, extract the following fields:",
        "",
    ]
    llm_fields = [f for f in fields if f.get('source') != 'regex']
    for i, f in enumerate(llm_fields, 1):
        label = f.get('label', f['name'])
        prompt = (f.get('prompt') or '').strip()
        parts.append(f"{i}. {label}: {prompt}")
        src = f.get('options_from')
        if src == 'stances':
            parts.append('\n'.join(f"   - {s['name']} — {s['indicator']}" for s in stances))
        elif src == 'entity_types':
            parts.append('\n'.join(f"   - {e}" for e in entity_types))
            if entity_rules:
                parts.append(entity_rules)
        elif f.get('options'):
            parts.append('\n'.join(f"   - {o}" for o in f['options']))
        parts.append("")
    if instructions:
        parts.append("Instructions:")
        parts.extend(f"- {i}" for i in instructions)
    return '\n'.join(parts)


class CommentAnalyzer:
    """OpenAI (via LiteLLM) analyzer for public comments using configurable prompts and categories."""

    def __init__(self, model=None, timeout_seconds=120, config_file="analyzer_config.yaml"):
        """
        Initialize the analyzer with configuration from YAML (or JSON) file.

        Args:
            model: LLM model to use (defaults to environment config)
            timeout_seconds: API timeout in seconds
            config_file: Path to YAML/JSON configuration file with regulation-specific settings
        """
        self.model = model or os.getenv('LLM_MODEL', 'gpt-5.4-nano')
        self.timeout_seconds = timeout_seconds

        # Load configuration from file
        self.config = self._load_config(config_file)
        self.stance_options = self.config.get('stance_options', [])
        self.entity_types = self.config.get('entity_types', [])
        # Always ensure Individual/Other is in the list as default
        if "Individual/Other" not in self.entity_types:
            self.entity_types.append("Individual/Other")
        self.system_prompt = self.config.get('system_prompt')
        # `fields:`-driven schema when the config declares one; else legacy schema.
        self.fields = self.config.get('fields')
        if self.fields:
            self.result_model = _build_result_model_from_fields(self.fields, self.stance_options, self.entity_types)
        else:
            self.result_model = _build_result_model(self.stance_options, self.entity_types)

        logger.info(f"Loaded configuration for: {self.config.get('regulation_name', 'Unknown Regulation')}")
        logger.info(f"Using {len(self.stance_options)} stance options")
        logger.info(f"Using {len(self.entity_types)} entity types")

        # Ensure API key is available
        api_key = os.getenv('OPENAI_API_KEY')
        if not api_key:
            raise ValueError("OPENAI_API_KEY not found in environment variables or .env file")

    def _load_config(self, config_file: str) -> Dict[str, Any]:
        """Load configuration from YAML or JSON file."""
        import yaml

        # Try YAML first, then JSON fallback
        yaml_file = config_file.replace('.json', '.yaml') if config_file.endswith('.json') else config_file
        json_file = config_file.replace('.yaml', '.json') if config_file.endswith('.yaml') else config_file

        try:
            if os.path.exists(yaml_file):
                with open(yaml_file, 'r', encoding='utf-8') as f:
                    raw = yaml.safe_load(f)
                return self._normalize_yaml_config(raw)
            elif os.path.exists(json_file):
                with open(json_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                logger.warning(f"No config file found ({yaml_file} or {json_file}), using defaults")
                return {}
        except Exception as e:
            logger.error(f"Failed to load configuration: {e}")
            return {}

    def _normalize_yaml_config(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Convert YAML config format into the internal config dict.

        When the config declares a `fields:` block it is the single source of
        truth: the system prompt (and, in __init__, the Pydantic schema) are built
        from it. Otherwise we fall back to the legacy hand-assembled prompt.
        """
        stances = raw.get('stances', [])
        stance_options = [s['name'] for s in stances]
        stance_indicators = {s['name']: s['indicator'] for s in stances}
        entity_types = raw.get('entity_types', [])
        entity_rules = raw.get('entity_classification_rules', '')
        additional = raw.get('additional_fields', {})
        instructions = raw.get('instructions', [])

        fields = _parse_fields(raw)

        if fields:
            system_prompt = _build_prompt_from_fields(raw, fields)
        else:
            # Legacy hand-assembled prompt (no `fields:` block).
            stance_lines = '\n'.join(f"- {s['name']} — {s['indicator']}" for s in stances)
            entity_lines = '\n'.join(f"- {e}" for e in entity_types)
            instruction_lines = '\n'.join(f"- {i}" for i in instructions)
            system_prompt = (
                f"You are analyzing public comments about a proposed rule: "
                f"{raw.get('regulation_name', '')}.\n"
                f"{raw.get('regulation_description', '').strip()}\n\n"
                f"For each comment, identify:\n\n"
                f"1. Position and Concerns: Select ALL that apply from the list below.\n"
                f"{stance_lines}\n\n"
                f"2. Entity Type: What type of entity is submitting this comment? Choose from:\n"
                f"{entity_lines}\n"
                f"{entity_rules.strip()}\n\n"
                f"3. Entity Name: {additional.get('entity_name', '').strip()}\n\n"
                f"4. State: {additional.get('state', '').strip()}\n\n"
                f"5. Political Affiliation: {additional.get('political_affiliation', '').strip()}\n\n"
                f"6. Key Quote: {additional.get('key_quote', '').strip()}\n\n"
                f"7. Rationale: {additional.get('rationale', '').strip()}\n\n"
                f"Instructions:\n{instruction_lines}"
            )

        return {
            'regulation_name': raw.get('regulation_name', ''),
            'regulation_description': raw.get('regulation_description', ''),
            'stance_options': stance_options,
            'stance_indicators': stance_indicators,
            'entity_types': entity_types,
            'system_prompt': system_prompt,
            'fields': fields,
        }
    
    def get_system_prompt(self):
        """Get the system prompt, using default if none provided"""
        if self.system_prompt:
            return self.system_prompt
            
        # Default generic prompt
        stance_list = "\n".join([f"- {stance}" for stance in self.stance_options])
        entity_list = "\n".join([f"- {entity}" for entity in self.entity_types])
        
        return f"""You are analyzing public comments submitted regarding a proposed regulation.

1. Stance: Determine the commenter's position on the proposed regulation. Choose from:
{stance_list}

2. Entity Type: Identify what type of entity is submitting this comment. Look for clues in the organization name, submitter title, and the comment text itself (e.g., "As a physician", "Our hospital", "I am a patient"). Only select a specific entity type if there's clear evidence. If you cannot determine the entity type from the available information, select "Other/Unknown". Choose from:
{entity_list}

3. Key Quote: Select the most important quote (max 100 words) that best captures the essence of the comment. The quote must be exactly present in the original text - do not paraphrase or modify.

4. Rationale: Briefly explain (1-2 sentences) why you classified the stance as you did.

Analyze objectively and avoid inserting personal opinions or biases."""

    def analyze_with_timeout(self, comment_text, comment_id=None, organization=None, submitter=None):
        """Analyze a comment with timeout protection"""
        identifier = f" (ID: {comment_id})" if comment_id else ""

        # Combine submitter info and comment text into one block
        full_text_parts = []
        if submitter:
            full_text_parts.append(f"Submitter: {submitter}")
        if organization:
            full_text_parts.append(f"Organization: {organization}")
        full_text_parts.append(comment_text)
        combined_text = "\n".join(full_text_parts)

        # Create a thread-safe container for the result
        result_container = {'result': None, 'error': None}

        def api_call():
            try:
                response = litellm.completion(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": self.get_system_prompt()},
                        {"role": "user", "content": f"Analyze the following public comment{identifier}:\n\n{combined_text}"},
                    ],
                    response_format=self.result_model,
                    temperature=0.0,
                    timeout=self.timeout_seconds,
                )

                # Parse the JSON response
                result_container['result'] = json.loads(response.choices[0].message.content)

            except Exception as e:
                result_container['error'] = e

        # Run API call in a thread
        thread = threading.Thread(target=api_call)
        thread.daemon = True
        thread.start()

        # Wait for thread to complete with timeout
        thread.join(timeout=self.timeout_seconds + 5)

        if thread.is_alive():
            logger.error(f"API call timed out for comment{identifier}")
            raise TimeoutError(f"Analysis timed out after {self.timeout_seconds + 5} seconds")

        # Check if there was an error
        if result_container['error']:
            raise result_container['error']

        return result_container['result']
    
    def analyze(self, comment_text, comment_id=None, organization=None, submitter=None, max_retries=3):
        """
        Analyze a comment with retries for robustness.
        
        Args:
            comment_text: The text to analyze
            comment_id: Optional comment ID for logging
            max_retries: Maximum number of retry attempts
            
        Returns:
            Dictionary with analysis results
        """
        last_error = None
        
        for attempt in range(max_retries + 1):
            try:
                result = self.analyze_with_timeout(comment_text, comment_id, organization, submitter)
                
                # Validate the result has required fields
                if not isinstance(result, dict):
                    raise ValueError("Result is not a dictionary")
                
                required_fields = ['stances', 'key_quote', 'rationale']
                for field in required_fields:
                    if field not in result:
                        raise ValueError(f"Missing required field: {field}")
                
                # Filter stances to only the configured stance options (drop anything the model invented)
                if 'stances' in result and isinstance(result['stances'], list):
                    result['stances'] = [s for s in result['stances'] if s in self.stance_options]

                # Handle entity_type - keep as string since LLM returns string
                if 'entity_type' in result:
                    # Ensure it's one of the allowed values
                    if result['entity_type'] not in self.entity_types:
                        result['entity_type'] = "Individual/Other"
                
                return result
                
            except Exception as e:
                last_error = e
                if attempt < max_retries:
                    logger.warning(f"Analysis attempt {attempt + 1} failed for comment{f' (ID: {comment_id})' if comment_id else ''}: {e}. Retrying...")
                    continue
                else:
                    logger.error(f"Analysis failed after {max_retries + 1} attempts for comment{f' (ID: {comment_id})' if comment_id else ''}: {e}")
                    
        # If we get here, all retries failed
        raise last_error

# Create a regulation-specific analyzer
def create_regulation_analyzer(model=None, timeout_seconds=None):
    """Create an analyzer configured for regulation analysis using analyzer_config.yaml."""
    return CommentAnalyzer(
        model=model,
        timeout_seconds=timeout_seconds or 120,
        config_file='analyzer_config.yaml'
    )