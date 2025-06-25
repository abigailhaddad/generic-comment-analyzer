#!/usr/bin/env python3
"""
Generic Comment Analyzer

A regulation-agnostic analyzer for public comments using LiteLLM.
The analysis configuration (stances, themes, prompts) should be defined
per regulation in a separate configuration.
"""

import os
import json
import threading
import logging
from enum import Enum
from dotenv import load_dotenv
from litellm import completion
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any

# Load environment variables
load_dotenv()

# Setup logging
logger = logging.getLogger(__name__)

class TimeoutError(Exception):
    """Custom timeout exception for API calls"""
    pass

# Placeholder enums - will be populated by discover_stances.py
class Stance(str, Enum):
    OPPOSITION_TO_COVID_VACCINES = "Opposition to COVID Vaccines"
    SUPPORT_FOR_UNIVERSAL_VACCINE_ACCESS = "Support for Universal Vaccine Access"
    CONCERNS_ABOUT_ACIP_CHANGES = "Concerns About ACIP Changes"
    DEFENSE_OF_VACCINES_EFFECTIVENESS = "Defense of Vaccines' Effectiveness"
    CALL_FOR_TRANSPARENCY_AND_RESEARCH_INTEGRITY = "Call for Transparency and Research Integrity"

class Theme(str, Enum):
    VACCINE_SAFETY_CONCERNS = "vaccine safety concerns"
    NEED_FOR_TRANSPARENCY = "need for transparency"
    IMPORTANCE_OF_PUBLIC_HEALTH = "importance of public health"
    INDIVIDUAL_MEDICAL_CHOICE = "individual medical choice"
    SCIENTIFIC_EXPERTISE = "scientific expertise"
    IMPACT_OF_ACIP_DECISIONS = "impact of ACIP decisions"
    UNIVERSAL_VACCINE_ACCESS = "universal vaccine access"
    LONG_COVID_CONSIDERATIONS = "long COVID considerations"
    COMMUNITY_HEALTH_RESPONSIBILITIES = "community health responsibilities"

class CommentAnalysisResult(BaseModel):
    """Standard model for comment analysis results"""
    stances: List[Stance] = Field(
        default_factory=list,
        description="List of stances/arguments expressed in the comment (0 or more)"
    )
    themes: List[Theme] = Field(
        default_factory=list,
        description="Key themes present in the comment"
    )
    key_quote: str = Field(
        description="The most important quote that captures the essence of the comment (max 100 words)"
    )
    rationale: str = Field(
        description="Brief explanation of the stance selection (1-2 sentences)"
    )

class CommentAnalyzer:
    """LiteLLM-based analyzer for public comments using configurable prompts and categories."""
    
    def __init__(self, model=None, timeout_seconds=120, config_file="analyzer_config.json"):
        """
        Initialize the analyzer with configuration from JSON file.
        
        Args:
            model: LLM model to use (defaults to environment config)
            timeout_seconds: API timeout in seconds
            config_file: Path to JSON configuration file with regulation-specific settings
        """
        self.model = model or os.getenv('LLM_MODEL', 'gpt-4o-mini')
        self.timeout_seconds = timeout_seconds
        
        # Load configuration from file
        self.config = self._load_config(config_file)
        self.stance_options = self.config.get('stance_options', [])
        self.theme_options = self.config.get('theme_options', [])
        self.system_prompt = self.config.get('system_prompt')
        
        logger.info(f"Loaded configuration for: {self.config.get('regulation_name', 'Unknown Regulation')}")
        logger.info(f"Using {len(self.theme_options)} themes and {len(self.stance_options)} stance options")
        
        # Ensure API key is available
        if "OPENAI_API_KEY" not in os.environ:
            raise ValueError("OPENAI_API_KEY not found in environment variables or .env file")
    
    def _load_config(self, config_file: str) -> Dict[str, Any]:
        """Load configuration from JSON file."""
        try:
            if os.path.exists(config_file):
                with open(config_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            else:
                logger.warning(f"Configuration file {config_file} not found, using defaults")
                return {}
        except Exception as e:
            logger.error(f"Failed to load configuration from {config_file}: {e}")
            return {}
    
    def get_system_prompt(self):
        """Get the system prompt, using default if none provided"""
        if self.system_prompt:
            return self.system_prompt
            
        # Default generic prompt
        stance_list = "\n".join([f"- {stance}" for stance in self.stance_options])
        theme_list = "\n".join([f"- {theme}" for theme in self.theme_options]) if self.theme_options else "- (No predefined themes - identify key themes from the content)"
        
        return f"""You are analyzing public comments submitted regarding a proposed regulation.

1. Stance: Determine the commenter's position on the proposed regulation. Choose from:
{stance_list}

2. Themes: Identify which themes are present in the comment. Available themes:
{theme_list}

3. Key Quote: Select the most important quote (max 100 words) that best captures the essence of the comment. The quote must be exactly present in the original text - do not paraphrase or modify.

4. Rationale: Briefly explain (1-2 sentences) why you classified the stance as you did.

Analyze objectively and avoid inserting personal opinions or biases."""

    def analyze_with_timeout(self, comment_text, comment_id=None):
        """Analyze a comment with timeout protection"""
        identifier = f" (ID: {comment_id})" if comment_id else ""
        
        # Create a thread-safe container for the result
        result_container = {'result': None, 'error': None}
        
        def api_call():
            try:
                response = completion(
                    temperature=0.0,
                    model=self.model,
                    messages=[
                        {"role": "system", "content": self.get_system_prompt()},
                        {"role": "user", "content": f"Analyze the following public comment{identifier}:\n\n{comment_text}"}
                    ],
                    response_format=CommentAnalysisResult,
                    timeout=self.timeout_seconds
                )
                
                # Process the response based on its format
                if hasattr(response.choices[0].message, 'content') and response.choices[0].message.content:
                    if isinstance(response.choices[0].message.content, str):
                        result_container['result'] = json.loads(response.choices[0].message.content)
                    else:
                        result_container['result'] = response.choices[0].message.content
                elif hasattr(response.choices[0].message, 'model_dump'):
                    result_container['result'] = response.choices[0].message.model_dump()
                else:
                    raise ValueError("Unexpected response format")
                    
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
    
    def analyze(self, comment_text, comment_id=None, max_retries=3):
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
                result = self.analyze_with_timeout(comment_text, comment_id)
                
                # Validate the result has required fields
                if not isinstance(result, dict):
                    raise ValueError("Result is not a dictionary")
                
                required_fields = ['stances', 'themes', 'key_quote', 'rationale']
                for field in required_fields:
                    if field not in result:
                        raise ValueError(f"Missing required field: {field}")
                
                # Convert string values to enum values if needed
                if 'stances' in result and isinstance(result['stances'], list):
                    result['stances'] = [s if isinstance(s, Stance) else s for s in result['stances']]
                if 'themes' in result and isinstance(result['themes'], list):
                    result['themes'] = [t if isinstance(t, Theme) else t for t in result['themes']]
                
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
    """Create an analyzer configured for regulation analysis"""
    
    stance_options = [
        "Opposition to COVID Vaccines",
        "Support for Universal Vaccine Access",
        "Concerns About ACIP Changes",
        "Defense of Vaccines' Effectiveness",
        "Call for Transparency and Research Integrity"
    ]
    theme_options = [
        "vaccine safety concerns",
        "need for transparency",
        "importance of public health",
        "individual medical choice",
        "scientific expertise",
        "impact of ACIP decisions",
        "universal vaccine access",
        "long COVID considerations",
        "community health responsibilities"
    ]
    
    system_prompt = """You are analyzing public comments about COVID-19 Vaccine Access and Safety.

Debates over the safety, efficacy, and access to COVID-19 vaccines following recent ACIP committee changes and vaccine recommendations.

For each comment, identify:

1. Stances: Which of these positions/arguments does the commenter express? Look for the indicators listed below. (Select ALL that apply, or none if none apply)
- Opposition to COVID Vaccines: remove all currently licensed COVID shots; reports of deaths following COVID vaccination; historical comparison with swine flu vaccine; calls for immediate action by ACIP; COVID shots are unnecessary if safety is in question
- Support for Universal Vaccine Access: universal access to vaccines; medical decisions up to individuals and their doctors; vaccines must be available for all populations; calls for financial coverage by insurance; importance of vaccination for personal health safety
- Concerns About ACIP Changes: grave concerns about recent termination of ACIP members; independent expert scientists; conflicts of interest among new members; impact on public trust in vaccines; emphasis on expertise in vaccination guidance
- Defense of Vaccines' Effectiveness: vaccines save lives; prevent serious outcomes from COVID; support for continued vaccination recommendations; importance of vaccines in public health; highlighting success of past vaccinations in society
- Call for Transparency and Research Integrity: calls for disclosure of financial conflicts; demands accurate research and consultation; highlighting need for scientific decisions; urgency for unbiased vaccine recommendations; concerns over politicization of health guidelines

2. Themes: Which of these themes are present in the comment? (Select all that apply)
- vaccine safety concerns
- need for transparency
- importance of public health
- individual medical choice
- scientific expertise
- impact of ACIP decisions
- universal vaccine access
- long COVID considerations
- community health responsibilities

3. Key Quote: Select the most important quote (max 100 words) that best captures the essence of the comment. Must be verbatim from the text.

4. Rationale: Briefly explain (1-2 sentences) why you selected these stances.

Instructions:
- A comment may express multiple stances or no clear stance
- Only select stances that are clearly expressed in the comment
- Be objective and avoid inserting personal opinions"""

    return CommentAnalyzer(
        model=model,
        timeout_seconds=timeout_seconds or 120,
        system_prompt=system_prompt,
        stance_options=stance_options,
        theme_options=theme_options
    )