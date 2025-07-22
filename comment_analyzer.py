#!/usr/bin/env python3
"""
Generic Comment Analyzer

A regulation-agnostic analyzer for public comments using LiteLLM.
The analysis configuration (stances, prompts) should be defined
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
    COVID_19_VACCINE_ACCESS_SUPPORT_FOR_COVID_19_VACCINE_ACCESS = "COVID-19 Vaccine Access: Support for COVID-19 Vaccine Access"
    COVID_19_VACCINE_ACCESS_OPPOSE_COVID_19_VACCINE_ACCESS = "COVID-19 Vaccine Access: Oppose COVID-19 Vaccine Access"
    VACCINE_SAFETY_CONCERNS_SUPPORT_FOR_VACCINE_SAFETY = "Vaccine Safety Concerns: Support for Vaccine Safety"
    VACCINE_SAFETY_CONCERNS_OPPOSE_VACCINE_SAFETY = "Vaccine Safety Concerns: Oppose Vaccine Safety"
    REGULATORY_CHANGES_AND_COMMITTEE_MEMBERSHIP_SUPPORT_FOR_CURRENT_REGULATORY_CHANGES = "Regulatory Changes and Committee Membership: Support for Current Regulatory Changes"
    REGULATORY_CHANGES_AND_COMMITTEE_MEMBERSHIP_OPPOSE_CURRENT_REGULATORY_CHANGES = "Regulatory Changes and Committee Membership: Oppose Current Regulatory Changes"
    INDEPENDENT_SCIENTIFIC_OVERSIGHT_SUPPORT_FOR_INDEPENDENT_OVERSIGHT = "Independent Scientific Oversight: Support for Independent Oversight"
    INDEPENDENT_SCIENTIFIC_OVERSIGHT_OPPOSE_INDEPENDENT_OVERSIGHT = "Independent Scientific Oversight: Oppose Independent Oversight"
    PUBLIC_TRUST_IN_VACCINATIONS_SUPPORT_FOR_PUBLIC_TRUST_INITIATIVES = "Public Trust in Vaccinations: Support for Public Trust Initiatives"
    PUBLIC_TRUST_IN_VACCINATIONS_OPPOSE_PUBLIC_TRUST_INITIATIVES = "Public Trust in Vaccinations: Oppose Public Trust Initiatives"
    RISK_ASSESSMENT_AND_VACCINE_RECOMMENDATIONS_SUPPORT_FOR_UNIVERSAL_RECOMMENDATIONS = "Risk Assessment and Vaccine Recommendations: Support for Universal Recommendations"
    RISK_ASSESSMENT_AND_VACCINE_RECOMMENDATIONS_OPPOSE_UNIVERSAL_RECOMMENDATIONS = "Risk Assessment and Vaccine Recommendations: Oppose Universal Recommendations"
    VACCINE_DISTRIBUTION_POLICY_SUPPORT_FOR_CURRENT_DISTRIBUTION_POLICY = "Vaccine Distribution Policy: Support for Current Distribution Policy"
    VACCINE_DISTRIBUTION_POLICY_OPPOSE_CURRENT_DISTRIBUTION_POLICY = "Vaccine Distribution Policy: Oppose Current Distribution Policy"
    ANTI_VACCINE_SENTIMENT_OPPOSE_ANTI_VACCINE_INFLUENCE = "Anti-Vaccine Sentiment: Oppose Anti-Vaccine Influence"
    ANTI_VACCINE_SENTIMENT_SUPPORT_ANTI_VACCINE_INFLUENCE = "Anti-Vaccine Sentiment: Support Anti-Vaccine Influence"


class CommentAnalysisResult(BaseModel):
    """Standard model for comment analysis results"""
    stances: List[Stance] = Field(
        default_factory=list,
        description="List of stances/arguments expressed in the comment (0 or more)"
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
        self.system_prompt = self.config.get('system_prompt')
        
        logger.info(f"Loaded configuration for: {self.config.get('regulation_name', 'Unknown Regulation')}")
        logger.info(f"Using {len(self.stance_options)} stance options")
        
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
        
        return f"""You are analyzing public comments submitted regarding a proposed regulation.

1. Stance: Determine the commenter's position on the proposed regulation. Choose from:
{stance_list}

2. Key Quote: Select the most important quote (max 100 words) that best captures the essence of the comment. The quote must be exactly present in the original text - do not paraphrase or modify.

3. Rationale: Briefly explain (1-2 sentences) why you classified the stance as you did.

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
                
                required_fields = ['stances', 'key_quote', 'rationale']
                for field in required_fields:
                    if field not in result:
                        raise ValueError(f"Missing required field: {field}")
                
                # Convert string values to enum values if needed
                if 'stances' in result and isinstance(result['stances'], list):
                    result['stances'] = [s if isinstance(s, Stance) else s for s in result['stances']]
                
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
        "COVID-19 Vaccine Access: Support for COVID-19 Vaccine Access",
        "COVID-19 Vaccine Access: Oppose COVID-19 Vaccine Access",
        "Vaccine Safety Concerns: Support for Vaccine Safety",
        "Vaccine Safety Concerns: Oppose Vaccine Safety",
        "Regulatory Changes and Committee Membership: Support for Current Regulatory Changes",
        "Regulatory Changes and Committee Membership: Oppose Current Regulatory Changes",
        "Independent Scientific Oversight: Support for Independent Oversight",
        "Independent Scientific Oversight: Oppose Independent Oversight",
        "Public Trust in Vaccinations: Support for Public Trust Initiatives",
        "Public Trust in Vaccinations: Oppose Public Trust Initiatives",
        "Risk Assessment and Vaccine Recommendations: Support for Universal Recommendations",
        "Risk Assessment and Vaccine Recommendations: Oppose Universal Recommendations",
        "Vaccine Distribution Policy: Support for Current Distribution Policy",
        "Vaccine Distribution Policy: Oppose Current Distribution Policy",
        "Anti-Vaccine Sentiment: Oppose Anti-Vaccine Influence",
        "Anti-Vaccine Sentiment: Support Anti-Vaccine Influence"
    ]
    
    system_prompt = """You are analyzing public comments about CDC ACIP's recommendations on COVID-19 vaccines.

The public comments discuss the CDC's Advisory Committee on Immunization Practices (ACIP) recommendations and actions regarding COVID-19 vaccines, including access to vaccines, changes in committee membership, and concerns about vaccine safety and efficacy.

For each comment, identify:

1. Stances: Which of these theme:position combinations does the commenter express? Look for the indicators listed below. (Select ALL that apply, or none if none apply)
- COVID-19 Vaccine Access: Support for COVID-19 Vaccine Access: universal recommendation and access; available for all; right to continue use; preserve access; full coverage by insurance
- COVID-19 Vaccine Access: Oppose COVID-19 Vaccine Access: remove access; limit access; suspend access; restrict access; prevent people from access
- Vaccine Safety Concerns: Support for Vaccine Safety: safe and effective; life-saving tools; no significant adverse effects; saved hundreds of thousands of lives
- Vaccine Safety Concerns: Oppose Vaccine Safety: remove all COVID shots; reports of deaths; serious adverse reactions; Vaccine Adverse Event Reporting System
- Regulatory Changes and Committee Membership: Support for Current Regulatory Changes: support for RFK Jr.'s decision; agree with committee changes; trust in new ACIP members
- Regulatory Changes and Committee Membership: Oppose Current Regulatory Changes: reinstate previous ACIP members; influence of anti-vaccine personalities; concerns about conflicts of interest; RFK Jr.'s actions are harmful
- Independent Scientific Oversight: Support for Independent Oversight: independent decision process; disclose all conflicts of interest; keep politics out of public health; scientifically based
- Independent Scientific Oversight: Oppose Independent Oversight: biased decisions; interference in decision-making; political influence on recommendations
- Public Trust in Vaccinations: Support for Public Trust Initiatives: regain public's trust; transparent processes; scientific evidence; inform access to safe vaccines
- Public Trust in Vaccinations: Oppose Public Trust Initiatives: eroded public confidence; mistrust in current vaccines; lack of transparency; concerns ignored
- Risk Assessment and Vaccine Recommendations: Support for Universal Recommendations: universal recommendation for all ages; same schedule for everyone; CDC must recommend vaccines for all
- Risk Assessment and Vaccine Recommendations: Oppose Universal Recommendations: individualized schedule; consider health status and risk factors; context-based vaccination
- Vaccine Distribution Policy: Support for Current Distribution Policy: preserve current guidelines; insurance coverage; protect vulnerable groups
- Vaccine Distribution Policy: Oppose Current Distribution Policy: change distribution priorities; concern about coverage inadequacies; vulnerable still at risk
- Anti-Vaccine Sentiment: Oppose Anti-Vaccine Influence: anti-vaxxer RFK is not unbiased; conspiracy mongers; scientific misinformation
- Anti-Vaccine Sentiment: Support Anti-Vaccine Influence: support for removal of vaccines; questions on vaccine effectiveness; skeptical of vaccine agendas

2. Key Quote: Select the most important quote (max 100 words) that best captures the essence of the comment. Must be verbatim from the text.

3. Rationale: Briefly explain (1-2 sentences) why you selected these theme:position combinations.

Instructions:
- A comment may express multiple stances or no clear stance
- Only select stances that are clearly expressed in the comment
- Be objective and avoid inserting personal opinions"""

    # Create temporary config dict
    temp_config = {
        'stance_options': stance_options,
        'system_prompt': system_prompt
    }
    
    # Write temporary config to use with CommentAnalyzer
    import tempfile
    import json
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(temp_config, f)
        temp_config_file = f.name
    
    analyzer = CommentAnalyzer(
        model=model,
        timeout_seconds=timeout_seconds or 120,
        config_file=temp_config_file
    )
    
    # Clean up temp file
    import os
    os.unlink(temp_config_file)
    
    return analyzer