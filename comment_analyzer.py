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
    DIGITAL_HEALTH_INFRASTRUCTURE_SUPPORT_FOR_DIGITAL_HEALTH_INFRASTRUCTURE = "Digital Health Infrastructure: Support for Digital Health Infrastructure"
    DIGITAL_HEALTH_INFRASTRUCTURE_OPPOSE_DIGITAL_HEALTH_INFRASTRUCTURE = "Digital Health Infrastructure: Oppose Digital Health Infrastructure"
    PATIENT_PRIVACY_AND_DATA_SECURITY_SUPPORT_ENHANCED_DATA_PRIVACY_MEASURES = "Patient Privacy and Data Security: Support Enhanced Data Privacy Measures"
    PATIENT_PRIVACY_AND_DATA_SECURITY_OPPOSE_ADDITIONAL_DATA_PRIVACY_REGULATIONS = "Patient Privacy and Data Security: Oppose Additional Data Privacy Regulations"
    INTEROPERABILITY_AND_DATA_EXCHANGE_SUPPORT_INTEROPERABILITY_INITIATIVES = "Interoperability and Data Exchange: Support Interoperability Initiatives"
    INTEROPERABILITY_AND_DATA_EXCHANGE_OPPOSE_BROAD_INTEROPERABILITY_MANDATES = "Interoperability and Data Exchange: Oppose Broad Interoperability Mandates"
    USE_OF_BIOMETRIC_DATA_AND_DIGITAL_IDS_SUPPORT_USE_OF_BIOMETRIC_DATA_AND_DIGITAL_IDS = "Use of Biometric Data and Digital IDs: Support Use of Biometric Data and Digital IDs"
    USE_OF_BIOMETRIC_DATA_AND_DIGITAL_IDS_OPPOSE_USE_OF_BIOMETRIC_DATA_AND_DIGITAL_IDS = "Use of Biometric Data and Digital IDs: Oppose Use of Biometric Data and Digital IDs"
    AUTONOMY_IN_HEALTHCARE_DECISIONS_SUPPORT_INDIVIDUAL_AUTONOMY_IN_HEALTHCARE = "Autonomy in Healthcare Decisions: Support Individual Autonomy in Healthcare"
    AUTONOMY_IN_HEALTHCARE_DECISIONS_SUPPORT_CENTRALIZED_HEALTHCARE_MANAGEMENT = "Autonomy in Healthcare Decisions: Support Centralized Healthcare Management"


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
        "Digital Health Infrastructure: Support for Digital Health Infrastructure",
        "Digital Health Infrastructure: Oppose Digital Health Infrastructure",
        "Patient Privacy and Data Security: Support Enhanced Data Privacy Measures",
        "Patient Privacy and Data Security: Oppose Additional Data Privacy Regulations",
        "Interoperability and Data Exchange: Support Interoperability Initiatives",
        "Interoperability and Data Exchange: Oppose Broad Interoperability Mandates",
        "Use of Biometric Data and Digital IDs: Support Use of Biometric Data and Digital IDs",
        "Use of Biometric Data and Digital IDs: Oppose Use of Biometric Data and Digital IDs",
        "Autonomy in Healthcare Decisions: Support Individual Autonomy in Healthcare",
        "Autonomy in Healthcare Decisions: Support Centralized Healthcare Management"
    ]
    
    system_prompt = """You are analyzing public comments about CMS Health Technology Ecosystem RFI (CMS-0042-NC).

The Centers for Medicare & Medicaid Services (CMS) and the Assistant Secretary for Technology Policy/Office of the National Coordinator for Health Information Technology (ASTP/ONC) issued a Request for Information (RFI) regarding digital health, interoperability, and development of a patient-centered health technology ecosystem.

For each comment, identify:

1. Stances: Which of these theme:position combinations does the commenter express? Look for the indicators listed below. (Select ALL that apply, or none if none apply)
- Digital Health Infrastructure: Support for Digital Health Infrastructure: advocate for digital health solutions; support interoperability advancements; emphasize improving patient outcomes through digital tools
- Digital Health Infrastructure: Oppose Digital Health Infrastructure: concerns about data privacy and security; fear of government overreach; believe technology is complicating healthcare
- Patient Privacy and Data Security: Support Enhanced Data Privacy Measures: advocate for stronger data privacy protections; ensure patients control their data; oppose compulsory data sharing without consent
- Patient Privacy and Data Security: Oppose Additional Data Privacy Regulations: believe existing regulations are sufficient; concern over hindering technological progress; view more regulations as burdensome
- Interoperability and Data Exchange: Support Interoperability Initiatives: advocate for interoperability standards like FHIR; support seamless data exchange; emphasize improved care coordination
- Interoperability and Data Exchange: Oppose Broad Interoperability Mandates: worry about data privacy risks; view current systems as adequate; fear data breaches from expanded sharing
- Use of Biometric Data and Digital IDs: Support Use of Biometric Data and Digital IDs: believe they enhance security and efficiency; support their use for easy access to healthcare records; view digital IDs as modernizing healthcare systems
- Use of Biometric Data and Digital IDs: Oppose Use of Biometric Data and Digital IDs: oppose mandatory biometric data collection; fear privacy violations; concern over government tracking
- Autonomy in Healthcare Decisions: Support Individual Autonomy in Healthcare: oppose mandatory technological solutions; advocate for informed consent; support patients' right to control their healthcare data
- Autonomy in Healthcare Decisions: Support Centralized Healthcare Management: advocate for streamlined technological integration; believe standardization improves care; view centralized data as enhancing efficiency

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