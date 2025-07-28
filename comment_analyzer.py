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
class EntityType(str, Enum):
    INDIVIDUAL_CITIZEN = "Individual/Citizen"
    HEALTHCARE_PROVIDER = "Healthcare Provider"
    TECHNOLOGY_COMPANY = "Technology Company"
    ADVOCACY_GROUP = "Advocacy Group"
    PROFESSIONAL_ASSOCIATION = "Professional Association"
    MEDICAL_DEVICE_MANUFACTURER = "Medical Device Manufacturer"
    INSURANCE_COMPANY = "Insurance Company"
    OTHER_UNKNOWN = "Other/Unknown"


class Stance(str, Enum):
    DIGITAL_IDENTIFICATION_AND_BIOMETRIC_DATA_SUPPORT_DIGITAL_IDS_AND_BIOMETRICS = "Digital Identification and Biometric Data: Support Digital IDs and Biometrics"
    DIGITAL_IDENTIFICATION_AND_BIOMETRIC_DATA_OPPOSE_DIGITAL_IDS_AND_BIOMETRICS = "Digital Identification and Biometric Data: Oppose Digital IDs and Biometrics"
    HEALTH_DATA_INTEROPERABILITY_SUPPORT_STANDARDIZED_PROTOCOLS_FOR_INTEROPERABILITY = "Health Data Interoperability: Support Standardized Protocols for Interoperability"
    HEALTH_DATA_INTEROPERABILITY_OPPOSE_MANDATING_SPECIFIC_PLATFORMS = "Health Data Interoperability: Oppose Mandating Specific Platforms"
    PATIENT_PRIVACY_AND_CONSENT_PRIORITIZE_PATIENT_CONSENT_AND_PRIVACY = "Patient Privacy and Consent: Prioritize Patient Consent and Privacy"
    PATIENT_PRIVACY_AND_CONSENT_FLEXIBLE_USE_OF_PATIENT_DATA_FOR_IMPROVED_CARE = "Patient Privacy and Consent: Flexible Use of Patient Data for Improved Care"
    ADOPTION_AND_INTEGRATION_OF_DIGITAL_HEALTH_TOOLS_PROMOTE_BROAD_ADOPTION_OF_DIGITAL_HEALTH_TOOLS = "Adoption and Integration of Digital Health Tools: Promote Broad Adoption of Digital Health Tools"
    ADOPTION_AND_INTEGRATION_OF_DIGITAL_HEALTH_TOOLS_CAUTIOUS_INTEGRATION_OF_DIGITAL_HEALTH_TOOLS = "Adoption and Integration of Digital Health Tools: Cautious Integration of Digital Health Tools"
    GOVERNMENT_ROLE_IN_HEALTH_TECHNOLOGY_ACTIVE_GOVERNMENT_ROLE_IN_STANDARDIZING_HEALTH_TECH = "Government Role in Health Technology: Active Government Role in Standardizing Health Tech"
    GOVERNMENT_ROLE_IN_HEALTH_TECHNOLOGY_LIMIT_GOVERNMENT_CONTROL_OVER_HEALTH_TECHNOLOGY = "Government Role in Health Technology: Limit Government Control Over Health Technology"


class CommentAnalysisResult(BaseModel):
    """Standard model for comment analysis results"""
    stances: List[Stance] = Field(
        default_factory=list,
        description="List of stances/arguments expressed in the comment (0 or more)"
    )
    entity_type: str = Field(
        description="Type of entity submitting the comment (must be one of the predefined entity types)"
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
        self.entity_types = self.config.get('entity_types', [])
        # Always ensure Other/Unknown is in the list
        if "Other/Unknown" not in self.entity_types:
            self.entity_types.append("Other/Unknown")
        self.system_prompt = self.config.get('system_prompt')
        
        logger.info(f"Loaded configuration for: {self.config.get('regulation_name', 'Unknown Regulation')}")
        logger.info(f"Using {len(self.stance_options)} stance options")
        logger.info(f"Using {len(self.entity_types)} entity types")
        
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
        
        # Build context about the submitter
        context_parts = []
        if organization:
            context_parts.append(f"Organization: {organization}")
        if submitter:
            context_parts.append(f"Submitter: {submitter}")
        
        context = " | ".join(context_parts) if context_parts else ""
        
        # Create a thread-safe container for the result
        result_container = {'result': None, 'error': None}
        
        def api_call():
            try:
                response = completion(
                    temperature=0.0,
                    model=self.model,
                    messages=[
                        {"role": "system", "content": self.get_system_prompt()},
                        {"role": "user", "content": f"Analyze the following public comment{identifier}:\n\n{context}\n\n{comment_text}" if context else f"Analyze the following public comment{identifier}:\n\n{comment_text}"}
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
                
                # Convert string values to enum values if needed
                if 'stances' in result and isinstance(result['stances'], list):
                    result['stances'] = [s if isinstance(s, Stance) else s for s in result['stances']]
                
                # Handle entity_type - keep as string since LLM returns string
                if 'entity_type' in result:
                    # Ensure it's one of the allowed values
                    if result['entity_type'] not in self.entity_types:
                        result['entity_type'] = "Other/Unknown"
                
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
        "Digital Identification and Biometric Data: Support Digital IDs and Biometrics",
        "Digital Identification and Biometric Data: Oppose Digital IDs and Biometrics",
        "Health Data Interoperability: Support Standardized Protocols for Interoperability",
        "Health Data Interoperability: Oppose Mandating Specific Platforms",
        "Patient Privacy and Consent: Prioritize Patient Consent and Privacy",
        "Patient Privacy and Consent: Flexible Use of Patient Data for Improved Care",
        "Adoption and Integration of Digital Health Tools: Promote Broad Adoption of Digital Health Tools",
        "Adoption and Integration of Digital Health Tools: Cautious Integration of Digital Health Tools",
        "Government Role in Health Technology: Active Government Role in Standardizing Health Tech",
        "Government Role in Health Technology: Limit Government Control Over Health Technology"
    ]
    
    system_prompt = """You are analyzing public comments about CMS Health Technology Ecosystem.

A request for information on advancing interoperability, data sharing, patient empowerment, and digital standards within the health technology ecosystem, specifically under Medicare and Medicaid systems.

For each comment, identify:

1. Stances: Which of these theme:position combinations does the commenter express? Look for the indicators listed below. (Select ALL that apply, or none if none apply)
- Digital Identification and Biometric Data: Support Digital IDs and Biometrics: support digital ID for secure access; biometric data enhances security; advocating for digital identity in healthcare
- Digital Identification and Biometric Data: Oppose Digital IDs and Biometrics: absolutely no digital ID requirement; never forced to provide biometric data; mandated wearable technologies; right to be forgotten
- Health Data Interoperability: Support Standardized Protocols for Interoperability: adopt open, standardized protocols; importance of HL7 FHIR standards; universal EMR system for patient data sharing
- Health Data Interoperability: Oppose Mandating Specific Platforms: fragmented by proprietary systems; do not mandate or favor specific platforms; services should communicate regardless of hardware
- Patient Privacy and Consent: Prioritize Patient Consent and Privacy: right to control personal data; informed consent must be standard; privacy rights must be protected
- Patient Privacy and Consent: Flexible Use of Patient Data for Improved Care: data sharing empowers better decision making; enhanced care through shared data; technology to improve outcomes despite privacy concerns
- Adoption and Integration of Digital Health Tools: Promote Broad Adoption of Digital Health Tools: expand Medicare coverage for digital tools; integrate RPM and telehealth tools; encourage the use of digital health products
- Adoption and Integration of Digital Health Tools: Cautious Integration of Digital Health Tools: concerns over privacy in digital health; importance of safeguarding patient interactions; require rigorous validation before adoption
- Government Role in Health Technology: Active Government Role in Standardizing Health Tech: government should create EMR standards; CMS involvement in tech innovation; government-owned solutions for standardization
- Government Role in Health Technology: Limit Government Control Over Health Technology: government intrusion risks privacy; focus on innovations not mandates; avoid government-mandated tech solutions

2. Entity Type: Identify what type of entity is submitting this comment. Look for clues in the organization name, submitter title, and the comment text itself (e.g., "As a physician", "Our hospital", "I am a patient"). Only select a specific entity type if there's clear evidence. If you cannot determine the entity type from the available information, select "Other/Unknown". Choose from:
- Individual/Citizen
- Healthcare Provider
- Technology Company
- Advocacy Group
- Professional Association
- Medical Device Manufacturer
- Insurance Company
- Other/Unknown

3. Key Quote: Select the most important quote (max 100 words) that best captures the essence of the comment. Must be verbatim from the text.

4. Rationale: Briefly explain (1-2 sentences) why you selected these theme:position combinations.

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