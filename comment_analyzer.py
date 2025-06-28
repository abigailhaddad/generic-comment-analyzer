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
    OPPOSITION_TO_COVID_19_VACCINES_CALL_FOR_REMOVAL = "Opposition to COVID-19 Vaccines (Call for Removal)"
    SUPPORT_FOR_CONTINUED_BROAD_ACCESS_TO_COVID_19_VACCINES = "Support for Continued/Broad Access to COVID-19 Vaccines"
    SUPPORT_FOR_SCIENCE_BASED_INDEPENDENT_ACIP_COMMITTEE = "Support for Science-Based, Independent ACIP Committee"
    OPPOSITION_TO_RESTRICTING_VACCINE_ACCESS_ELIGIBILITY_INCLUDING_CHILDREN_AND_HIGH_RISK = "Opposition to Restricting Vaccine Access/Eligibility (including Children and High-Risk)"
    OPPOSITION_TO_POLITICIZATION_DELEGITIMIZATION_OF_VACCINE_POLICY = "Opposition to Politicization/Delegitimization of Vaccine Policy"
    SUPPORT_FOR_VACCINE_PREVENTABLE_DISEASE_PROTECTION_AND_BROAD_IMMUNIZATION_POLICY = "Support for Vaccine-Preventable Disease Protection and Broad Immunization Policy"


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
        "Opposition to COVID-19 Vaccines (Call for Removal)",
        "Support for Continued/Broad Access to COVID-19 Vaccines",
        "Support for Science-Based, Independent ACIP Committee",
        "Opposition to Restricting Vaccine Access/Eligibility (including Children and High-Risk)",
        "Opposition to Politicization/Delegitimization of Vaccine Policy",
        "Support for Vaccine-Preventable Disease Protection and Broad Immunization Policy"
    ]
    
    system_prompt = """You are analyzing public comments about CDC ACIP COVID-19 and Vaccine Committee Policy Changes, June 2025.

A set of proposed and implemented changes affecting the Advisory Committee on Immunization Practices (ACIP) at CDC, including the removal of all 17 previous members by RFK Jr. and policies about the continued availability, restriction, or removal of COVID-19 vaccines and other immunization recommendations and coverage.

For each comment, identify:

1. Stances: Which of these positions/arguments does the commenter express? Look for the indicators listed below. (Select ALL that apply, or none if none apply)
- Opposition to COVID-19 Vaccines (Call for Removal): remove all currently licensed COVID shots from the market; destroy the deadly vaccines; get rid of these vaccines; these vaccines cause more harm than good; deaths following COVID vaccination in the Vaccine Adverse Event Reporting System; comparing to swine flu vaccine recall; urge ACIP to take a stance for public health by pulling COVID vaccines; routine and repeated COVID shots should stop; evidence is clear, remove COVID shots; toxins knowingly placed in these injections to harm
- Support for Continued/Broad Access to COVID-19 Vaccines: keep COVID-19 vaccines available to everyone; protect access to covid 19 vaccines; do not restrict the Covid vaccine; please don't take it away; vaccines save lives; vaccines should be covered by insurance; COVID vaccine has proven to be safe and effective; need access for high-risk/disabled/immunocompromised people; demand full access to all healthcare including vaccines; please keep all Vaccines available to whom ever would like to do so
- Support for Science-Based, Independent ACIP Committee: grave/serious concerns about the recent termination of all 17 ACIP committee members; independent expert scientific and medical input is crucial; oppose replacement with unqualified or anti-vaccine figures; RFK Jr. must preserve an independent decision process; committee must be made up of experienced, qualified people; removal of previous members harms public health; demand restoration of former ACIP members; distrust of politically-motivated removals; platforming of pseudoscience profiteers; calls for transparency and conflict of interest disclosures
- Opposition to Restricting Vaccine Access/Eligibility (including Children and High-Risk): oppose unnecessary restrictions placed on COVID vaccinations; do not change vaccine policies that would make vaccines more difficult to access; setting restrictions on vaccines is not only unnecessary, it's anti-science; universal or broad access is necessary; removal or restriction would endanger vulnerable groups; insurance should cover vaccines for all ages; keep covid vaccines on the recommended schedule; Novavax (non mRNA) should be approved for all ages and covered
- Opposition to Politicization/Delegitimization of Vaccine Policy: RFK Jr. has politicized science; political shenanigans; politics should not make this decision; removal/appointment of ACIP members for political, not scientific, reasons; keep the committee apolitical; must be actual experts, not political appointees; government interference in public health
- Support for Vaccine-Preventable Disease Protection and Broad Immunization Policy: vaccines are a proven way out of polio, measles, covid, etc.; broad access to all FDA-approved and recommended vaccines; keep access to vaccines for all vaccine-preventable diseases; importance of vaccination for community health, children, workplace, school safety; calls to keep or expand coverage through Vaccines for Children and insurance; concerns about declining vaccination rates and outbreaks

2. Key Quote: Select the most important quote (max 100 words) that best captures the essence of the comment. Must be verbatim from the text.

3. Rationale: Briefly explain (1-2 sentences) why you selected these stances.

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