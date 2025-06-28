def analyze_comments_sample_fixed(comments, discovered_stances, sample_size=50, max_workers=2, batch_size=10):
    """Analyze a sample of comments using direct LLM calls to bypass hardcoded enums."""
    import random
    import litellm
    import os
    import time
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from tqdm import tqdm
    from datetime import datetime
    
    # Sample comments if needed
    if len(comments) > sample_size:
        sampled_comments = random.sample(comments, sample_size)
    else:
        sampled_comments = comments
    
    print(f"Analyzing {len(sampled_comments)} comments with {max_workers} workers in batches of {batch_size}...")
    
    analyzed_comments = []
    
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
    
    def analyze_single_comment(comment, comment_index):
        """Analyze a single comment using direct LLM call."""
        try:
            # Truncate comment text if too long
            comment_text = comment['text']
            if len(comment_text) > 2000:
                comment_text = comment_text[:2000]
            
            # Direct LLM call without constrained response format
            user_prompt = f"""Analyze this comment and identify stances/positions:

{comment_text}

Respond in JSON format with:
- stances: list of stance names that apply (from the stance options in the system prompt)
- new_stances: list of NEW stance names ONLY if the comment expresses positions that are FUNDAMENTALLY DIFFERENT from all existing stances (not just variations or nuances). This should be very rare - only use if the comment takes a completely different angle that doesn't fit any existing stance at all.
- key_quote: most important quote from the comment (max 100 words)
- rationale: brief explanation of why these stances were selected

IMPORTANT: Only add to new_stances if the position is REALLY different - like a completely different perspective, argument, or approach that doesn't map to any existing stance. Minor variations or nuanced differences should still use existing stances.
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
                'new_stances': result.get('new_stances', []),
                'key_quote': result.get('key_quote', ''),
                'rationale': result.get('rationale', '')
            }
            
            # Create comment record
            analyzed_comment = {
                'id': comment.get('id', f'test-{comment_index}'),
                'text': comment['text'],
                'comment_text': comment['text'],
                'date': datetime.now().isoformat(),
                'first_name': 'Test',
                'last_name': 'User', 
                'organization': '',
                'attachment_text': '',
                'duplication_count': 1,
                'duplication_ratio': 1,
                'analysis': analysis_result
            }
            
            return analyzed_comment
            
        except Exception as e:
            print(f"Failed to analyze comment {comment_index}: {e}")
            return None
    
    # Process in batches to avoid overwhelming the API
    for batch_start in tqdm(range(0, len(sampled_comments), batch_size), desc="Processing batches", unit="batch"):
        batch_end = min(batch_start + batch_size, len(sampled_comments))
        batch_comments = sampled_comments[batch_start:batch_end]
        
        batch_results = []
        
        # Use ThreadPoolExecutor for parallel API calls
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all comments in this batch
            future_to_comment = {}
            for i, comment in enumerate(batch_comments):
                comment_index = batch_start + i
                future = executor.submit(analyze_single_comment, comment, comment_index)
                future_to_comment[future] = comment
            
            # Collect results as they complete
            with tqdm(total=len(batch_comments), desc=f"Batch {batch_start//batch_size + 1}", leave=False) as pbar:
                for future in as_completed(future_to_comment, timeout=180):
                    try:
                        result = future.result(timeout=60)
                        if result is not None:
                            batch_results.append(result)
                        pbar.update(1)
                    except Exception as e:
                        print(f"Comment analysis failed or timed out: {e}")
                        pbar.update(1)
        
        analyzed_comments.extend(batch_results)
        
        # Brief pause between batches to be courteous to the API
        if batch_end < len(sampled_comments):
            time.sleep(2)
    
    print(f"Successfully analyzed {len(analyzed_comments)} comments")
    return analyzed_comments