#!/usr/bin/env python3
"""
Generate HTML Report from Comment Analysis Results

Creates an interactive HTML report with summary statistics and searchable table.
"""

import argparse
import json
import os
from datetime import datetime
from typing import Dict, Any, List
import pandas as pd

def load_results(json_file: str) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Load analyzed comments from JSON file and return comments plus metadata."""
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
        # Handle both direct list format and {"comments": [...]} format
        if isinstance(data, dict) and 'comments' in data:
            return data['comments'], data
        elif isinstance(data, list):
            return data, {}
        else:
            raise ValueError(f"Unexpected JSON format in {json_file}")

def load_results_parquet(parquet_file: str) -> List[Dict[str, Any]]:
    """Load analyzed comments from Parquet file."""
    df = pd.read_parquet(parquet_file)
    # Convert to dict and handle numpy arrays
    records = df.to_dict('records')
    
    # Convert numpy arrays to lists for proper JSON serialization
    import numpy as np
    for record in records:
        if 'analysis' in record and record['analysis']:
            analysis = record['analysis']
            if 'stances' in analysis and isinstance(analysis['stances'], np.ndarray):
                analysis['stances'] = analysis['stances'].tolist()
    
    return records

def analyze_field_types(comments: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Analyze standard analysis fields."""
    field_analysis = {}
    
    # Standard fields we expect - only show stats for checkbox fields
    fields = {
        'stances': {'type': 'checkbox', 'is_list': True},
        'new_stances': {'type': 'checkbox', 'is_list': True}
    }
    
    for field_name, field_info in fields.items():
        unique_values = set()
        total_values = 0
        
        for comment in comments:
            analysis = comment.get('analysis', {})
            if analysis and field_name in analysis:
                value = analysis[field_name]
                total_values += 1
                
                if field_info['is_list'] and isinstance(value, list):
                    for item in value:
                        if isinstance(item, str):
                            unique_values.add(item.strip())
                elif isinstance(value, str):
                    unique_values.add(value.strip())
        
        field_analysis[field_name] = {
            'type': field_info['type'],
            'unique_values': sorted(list(unique_values)),
            'num_unique': len(unique_values),
            'is_list': field_info['is_list'],
            'total_occurrences': total_values
        }
    
    # Add duplication count analysis
    dup_counts = {}
    dup_ratios = {}
    
    for comment in comments:
        count = comment.get('duplication_count', 1)
        ratio = comment.get('duplication_ratio', 1)
        
        dup_counts[count] = dup_counts.get(count, 0) + 1
        dup_ratios[ratio] = dup_ratios.get(ratio, 0) + 1
    
    # Sort by count/ratio value
    field_analysis['duplication_count'] = {
        'type': 'checkbox',
        'unique_values': sorted(dup_counts.keys()),
        'num_unique': len(dup_counts),
        'is_list': False,
        'counts': dup_counts
    }
    
    field_analysis['duplication_ratio'] = {
        'type': 'checkbox', 
        'unique_values': sorted(dup_ratios.keys()),
        'num_unique': len(dup_ratios),
        'is_list': False,
        'counts': dup_ratios
    }
    
    return field_analysis

def calculate_stance_cooccurrence(comments: List[Dict[str, Any]], stance_list: List[str]) -> Dict[str, Dict[str, int]]:
    """Calculate how often stances appear together."""
    cooccurrence = {}
    
    # Initialize matrix
    for stance1 in stance_list:
        cooccurrence[stance1] = {}
        for stance2 in stance_list:
            cooccurrence[stance1][stance2] = 0
    
    # Count co-occurrences
    for comment in comments:
        analysis = comment.get('analysis', {})
        if analysis:
            stances = analysis.get('stances', [])
            if isinstance(stances, list):
                for stance1 in stances:
                    if stance1 in stance_list:
                        for stance2 in stances:
                            if stance2 in stance_list:
                                cooccurrence[stance1][stance2] += 1
    
    return cooccurrence

def identify_unusual_combinations(stances: List[str], cooccurrence: Dict[str, Dict[str, int]], total_comments: int, threshold: float = 0.02) -> bool:
    """Check if a comment has an unusual combination of stances.
    
    Args:
        stances: List of stances in the comment
        cooccurrence: Co-occurrence matrix
        total_comments: Total number of comments
        threshold: Consider combinations unusual if they occur in less than this fraction of comments
    
    Returns:
        True if the comment has an unusual combination
    """
    if len(stances) < 2:
        return False
    
    # Check all pairs of stances
    for i, stance1 in enumerate(stances):
        for stance2 in stances[i+1:]:
            if stance1 in cooccurrence and stance2 in cooccurrence[stance1]:
                # How often do these two stances appear together?
                pair_count = cooccurrence[stance1][stance2]
                pair_frequency = pair_count / total_comments if total_comments > 0 else 0
                
                # If this pair is very rare (appears in less than threshold of comments), it's unusual
                if pair_count > 0 and pair_frequency < threshold:
                    return True
    
    return False

def calculate_agreement_scores(comments: List[Dict[str, Any]], field_analysis: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    """Calculate agreement scores matrix for themes and positions.
    
    Agreement scores = co-occurrence percentages between every pair of positions.
    For each position A, shows what percentage of comments with A also have position B.
    """
    agreement_scores = {}
    
    if 'stances' in field_analysis:
        stance_list = field_analysis['stances'].get('unique_values', [])
        
        # Calculate co-occurrence matrix
        stance_cooccurrence = calculate_stance_cooccurrence(comments, stance_list)
        
        # Calculate agreement scores for each stance pair
        for stance1 in stance_list:
            if stance1 in stance_cooccurrence:
                total_with_stance1 = stance_cooccurrence[stance1][stance1]  # How many comments have this stance
                agreement_scores[stance1] = {}
                
                for stance2 in stance_list:
                    if stance2 in stance_cooccurrence[stance1]:
                        if stance1 == stance2:
                            # Self-agreement is always 100%
                            agreement_scores[stance1][stance2] = 100.0
                        elif total_with_stance1 > 0:
                            # Calculate percentage of stance1 comments that also have stance2
                            overlap_count = stance_cooccurrence[stance1][stance2]
                            agreement_percentage = (overlap_count / total_with_stance1) * 100
                            agreement_scores[stance1][stance2] = agreement_percentage
                        else:
                            agreement_scores[stance1][stance2] = 0.0
                    else:
                        agreement_scores[stance1][stance2] = 0.0
    
    return agreement_scores

def calculate_stats(comments: List[Dict[str, Any]], field_analysis: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Calculate summary statistics."""
    total_comments = len(comments)
    
    # Dynamic field counting based on field analysis
    field_counts = {}
    for field, info in field_analysis.items():
        field_counts[field] = {}
        
        for comment in comments:
            analysis = comment.get('analysis', {})
            if analysis and field in analysis:
                value = analysis[field]
                
                if info['is_list'] and isinstance(value, list):
                    # Count each item in list
                    for item in value:
                        if isinstance(item, str):
                            item = item.strip()
                            field_counts[field][item] = field_counts[field].get(item, 0) + 1
                elif isinstance(value, str):
                    value = value.strip()
                    field_counts[field][value] = field_counts[field].get(value, 0) + 1
    
    # Calculate stance co-occurrences
    stance_cooccurrence = {}
    if 'stances' in field_analysis:
        stance_list = field_analysis['stances'].get('unique_values', [])
        if stance_list:
            stance_cooccurrence = calculate_stance_cooccurrence(comments, stance_list)
    
    # Calculate agreement scores
    agreement_scores = calculate_agreement_scores(comments, field_analysis)
    
    # Comments with attachments
    with_attachments = sum(1 for c in comments if c.get('attachment_text', '').strip())
    
    # Attachment processing statistics
    attachment_stats = {
        'total_with_attachments': 0,
        'total_attachments': 0,
        'processed_successfully': 0,
        'failed_attachments': 0,
        'comments_with_failures': 0,
        'failure_reasons': {'download_failed': 0, 'no_text_extracted': 0}
    }
    
    for comment in comments:
        attachment_status = comment.get('attachment_status')
        if attachment_status:
            attachment_stats['total_with_attachments'] += 1
            attachment_stats['total_attachments'] += attachment_status.get('total', 0)
            attachment_stats['processed_successfully'] += attachment_status.get('processed', 0)
            attachment_stats['failed_attachments'] += attachment_status.get('failed', 0)
            
            if attachment_status.get('failed', 0) > 0:
                attachment_stats['comments_with_failures'] += 1
                
                for failure in attachment_status.get('failures', []):
                    reason = failure.get('reason', 'unknown')
                    if reason in attachment_stats['failure_reasons']:
                        attachment_stats['failure_reasons'][reason] += 1
    
    # Average text length
    text_lengths = [len(c.get('text', '')) for c in comments]
    avg_length = sum(text_lengths) / len(text_lengths) if text_lengths else 0
    
    return {
        'total_comments': total_comments,
        'field_counts': field_counts,
        'stance_cooccurrence': stance_cooccurrence,
        'agreement_scores': agreement_scores,
        'with_attachments': with_attachments,
        'attachment_stats': attachment_stats,
        'avg_text_length': int(avg_length),
        'date_range': get_date_range(comments)
    }

def get_date_range(comments: List[Dict[str, Any]]) -> str:
    """Get date range of comments."""
    dates = []
    for comment in comments:
        date_str = comment.get('date', '')
        if date_str:
            try:
                # Parse ISO date
                date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                dates.append(date)
            except:
                pass
    
    if dates:
        min_date_obj = min(dates)
        max_date_obj = max(dates)
        
        # Format dates more intuitively
        min_date = min_date_obj.strftime('%B %d, %Y')  # e.g., "June 17, 2025"
        max_date = max_date_obj.strftime('%B %d, %Y')
        
        if min_date == max_date:
            return min_date
        
        # If same month and year, simplify the range
        if min_date_obj.strftime('%B %Y') == max_date_obj.strftime('%B %Y'):
            return f"{min_date_obj.strftime('%B %d')}-{max_date_obj.strftime('%d, %Y')}"  # e.g., "June 17-24, 2025"
        else:
            return f"{min_date} to {max_date}"
    return "Unknown"

def calculate_cooccurrence_percentages(stats: Dict[str, Any], field_analysis: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    """Calculate what percentage of comments with each stance also have other stances."""
    cooccurrence = stats.get('stance_cooccurrence', {})
    if not cooccurrence:
        return {}
    
    percentages = {}
    stance_names = list(cooccurrence.keys())
    
    # Create stance number mapping
    stance_to_num = {stance: i+1 for i, stance in enumerate(stance_names)}
    
    for stance1 in stance_names:
        total_with_stance1 = cooccurrence[stance1][stance1]  # Diagonal value
        if total_with_stance1 == 0:
            continue
            
        percentages[stance1] = {}
        for stance2 in stance_names:
            if stance1 != stance2:  # Skip self
                count = cooccurrence[stance1][stance2]
                percentage = (count / total_with_stance1) * 100 if total_with_stance1 > 0 else 0
                if percentage > 0:  # Only include non-zero percentages
                    percentages[stance1][stance_to_num[stance2]] = percentage
    
    return percentages, stance_to_num

def generate_new_stances_compact_html(field_info: Dict[str, Any], stats: Dict[str, Any]) -> str:
    """Generate compact HTML for new stances distribution."""
    if not field_info or not field_info.get('unique_values'):
        return ""
    
    field_counts = stats['field_counts'].get('new_stances', {})
    if not field_counts:
        return ""
    
    # Sort by count
    sorted_items = sorted(field_counts.items(), key=lambda x: x[1], reverse=True)
    
    # Create table rows
    table_rows = []
    for stance, count in sorted_items:
        percentage = (count / stats['total_comments']) * 100
        table_rows.append(f'''
            <tr>
                <td class="new-stance-name">{stance}</td>
                <td class="new-stance-count">{count}</td>
                <td class="new-stance-percentage">{percentage:.1f}%</td>
            </tr>''')
    
    return f'''
        <div class="section compact-section">
            <h3>🆕 New Stances</h3>
            <p class="section-note">Additional stances discovered during analysis that didn't fit predefined categories</p>
            {f'''
            <table class="new-stances-table">
                <thead>
                    <tr>
                        <th>Stance</th>
                        <th>Count</th>
                        <th>%</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join(table_rows)}
                </tbody>
            </table>''' if table_rows else '<p><em>No new stances discovered</em></p>'}
        </div>
        
        <style>
        .compact-section {{
            padding: 15px;
            margin: 15px 0;
        }}
        
        .compact-section h3 {{
            font-size: 1.2em;
            margin-bottom: 8px;
            color: #666;
        }}
        
        .section-note {{
            font-size: 0.9em;
            color: #888;
            margin-bottom: 10px;
            font-style: italic;
        }}
        
        .new-stances-table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9em;
        }}
        
        .new-stances-table th {{
            background: #f8f9fa;
            padding: 8px;
            text-align: left;
            border-bottom: 1px solid #dee2e6;
            font-weight: 600;
        }}
        
        .new-stances-table td {{
            padding: 6px 8px;
            border-bottom: 1px solid #eee;
            vertical-align: top;
        }}
        
        .new-stance-name {{
            font-weight: 500;
        }}
        
        .new-stance-count {{
            text-align: center;
            width: 80px;
        }}
        
        .new-stance-percentage {{
            text-align: center;
            width: 60px;
        }}
        </style>'''

def generate_field_distribution_html(field_name: str, field_info: Dict[str, Any], stats: Dict[str, Any]) -> str:
    """Generate distribution HTML for a specific analysis field."""
    field_counts = stats['field_counts'].get(field_name, {})
    if not field_counts:
        return ""
    
    field_label = field_name.replace('_', ' ').title()
    
    # Sort by count
    sorted_items = sorted(field_counts.items(), key=lambda x: x[1], reverse=True)
    
    # For checkbox fields (few unique values), show as stat cards
    if field_info['type'] == 'checkbox':
        # Special handling for stances with co-occurrence
        if field_name == 'stances' and 'stance_cooccurrence' in stats:
            return generate_stance_cooccurrence_html(sorted_items, stats, field_info)
        
        return f'''
        <div class="section">
            <h2>📊 {field_label} Distribution</h2>
            <div class="stats-grid">
                {"".join(f'''
                <div class="stat-card">
                    <div class="stat-number">{count:,}</div>
                    <div class="stat-label">{value}</div>
                </div>
                ''' for value, count in sorted_items[:10])}
            </div>
        </div>'''
    else:
        # For text fields with many values, show top 10 as a list
        return f'''
        <div class="section">
            <h2>📝 Top {field_label}s</h2>
            <ul class="field-list">
                {"".join(f'''<li>
                    <span class="field-name">{value}</span>
                    <span class="field-count">{count:,}</span>
                </li>''' for value, count in sorted_items[:10])}
            </ul>
        </div>'''

def generate_stance_cooccurrence_html(sorted_items, stats, field_info):
    """Generate expandable stance cards with co-occurrence details, grouped by themes."""
    try:
        # Check if we have theme-formatted stances (contain ':')
        has_themes = any(':' in stance for stance, _ in sorted_items)
        
        if has_themes:
            # Group stances by theme
            themes = {}
            for stance, count in sorted_items:
                if ':' in stance:
                    theme, position = stance.split(':', 1)
                    theme = theme.strip()
                    position = position.strip()
                    if theme not in themes:
                        themes[theme] = []
                    themes[theme].append((position, count, stance))
                else:
                    # Fallback for stances without theme
                    if 'Other' not in themes:
                        themes['Other'] = []
                    themes['Other'].append((stance, count, stance))
            
            # Calculate co-occurrence percentages
            percentages, stance_to_num = calculate_cooccurrence_percentages(stats, {'stances': field_info})
            
            theme_cards = []
            theme_id = 0
            for theme_name, positions in themes.items():
                # Sort positions by count
                positions.sort(key=lambda x: x[1], reverse=True)
                
                # Create position cards for this theme
                position_cards = []
                for position, count, full_stance in positions:
                    # Get agreement scores for this stance
                    agreement_scores = stats.get('agreement_scores', {})
                    stance_agreement_scores = agreement_scores.get(full_stance, {})
                    
                    # Build connections for this position
                    connections = []
                    if full_stance in percentages:
                        for other_stance, _ in sorted_items:  # Check all items, not just first 6
                            if other_stance != full_stance:
                                for other_num, pct in percentages[full_stance].items():
                                    if stance_to_num.get(other_stance) == other_num and pct >= 5:
                                        connections.append((pct, other_stance))
                    
                    connections.sort(reverse=True)
                    
                    # Build the details content including agreement scores and connections
                    details_content = []
                    
                    # Find highest and lowest agreement scores
                    agreement_pairs = [(stance, pct) for stance, pct in stance_agreement_scores.items() 
                                     if stance != full_stance]
                    
                    agreement_items = []
                    if agreement_pairs:
                        # Sort by agreement percentage
                        agreement_pairs.sort(key=lambda x: x[1], reverse=True)
                        
                        # Get highest agreement (if > 0%)
                        for stance, pct in agreement_pairs:
                            if pct > 0:
                                agreement_items.append(f'<div class="agreement-item">📊 Highest agreement ({pct:.0f}%): {stance}</div>')
                                break
                        
                        # Get lowest agreement
                        if len(agreement_pairs) > 1:
                            lowest_stance, lowest_pct = agreement_pairs[-1]
                            # Only show if it's different from highest and meaningful
                            if not agreement_items or (agreement_items and lowest_pct < agreement_pairs[0][1]):
                                agreement_items.append(f'<div class="agreement-item">📊 Lowest agreement ({lowest_pct:.0f}%): {lowest_stance}</div>')
                    
                    if agreement_items:
                        details_content.extend(agreement_items)
                    else:
                        details_content.append('<div class="no-agreements">📊 No overlaps with other positions</div>')
                    
                    connections_html = "".join(details_content)
                    
                    position_cards.append(f'''
                        <div class="position-card">
                            <div class="position-header" onclick="togglePositionDetails('{theme_id}_{len(position_cards)}')">
                                <span class="position-count">{count:,}</span>
                                <span class="position-name">{position}</span>
                                <span class="expand-icon" id="pos-icon-{theme_id}_{len(position_cards)}">▼</span>
                            </div>
                            <div class="position-details" id="pos-details-{theme_id}_{len(position_cards)}" style="display: none;">
                                {connections_html}
                            </div>
                        </div>''')
                
                theme_cards.append(f'''
                    <div class="theme-card">
                        <div class="theme-header" onclick="toggleThemeDetails('{theme_id}')">
                            <span class="theme-name">🔹 {theme_name}</span>
                            <span class="theme-count">({len(positions)} positions)</span>
                            <span class="expand-icon" id="theme-icon-{theme_id}">▼</span>
                        </div>
                        <div class="theme-details" id="theme-details-{theme_id}" style="display: none;">
                            {"".join(position_cards)}
                        </div>
                    </div>''')
                theme_id += 1
            
            return f'''
            <div class="section">
                <h2>📊 Themes Distribution</h2>
                <p style="color: #666; margin-bottom: 20px; font-style: italic;">Click on any theme to see positions, then click positions to see relationships</p>
                <div class="theme-stances">
                    {"".join(theme_cards)}
                </div>
            </div>
            
            <style>
            .theme-stances {{
                display: grid;
                gap: 15px;
            }}
            
            .theme-card {{
                background: white;
                border-radius: 8px;
                border-left: 4px solid #2c3e50;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                overflow: hidden;
            }}
            
            .theme-header {{
                display: flex;
                align-items: center;
                gap: 15px;
                padding: 15px;
                cursor: pointer;
                transition: background-color 0.2s;
                background: #f8f9fa;
                font-weight: 600;
            }}
            
            .theme-header:hover {{
                background-color: #e9ecef;
            }}
            
            .theme-name {{
                font-size: 1.1em;
                color: #2c3e50;
                flex: 1;
            }}
            
            .theme-count {{
                color: #666;
                font-size: 0.9em;
            }}
            
            .theme-details {{
                padding: 0 15px 15px 15px;
            }}
            
            .position-card {{
                background: #f8f9fa;
                border-radius: 6px;
                margin: 8px 0;
                border-left: 3px solid #3498db;
            }}
            
            .position-header {{
                display: flex;
                align-items: flex-start;
                gap: 12px;
                padding: 12px;
                cursor: pointer;
                transition: background-color 0.2s;
            }}
            
            .position-header:hover {{
                background-color: #ffffff;
            }}
            
            .position-count {{
                font-size: 1.4em;
                font-weight: bold;
                color: #3498db;
                min-width: 40px;
            }}
            
            .position-name {{
                flex: 1;
                font-weight: 500;
                color: #2c3e50;
            }}
            
            .position-details {{
                padding: 0 12px 12px 12px;
                background: white;
                margin: 0 12px 12px 12px;
                border-radius: 4px;
            }}
            
            .agreements-header {{
                font-weight: 600;
                color: #2c3e50;
                margin: 12px 0 8px 0;
                font-size: 0.9em;
            }}
            
            .agreement-item {{
                padding: 6px 0;
                border-bottom: 1px solid #eee;
                color: #555;
                font-size: 0.9em;
            }}
            
            .agreement-item:last-child {{
                border-bottom: none;
            }}
            
            .no-agreements {{
                padding: 10px 0;
                color: #999;
                font-style: italic;
                font-size: 0.9em;
            }}
            
            .expand-icon {{
                font-size: 0.8em;
                color: #666;
                transition: transform 0.2s;
                min-width: 20px;
                text-align: center;
            }}
            
            .expand-icon.expanded {{
                transform: rotate(180deg);
            }}
            
            .connection-item {{
                padding: 6px 0;
                border-bottom: 1px solid #eee;
                color: #555;
                font-size: 0.9em;
            }}
            
            .connection-item:last-child {{
                border-bottom: none;
            }}
            
            .no-connections {{
                padding: 10px 0;
                color: #999;
                font-style: italic;
                font-size: 0.9em;
            }}
            </style>
            
            <script>
            function toggleThemeDetails(themeId) {{
                const details = document.getElementById('theme-details-' + themeId);
                const icon = document.getElementById('theme-icon-' + themeId);
                
                if (details.style.display === 'none' || details.style.display === '') {{
                    details.style.display = 'block';
                    icon.classList.add('expanded');
                }} else {{
                    details.style.display = 'none';
                    icon.classList.remove('expanded');
                }}
            }}
            
            function togglePositionDetails(positionId) {{
                const details = document.getElementById('pos-details-' + positionId);
                const icon = document.getElementById('pos-icon-' + positionId);
                
                if (details.style.display === 'none' || details.style.display === '') {{
                    details.style.display = 'block';
                    icon.classList.add('expanded');
                }} else {{
                    details.style.display = 'none';
                    icon.classList.remove('expanded');
                }}
            }}
            </script>'''
        
        else:
            # Fallback to original behavior if no themes detected
            percentages, stance_to_num = calculate_cooccurrence_percentages(stats, {'stances': field_info})
            
            stance_cards = []
            for i, (stance, count) in enumerate(sorted_items[:6]):
                # Build all connections for this stance
                connections = []
            if stance in percentages and percentages[stance]:
                for other_stance, _ in sorted_items[:6]:
                    if other_stance != stance:
                        for other_num, pct in percentages[stance].items():
                            if stance_to_num.get(other_stance) == other_num and pct >= 5:  # Show if 5% or higher
                                connections.append((pct, other_stance))
                
                # Sort by percentage, highest first
                connections.sort(reverse=True, key=lambda x: x[0])
            
            # Create connections HTML
            connections_html = ""
            if connections:
                connection_items = []
                for pct, other_stance in connections:
                    connection_items.append(f'<div class="connection-item">{pct:.0f}% also: {other_stance}</div>')
                connections_html = "".join(connection_items)
            else:
                connections_html = '<div class="no-connections">No significant overlaps with other stances</div>'
            
            stance_cards.append(f'''
                <div class="expandable-stance-card">
                    <div class="stance-header" onclick="toggleStanceDetails({i})">
                        <span class="stance-count">{count:,}</span>
                        <span class="stance-name">{stance}</span>
                        <span class="expand-icon" id="icon-{i}">▼</span>
                    </div>
                    <div class="stance-details" id="details-{i}" style="display: none;">
                        {connections_html}
                    </div>
                </div>''')
        
        return f'''
        <div class="section">
            <h2>📊 Stances Distribution</h2>
            <p style="color: #666; margin-bottom: 20px; font-style: italic;">Click on any stance to see how it relates to others</p>
            <div class="expandable-stances">
                {"".join(stance_cards)}
            </div>
        </div>
        
        <style>
        .expandable-stances {{
            display: grid;
            gap: 12px;
        }}
        
        .expandable-stance-card {{
            background: white;
            border-radius: 8px;
            border-left: 4px solid #3498db;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        
        .stance-header {{
            display: flex;
            align-items: flex-start;
            gap: 15px;
            padding: 15px;
            cursor: pointer;
            transition: background-color 0.2s;
        }}
        
        .stance-header:hover {{
            background-color: #f8f9fa;
        }}
        
        .stance-count {{
            font-size: 1.6em;
            font-weight: bold;
            color: #2c3e50;
            min-width: 80px;
            flex-shrink: 0;
        }}
        
        .stance-name {{
            font-size: 1em;
            color: #2c3e50;
            line-height: 1.3;
            font-weight: 500;
            flex-grow: 1;
        }}
        
        .expand-icon {{
            font-size: 1.2em;
            color: #7f8c8d;
            transition: transform 0.3s;
            flex-shrink: 0;
        }}
        
        .expand-icon.expanded {{
            transform: rotate(180deg);
        }}
        
        .stance-details {{
            padding: 0 15px 15px 15px;
            border-top: 1px solid #ecf0f1;
            background: #f8f9fa;
        }}
        
        .connection-item {{
            padding: 8px 10px;
            margin: 8px 0;
            background: white;
            border-radius: 4px;
            border-left: 3px solid #e67e22;
            font-size: 0.9em;
            color: #2c3e50;
        }}
        
        .no-connections {{
            padding: 15px;
            text-align: center;
            color: #888;
            font-style: italic;
        }}
        </style>
        
        <script>
        function toggleStanceDetails(index) {{
            const details = document.getElementById('details-' + index);
            const icon = document.getElementById('icon-' + index);
            
            if (details.style.display === 'none' || details.style.display === '') {{
                details.style.display = 'block';
                icon.classList.add('expanded');
            }} else {{
                details.style.display = 'none';
                icon.classList.remove('expanded');
            }}
        }}
        </script>'''
        
    except Exception as e:
        # Fallback to simple display if co-occurrence fails
        return f'''
        <div class="section">
            <h2>📊 Themes Distribution</h2>
            <div class="stats-grid">
                {"".join(f'''
                <div class="stat-card">
                    <div class="stat-number">{count:,}</div>
                    <div class="stat-label">{value}</div>
                </div>
                ''' for value, count in sorted_items[:10])}
            </div>
        </div>'''

def generate_filter_html(field_name: str, field_info: Dict[str, Any], column_index: int) -> str:
    """Generate filter HTML for a specific field based on its type."""
    field_label = field_name.replace('_', ' ').title()
    
    if field_info['type'] == 'checkbox':
        # Generate checkbox filter
        checkboxes = []
        for i, value in enumerate(field_info['unique_values']):
            safe_id = f"{field_name}-{i}"
            checkboxes.append(f'''<div class="checkbox-item">
                <input type="checkbox" id="{safe_id}" data-filter="{field_name}" value="{value}" onchange="filterTable()">
                <label for="{safe_id}">{value}</label>
            </div>''')
        
        return f'''<div class="filter-group">
            <div class="filter-label">{field_label}</div>
            <div class="checkbox-group">
                {"".join(checkboxes)}
            </div>
        </div>'''
    else:
        # Generate text input filter
        return f'''<div class="filter-group">
            <div class="filter-label">{field_label}</div>
            <input type="text" class="filter-input" data-column="{column_index}" placeholder="Filter by {field_label.lower()}..." onkeyup="filterTable()">
        </div>'''

def load_regulation_metadata() -> Dict[str, str]:
    """Load regulation metadata if available."""
    try:
        if os.path.exists('regulation_metadata.json'):
            with open('regulation_metadata.json', 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception:
        pass
    
    # Fallback metadata
    return {
        "regulation_name": "Regulation Comments Analysis",
        "docket_id": "",
        "agency": "",
        "brief_description": "Analysis of public comments on federal regulation"
    }

def escape_html(text: str) -> str:
    """Escape HTML characters for use in attributes."""
    return str(text).replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;').replace('"', '&quot;').replace("'", '&#039;')

def create_tooltip_cell(content: str, max_length: int, css_class: str = "", tooltip_max_length: int = 1000) -> str:
    """Create a table cell with Bootstrap tooltip for truncated content."""
    if len(content) <= max_length:
        return f'<td class="{css_class}">{content}</td>' if css_class else f'<td>{content}</td>'
    
    truncated = content[:max_length] + '...'
    
    # Truncate tooltip content to reasonable length
    tooltip_content = content
    if len(tooltip_content) > tooltip_max_length:
        tooltip_content = tooltip_content[:tooltip_max_length] + '... [truncated]'
    
    escaped_content = escape_html(tooltip_content)
    css_classes = f"{css_class} char-limited" if css_class else "char-limited"
    
    return f'<td class="{css_classes}" data-bs-toggle="tooltip" data-bs-placement="top" data-bs-html="false" title="{escaped_content}">{truncated}</td>'

def generate_html(comments: List[Dict[str, Any]], stats: Dict[str, Any], field_analysis: Dict[str, Dict[str, Any]], output_file: str):
    """Generate HTML report."""
    
    # Get metadata
    model_used = "gpt-4o-mini"  # Default assumption
    generated_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    regulation_metadata = load_regulation_metadata()
    
    html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{regulation_metadata['regulation_name']} - Comment Analysis Report</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;
            line-height: 1.5;
            background: #f8f9fa;
            color: #333;
        }}
        
        .container {{
            max-width: 1600px;
            margin: 0 auto;
            padding: 20px;
        }}
        
        .header {{
            text-align: center;
            margin-bottom: 40px;
            padding: 20px;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        
        h1 {{
            font-size: 2em;
            color: #333;
            margin-bottom: 10px;
        }}
        
        .subtitle {{
            color: #666;
            font-size: 1.1em;
        }}
        
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin: 30px 0;
        }}
        
        .stat-card {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            text-align: center;
        }}
        
        .stat-number {{
            font-size: 2em;
            font-weight: bold;
            color: #333;
            margin-bottom: 5px;
        }}
        
        .stat-label {{
            color: #666;
            font-size: 0.9em;
        }}
        
        .section {{
            background: white;
            margin: 30px 0;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        
        h2 {{
            font-size: 1.5em;
            color: #333;
            margin-bottom: 20px;
            border-bottom: 2px solid #e9ecef;
            padding-bottom: 10px;
        }}
        
        .filters-container {{
            background: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            border: 1px solid #dee2e6;
        }}
        
        .filters-header {{
            font-weight: 600;
            margin-bottom: 15px;
            font-size: 1.1em;
        }}
        
        .filters-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
        }}
        
        .filter-group {{
            display: flex;
            flex-direction: column;
            gap: 8px;
        }}
        
        .filter-label {{
            font-weight: 500;
            color: #333;
            font-size: 0.9em;
        }}
        
        .filterable {{
            position: relative;
            cursor: pointer;
        }}
        
        .filter-arrow {{
            float: right;
            font-size: 12px;
            color: #666;
            margin-left: 8px;
            user-select: none;
        }}
        
        .filter-arrow:hover {{
            color: #007bff;
        }}
        
        .filter-dropdown {{
            position: absolute;
            top: 100%;
            left: 0;
            right: 0;
            background: white;
            border: 1px solid #ddd;
            border-radius: 4px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.15);
            z-index: 1000;
            padding: 10px;
            min-width: 200px;
        }}
        
        .filter-input {{
            width: 100%;
            padding: 6px 8px;
            border: 1px solid #ced4da;
            border-radius: 4px;
            font-size: 12px;
            margin-bottom: 5px;
        }}
        
        .filter-input:focus {{
            outline: none;
            border-color: #007bff;
            box-shadow: 0 0 0 1px rgba(0,123,255,0.25);
        }}
        
        .filter-checkbox {{
            display: block;
            margin: 5px 0;
            font-size: 13px;
            cursor: pointer;
        }}
        
        .filter-checkbox input {{
            margin-right: 6px;
        }}
        
        .checkbox-group {{
            display: flex;
            flex-wrap: wrap;
            gap: 10px;
        }}
        
        .checkbox-item {{
            display: flex;
            align-items: center;
            gap: 5px;
            font-size: 0.9em;
        }}
        
        .checkbox-item input[type="checkbox"] {{
            margin: 0;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 14px;
        }}
        
        th {{
            background: #e9ecef;
            padding: 12px 8px;
            text-align: left;
            font-weight: 600;
            border-bottom: 2px solid #dee2e6;
            position: sticky;
            top: 0;
        }}
        
        td {{
            padding: 10px 8px;
            border-bottom: 1px solid #dee2e6;
            vertical-align: top;
        }}
        
        tr:nth-child(even) {{
            background-color: #f8f9fa;
        }}
        
        tr:hover {{
            background-color: #e3f2fd;
        }}
        
        .stance-for {{
            background: #28a745;
            color: white;
            padding: 3px 8px;
            border-radius: 12px;
            font-size: 11px;
            font-weight: 500;
        }}
        
        .stance-against {{
            background: #dc3545;
            color: white;
            padding: 3px 8px;
            border-radius: 12px;
            font-size: 11px;
            font-weight: 500;
        }}
        
        .stance-neutral {{
            background: #ffc107;
            color: #212529;
            padding: 3px 8px;
            border-radius: 12px;
            font-size: 11px;
            font-weight: 500;
        }}
        
        .stance-unknown {{
            background: #6c757d;
            color: white;
            padding: 3px 8px;
            border-radius: 12px;
            font-size: 11px;
            font-weight: 500;
        }}
        
        .stances-container {{
            display: flex;
            flex-wrap: wrap;
            gap: 4px;
            max-width: 300px;
        }}
        
        .stance-tag {{
            background: #e3f2fd;
            color: #1976d2;
            padding: 2px 6px;
            border-radius: 8px;
            font-size: 10px;
            border: 1px solid #bbdefb;
        }}
        
        .text-preview {{
            max-width: 300px;
            max-height: 100px;
            overflow: hidden;
            font-size: 12px;
        }}
        
        .comment-id {{
            font-family: monospace;
            background: #f8f9fa;
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 11px;
        }}
        
        .comment-id a:hover {{
            color: #0056b3 !important;
            background-color: #e3f2fd;
        }}
        
        .date-cell {{
            white-space: nowrap;
            font-size: 12px;
        }}
        
        .attachment-indicator {{
            font-size: 16px;
            color: #28a745;
        }}
        
        .unique-indicator {{
            background: #28a745;
            color: white;
            padding: 2px 6px;
            border-radius: 8px;
            font-size: 11px;
            font-weight: 500;
        }}
        
        .duplicate-indicator {{
            background: #ffc107;
            color: #212529;
            padding: 2px 6px;
            border-radius: 8px;
            font-size: 11px;
            font-weight: 500;
        }}
        
        .meta-info {{
            background: #343a40;
            color: white;
            padding: 20px;
            border-radius: 8px;
            margin-top: 30px;
        }}
        
        .meta-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
        }}
        
        .meta-item {{
            display: flex;
            flex-direction: column;
            gap: 5px;
        }}
        
        .meta-label {{
            font-size: 0.8em;
            opacity: 0.8;
        }}
        
        .meta-value {{
            font-weight: 600;
        }}
        
        
        .clear-filters {{
            background: #dc3545;
            color: white;
            border: none;
            padding: 8px 16px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 14px;
            margin-right: 10px;
        }}
        
        .clear-filters:hover {{
            background: #c82333;
        }}
        
        .show-hide-columns {{
            background: #007bff;
            color: white;
            border: none;
            padding: 8px 16px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 14px;
            margin-right: 10px;
        }}
        
        .show-hide-columns:hover {{
            background: #0056b3;
        }}
        
        .column-visibility-dropdown {{
            position: relative;
            display: inline-block;
        }}
        
        .column-visibility-content {{
            display: none;
            position: absolute;
            background-color: white;
            min-width: 250px;
            box-shadow: 0px 8px 16px 0px rgba(0,0,0,0.2);
            z-index: 1000;
            border-radius: 4px;
            padding: 10px;
            border: 1px solid #ddd;
            top: 100%;
            left: 0;
        }}
        
        .column-visibility-content.show {{
            display: block;
        }}
        
        .column-visibility-item {{
            display: flex;
            align-items: center;
            padding: 5px 0;
            gap: 8px;
        }}
        
        .column-visibility-item input[type="checkbox"] {{
            margin: 0;
        }}
        
        .column-visibility-item label {{
            cursor: pointer;
            font-size: 14px;
            margin: 0;
        }}
        
        .char-limited {{
            cursor: help;
        }}
        
        /* Custom tooltip styling for wider tooltips */
        .tooltip .tooltip-inner {{
            max-width: 600px !important;
            width: auto !important;
            text-align: left !important;
            font-size: 13px !important;
            line-height: 1.4 !important;
            padding: 8px 12px !important;
        }}
        
        /* Allow table to be wider and let columns size naturally */
        table {{
            width: 100%;
            table-layout: auto;
            word-wrap: break-word;
            overflow-wrap: break-word;
        }}
        
        /* Only constrain the comment column to prevent it from being too wide */
        th[data-column="5"] {{
            width: 25%;
            max-width: 400px;
        }}
        
        .text-preview {{
            max-width: 400px;
            word-wrap: break-word;
            overflow-wrap: break-word;
        }}
        
        /* Co-occurrence table styles */
        .cooccurrence-table {{
            border-collapse: collapse;
            font-size: 12px;
            margin: 0 auto;
        }}
        
        .cooccurrence-table th,
        .cooccurrence-table td {{
            border: 1px solid #dee2e6;
            text-align: center;
            padding: 8px;
            min-width: 40px;
        }}
        
        .cooccurrence-table .stance-label {{
            text-align: left;
            font-weight: 500;
            max-width: 200px;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }}
        
        .rotate-header {{
            height: 150px;
            white-space: nowrap;
            vertical-align: bottom;
        }}
        
        .rotate-header > div {{
            transform: translate(15px, 65px) rotate(-45deg);
            width: 30px;
            transform-origin: bottom left;
            font-size: 11px;
            font-weight: 500;
        }}
        
        .diagonal-cell {{
            background-color: #f8f9fa;
            font-weight: bold;
            color: #333;
        }}
        
        .cooccurrence-cell {{
            color: #333;
            font-weight: 500;
        }}
        
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>{regulation_metadata['regulation_name']}</h1>
            <div class="subtitle">{regulation_metadata['brief_description']}</div>
            {f'<div style="margin-top: 10px; color: #666; font-size: 0.9em;">{regulation_metadata["agency"]} Docket: <a href="https://www.regulations.gov/docket/{regulation_metadata["docket_id"]}" target="_blank" style="color: #007bff; text-decoration: none;">{regulation_metadata["docket_id"]}</a></div>' if regulation_metadata['docket_id'] else ''}
        </div>
        
        <!-- Summary Statistics -->
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-number">{stats['total_comments']:,}</div>
                <div class="stat-label">Total Comments</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{stats['with_attachments']:,}</div>
                <div class="stat-label">With Attachments</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{stats['date_range']}</div>
                <div class="stat-label">Date Range</div>
            </div>
        </div>


        <!-- Analysis Field Distributions -->
        {"".join(generate_field_distribution_html(field_name, field_info, stats) for field_name, field_info in field_analysis.items() if field_name != 'new_stances')}

        <!-- New Stances (Compact) -->
        {generate_new_stances_compact_html(field_analysis.get('new_stances', {}), stats)}

        <!-- Comments Table -->
        <div class="section">
            <h2>Comments</h2>
            <div class="table-controls">
                <button class="clear-filters" onclick="clearAllFilters()">Clear All Filters</button>
                <div class="column-visibility-dropdown">
                    <button class="show-hide-columns" onclick="toggleColumnVisibility()">Show/Hide Columns</button>
                    <div class="column-visibility-content" id="columnVisibilityDropdown">
                        <div class="column-visibility-item">
                            <input type="checkbox" id="col-0" checked onchange="toggleColumn(0)">
                            <label for="col-0">Comment ID</label>
                        </div>
                        <div class="column-visibility-item">
                            <input type="checkbox" id="col-1" checked onchange="toggleColumn(1)">
                            <label for="col-1">Date</label>
                        </div>
                        <div class="column-visibility-item">
                            <input type="checkbox" id="col-2" checked onchange="toggleColumn(2)">
                            <label for="col-2">Submitter</label>
                        </div>
                        <div class="column-visibility-item">
                            <input type="checkbox" id="col-3" checked onchange="toggleColumn(3)">
                            <label for="col-3">Organization</label>
                        </div>
                        <div class="column-visibility-item">
                            <input type="checkbox" id="col-4" checked onchange="toggleColumn(4)">
                            <label for="col-4">Stances</label>
                        </div>"""
    
    # Always add new_stances visibility control
    html_template += """
                        <div class="column-visibility-item">
                            <input type="checkbox" id="col-5" checked onchange="toggleColumn(5)">
                            <label for="col-5">New Stances</label>
                        </div>
                        <div class="column-visibility-item">
                            <input type="checkbox" id="col-6" checked onchange="toggleColumn(6)">
                            <label for="col-6">Comment</label>
                        </div>
                        <div class="column-visibility-item">
                            <input type="checkbox" id="col-7" checked onchange="toggleColumn(7)">
                            <label for="col-7">Attachments</label>
                        </div>
                        <div class="column-visibility-item">
                            <input type="checkbox" id="col-8" checked onchange="toggleColumn(8)">
                            <label for="col-8">Dup Count</label>
                        </div>
                        <div class="column-visibility-item">
                            <input type="checkbox" id="col-9" checked onchange="toggleColumn(9)">
                            <label for="col-9">Dup Ratio</label>
                        </div>
                        <div class="column-visibility-item">
                            <input type="checkbox" id="col-10" onchange="toggleColumn(10)">
                            <label for="col-10">Attachment</label>
                        </div>
                        <div class="column-visibility-item">
                            <input type="checkbox" id="col-11" onchange="toggleColumn(11)">
                            <label for="col-11">Key Quote (LLM)</label>
                        </div>
                        <div class="column-visibility-item">
                            <input type="checkbox" id="col-12" onchange="toggleColumn(12)">
                            <label for="col-12">Rationale (LLM)</label>
                        </div>
                    </div>
                </div>
            </div>
                
            <div style="overflow-x: auto;">
                <table id="commentsTable" class="table table-striped">
                    <thead>
                        <tr>
                            <th class="filterable" data-column="0">
                                Comment ID <span class="filter-arrow" onclick="toggleFilter(0)">▼</span>
                                <div class="filter-dropdown" id="filter-0" style="display: none;">
                                    <input type="text" class="filter-input" data-column="0" placeholder="Filter ID..." onkeyup="filterTable()">
                                </div>
                            </th>
                            <th data-column="1">Date</th>
                            <th class="filterable" data-column="2">
                                Submitter <span class="filter-arrow" onclick="toggleFilter(2)">▼</span>
                                <div class="filter-dropdown" id="filter-2" style="display: none;">
                                    <input type="text" class="filter-input" data-column="2" placeholder="Filter name..." onkeyup="filterTable()">
                                </div>
                            </th>
                            <th class="filterable" data-column="3">
                                Organization <span class="filter-arrow" onclick="toggleFilter(3)">▼</span>
                                <div class="filter-dropdown" id="filter-3" style="display: none;">
                                    <input type="text" class="filter-input" data-column="3" placeholder="Filter organization..." onkeyup="filterTable()">
                                </div>
                            </th>
                            <th class="filterable" data-column="4">
                                Stances <span class="filter-arrow" onclick="toggleFilter(4)">▼</span>
                                <div class="filter-dropdown" id="filter-4" style="display: none;">"""
    
    # Build stance checkboxes
    stance_checkboxes = ''.join(f'<label class="filter-checkbox"><input type="checkbox" data-filter="stances" value="{stance}" onchange="filterTable()"> {stance}</label>' 
                                for stance in field_analysis.get('stances', {}).get('unique_values', []))
    
    html_template += stance_checkboxes
    html_template += """
                                    <hr style="margin: 5px 0;">
                                    <label class="filter-checkbox"><input type="checkbox" data-filter="unusual_combo" value="yes" onchange="filterTable()"> ⚠️ Unusual Combinations</label>
                                </div>
                            </th>"""
    
    # Always add new_stances column
    html_template += """
                            <th class="filterable" data-column="5">
                                New Stances <span class="filter-arrow" onclick="toggleFilter(5)">▼</span>
                                <div class="filter-dropdown" id="filter-5" style="display: none;">"""
    
    # Build new stance checkboxes
    new_stance_checkboxes = ''.join(f'<label class="filter-checkbox"><input type="checkbox" data-filter="new_stances" value="{stance}" onchange="filterTable()"> {stance}</label>' 
                                    for stance in field_analysis.get('new_stances', {}).get('unique_values', []))
    
    html_template += new_stance_checkboxes
    html_template += """
                                </div>
                            </th>
                            <th class="filterable" data-column="6">
                                Comment <span class="filter-arrow" onclick="toggleFilter(6)">▼</span>
                                <div class="filter-dropdown" id="filter-6" style="display: none;">
                                    <input type="text" class="filter-input" data-column="6" placeholder="Search comment text..." onkeyup="filterTable()">
                                </div>
                            </th>
                            <th class="filterable" data-column="7">
                                📎 <span class="filter-arrow" onclick="toggleFilter(7)">▼</span>
                                <div class="filter-dropdown" id="filter-7" style="display: none;">
                                    <label class="filter-checkbox"><input type="checkbox" data-filter="attachments" value="yes" onchange="filterTable()"> With attachments</label>
                                    <label class="filter-checkbox"><input type="checkbox" data-filter="attachments" value="no" onchange="filterTable()"> No attachments</label>
                                </div>
                            </th>
                            <th class="filterable" data-column="8">
                                Dup Count <span class="filter-arrow" onclick="toggleFilter(8)">▼</span>
                                <div class="filter-dropdown" id="filter-8" style="display: none;">"""
    
    # Build duplication count checkboxes
    dup_count_checkboxes = ''.join(f'<label class="filter-checkbox"><input type="checkbox" data-filter="duplication_count" value="{count}" onchange="filterTable()"> {count}</label>' 
                                   for count in sorted(field_analysis.get('duplication_count', {}).get('unique_values', []), reverse=True))
    
    html_template += dup_count_checkboxes
    html_template += """
                                </div>
                            </th>
                            <th class="filterable" data-column="9">
                                Dup Ratio <span class="filter-arrow" onclick="toggleFilter(9)">▼</span>
                                <div class="filter-dropdown" id="filter-9" style="display: none;">"""
    
    # Build duplication ratio checkboxes
    dup_ratio_checkboxes = ''.join(f'<label class="filter-checkbox"><input type="checkbox" data-filter="duplication_ratio" value="{ratio}" onchange="filterTable()"> {ratio}</label>' 
                                   for ratio in sorted(field_analysis.get('duplication_ratio', {}).get('unique_values', []), reverse=True))
    
    html_template += dup_ratio_checkboxes
    html_template += """
                                </div>
                            </th>
                            <th class="filterable" data-column="10" style="display: none;">
                                Attachment <span class="filter-arrow" onclick="toggleFilter(10)">▼</span>
                                <div class="filter-dropdown" id="filter-10" style="display: none;">
                                    <input type="text" class="filter-input" data-column="10" placeholder="Search attachment text..." onkeyup="filterTable()">
                                </div>
                            </th>
                            <th class="filterable" data-column="11" style="display: none;">
                                Key Quote <span class="filter-arrow" onclick="toggleFilter(11)">▼</span>
                                <div class="filter-dropdown" id="filter-11" style="display: none;">
                                    <input type="text" class="filter-input" data-column="11" placeholder="Search quotes..." onkeyup="filterTable()">
                                </div>
                            </th>
                            <th class="filterable" data-column="12" style="display: none;">
                                Rationale <span class="filter-arrow" onclick="toggleFilter(12)">▼</span>
                                <div class="filter-dropdown" id="filter-12" style="display: none;">
                                    <input type="text" class="filter-input" data-column="12" placeholder="Search rationale..." onkeyup="filterTable()">
                                </div>
                            </th>
                        </tr>
                    </thead>
                        <tbody>
"""

    # Add table rows
    for comment in comments:
        analysis = comment.get('analysis', {}) or {}
        
        # Handle stance as either string or list
        stance_data = analysis.get('stance', analysis.get('stances', []))
        if isinstance(stance_data, str):
            # Legacy single stance
            all_stances = [stance_data] if stance_data else []
        elif isinstance(stance_data, list):
            # New multi-stance format
            all_stances = stance_data
        else:
            all_stances = []
        
        # Use all stances from the stances field (original discovered stances)
        stances = all_stances
        
        key_quote = analysis.get('key_quote', '')
        rationale = analysis.get('rationale', '')
        
        # Format date
        date_str = comment.get('date', '')
        formatted_date = ''
        if date_str:
            try:
                date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                formatted_date = date.strftime('%Y-%m-%d')
            except:
                formatted_date = date_str[:10] if len(date_str) >= 10 else date_str
        
        # Check if this comment has unusual stance combinations
        # Use pre-calculated flag if available, otherwise calculate it
        has_unusual_combo = analysis.get('has_unusual_combination', False)
        if 'has_unusual_combination' not in analysis:
            # Fallback: calculate it if not pre-processed
            has_unusual_combo = identify_unusual_combinations(stances, stats.get('stance_cooccurrence', {}), stats['total_comments'])
        
        # Stances display (add warning icon if unusual combination)
        unusual_indicator = ' <span style="color: #ff9800; font-weight: bold;">⚠️</span>' if has_unusual_combo else ''
        stances_html = '<div class="stances-container">' + ' '.join(f'<span class="stance-tag">{stance}</span>' for stance in stances) + unusual_indicator + '</div>' if stances else '<span style="color: #999;">None</span>'
        
        # New stances display
        new_stances = analysis.get('new_stances', [])
        new_stances_html = '<div class="stances-container">' + ' '.join(f'<span class="stance-tag" style="background: #e8f5e8; color: #2e7d32; border: 1px solid #c8e6c9;">{stance}</span>' for stance in new_stances) + '</div>' if new_stances else '<span style="color: #999;">None</span>'
        
        # Full comment text with tooltip
        full_text = comment.get('comment_text', '')
        comment_cell = create_tooltip_cell(full_text, 300, "text-preview", tooltip_max_length=1500)
        
        # Attachment text with tooltip
        attachment_text = comment.get('attachment_text', '')
        attachment_cell = create_tooltip_cell(attachment_text, 300, tooltip_max_length=1200).replace('<td', '<td style="display: none;"')
        
        # Attachments with failure indicator
        attachment_status = comment.get('attachment_status')
        if attachment_status and attachment_status.get('total', 0) > 0:
            failed = attachment_status.get('failed', 0)
            total = attachment_status.get('total', 0)
            if failed > 0:
                has_attachments = f'<span class="attachment-indicator">📎</span> <span style="color: #ff6b6b; font-weight: bold;">({failed}/{total} failed)</span>'
            else:
                has_attachments = '<span class="attachment-indicator">📎</span>'
        else:
            has_attachments = ''
        
        # Duplication count and ratio
        duplication_count = comment.get('duplication_count', 1)
        duplication_ratio = comment.get('duplication_ratio', 1)
        
        if duplication_count == 1:
            count_display = '<span class="unique-indicator">1</span>'
            ratio_display = f'<span class="unique-indicator">{duplication_ratio}</span>'
        else:
            count_display = f'<span class="duplicate-indicator">{duplication_count}</span>'
            ratio_display = f'<span class="duplicate-indicator">{duplication_ratio}</span>'
        
        # Create tooltip cells for truncated content
        key_quote_cell = create_tooltip_cell(key_quote, 300, tooltip_max_length=500).replace('<td', '<td style="display: none;"')
        rationale_cell = create_tooltip_cell(rationale, 500, tooltip_max_length=500).replace('<td', '<td style="display: none;"')
        submitter_cell = create_tooltip_cell(comment.get('submitter', ''), 50, tooltip_max_length=200)
        organization_cell = create_tooltip_cell(comment.get('organization', ''), 50, tooltip_max_length=200)

        html_template += f"""
                            <tr>
                                <td><span class="comment-id"><a href="https://www.regulations.gov/comment/{comment.get('id', '')}" target="_blank" style="color: #007bff; text-decoration: underline;">{comment.get('id', '')}</a></span></td>
                                <td class="date-cell">{formatted_date}</td>
                                {submitter_cell}
                                {organization_cell}
                                <td>{stances_html}</td>
                                <td>{new_stances_html}</td>
                                {comment_cell}
                                <td>{has_attachments}</td>
                                <td>{count_display}</td>
                                <td>{ratio_display}</td>
                                {attachment_cell}
                                {key_quote_cell}
                                {rationale_cell}
                            </tr>
"""

    html_template += f"""
                        </tbody>
                    </table>
                </div>
            </div>
        </div>

        <!-- Meta Information -->
        <div class="meta-info">
            <h3 style="color: white; margin-bottom: 20px;">📋 Report Details</h3>
            <div class="meta-grid">
                <div class="meta-item">
                    <div class="meta-label">Generated</div>
                    <div class="meta-value">{generated_time}</div>
                </div>
                <div class="meta-item">
                    <div class="meta-label">Model Used</div>
                    <div class="meta-value">{model_used}</div>
                </div>
                <div class="meta-item">
                    <div class="meta-label">Total Comments</div>
                    <div class="meta-value">{stats['total_comments']:,}</div>
                </div>
                <div class="meta-item">
                    <div class="meta-label">With Attachments</div>
                    <div class="meta-value">{stats['with_attachments']:,}</div>
                </div>
                <div class="meta-item">
                    <div class="meta-label">Date Range</div>
                    <div class="meta-value">{stats['date_range']}</div>
                </div>
            </div>"""
    
    # Add attachment processing details if there are any attachments
    attachment_stats = stats.get('attachment_stats', {})
    if attachment_stats.get('total_attachments', 0) > 0:
        html_template += f"""
            <div style="margin-top: 30px;">
                <h3 style="color: white; margin-bottom: 20px;">📎 Attachment Processing Details</h3>
                <div class="meta-grid">
                    <div class="meta-item">
                        <div class="meta-label">Total Attachments</div>
                        <div class="meta-value">{attachment_stats['total_attachments']:,}</div>
                    </div>
                    <div class="meta-item">
                        <div class="meta-label">Processed Successfully</div>
                        <div class="meta-value">{attachment_stats['processed_successfully']:,}</div>
                    </div>
                    <div class="meta-item">
                        <div class="meta-label">Failed to Process</div>
                        <div class="meta-value" style="color: #ff6b6b;">{attachment_stats['failed_attachments']:,}</div>
                    </div>
                    <div class="meta-item">
                        <div class="meta-label">Comments with Failures</div>
                        <div class="meta-value" style="color: #ff6b6b;">{attachment_stats['comments_with_failures']:,}</div>
                    </div>
                    <div class="meta-item">
                        <div class="meta-label">Download Failures</div>
                        <div class="meta-value">{attachment_stats['failure_reasons']['download_failed']:,}</div>
                    </div>
                    <div class="meta-item">
                        <div class="meta-label">Extraction Failures</div>
                        <div class="meta-value">{attachment_stats['failure_reasons']['no_text_extracted']:,}</div>
                    </div>
                </div>
            </div>"""
    
    html_template += """
        </div>
    </div>

    <script>
        // Column visibility state (13 columns with new_stances)
        const columnVisibility = {
            0: true, 1: true, 2: true, 3: true, 4: true, 5: true, 6: true, 7: true, 8: true, 9: true, 10: false, 11: false, 12: false
        };

        // Initialize column visibility on page load
        document.addEventListener('DOMContentLoaded', function() {
            for (const [col, visible] of Object.entries(columnVisibility)) {
                updateColumnVisibility(parseInt(col), visible);
                // Also update the checkbox state
                const checkbox = document.getElementById('col-' + col);
                if (checkbox) {
                    checkbox.checked = visible;
                }
            }
        });

        function toggleColumnVisibility() {
            const dropdown = document.getElementById('columnVisibilityDropdown');
            dropdown.classList.toggle('show');
        }

        function toggleColumn(columnIndex) {
            const checkbox = document.getElementById('col-' + columnIndex);
            const visible = checkbox.checked;
            columnVisibility[columnIndex] = visible;
            updateColumnVisibility(columnIndex, visible);
        }

        function updateColumnVisibility(columnIndex, visible) {
            const table = document.getElementById('commentsTable');
            const rows = table.getElementsByTagName('tr');
            
            // Update header
            const headers = rows[0].getElementsByTagName('th');
            if (headers[columnIndex]) {
                headers[columnIndex].style.display = visible ? '' : 'none';
            }
            
            // Update all data rows
            for (let i = 1; i < rows.length; i++) {
                const cells = rows[i].getElementsByTagName('td');
                if (cells[columnIndex]) {
                    cells[columnIndex].style.display = visible ? '' : 'none';
                }
            }
        }

        function filterTable() {
            const table = document.getElementById('commentsTable');
            const rows = table.getElementsByTagName('tr');
            
            // Get text input filters
            const textFilters = document.querySelectorAll('.filter-input');
            const textFilterValues = Array.from(textFilters).map(filter => {
                return {
                    column: parseInt(filter.getAttribute('data-column')),
                    value: filter.value.toLowerCase()
                };
            });
            
            // Get checkbox filters
            const stanceCheckboxes = document.querySelectorAll('input[data-filter="stances"]:checked');
            const newStanceCheckboxes = document.querySelectorAll('input[data-filter="new_stances"]:checked');
            const unusualComboCheckboxes = document.querySelectorAll('input[data-filter="unusual_combo"]:checked');
            const attachmentCheckboxes = document.querySelectorAll('input[data-filter="attachments"]:checked');
            const duplicationCountCheckboxes = document.querySelectorAll('input[data-filter="duplication_count"]:checked');
            const duplicationRatioCheckboxes = document.querySelectorAll('input[data-filter="duplication_ratio"]:checked');
            
            const selectedStances = Array.from(stanceCheckboxes).map(cb => cb.value.toLowerCase());
            const selectedNewStances = Array.from(newStanceCheckboxes).map(cb => cb.value.toLowerCase());
            const filterUnusualCombos = unusualComboCheckboxes.length > 0;
            const selectedAttachments = Array.from(attachmentCheckboxes).map(cb => cb.value);
            const selectedDuplicationCounts = Array.from(duplicationCountCheckboxes).map(cb => parseInt(cb.value));
            const selectedDuplicationRatios = Array.from(duplicationRatioCheckboxes).map(cb => parseInt(cb.value));

            // Always assume new_stances column exists (column 5)
            const hasNewStances = true;
            
            // Filter each row
            for (let i = 1; i < rows.length; i++) {
                const row = rows[i];
                const cells = row.getElementsByTagName('td');
                let showRow = true;

                // Check text filters
                for (const filter of textFilterValues) {
                    if (filter.value && filter.column < cells.length) {
                        const cellText = cells[filter.column].textContent.toLowerCase();
                        if (!cellText.includes(filter.value)) {
                            showRow = false;
                            break;
                        }
                    }
                }
                
                // Check stance filter (column 4)
                if (showRow && selectedStances.length > 0) {
                    const stanceText = cells[4].textContent.toLowerCase();
                    if (!selectedStances.some(stance => stanceText.includes(stance))) {
                        showRow = false;
                    }
                }
                
                // Check unusual combination filter
                if (showRow && filterUnusualCombos) {
                    const stanceCell = cells[4].innerHTML;
                    if (!stanceCell.includes('⚠️')) {
                        showRow = false;
                    }
                }
                
                // Check new stance filter (column 5 if exists)
                if (showRow && hasNewStances && selectedNewStances.length > 0) {
                    const newStanceText = cells[5].textContent.toLowerCase();
                    if (!selectedNewStances.some(stance => newStanceText.includes(stance))) {
                        showRow = false;
                    }
                }
                
                // Column positions shift by 1 when new_stances exists
                const attachmentCol = hasNewStances ? 7 : 6;
                const dupCountCol = hasNewStances ? 8 : 7;
                const dupRatioCol = hasNewStances ? 9 : 8;
                
                // Check attachments filter
                if (showRow && selectedAttachments.length > 0) {
                    const attachmentCell = cells[attachmentCol];
                    const hasAttachment = attachmentCell.textContent.trim() !== '';
                    
                    let attachmentMatch = false;
                    for (const filter of selectedAttachments) {
                        if (filter === 'yes' && hasAttachment) {
                            attachmentMatch = true;
                            break;
                        }
                        if (filter === 'no' && !hasAttachment) {
                            attachmentMatch = true;
                            break;
                        }
                    }
                    
                    if (!attachmentMatch) {
                        showRow = false;
                    }
                }
                
                // Check duplication count filter
                if (showRow && selectedDuplicationCounts.length > 0) {
                    const duplicationCell = cells[dupCountCol];
                    const duplicationText = duplicationCell.textContent.trim();
                    const duplicationCount = parseInt(duplicationText);
                    
                    if (!selectedDuplicationCounts.includes(duplicationCount)) {
                        showRow = false;
                    }
                }
                
                // Check duplication ratio filter
                if (showRow && selectedDuplicationRatios.length > 0) {
                    const ratioCell = cells[dupRatioCol];
                    const ratioText = ratioCell.textContent.trim();
                    // Extract number after "1:" (e.g., "1:10" -> 10)
                    const ratioMatch = ratioText.match(/1:(\\d+)/);
                    const ratioValue = ratioMatch ? parseInt(ratioMatch[1]) : 1;
                    
                    if (!selectedDuplicationRatios.includes(ratioValue)) {
                        showRow = false;
                    }
                }

                row.style.display = showRow ? '' : 'none';
            }
        }
        
        function toggleFilter(columnIndex) {
            const dropdown = document.getElementById('filter-' + columnIndex);
            const allDropdowns = document.querySelectorAll('.filter-dropdown');
            
            // Close all other dropdowns
            allDropdowns.forEach(dd => {
                if (dd !== dropdown) {
                    dd.style.display = 'none';
                }});
            
            // Toggle current dropdown
            dropdown.style.display = dropdown.style.display === 'none' ? 'block' : 'none';
        }
        
        function clearAllFilters() {
            const textFilters = document.querySelectorAll('.filter-input');
            const checkboxes = document.querySelectorAll('input[type="checkbox"]');
            
            textFilters.forEach(filter => filter.value = '');
            checkboxes.forEach(checkbox => checkbox.checked = false);
            
            filterTable();
        }
        
        // Close dropdowns when clicking outside
        document.addEventListener('click', function(event) {
            const isFilterClick = event.target.closest('.filterable') || event.target.closest('.filter-dropdown');
            const isColumnVisClick = event.target.closest('.column-visibility-dropdown');
            
            if (!isFilterClick) {
                document.querySelectorAll('.filter-dropdown').forEach(dd => {
                    dd.style.display = 'none';
                });
            }
            
            if (!isColumnVisClick) {
                document.getElementById('columnVisibilityDropdown').classList.remove('show');
            }
        });
        
        // Initialize Bootstrap tooltips
        function initializeTooltips() {
            const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]');
            [...tooltipTriggerList].forEach(tooltipTriggerEl => {
                try {
                    // Dispose existing tooltip if any
                    const existingTooltip = bootstrap.Tooltip.getInstance(tooltipTriggerEl);
                    if (existingTooltip) {
                        existingTooltip.dispose();
                    }
                    
                    // Create new tooltip
                    new bootstrap.Tooltip(tooltipTriggerEl, {
                        boundary: document.body,
                        trigger: 'hover focus',
                        container: 'body'
                    });
                } catch (err) {
                    console.error('Error initializing tooltip:', err);
                }
            });
        }
        
        // Initialize tooltips when page loads
        document.addEventListener('DOMContentLoaded', function() {
            initializeTooltips();
        });
    </script>
    
    <!-- Bootstrap JS -->
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
"""

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_template)

def main():
    parser = argparse.ArgumentParser(description='Generate HTML report from comment analysis results')
    parser.add_argument('--json', type=str, help='Input JSON file')
    parser.add_argument('--parquet', type=str, default='analyzed_comments.parquet', help='Input Parquet file')
    parser.add_argument('--output', type=str, default='index.html', help='Output HTML file')
    
    args = parser.parse_args()
    
    # Try Parquet first, then JSON
    if args.json and os.path.exists(args.json):
        print(f"Loading results from {args.json}...")
        comments, metadata = load_results(args.json)
    elif os.path.exists(args.parquet):
        print(f"Loading results from {args.parquet}...")
        comments = load_results_parquet(args.parquet)
    else:
        print(f"Error: Neither JSON file '{args.json}' nor Parquet file '{args.parquet}' found")
        return
    
    print("Analyzing field types...")
    field_analysis = analyze_field_types(comments)
    
    print("Calculating statistics...")
    stats = calculate_stats(comments, field_analysis)
    
    print(f"Generating HTML report: {args.output}")
    generate_html(comments, stats, field_analysis, args.output)
    
    print(f"✅ Report generated: {args.output}")
    print(f"📊 {stats['total_comments']:,} comments analyzed")

if __name__ == "__main__":
    main()