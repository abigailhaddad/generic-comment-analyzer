#!/usr/bin/env python3
"""
Generate Theme-Based HTML Report from Comment Analysis Results

Creates an interactive HTML report that groups stances by themes and shows
co-occurrence patterns within each theme.
"""

import argparse
import json
import os
from datetime import datetime
from typing import Dict, Any, List, Tuple
import pandas as pd
import re
from collections import defaultdict

def load_results(json_file: str) -> List[Dict[str, Any]]:
    """Load analyzed comments from JSON file."""
    with open(json_file, 'r', encoding='utf-8') as f:
        return json.load(f)

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

def parse_theme_position(stance: str) -> Tuple[str, str]:
    """Parse a stance string into theme and position.
    
    Args:
        stance: A stance string like "COVID Vaccine Market Status: Support for Removal of COVID Vaccines from Market"
        
    Returns:
        Tuple of (theme, position)
    """
    # Look for pattern "Theme: Position"
    match = re.match(r'^([^:]+):\s*(.+)$', stance)
    if match:
        return match.group(1).strip(), match.group(2).strip()
    else:
        # If no colon found, treat entire string as position with "Other" theme
        return "Other", stance.strip()

def group_stances_by_theme(comments: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Group all stances by their themes and analyze patterns.
    
    Returns:
        Dictionary mapping theme names to:
        - positions: List of unique positions in this theme
        - position_counts: Count of each position
        - total_occurrences: Total number of comments with this theme
        - cooccurrence: Co-occurrence matrix for positions within this theme
    """
    themes = defaultdict(lambda: {
        'positions': set(),
        'position_counts': defaultdict(int),
        'total_occurrences': 0,
        'comments_with_theme': set()
    })
    
    # First pass: collect all positions and counts
    for comment in comments:
        analysis = comment.get('analysis', {})
        if analysis:
            stances = analysis.get('stances', [])
            if isinstance(stances, list):
                comment_themes = set()
                for stance in stances:
                    theme, position = parse_theme_position(stance)
                    themes[theme]['positions'].add(position)
                    themes[theme]['position_counts'][position] += 1
                    comment_themes.add(theme)
                
                # Track which comments have each theme
                for theme in comment_themes:
                    themes[theme]['comments_with_theme'].add(comment.get('id', ''))
                    themes[theme]['total_occurrences'] += 1
    
    # Convert sets to sorted lists
    for theme_data in themes.values():
        theme_data['positions'] = sorted(list(theme_data['positions']))
        theme_data['position_counts'] = dict(theme_data['position_counts'])
    
    # Second pass: calculate co-occurrence within themes
    for theme, theme_data in themes.items():
        positions = theme_data['positions']
        cooccurrence = {}
        
        # Initialize co-occurrence matrix
        for pos1 in positions:
            cooccurrence[pos1] = {}
            for pos2 in positions:
                cooccurrence[pos1][pos2] = 0
        
        # Count co-occurrences within this theme
        for comment in comments:
            analysis = comment.get('analysis', {})
            if analysis:
                stances = analysis.get('stances', [])
                if isinstance(stances, list):
                    # Get only positions from this theme
                    theme_positions = []
                    for stance in stances:
                        t, p = parse_theme_position(stance)
                        if t == theme:
                            theme_positions.append(p)
                    
                    # Count co-occurrences
                    for pos1 in theme_positions:
                        for pos2 in theme_positions:
                            cooccurrence[pos1][pos2] += 1
        
        theme_data['cooccurrence'] = cooccurrence
    
    return dict(themes)

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

def calculate_stats(comments: List[Dict[str, Any]], field_analysis: Dict[str, Dict[str, Any]], theme_data: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """Calculate summary statistics including theme-based analysis."""
    total_comments = len(comments)
    
    # Comments with attachments
    with_attachments = sum(1 for c in comments if c.get('attachment_text', '').strip())
    
    # Average text length
    text_lengths = [len(c.get('text', '')) for c in comments]
    avg_length = sum(text_lengths) / len(text_lengths) if text_lengths else 0
    
    return {
        'total_comments': total_comments,
        'theme_data': theme_data,
        'with_attachments': with_attachments,
        'avg_text_length': int(avg_length),
        'date_range': get_date_range(comments),
        'num_themes': len(theme_data)
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
        min_date = min(dates).strftime('%Y-%m-%d')
        max_date = max(dates).strftime('%Y-%m-%d')
        if min_date == max_date:
            return min_date
        return f"{min_date} to {max_date}"
    return "Unknown"

def calculate_theme_cooccurrence_percentages(theme_data: Dict[str, Any]) -> Tuple[Dict[str, Dict[str, float]], Dict[str, int]]:
    """Calculate what percentage of comments with each position also have other positions within a theme."""
    cooccurrence = theme_data.get('cooccurrence', {})
    if not cooccurrence:
        return {}, {}
    
    percentages = {}
    position_names = list(cooccurrence.keys())
    
    # Create position number mapping
    position_to_num = {position: i+1 for i, position in enumerate(position_names)}
    
    for pos1 in position_names:
        total_with_pos1 = cooccurrence[pos1][pos1]  # Diagonal value
        if total_with_pos1 == 0:
            continue
            
        percentages[pos1] = {}
        for pos2 in position_names:
            if pos1 != pos2:  # Skip self
                count = cooccurrence[pos1][pos2]
                percentage = (count / total_with_pos1) * 100 if total_with_pos1 > 0 else 0
                if percentage > 0:  # Only include non-zero percentages
                    percentages[pos1][position_to_num[pos2]] = percentage
    
    return percentages, position_to_num

def generate_theme_section_html(theme_name: str, theme_info: Dict[str, Any]) -> str:
    """Generate HTML for a single theme section with expandable positions and co-occurrence."""
    positions = theme_info['positions']
    position_counts = theme_info['position_counts']
    total_theme_occurrences = theme_info['total_occurrences']
    
    # Sort positions by count
    sorted_positions = sorted(positions, key=lambda p: position_counts.get(p, 0), reverse=True)
    
    # Calculate co-occurrence percentages
    percentages, position_to_num = calculate_theme_cooccurrence_percentages(theme_info)
    
    # Build position cards
    position_cards = []
    for i, position in enumerate(sorted_positions[:10]):  # Show top 10 positions
        count = position_counts.get(position, 0)
        
        # Build connections for this position
        connections = []
        if position in percentages and percentages[position]:
            for other_position in sorted_positions[:10]:
                if other_position != position:
                    for other_num, pct in percentages[position].items():
                        if position_to_num.get(other_position) == other_num and pct >= 5:  # Show if 5% or higher
                            connections.append((pct, other_position))
            
            # Sort by percentage, highest first
            connections.sort(reverse=True, key=lambda x: x[0])
        
        # Create connections HTML
        connections_html = ""
        if connections:
            connection_items = []
            for pct, other_position in connections:
                connection_items.append(f'<div class="connection-item">{pct:.0f}% also: {other_position}</div>')
            connections_html = "".join(connection_items)
        else:
            connections_html = '<div class="no-connections">No significant overlaps with other positions in this theme</div>'
        
        position_cards.append(f'''
            <div class="expandable-position-card">
                <div class="position-header" onclick="togglePositionDetails('{theme_name}-{i}')">
                    <span class="position-count">{count:,}</span>
                    <span class="position-name">{position}</span>
                    <span class="expand-icon" id="icon-{theme_name}-{i}">▼</span>
                </div>
                <div class="position-details" id="details-{theme_name}-{i}" style="display: none;">
                    {connections_html}
                </div>
            </div>''')
    
    return f'''
    <div class="theme-section">
        <h3 class="theme-title">
            <span class="theme-icon">📊</span>
            {theme_name}
            <span class="theme-stats">({total_theme_occurrences:,} comments)</span>
        </h3>
        <p class="theme-description">Click on any position to see how it relates to others within this theme</p>
        <div class="positions-grid">
            {"".join(position_cards)}
        </div>
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
    """Generate HTML report with theme-based organization."""
    
    # Get metadata
    model_used = "gpt-4o-mini"  # Default assumption
    generated_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    regulation_metadata = load_regulation_metadata()
    theme_data = stats['theme_data']
    
    # Sort themes by total occurrences
    sorted_themes = sorted(theme_data.items(), key=lambda x: x[1]['total_occurrences'], reverse=True)
    
    html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{regulation_metadata['regulation_name']} - Theme-Based Comment Analysis Report</title>
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
        
        /* Theme-specific styles */
        .theme-section {{
            background: #f8f9fa;
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 30px;
            border: 1px solid #e9ecef;
        }}
        
        .theme-title {{
            font-size: 1.3em;
            color: #2c3e50;
            margin-bottom: 10px;
            display: flex;
            align-items: center;
            gap: 10px;
        }}
        
        .theme-icon {{
            font-size: 1.2em;
        }}
        
        .theme-stats {{
            font-size: 0.8em;
            color: #7f8c8d;
            font-weight: normal;
            margin-left: auto;
        }}
        
        .theme-description {{
            color: #666;
            margin-bottom: 20px;
            font-style: italic;
            font-size: 0.9em;
        }}
        
        .positions-grid {{
            display: grid;
            gap: 12px;
        }}
        
        .expandable-position-card {{
            background: white;
            border-radius: 8px;
            border-left: 4px solid #3498db;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        
        .position-header {{
            display: flex;
            align-items: flex-start;
            gap: 15px;
            padding: 15px;
            cursor: pointer;
            transition: background-color 0.2s;
        }}
        
        .position-header:hover {{
            background-color: #f8f9fa;
        }}
        
        .position-count {{
            font-size: 1.4em;
            font-weight: bold;
            color: #2c3e50;
            min-width: 60px;
            flex-shrink: 0;
        }}
        
        .position-name {{
            font-size: 0.95em;
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
        
        .position-details {{
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
        
        /* Filters */
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
            max-height: 300px;
            overflow-y: auto;
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
        
        /* Table styles */
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
        
        .theme-tag {{
            background: #e8f5e9;
            color: #2e7d32;
            padding: 2px 6px;
            border-radius: 8px;
            font-size: 10px;
            border: 1px solid #c8e6c9;
            font-weight: 500;
            margin-right: 4px;
        }}
        
        .position-tag {{
            background: #e3f2fd;
            color: #1976d2;
            padding: 2px 6px;
            border-radius: 8px;
            font-size: 10px;
            border: 1px solid #bbdefb;
        }}
        
        .stances-container {{
            display: flex;
            flex-wrap: wrap;
            gap: 4px;
            max-width: 400px;
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
                <div class="stat-number">{stats['num_themes']:,}</div>
                <div class="stat-label">Unique Themes</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{stats['with_attachments']:,}</div>
                <div class="stat-label">With Attachments</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{stats['avg_text_length']:,}</div>
                <div class="stat-label">Avg. Characters</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{stats['date_range']}</div>
                <div class="stat-label">Date Range</div>
            </div>
        </div>

        <!-- Themes Section -->
        <div class="section">
            <h2>📊 Themes and Positions</h2>
            {"".join(generate_theme_section_html(theme_name, theme_info) for theme_name, theme_info in sorted_themes)}
        </div>

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
                            <label for="col-4">Themes & Positions</label>
                        </div>
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
                            <th>Date</th>
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
                                Themes & Positions <span class="filter-arrow" onclick="toggleFilter(4)">▼</span>
                                <div class="filter-dropdown" id="filter-4" style="display: none;">"""
    
    # Build theme checkboxes
    theme_checkboxes = ''.join(f'<label class="filter-checkbox"><input type="checkbox" data-filter="themes" value="{theme}" onchange="filterTable()"> {theme} ({info["total_occurrences"]:,} comments)</label>' 
                               for theme, info in sorted_themes)
    
    html_template += theme_checkboxes
    html_template += """
                                </div>
                            </th>
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
    dup_ratio_checkboxes = ''.join(f'<label class="filter-checkbox"><input type="checkbox" data-filter="duplication_ratio" value="{ratio}" onchange="filterTable()"> 1:{ratio}</label>' 
                                   for ratio in sorted(field_analysis.get('duplication_ratio', {}).get('unique_values', []), reverse=True))
    
    html_template += dup_ratio_checkboxes
    html_template += """
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
            stances = [stance_data] if stance_data else []
        elif isinstance(stance_data, list):
            # New multi-stance format
            stances = stance_data
        else:
            stances = []
        
        # Parse themes and positions
        themes_positions = defaultdict(list)
        for stance in stances:
            theme, position = parse_theme_position(stance)
            themes_positions[theme].append(position)
        
        # Create theme/position display
        stance_elements = []
        for theme, positions in sorted(themes_positions.items()):
            stance_elements.append(f'<span class="theme-tag">{theme}</span>')
            for position in positions:
                stance_elements.append(f'<span class="position-tag">{position}</span>')
        
        stances_html = '<div class="stances-container">' + ' '.join(stance_elements) + '</div>' if stance_elements else '<span style="color: #999;">None</span>'
        
        # Format date
        date_str = comment.get('date', '')
        formatted_date = ''
        if date_str:
            try:
                date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                formatted_date = date.strftime('%Y-%m-%d')
            except:
                formatted_date = date_str[:10] if len(date_str) >= 10 else date_str
        
        # New stances display
        new_stances = analysis.get('new_stances', [])
        new_stances_html = '<div class="stances-container">' + ' '.join(f'<span class="position-tag" style="background: #fff3cd; color: #856404; border: 1px solid #ffeaa7;">{stance}</span>' for stance in new_stances) + '</div>' if new_stances else '<span style="color: #999;">None</span>'
        
        # Full comment text with tooltip
        full_text = comment.get('comment_text', '')
        comment_cell = create_tooltip_cell(full_text, 300, "text-preview", tooltip_max_length=1500)
        
        # Attachments
        has_attachments = '<span class="attachment-indicator">📎</span>' if comment.get('attachment_text', '').strip() else ''
        
        # Duplication count and ratio
        duplication_count = comment.get('duplication_count', 1)
        duplication_ratio = comment.get('duplication_ratio', 1)
        
        if duplication_count == 1:
            count_display = '<span class="unique-indicator">1</span>'
            ratio_display = f'<span class="unique-indicator">1:{duplication_ratio}</span>'
        else:
            count_display = f'<span class="duplicate-indicator">{duplication_count}</span>'
            ratio_display = f'<span class="duplicate-indicator">1:{duplication_ratio}</span>'
        
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
                        </tr>
"""

    html_template += f"""
                    </tbody>
                </table>
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
                    <div class="meta-label">Unique Themes</div>
                    <div class="meta-value">{stats['num_themes']:,}</div>
                </div>
                <div class="meta-item">
                    <div class="meta-label">With Attachments</div>
                    <div class="meta-value">{stats['with_attachments']:,}</div>
                </div>
                <div class="meta-item">
                    <div class="meta-label">Average Length</div>
                    <div class="meta-value">{stats['avg_text_length']:,} chars</div>
                </div>
                <div class="meta-item">
                    <div class="meta-label">Date Range</div>
                    <div class="meta-value">{stats['date_range']}</div>
                </div>
            </div>
        </div>
    </div>

    <script>
        // Column visibility state
        const columnVisibility = {{
            0: true, 1: true, 2: true, 3: true, 4: true, 5: true, 6: true, 7: true, 8: true, 9: true
        }};

        // Initialize column visibility on page load
        document.addEventListener('DOMContentLoaded', function() {{
            for (const [col, visible] of Object.entries(columnVisibility)) {{
                updateColumnVisibility(parseInt(col), visible);
                // Also update the checkbox state
                const checkbox = document.getElementById('col-' + col);
                if (checkbox) {{
                    checkbox.checked = visible;
                }}
            }}
        }});

        function toggleColumnVisibility() {{
            const dropdown = document.getElementById('columnVisibilityDropdown');
            dropdown.classList.toggle('show');
        }}

        function toggleColumn(columnIndex) {{
            const checkbox = document.getElementById('col-' + columnIndex);
            const visible = checkbox.checked;
            columnVisibility[columnIndex] = visible;
            updateColumnVisibility(columnIndex, visible);
        }}

        function updateColumnVisibility(columnIndex, visible) {{
            const table = document.getElementById('commentsTable');
            const rows = table.getElementsByTagName('tr');
            
            // Update header
            const headers = rows[0].getElementsByTagName('th');
            if (headers[columnIndex]) {{
                headers[columnIndex].style.display = visible ? '' : 'none';
            }}
            
            // Update all data rows
            for (let i = 1; i < rows.length; i++) {{
                const cells = rows[i].getElementsByTagName('td');
                if (cells[columnIndex]) {{
                    cells[columnIndex].style.display = visible ? '' : 'none';
                }}
            }}
        }}

        function togglePositionDetails(id) {{
            const details = document.getElementById('details-' + id);
            const icon = document.getElementById('icon-' + id);
            
            if (details.style.display === 'none' || details.style.display === '') {{
                details.style.display = 'block';
                icon.classList.add('expanded');
            }} else {{
                details.style.display = 'none';
                icon.classList.remove('expanded');
            }}
        }}

        function filterTable() {{
            const table = document.getElementById('commentsTable');
            const rows = table.getElementsByTagName('tr');
            
            // Get text input filters
            const textFilters = document.querySelectorAll('.filter-input');
            const textFilterValues = Array.from(textFilters).map(filter => {{
                return {{
                    column: parseInt(filter.getAttribute('data-column')),
                    value: filter.value.toLowerCase()
                }};
            }});
            
            // Get checkbox filters
            const themeCheckboxes = document.querySelectorAll('input[data-filter="themes"]:checked');
            const newStanceCheckboxes = document.querySelectorAll('input[data-filter="new_stances"]:checked');
            const attachmentCheckboxes = document.querySelectorAll('input[data-filter="attachments"]:checked');
            const duplicationCountCheckboxes = document.querySelectorAll('input[data-filter="duplication_count"]:checked');
            const duplicationRatioCheckboxes = document.querySelectorAll('input[data-filter="duplication_ratio"]:checked');
            
            const selectedThemes = Array.from(themeCheckboxes).map(cb => cb.value.toLowerCase());
            const selectedNewStances = Array.from(newStanceCheckboxes).map(cb => cb.value.toLowerCase());
            const selectedAttachments = Array.from(attachmentCheckboxes).map(cb => cb.value);
            const selectedDuplicationCounts = Array.from(duplicationCountCheckboxes).map(cb => parseInt(cb.value));
            const selectedDuplicationRatios = Array.from(duplicationRatioCheckboxes).map(cb => parseInt(cb.value));
            
            // Filter each row
            for (let i = 1; i < rows.length; i++) {{
                const row = rows[i];
                const cells = row.getElementsByTagName('td');
                let showRow = true;

                // Check text filters
                for (const filter of textFilterValues) {{
                    if (filter.value && filter.column < cells.length) {{
                        const cellText = cells[filter.column].textContent.toLowerCase();
                        if (!cellText.includes(filter.value)) {{
                            showRow = false;
                            break;
                        }}
                    }}
                }}
                
                // Check theme filter (column 4)
                if (showRow && selectedThemes.length > 0) {{
                    const themeText = cells[4].textContent.toLowerCase();
                    if (!selectedThemes.some(theme => themeText.includes(theme))) {{
                        showRow = false;
                    }}
                }}
                
                // Check new stance filter (column 5)
                if (showRow && selectedNewStances.length > 0) {{
                    const newStanceText = cells[5].textContent.toLowerCase();
                    if (!selectedNewStances.some(stance => newStanceText.includes(stance))) {{
                        showRow = false;
                    }}
                }}
                
                // Check attachments filter (column 7)
                if (showRow && selectedAttachments.length > 0) {{
                    const attachmentCell = cells[7];
                    const hasAttachment = attachmentCell.textContent.trim() !== '';
                    
                    let attachmentMatch = false;
                    for (const filter of selectedAttachments) {{
                        if (filter === 'yes' && hasAttachment) {{
                            attachmentMatch = true;
                            break;
                        }}
                        if (filter === 'no' && !hasAttachment) {{
                            attachmentMatch = true;
                            break;
                        }}
                    }}
                    
                    if (!attachmentMatch) {{
                        showRow = false;
                    }}
                }}
                
                // Check duplication count filter (column 8)
                if (showRow && selectedDuplicationCounts.length > 0) {{
                    const duplicationCell = cells[8];
                    const duplicationText = duplicationCell.textContent.trim();
                    const duplicationCount = parseInt(duplicationText);
                    
                    if (!selectedDuplicationCounts.includes(duplicationCount)) {{
                        showRow = false;
                    }}
                }}
                
                // Check duplication ratio filter (column 9)
                if (showRow && selectedDuplicationRatios.length > 0) {{
                    const ratioCell = cells[9];
                    const ratioText = ratioCell.textContent.trim();
                    // Extract number after "1:" (e.g., "1:10" -> 10)
                    const ratioMatch = ratioText.match(/1:(\\d+)/);
                    const ratioValue = ratioMatch ? parseInt(ratioMatch[1]) : 1;
                    
                    if (!selectedDuplicationRatios.includes(ratioValue)) {{
                        showRow = false;
                    }}
                }}

                row.style.display = showRow ? '' : 'none';
            }}
        }}
        
        function toggleFilter(columnIndex) {{
            const dropdown = document.getElementById('filter-' + columnIndex);
            const allDropdowns = document.querySelectorAll('.filter-dropdown');
            
            // Close all other dropdowns
            allDropdowns.forEach(dd => {{
                if (dd !== dropdown) {{
                    dd.style.display = 'none';
                }}
            }});
            
            // Toggle current dropdown
            dropdown.style.display = dropdown.style.display === 'none' ? 'block' : 'none';
        }}
        
        function clearAllFilters() {{
            const textFilters = document.querySelectorAll('.filter-input');
            const checkboxes = document.querySelectorAll('input[type="checkbox"]');
            
            textFilters.forEach(filter => filter.value = '');
            checkboxes.forEach(checkbox => checkbox.checked = false);
            
            filterTable();
        }}
        
        // Close dropdowns when clicking outside
        document.addEventListener('click', function(event) {{
            const isFilterClick = event.target.closest('.filterable') || event.target.closest('.filter-dropdown');
            const isColumnVisClick = event.target.closest('.column-visibility-dropdown');
            
            if (!isFilterClick) {{
                document.querySelectorAll('.filter-dropdown').forEach(dd => {{
                    dd.style.display = 'none';
                }});
            }}
            
            if (!isColumnVisClick) {{
                document.getElementById('columnVisibilityDropdown').classList.remove('show');
            }}
        }});
        
        // Initialize Bootstrap tooltips
        function initializeTooltips() {{
            const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]');
            [...tooltipTriggerList].forEach(tooltipTriggerEl => {{
                try {{
                    // Dispose existing tooltip if any
                    const existingTooltip = bootstrap.Tooltip.getInstance(tooltipTriggerEl);
                    if (existingTooltip) {{
                        existingTooltip.dispose();
                    }}
                    
                    // Create new tooltip
                    new bootstrap.Tooltip(tooltipTriggerEl, {{
                        boundary: document.body,
                        trigger: 'hover focus',
                        container: 'body'
                    }});
                }} catch (err) {{
                    console.error('Error initializing tooltip:', err);
                }}
            }});
        }}
        
        // Initialize tooltips when page loads
        document.addEventListener('DOMContentLoaded', function() {{
            initializeTooltips();
        }});
    </script>
    
    <!-- Bootstrap JS -->
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
"""

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_template)

def main():
    parser = argparse.ArgumentParser(description='Generate theme-based HTML report from comment analysis results')
    parser.add_argument('--json', type=str, help='Input JSON file')
    parser.add_argument('--parquet', type=str, default='analyzed_comments.parquet', help='Input Parquet file')
    parser.add_argument('--output', type=str, default='theme_report.html', help='Output HTML file')
    
    args = parser.parse_args()
    
    # Load comments
    if args.json and os.path.exists(args.json):
        print(f"Loading results from {args.json}...")
        # Check if it's a processed JSON file or raw comments
        with open(args.json, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if isinstance(data, dict) and 'comments' in data:
                comments = data['comments']
            else:
                comments = data
    elif os.path.exists(args.parquet):
        print(f"Loading results from {args.parquet}...")
        comments = load_results_parquet(args.parquet)
    else:
        print(f"Error: Neither JSON file '{args.json}' nor Parquet file '{args.parquet}' found")
        return
    
    print(f"Loaded {len(comments)} comments")
    
    print("Grouping stances by theme...")
    theme_data = group_stances_by_theme(comments)
    print(f"Found {len(theme_data)} unique themes")
    
    print("Analyzing field types...")
    field_analysis = analyze_field_types(comments)
    
    print("Calculating statistics...")
    stats = calculate_stats(comments, field_analysis, theme_data)
    
    print(f"Generating HTML report: {args.output}")
    generate_html(comments, stats, field_analysis, args.output)
    
    print(f"✅ Report generated: {args.output}")
    print(f"📊 {stats['total_comments']:,} comments analyzed")
    print(f"🏷️  {stats['num_themes']:,} unique themes identified")

if __name__ == "__main__":
    main()