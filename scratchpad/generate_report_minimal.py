# Copy the necessary imports and helper functions from main generate_report.py
from typing import List, Dict, Any
from datetime import datetime
import os
import json

def load_regulation_metadata():
    """Load regulation metadata from config or return defaults."""
    default_metadata = {
        'regulation_name': 'Regulation Comments Analysis',
        'brief_description': 'Analysis of public comments',
        'agency': '',
        'docket_id': ''
    }
    
    # Try to load from analyzer_config.json if it exists
    config_files = ['../analyzer_config.json', 'analyzer_config.json']
    for config_file in config_files:
        if os.path.exists(config_file):
            try:
                with open(config_file, 'r') as f:
                    config = json.load(f)
                    return {
                        'regulation_name': config.get('regulation_name', default_metadata['regulation_name']),
                        'brief_description': config.get('regulation_description', default_metadata['brief_description']),
                        'agency': default_metadata['agency'],
                        'docket_id': default_metadata['docket_id']
                    }
            except:
                pass
    
    return default_metadata

def create_tooltip_cell(text: str, max_display_length: int = 50, css_class: str = "", tooltip_max_length: int = 300) -> str:
    """Create a table cell with text truncation and tooltip."""
    if not text or len(text.strip()) == 0:
        return '<td><span style="color: #999;">None</span></td>'
    
    text = text.strip()
    
    if len(text) <= max_display_length:
        return f'<td class="{css_class}">{text}</td>'
    
    # Truncate display text
    display_text = text[:max_display_length] + '...'
    
    # Prepare tooltip text (also truncate if very long)
    tooltip_text = text[:tooltip_max_length]
    if len(text) > tooltip_max_length:
        tooltip_text += '... [truncated]'
    
    # Escape HTML characters for both display and tooltip
    import html
    display_text = html.escape(display_text)
    tooltip_text = html.escape(tooltip_text)
    
    return f'''<td class="{css_class}">
        <span class="char-limited" 
              data-bs-toggle="tooltip" 
              data-bs-placement="top" 
              data-bs-html="true"
              title="{tooltip_text}">{display_text}</span>
    </td>'''

def generate_field_distribution_html(field_name: str, field_info: Dict[str, Any], stats: Dict[str, Any]) -> str:
    """Generate HTML for field distribution."""
    # This is a simplified version - you may want to copy the full function from main file
    if field_name not in stats:
        return ""
    
    field_stats = stats[field_name]
    
    if field_info.get('type') == 'checkbox':
        sorted_items = sorted(field_stats.items(), key=lambda x: x[1], reverse=True)
        
        # Just return a simple distribution for now
        return f'''
        <div class="section">
            <h2>{field_name.replace('_', ' ').title()} Distribution</h2>
            <div>
                {'; '.join([f"{item}: {count}" for item, count in sorted_items])}
            </div>
        </div>'''
    
    return ""

def generate_html_minimal_changes(comments: List[Dict[str, Any]], stats: Dict[str, Any], field_analysis: Dict[str, Dict[str, Any]], output_file: str, discovered_stances: Dict[str, Any] = None):
    """Generate HTML report with minimal changes to add new_stances column."""
    
    # Get metadata
    model_used = "gpt-4o-mini"  # Default assumption
    generated_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Use discovered stances metadata if provided, otherwise load from config
    if discovered_stances:
        regulation_metadata = {
            'regulation_name': discovered_stances['regulation_name'],
            'brief_description': discovered_stances['regulation_description'],
            'agency': '',
            'docket_id': ''
        }
    else:
        regulation_metadata = load_regulation_metadata()
    
    # Always show new_stances column (even if empty)
    has_new_stances = True
    
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
        
        .new-stance-tag {{
            background: #e8f5e8;
            color: #2e7d32;
            padding: 2px 6px;
            border-radius: 8px;
            font-size: 10px;
            border: 1px solid #c8e6c9;
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


        <!-- Analysis Field Distributions -->
        {"".join(generate_field_distribution_html(field_name, field_info, stats) for field_name, field_info in field_analysis.items())}

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
    
    # Add new_stances column visibility control if it exists
    if has_new_stances:
        html_template += """
                        <div class="column-visibility-item">
                            <input type="checkbox" id="col-5" checked onchange="toggleColumn(5)">
                            <label for="col-5">New Stances</label>
                        </div>"""
    
    # Continue with the rest of the column controls (adjust column numbers)
    col_offset = 1 if has_new_stances else 0
    html_template += f"""
                        <div class="column-visibility-item">
                            <input type="checkbox" id="col-{5 + col_offset}" checked onchange="toggleColumn({5 + col_offset})">
                            <label for="col-{5 + col_offset}">Comment</label>
                        </div>
                        <div class="column-visibility-item">
                            <input type="checkbox" id="col-{6 + col_offset}" checked onchange="toggleColumn({6 + col_offset})">
                            <label for="col-{6 + col_offset}">Attachments</label>
                        </div>
                        <div class="column-visibility-item">
                            <input type="checkbox" id="col-{7 + col_offset}" checked onchange="toggleColumn({7 + col_offset})">
                            <label for="col-{7 + col_offset}">Dup Count</label>
                        </div>
                        <div class="column-visibility-item">
                            <input type="checkbox" id="col-{8 + col_offset}" checked onchange="toggleColumn({8 + col_offset})">
                            <label for="col-{8 + col_offset}">Dup Ratio</label>
                        </div>
                        <div class="column-visibility-item">
                            <input type="checkbox" id="col-{9 + col_offset}" onchange="toggleColumn({9 + col_offset})">
                            <label for="col-{9 + col_offset}">Attachment</label>
                        </div>
                        <div class="column-visibility-item">
                            <input type="checkbox" id="col-{10 + col_offset}" onchange="toggleColumn({10 + col_offset})">
                            <label for="col-{10 + col_offset}">Key Quote (LLM)</label>
                        </div>
                        <div class="column-visibility-item">
                            <input type="checkbox" id="col-{11 + col_offset}" onchange="toggleColumn({11 + col_offset})">
                            <label for="col-{11 + col_offset}">Rationale (LLM)</label>
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
                                Stances <span class="filter-arrow" onclick="toggleFilter(4)">▼</span>
                                <div class="filter-dropdown" id="filter-4" style="display: none;">
                                    {"".join(f'<label class="filter-checkbox"><input type="checkbox" data-filter="stances" value="{stance}" onchange="filterTable()"> {stance}</label>' for stance in field_analysis.get('stances', {}).get('unique_values', []))}
                                </div>
                            </th>"""
    
    # Add new_stances header if it exists
    if has_new_stances:
        html_template += f"""
                            <th class="filterable" data-column="5">
                                New Stances <span class="filter-arrow" onclick="toggleFilter(5)">▼</span>
                                <div class="filter-dropdown" id="filter-5" style="display: none;">
                                    {"".join(f'<label class="filter-checkbox"><input type="checkbox" data-filter="new_stances" value="{stance}" onchange="filterTable()"> {stance}</label>' for stance in field_analysis.get('new_stances', {}).get('unique_values', []))}
                                </div>
                            </th>"""
    
    # Continue with remaining headers (adjust column numbers)
    comment_col = 6 if has_new_stances else 5
    html_template += f"""
                            <th class="filterable" data-column="{comment_col}">
                                Comment <span class="filter-arrow" onclick="toggleFilter({comment_col})">▼</span>
                                <div class="filter-dropdown" id="filter-{comment_col}" style="display: none;">
                                    <input type="text" class="filter-input" data-column="{comment_col}" placeholder="Search comment text..." onkeyup="filterTable()">
                                </div>
                            </th>
                            <th class="filterable" data-column="{comment_col + 1}">
                                📎 <span class="filter-arrow" onclick="toggleFilter({comment_col + 1})">▼</span>
                                <div class="filter-dropdown" id="filter-{comment_col + 1}" style="display: none;">
                                    <label class="filter-checkbox"><input type="checkbox" data-filter="attachments" value="yes" onchange="filterTable()"> With attachments</label>
                                    <label class="filter-checkbox"><input type="checkbox" data-filter="attachments" value="no" onchange="filterTable()"> No attachments</label>
                                </div>
                            </th>
                            <th class="filterable" data-column="{comment_col + 2}">
                                Dup Count <span class="filter-arrow" onclick="toggleFilter({comment_col + 2})">▼</span>
                                <div class="filter-dropdown" id="filter-{comment_col + 2}" style="display: none;">
                                    {"".join(f'<label class="filter-checkbox"><input type="checkbox" data-filter="duplication_count" value="{count}" onchange="filterTable()"> {count}</label>' for count in sorted(field_analysis.get('duplication_count', {}).get('unique_values', []), reverse=True))}
                                </div>
                            </th>
                            <th class="filterable" data-column="{comment_col + 3}">
                                Dup Ratio <span class="filter-arrow" onclick="toggleFilter({comment_col + 3})">▼</span>
                                <div class="filter-dropdown" id="filter-{comment_col + 3}" style="display: none;">
                                    {"".join(f'<label class="filter-checkbox"><input type="checkbox" data-filter="duplication_ratio" value="{ratio}" onchange="filterTable()"> 1:{ratio}</label>' for ratio in sorted(field_analysis.get('duplication_ratio', {}).get('unique_values', []), reverse=True))}
                                </div>
                            </th>
                            <th class="filterable" data-column="{comment_col + 4}" style="display: none;">
                                Attachment <span class="filter-arrow" onclick="toggleFilter({comment_col + 4})">▼</span>
                                <div class="filter-dropdown" id="filter-{comment_col + 4}" style="display: none;">
                                    <input type="text" class="filter-input" data-column="{comment_col + 4}" placeholder="Search attachment text..." onkeyup="filterTable()">
                                </div>
                            </th>
                            <th class="filterable" data-column="{comment_col + 5}" style="display: none;">
                                Key Quote <span class="filter-arrow" onclick="toggleFilter({comment_col + 5})">▼</span>
                                <div class="filter-dropdown" id="filter-{comment_col + 5}" style="display: none;">
                                    <input type="text" class="filter-input" data-column="{comment_col + 5}" placeholder="Search quotes..." onkeyup="filterTable()">
                                </div>
                            </th>
                            <th class="filterable" data-column="{comment_col + 6}" style="display: none;">
                                Rationale <span class="filter-arrow" onclick="toggleFilter({comment_col + 6})">▼</span>
                                <div class="filter-dropdown" id="filter-{comment_col + 6}" style="display: none;">
                                    <input type="text" class="filter-input" data-column="{comment_col + 6}" placeholder="Search rationale..." onkeyup="filterTable()">
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
        
        # Handle new_stances
        new_stances = analysis.get('new_stances', []) if has_new_stances else []
        
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
        
        # Stances display
        stances_html = '<div class="stances-container">' + ' '.join(f'<span class="stance-tag">{stance}</span>' for stance in stances) + '</div>' if stances else '<span style="color: #999;">None</span>'
        
        # New stances display
        new_stances_html = '<div class="stances-container">' + ' '.join(f'<span class="new-stance-tag">{stance}</span>' for stance in new_stances) + '</div>' if new_stances else '<span style="color: #999;">None</span>'
        
        # Full comment text with tooltip
        full_text = comment.get('comment_text', '')
        comment_cell = create_tooltip_cell(full_text, 300, "text-preview", tooltip_max_length=1500)
        
        # Attachment text with tooltip
        attachment_text = comment.get('attachment_text', '')
        attachment_cell = create_tooltip_cell(attachment_text, 300, tooltip_max_length=1200).replace('<td', '<td style="display: none;"')
        
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
        
        # Create tooltip cells for truncated content
        key_quote_cell = create_tooltip_cell(key_quote, 300, tooltip_max_length=500).replace('<td', '<td style="display: none;"')
        rationale_cell = create_tooltip_cell(rationale, 500, tooltip_max_length=500).replace('<td', '<td style="display: none;"')
        submitter_cell = create_tooltip_cell(comment.get('submitter', ''), 50, tooltip_max_length=200)
        organization_cell = create_tooltip_cell(comment.get('organization', ''), 50, tooltip_max_length=200)

        # Add new_stances cell if needed
        new_stances_cell = f'<td>{new_stances_html}</td>' if has_new_stances else ''

        html_template += f"""
                            <tr>
                                <td><span class="comment-id"><a href="https://www.regulations.gov/comment/{comment.get('id', '')}" target="_blank" style="color: #007bff; text-decoration: underline;">{comment.get('id', '')}</a></span></td>
                                <td class="date-cell">{formatted_date}</td>
                                {submitter_cell}
                                {organization_cell}
                                <td>{stances_html}</td>
                                {new_stances_cell}
                                {comment_cell}
                                <td>{has_attachments}</td>
                                <td>{count_display}</td>
                                <td>{ratio_display}</td>
                                {attachment_cell}
                                {key_quote_cell}
                                {rationale_cell}
                            </tr>
"""

    # Set up column visibility based on whether new_stances exists
    if has_new_stances:
        column_visibility = "{0: true, 1: true, 2: true, 3: true, 4: true, 5: true, 6: true, 7: true, 8: true, 9: true, 10: false, 11: false, 12: false}"
    else:
        column_visibility = "{0: true, 1: true, 2: true, 3: true, 4: true, 5: true, 6: true, 7: true, 8: true, 9: false, 10: false, 11: false}"

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
        const columnVisibility = {column_visibility};

        // Initialize column visibility on page load
        document.addEventListener('DOMContentLoaded', function() {{
            for (const [col, visible] of Object.entries(columnVisibility)) {{
                updateColumnVisibility(parseInt(col), visible);
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
            const stanceCheckboxes = document.querySelectorAll('input[data-filter="stances"]:checked');
            const newStanceCheckboxes = document.querySelectorAll('input[data-filter="new_stances"]:checked');
            const attachmentCheckboxes = document.querySelectorAll('input[data-filter="attachments"]:checked');
            const duplicationCountCheckboxes = document.querySelectorAll('input[data-filter="duplication_count"]:checked');
            const duplicationRatioCheckboxes = document.querySelectorAll('input[data-filter="duplication_ratio"]:checked');
            
            const selectedStances = Array.from(stanceCheckboxes).map(cb => cb.value.toLowerCase());
            const selectedNewStances = Array.from(newStanceCheckboxes).map(cb => cb.value.toLowerCase());
            const selectedAttachments = Array.from(attachmentCheckboxes).map(cb => cb.value);
            const selectedDuplicationCounts = Array.from(duplicationCountCheckboxes).map(cb => parseInt(cb.value));
            const selectedDuplicationRatios = Array.from(duplicationRatioCheckboxes).map(cb => parseInt(cb.value));

            // Determine column indices based on whether new_stances exists
            const hasNewStances = {str(has_new_stances).lower()};
            const stanceCol = 4;
            const newStanceCol = hasNewStances ? 5 : -1;
            const attachmentCol = hasNewStances ? 7 : 6;
            const duplicationCountCol = hasNewStances ? 8 : 7;
            const duplicationRatioCol = hasNewStances ? 9 : 8;

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
                
                // Check stance filter
                if (showRow && selectedStances.length > 0) {{
                    const stanceText = cells[stanceCol].textContent.toLowerCase();
                    if (!selectedStances.some(stance => stanceText.includes(stance))) {{
                        showRow = false;
                    }}
                }}
                
                // Check new stance filter
                if (showRow && selectedNewStances.length > 0 && hasNewStances) {{
                    const newStanceText = cells[newStanceCol].textContent.toLowerCase();
                    if (!selectedNewStances.some(stance => newStanceText.includes(stance))) {{
                        showRow = false;
                    }}
                }}
                
                // Check attachments filter
                if (showRow && selectedAttachments.length > 0) {{
                    const attachmentCell = cells[attachmentCol];
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
                
                // Check duplication count filter
                if (showRow && selectedDuplicationCounts.length > 0) {{
                    const duplicationCell = cells[duplicationCountCol];
                    const duplicationText = duplicationCell.textContent.trim();
                    const duplicationCount = parseInt(duplicationText);
                    
                    if (!selectedDuplicationCounts.includes(duplicationCount)) {{
                        showRow = false;
                    }}
                }}
                
                // Check duplication ratio filter
                if (showRow && selectedDuplicationRatios.length > 0) {{
                    const ratioCell = cells[duplicationRatioCol];
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
                }}}});
            
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
    
    print(f"✅ Report generated with new_stances support: {output_file}")