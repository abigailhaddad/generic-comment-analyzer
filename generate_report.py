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

def load_results(json_file: str) -> List[Dict[str, Any]]:
    """Load analyzed comments from JSON file."""
    with open(json_file, 'r', encoding='utf-8') as f:
        return json.load(f)

def calculate_stats(comments: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Calculate summary statistics."""
    total_comments = len(comments)
    
    # Count stances
    stance_counts = {}
    for comment in comments:
        analysis = comment.get('analysis', {})
        if analysis:
            stance = analysis.get('stance', 'Unknown')
            stance_counts[stance] = stance_counts.get(stance, 0) + 1
    
    # Count themes
    theme_counts = {}
    for comment in comments:
        analysis = comment.get('analysis', {})
        if analysis and analysis.get('themes'):
            for theme in analysis['themes']:
                theme_counts[theme] = theme_counts.get(theme, 0) + 1
    
    # Top themes
    top_themes = sorted(theme_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    
    # Comments with attachments
    with_attachments = sum(1 for c in comments if c.get('attachment_text', '').strip())
    
    # Average text length
    text_lengths = [len(c.get('text', '')) for c in comments]
    avg_length = sum(text_lengths) / len(text_lengths) if text_lengths else 0
    
    return {
        'total_comments': total_comments,
        'stance_counts': stance_counts,
        'top_themes': top_themes,
        'with_attachments': with_attachments,
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
        min_date = min(dates).strftime('%Y-%m-%d')
        max_date = max(dates).strftime('%Y-%m-%d')
        if min_date == max_date:
            return min_date
        return f"{min_date} to {max_date}"
    return "Unknown"

def get_unique_values(comments: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    """Get unique values for enumerated fields."""
    stances = set()
    themes = set()
    
    for comment in comments:
        analysis = comment.get('analysis', {})
        if analysis:
            stance = analysis.get('stance', 'Unknown')
            stances.add(stance)
            
            comment_themes = analysis.get('themes', [])
            themes.update(comment_themes)
    
    return {
        'stances': sorted(list(stances)),
        'themes': sorted(list(themes))
    }

def generate_html(comments: List[Dict[str, Any]], stats: Dict[str, Any], output_file: str):
    """Generate HTML report."""
    
    # Get metadata and unique values
    model_used = "gpt-4o-mini"  # Default assumption
    generated_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    unique_values = get_unique_values(comments)
    
    html_template = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Comment Analysis Report</title>
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
            max-width: 1200px;
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
        
        .filter-input {{
            padding: 8px 12px;
            border: 1px solid #ced4da;
            border-radius: 4px;
            font-size: 14px;
        }}
        
        .filter-input:focus {{
            outline: none;
            border-color: #007bff;
            box-shadow: 0 0 0 2px rgba(0,123,255,0.25);
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
        
        .themes-container {{
            display: flex;
            flex-wrap: wrap;
            gap: 4px;
            max-width: 300px;
        }}
        
        .theme-tag {{
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
        
        .date-cell {{
            white-space: nowrap;
            font-size: 12px;
        }}
        
        .attachment-indicator {{
            font-size: 16px;
            color: #28a745;
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
        
        .themes-list {{
            list-style: none;
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 10px;
        }}
        
        .themes-list li {{
            background: #f8f9fa;
            padding: 10px 15px;
            border-radius: 4px;
            border-left: 3px solid #007bff;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }}
        
        .theme-name {{
            font-weight: 500;
        }}
        
        .theme-count {{
            background: #007bff;
            color: white;
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 11px;
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
        }}
        
        .clear-filters:hover {{
            background: #c82333;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Comment Analysis Report</h1>
            <div class="subtitle">Analysis of public comments with interactive filtering</div>
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

        <!-- Stance Breakdown -->
        <div class="section">
            <h2>📈 Stance Distribution</h2>
            <div class="stats-grid">
                {"".join(f'''
                <div class="stat-card">
                    <div class="stat-number">{count:,}</div>
                    <div class="stat-label">{stance}</div>
                </div>
                ''' for stance, count in stats['stance_counts'].items())}
            </div>
        </div>

        <!-- Top Themes -->
        <div class="section">
            <h2>🏷️ Top Themes</h2>
            <ul class="themes-list">
                {"".join(f'''<li>
                    <span class="theme-name">{theme}</span>
                    <span class="theme-count">{count:,}</span>
                </li>''' for theme, count in stats['top_themes'])}
            </ul>
        </div>

        <!-- Comments Table -->
        <div class="section">
            <h2>Comments</h2>
            <div class="filters-container">
                <div class="filters-header">Filter Comments</div>
                <div class="filters-grid">
                    <div class="filter-group">
                        <div class="filter-label">Comment ID</div>
                        <input type="text" class="filter-input" data-column="0" placeholder="Filter by ID..." onkeyup="filterTable()">
                    </div>
                    <div class="filter-group">
                        <div class="filter-label">Date</div>
                        <input type="text" class="filter-input" data-column="1" placeholder="Filter by date..." onkeyup="filterTable()">
                    </div>
                    <div class="filter-group">
                        <div class="filter-label">Stance</div>
                        <div class="checkbox-group">
                            {"".join(f'''<div class="checkbox-item">
                                <input type="checkbox" id="stance-{stance.lower()}" data-filter="stance" value="{stance}" onchange="filterTable()">
                                <label for="stance-{stance.lower()}">{stance}</label>
                            </div>''' for stance in unique_values['stances'])}
                        </div>
                    </div>
                    <div class="filter-group">
                        <div class="filter-label">Themes</div>
                        <div class="checkbox-group">
                            {"".join(f'''<div class="checkbox-item">
                                <input type="checkbox" id="theme-{i}" data-filter="themes" value="{theme}" onchange="filterTable()">
                                <label for="theme-{i}">{theme}</label>
                            </div>''' for i, theme in enumerate(unique_values['themes']))}
                        </div>
                    </div>
                    <div class="filter-group">
                        <div class="filter-label">Has Attachments</div>
                        <div class="checkbox-group">
                            <div class="checkbox-item">
                                <input type="checkbox" id="has-attachments" data-filter="attachments" value="yes" onchange="filterTable()">
                                <label for="has-attachments">Yes</label>
                            </div>
                            <div class="checkbox-item">
                                <input type="checkbox" id="no-attachments" data-filter="attachments" value="no" onchange="filterTable()">
                                <label for="no-attachments">No</label>
                            </div>
                        </div>
                    </div>
                    <div class="filter-group">
                        <div class="filter-label">Key Quote</div>
                        <input type="text" class="filter-input" data-column="4" placeholder="Search quotes..." onkeyup="filterTable()">
                    </div>
                    <div class="filter-group">
                        <div class="filter-label">Text Content</div>
                        <input type="text" class="filter-input" data-column="5" placeholder="Search text..." onkeyup="filterTable()">
                    </div>
                </div>
                <div style="margin-top: 15px;">
                    <button class="clear-filters" onclick="clearAllFilters()">Clear All Filters</button>
                </div>
            </div>
                
                <div style="overflow-x: auto;">
                    <table id="commentsTable">
                        <thead>
                            <tr>
                                <th>Comment ID</th>
                                <th>Date</th>
                                <th>Stance</th>
                                <th>Themes</th>
                                <th>Key Quote</th>
                                <th>Text Preview</th>
                                <th>📎</th>
                            </tr>
                        </thead>
                        <tbody>
"""

    # Add table rows
    for comment in comments:
        analysis = comment.get('analysis', {}) or {}
        stance = analysis.get('stance', 'Unknown')
        themes = analysis.get('themes', [])
        key_quote = analysis.get('key_quote', '')
        
        # Format date
        date_str = comment.get('date', '')
        formatted_date = ''
        if date_str:
            try:
                date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                formatted_date = date.strftime('%Y-%m-%d')
            except:
                formatted_date = date_str[:10] if len(date_str) >= 10 else date_str
        
        # Stance styling
        stance_class = f"stance-{stance.lower()}" if stance.lower() in ['for', 'against', 'neutral'] else "stance-unknown"
        
        # Themes display
        themes_html = '<div class="themes-container">' + ' '.join(f'<span class="theme-tag">{theme}</span>' for theme in themes) + '</div>'
        
        # Text preview
        text_preview = comment.get('comment_text', '')[:300]
        if len(comment.get('comment_text', '')) > 300:
            text_preview += '...'
        
        # Attachments
        has_attachments = '<span class="attachment-indicator">📎</span>' if comment.get('attachment_text', '').strip() else ''
        
        html_template += f"""
                            <tr>
                                <td><span class="comment-id">{comment.get('id', '')}</span></td>
                                <td class="date-cell">{formatted_date}</td>
                                <td><span class="{stance_class}">{stance}</span></td>
                                <td>{themes_html}</td>
                                <td>{key_quote[:200]}{'...' if len(key_quote) > 200 else ''}</td>
                                <td class="text-preview">{text_preview}</td>
                                <td>{has_attachments}</td>
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
            const stanceCheckboxes = document.querySelectorAll('input[data-filter="stance"]:checked');
            const themeCheckboxes = document.querySelectorAll('input[data-filter="themes"]:checked');
            const attachmentCheckboxes = document.querySelectorAll('input[data-filter="attachments"]:checked');
            
            const selectedStances = Array.from(stanceCheckboxes).map(cb => cb.value.toLowerCase());
            const selectedThemes = Array.from(themeCheckboxes).map(cb => cb.value.toLowerCase());
            const selectedAttachments = Array.from(attachmentCheckboxes).map(cb => cb.value);

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
                
                // Check stance filter (column 2)
                if (showRow && selectedStances.length > 0) {{
                    const stanceText = cells[2].textContent.toLowerCase();
                    if (!selectedStances.some(stance => stanceText.includes(stance))) {{
                        showRow = false;
                    }}
                }}
                
                // Check themes filter (column 3)
                if (showRow && selectedThemes.length > 0) {{
                    const themesText = cells[3].textContent.toLowerCase();
                    if (!selectedThemes.some(theme => themesText.includes(theme.toLowerCase()))) {{
                        showRow = false;
                    }}
                }}
                
                // Check attachments filter (column 6)
                if (showRow && selectedAttachments.length > 0) {{
                    const attachmentCell = cells[6];
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

                row.style.display = showRow ? '' : 'none';
            }}
        }}
        
        function clearAllFilters() {{
            const textFilters = document.querySelectorAll('.filter-input');
            const checkboxes = document.querySelectorAll('input[type="checkbox"]');
            
            textFilters.forEach(filter => filter.value = '');
            checkboxes.forEach(checkbox => checkbox.checked = false);
            
            filterTable();
        }}
    </script>
</body>
</html>
"""

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_template)

def main():
    parser = argparse.ArgumentParser(description='Generate HTML report from comment analysis results')
    parser.add_argument('--json', type=str, default='analyzed_comments.json', help='Input JSON file')
    parser.add_argument('--output', type=str, default='report.html', help='Output HTML file')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.json):
        print(f"Error: JSON file '{args.json}' not found")
        return
    
    print(f"Loading results from {args.json}...")
    comments = load_results(args.json)
    
    print("Calculating statistics...")
    stats = calculate_stats(comments)
    
    print(f"Generating HTML report: {args.output}")
    generate_html(comments, stats, args.output)
    
    print(f"✅ Report generated: {args.output}")
    print(f"📊 {stats['total_comments']:,} comments analyzed")

if __name__ == "__main__":
    main()