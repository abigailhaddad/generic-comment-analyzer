"""
Custom report generator for scratchpad with new_stances support.
Modified version of the main generate_html function.
"""

def generate_html_with_new_stances(comments, stats, field_analysis, output_file):
    """Generate HTML report with support for new_stances field."""
    
    # Generate field distribution sections
    def generate_field_distribution_html(field_name, field_info, stats):
        if field_name not in stats:
            return ""
        
        field_stats = stats[field_name]
        
        # Custom titles
        display_names = {
            'stances': 'Stances Distribution',
            'new_stances': 'New Stances Distribution'
        }
        
        title = display_names.get(field_name, field_name.replace('_', ' ').title())
        
        if field_info.get('type') == 'checkbox':
            # Generate distribution cards for stances/new_stances
            sorted_items = sorted(field_stats.items(), key=lambda x: x[1], reverse=True)
            
            cards_html = ""
            for i, (item, count) in enumerate(sorted_items):
                percentage = (count / len(comments)) * 100 if comments else 0
                color_class = f"stance-card-{i % 8}"  # Cycle through 8 colors
                
                cards_html += f'''
                <div class="stance-card {color_class}">
                    <div class="stance-header">
                        <span class="stance-name">{item}</span>
                        <span class="stance-count">{count}</span>
                    </div>
                    <div class="stance-percentage">{percentage:.1f}%</div>
                </div>'''
            
            return f'''
            <div class="section">
                <h2>{title}</h2>
                <div class="stance-grid">
                    {cards_html}
                </div>
            </div>'''
        
        return ""
    
    # Generate filter checkboxes for a field
    def generate_filter_checkboxes(field_name, field_info):
        if field_name not in field_analysis:
            return ""
        
        unique_values = field_info.get('unique_values', [])
        return "".join(
            f'<label class="filter-checkbox">'
            f'<input type="checkbox" data-filter="{field_name}" value="{value}" onchange="filterTable()"> {value}'
            f'</label>' 
            for value in unique_values
        )
    
    # Build the complete HTML
    html_content = f'''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Stance Test Report - Comment Analysis</title>
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
        
        .stance-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 15px;
            margin-top: 20px;
        }}
        
        .stance-card {{
            background: #f8f9fa;
            border-radius: 8px;
            padding: 15px;
            border-left: 4px solid #007bff;
        }}
        
        .stance-card-0 {{ border-left-color: #007bff; }}
        .stance-card-1 {{ border-left-color: #28a745; }}
        .stance-card-2 {{ border-left-color: #dc3545; }}
        .stance-card-3 {{ border-left-color: #ffc107; }}
        .stance-card-4 {{ border-left-color: #6f42c1; }}
        .stance-card-5 {{ border-left-color: #fd7e14; }}
        .stance-card-6 {{ border-left-color: #20c997; }}
        .stance-card-7 {{ border-left-color: #e83e8c; }}
        
        .stance-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 8px;
        }}
        
        .stance-name {{
            font-weight: 600;
            color: #333;
            flex: 1;
        }}
        
        .stance-count {{
            background: #007bff;
            color: white;
            padding: 2px 8px;
            border-radius: 12px;
            font-size: 0.85em;
            font-weight: 600;
        }}
        
        .stance-percentage {{
            color: #666;
            font-size: 0.9em;
        }}
        
        .filters-container {{
            background: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            border: 1px solid #dee2e6;
        }}
        
        .filter-checkbox {{
            display: inline-block;
            margin: 5px 10px 5px 0;
            padding: 5px 10px;
            background: white;
            border: 1px solid #ddd;
            border-radius: 4px;
            cursor: pointer;
            font-size: 0.9em;
        }}
        
        .filter-checkbox input {{
            margin-right: 5px;
        }}
        
        .table-container {{
            overflow-x: auto;
            background: white;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            min-width: 1000px;
        }}
        
        th {{
            background: #343a40;
            color: white;
            padding: 12px;
            text-align: left;
            font-weight: 600;
            position: sticky;
            top: 0;
            z-index: 10;
        }}
        
        td {{
            padding: 8px 12px;
            border-bottom: 1px solid #dee2e6;
            vertical-align: top;
        }}
        
        tr:nth-child(even) {{
            background: #f8f9fa;
        }}
        
        .stance-tag {{
            display: inline-block;
            background: #007bff;
            color: white;
            padding: 2px 6px;
            border-radius: 3px;
            font-size: 0.75em;
            margin: 1px 2px;
        }}
        
        .new-stance-tag {{
            display: inline-block;
            background: #28a745;
            color: white;
            padding: 2px 6px;
            border-radius: 3px;
            font-size: 0.75em;
            margin: 1px 2px;
        }}
        
        .stances-container {{
            max-width: 200px;
        }}
        
        .column-controls {{
            margin-bottom: 15px;
        }}
        
        .column-controls button {{
            margin-right: 10px;
            padding: 5px 10px;
            background: #007bff;
            color: white;
            border: none;
            border-radius: 4px;
            cursor: pointer;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Stance Test Report</h1>
            <div class="subtitle">Schedule F Comment Analysis with New Stance Detection</div>
        </div>
        
        <!-- Field Distributions -->
        {generate_field_distribution_html('stances', field_analysis.get('stances', {}), stats)}
        {generate_field_distribution_html('new_stances', field_analysis.get('new_stances', {}), stats) if 'new_stances' in field_analysis else ''}
        
        <!-- Comments Table -->
        <div class="section">
            <h2>Comments Analysis</h2>
            
            <div class="filters-container">
                <strong>Filter by Stances:</strong><br>
                {generate_filter_checkboxes('stances', field_analysis.get('stances', {}))}
                
                {'<br><br><strong>Filter by New Stances:</strong><br>' + generate_filter_checkboxes('new_stances', field_analysis.get('new_stances', {})) if 'new_stances' in field_analysis else ''}
            </div>
            
            <div class="column-controls">
                <button onclick="showAllColumns()">Show All Columns</button>
                <button onclick="hideOptionalColumns()">Hide Optional Columns</button>
            </div>
            
            <div class="table-container">
                <table id="commentsTable">
                    <thead>
                        <tr>
                            <th>ID</th>
                            <th>Comment Text</th>
                            <th>Stances</th>
                            {'<th>New Stances</th>' if 'new_stances' in field_analysis else ''}
                            <th>Key Quote</th>
                            <th>Rationale</th>
                        </tr>
                    </thead>
                    <tbody>'''
    
    # Generate table rows
    for comment in comments:
        analysis = comment.get('analysis', {})
        
        # Handle stances
        stances = analysis.get('stances', [])
        stances_html = '<div class="stances-container">' + ' '.join(f'<span class="stance-tag">{stance}</span>' for stance in stances) + '</div>' if stances else '<span style="color: #999;">None</span>'
        
        # Handle new stances
        new_stances = analysis.get('new_stances', [])
        new_stances_html = '<div class="stances-container">' + ' '.join(f'<span class="new-stance-tag">{stance}</span>' for stance in new_stances) + '</div>' if new_stances else '<span style="color: #999;">None</span>'
        
        # Truncate comment text for display
        comment_text = comment.get('text', '')[:200]
        if len(comment.get('text', '')) > 200:
            comment_text += '...'
        
        key_quote = analysis.get('key_quote', '')[:150]
        if len(analysis.get('key_quote', '')) > 150:
            key_quote += '...'
            
        rationale = analysis.get('rationale', '')[:150]
        if len(analysis.get('rationale', '')) > 150:
            rationale += '...'
        
        new_stances_cell = f'<td>{new_stances_html}</td>' if 'new_stances' in field_analysis else ''
        
        html_content += f'''
                        <tr>
                            <td>{comment.get('id', '')}</td>
                            <td>{comment_text}</td>
                            <td>{stances_html}</td>
                            {new_stances_cell}
                            <td>{key_quote}</td>
                            <td>{rationale}</td>
                        </tr>'''
    
    # Close HTML and add JavaScript
    html_content += f'''
                    </tbody>
                </table>
            </div>
        </div>
    </div>
    
    <script>
        function filterTable() {{
            const table = document.getElementById('commentsTable');
            const rows = table.getElementsByTagName('tr');
            
            // Get checked filters
            const stanceCheckboxes = document.querySelectorAll('input[data-filter="stances"]:checked');
            const selectedStances = Array.from(stanceCheckboxes).map(cb => cb.value.toLowerCase());
            
            const newStanceCheckboxes = document.querySelectorAll('input[data-filter="new_stances"]:checked');
            const selectedNewStances = Array.from(newStanceCheckboxes).map(cb => cb.value.toLowerCase());
            
            // Filter rows (skip header)
            for (let i = 1; i < rows.length; i++) {{
                const row = rows[i];
                const cells = row.getElementsByTagName('td');
                
                let showRow = true;
                
                // Check stance filters
                if (selectedStances.length > 0) {{
                    const stancesText = cells[2] ? cells[2].textContent.toLowerCase() : '';
                    const hasMatchingStance = selectedStances.some(stance => stancesText.includes(stance));
                    if (!hasMatchingStance) showRow = false;
                }}
                
                // Check new stance filters
                if (selectedNewStances.length > 0 && cells.length > 3) {{
                    const newStancesText = cells[3] ? cells[3].textContent.toLowerCase() : '';
                    const hasMatchingNewStance = selectedNewStances.some(stance => newStancesText.includes(stance));
                    if (!hasMatchingNewStance) showRow = false;
                }}
                
                row.style.display = showRow ? '' : 'none';
            }}
        }}
        
        function showAllColumns() {{
            const table = document.getElementById('commentsTable');
            const cells = table.querySelectorAll('th, td');
            cells.forEach(cell => cell.style.display = '');
        }}
        
        function hideOptionalColumns() {{
            // Keep only essential columns visible
            showAllColumns(); // Start fresh
        }}
    </script>
</body>
</html>'''

    # Write the file
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"✅ Custom report with new_stances support generated: {output_file}")