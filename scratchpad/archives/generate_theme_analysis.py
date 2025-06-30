#!/usr/bin/env python3
"""
Generate Theme-Based Analysis Report

A simple wrapper script that processes stance analysis data and generates
a theme-organized HTML report. This script automatically handles both
raw analyzed comments and processed stance data.
"""

import argparse
import json
import os
from datetime import datetime
from generate_theme_report import main as generate_theme_main, group_stances_by_theme, analyze_field_types, calculate_stats, generate_html

def process_and_generate_theme_report(input_file: str, output_html: str = None):
    """Process stance data and generate theme-organized report."""
    
    if not output_html:
        # Generate output filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        base_name = os.path.splitext(os.path.basename(input_file))[0]
        output_html = f"theme_report_{base_name}_{timestamp}.html"
    
    print(f"📊 Processing stance data from: {input_file}")
    print(f"🎯 Output will be saved to: {output_html}")
    print()
    
    # Load the data
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # Handle both processed data and raw comments
    if isinstance(data, dict) and 'comments' in data:
        comments = data['comments']
        print(f"✅ Loaded processed data with {len(comments)} comments")
    else:
        comments = data
        print(f"✅ Loaded raw comments: {len(comments)} entries")
    
    # Group stances by theme
    print("🔍 Analyzing themes and positions...")
    theme_data = group_stances_by_theme(comments)
    
    # Display theme summary
    print(f"📋 Found {len(theme_data)} unique themes:")
    sorted_themes = sorted(theme_data.items(), key=lambda x: x[1]['total_occurrences'], reverse=True)
    for theme_name, theme_info in sorted_themes:
        print(f"   • {theme_name}: {theme_info['total_occurrences']} comments, {len(theme_info['positions'])} positions")
    print()
    
    # Analyze fields and calculate stats
    print("📈 Calculating statistics...")
    field_analysis = analyze_field_types(comments)
    stats = calculate_stats(comments, field_analysis, theme_data)
    
    # Generate HTML report
    print("🎨 Generating HTML report...")
    generate_html(comments, stats, field_analysis, output_html)
    
    print(f"✅ Theme-based report generated successfully!")
    print(f"📁 Report saved to: {output_html}")
    print()
    print("📊 Summary:")
    print(f"   • Total comments: {stats['total_comments']:,}")
    print(f"   • Unique themes: {stats['num_themes']:,}")
    print(f"   • Comments with attachments: {stats['with_attachments']:,}")
    print(f"   • Date range: {stats['date_range']}")
    
    return output_html

def main():
    parser = argparse.ArgumentParser(
        description='Generate theme-based analysis report from stance data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  # Generate report from processed stance data
  python generate_theme_analysis.py processed_stance_data.json
  
  # Specify custom output filename
  python generate_theme_analysis.py data.json --output my_theme_report.html
  
  # Generate report with timestamp
  python generate_theme_analysis.py data.json --timestamp
'''
    )
    
    parser.add_argument('input_file', type=str, 
                       help='Input JSON file (processed stance data or raw analyzed comments)')
    parser.add_argument('--output', '-o', type=str, 
                       help='Output HTML filename (default: auto-generated with timestamp)')
    parser.add_argument('--timestamp', action='store_true',
                       help='Add timestamp to output filename')
    
    args = parser.parse_args()
    
    # Check if input file exists
    if not os.path.exists(args.input_file):
        print(f"❌ Error: Input file '{args.input_file}' not found")
        return 1
    
    # Determine output filename
    output_html = args.output
    if args.timestamp or not output_html:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        base_name = os.path.splitext(os.path.basename(args.input_file))[0]
        if args.output:
            # Add timestamp to specified filename
            name, ext = os.path.splitext(args.output)
            output_html = f"{name}_{timestamp}{ext}"
        else:
            # Auto-generate filename
            output_html = f"theme_report_{base_name}_{timestamp}.html"
    
    try:
        process_and_generate_theme_report(args.input_file, output_html)
        return 0
    except Exception as e:
        print(f"❌ Error generating report: {e}")
        return 1

if __name__ == "__main__":
    exit(main())