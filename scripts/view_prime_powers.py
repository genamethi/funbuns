#!/usr/bin/env python3
"""
Create a clean Altair table view of the prime powers data.
"""

import polars as pl
import altair as alt
from pathlib import Path

def create_prime_powers_table():
    """Create a clean HTML table view of prime powers data using Altair."""
    
    data_dir = Path('data')
    
    # Find the prime powers file - prefer bounded versions
    bounded_files = list(data_dir.glob('prime_powers_*_bounded_*.parquet'))
    other_files = list(data_dir.glob('prime_powers_*.parquet'))
    
    if bounded_files:
        # Use the most recent bounded file
        file_path = max(bounded_files, key=lambda f: f.stat().st_mtime)
        print("Using bounded prime powers file (no overflow issues)")
    elif other_files:
        # Fall back to other files
        file_path = max(other_files, key=lambda f: f.stat().st_size)
        print("Using unbounded file (may have overflow issues)")
    else:
        print("No prime powers files found")
        return
    print(f"Loading: {file_path}")
    
    # Load data with lazy evaluation
    df = pl.scan_parquet(file_path).head(100).collect()  # First 100 rows for display
    
    print(f"Loaded {len(df)} primes with {len(df.columns)} power columns")
    
    # Create simple HTML table for browsing
    # Show first 100 rows and first 15 columns for reasonable viewing
    table_df = df.head(100).select([str(i) for i in range(1, 16) if str(i) in df.columns])
    
    # Generate HTML table
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Prime Powers Data Table</title>
    <style>
        body {{
            font-family: 'Courier New', monospace;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 95%;
            margin: 0 auto;
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #333;
            text-align: center;
            margin-bottom: 20px;
        }}
        .info {{
            background-color: #e3f2fd;
            padding: 15px;
            border-radius: 5px;
            margin-bottom: 20px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 11px;
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 4px 6px;
            text-align: right;
        }}
        th {{
            background-color: #4CAF50;
            color: white;
            position: sticky;
            top: 0;
            z-index: 10;
        }}
        tr:nth-child(even) {{
            background-color: #f2f2f2;
        }}
        tr:hover {{
            background-color: #ddd;
        }}
        .prime-col {{
            background-color: #e8f5e8;
            font-weight: bold;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Prime Powers Data Table</h1>
        
        <div class="info">
            <strong>File:</strong> {file_path.name}<br>
            <strong>Showing:</strong> First 100 primes, powers 1-15<br>
            <strong>Total primes in file:</strong> {len(df):,}
        </div>
        
        <table>
            <thead>
                <tr>
"""
    
    # Add column headers
    for col in table_df.columns:
        if col == '1':
            html_content += f'                    <th>Prime</th>\n'
        else:
            html_content += f'                    <th>Prime^{col}</th>\n'
    
    html_content += """                </tr>
            </thead>
            <tbody>
"""
    
    # Add data rows
    for row in table_df.iter_rows(named=True):
        html_content += "                <tr>\n"
        for i, (col, value) in enumerate(row.items()):
            css_class = 'prime-col' if col == '1' else ''
            formatted_value = f"{value:,}"
            html_content += f'                    <td class="{css_class}">{formatted_value}</td>\n'
        html_content += "                </tr>\n"
    
    html_content += """
            </tbody>
        </table>
        
        <div style="margin-top: 20px; text-align: center; color: #666;">
            <p>Scroll to browse the data. Prime column highlighted in green.</p>
        </div>
    </div>
</body>
</html>
"""
    
    # Save HTML file
    output_file = data_dir / 'prime_powers_view.html'
    with open(output_file, 'w') as f:
        f.write(html_content)
    
    print(f"✅ Saved to: {output_file}")
    print(f"🌐 Access at: http://localhost:8000/prime_powers_view.html")
    print(f"📊 Run 'pixi run serve' to start the server")

if __name__ == "__main__":
    create_prime_powers_table()
