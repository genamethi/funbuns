"""
Interactive data viewer using Altair for prime decomposition results.
"""

import altair as alt
import polars as pl
from pathlib import Path
from .utils import get_default_data_file


def load_data_for_viz(data_path=None):
    """
    Load data from parquet file for visualization.
    
    Args:
        data_path: Path to data file (if None, uses default)
        
    Returns:
        Polars DataFrame
    """
    try:
        if data_path is None:
            filepath = get_default_data_file()
        else:
            filepath = Path(data_path)
        
        df = pl.read_parquet(filepath)
        return df
    except Exception as e:
        raise FileNotFoundError(f"No data found. Please run analysis first. Error: {e}")


def create_summary_chart(df):
    """Create summary charts of partition statistics."""
    # Get unique partition counts per prime (keep in Polars)
    # Count non-zero partitions per prime (where m_k > 0)
    partition_counts_pl = (
        df.group_by('p')
        .agg(pl.col('m_k').filter(pl.col('m_k') > 0).len().alias('partition_count'))
    )
    
    # Create frequency table for dashboard (in Polars)
    freq_table_pl = (
        partition_counts_pl
        .group_by('partition_count')
        .agg(pl.len().alias('prime_count'))
        .with_columns(
            (pl.col('prime_count') / partition_counts_pl.height * 100)
            .round(2)
            .alias('percentage')
        )
        .sort('partition_count')
    )
    
    # Bar chart showing frequency distribution
    count_chart = alt.Chart(freq_table_pl).mark_bar(color='steelblue').encode(
        x=alt.X('partition_count:O', title='Number of Partitions'),
        y=alt.Y('prime_count:Q', title='Number of Primes'),
        tooltip=['partition_count:O', 'prime_count:Q', 'percentage:Q']
    ).properties(
        title='Partition Count Distribution',
        width=400,
        height=300
    )
    
    # Improved scatter plot - sample larger primes to reduce density
    large_primes = partition_counts_pl.filter(pl.col('p') > 100)  # Focus on larger primes
    
    scatter_chart = alt.Chart(large_primes).mark_circle(size=50, opacity=0.7).encode(
        x=alt.X('p:Q', title='Prime (p > 100)', scale=alt.Scale(type='log')),
        y=alt.Y('partition_count:Q', title='Number of Partitions'),
        color=alt.Color('partition_count:Q', scale=alt.Scale(scheme='viridis'), 
                       legend=alt.Legend(title="Partition Count")),
        tooltip=['p:Q', 'partition_count:Q']
    ).properties(
        title='Larger Primes vs Partition Count',
        width=500,
        height=350
    )
    
    # Simple bar chart for frequency instead of text table
    freq_chart = alt.Chart(freq_table_pl).mark_bar(color='orange').encode(
        x=alt.X('partition_count:O', title='Partition Count'),
        y=alt.Y('prime_count:Q', title='Number of Primes'),
        tooltip=[
            alt.Tooltip('partition_count:O', title='Partitions'),
            alt.Tooltip('prime_count:Q', title='Primes'), 
            alt.Tooltip('percentage:Q', title='Percentage', format='.1f')
        ]
    ).properties(
        title='Frequency by Partition Count',
        width=300,
        height=200
    )
    
    return count_chart, scatter_chart, freq_chart


def create_partition_pattern_chart(df):
    """Create charts showing partition patterns."""
    # Filter out empty partitions for pattern analysis (keep in Polars)
    valid_partitions_pl = df.filter(pl.col('m_k') > 0)
    
    if valid_partitions_pl.height == 0:
        return alt.Chart().mark_text(text="No partitions found")
    
        # m vs n scatter plot
    mn_chart = alt.Chart(valid_partitions_pl).mark_circle(size=100, opacity=0.7).encode(
        x=alt.X('m:O', title='m (power of 2)'),
        y=alt.Y('n:O', title='n (power of q)'),
        color=alt.Color('p:Q', scale=alt.Scale(scheme='plasma')),
        size=alt.Size('q:Q', scale=alt.Scale(range=[50, 400])),
        tooltip=['p:Q', 'm:O', 'n:O', 'q:Q']
    ).properties(
        title='Partition Patterns: m vs n (size = q, color = p)',
        width=500,
        height=400
    )
    
    # Distribution of m values
    m_dist = alt.Chart(valid_partitions_pl).mark_bar().encode(
        x=alt.X('m:O', title='m (power of 2)'),
        y=alt.Y('count():Q', title='Frequency'),
        tooltip=['m:O', 'count():Q']
    ).properties(
        title='Distribution of m values',
        width=300,
        height=200
    )
    
    # Distribution of n values  
    n_dist = alt.Chart(valid_partitions_pl).mark_bar().encode(
        x=alt.X('n:O', title='n (power of q)'),
        y=alt.Y('count():Q', title='Frequency'),
        tooltip=['n:O', 'count():Q']
    ).properties(
        title='Distribution of n values',
        width=300,
        height=200
    )
    
    return mn_chart, m_dist, n_dist


def create_interactive_data_explorer(df):
    """Create comprehensive interactive data explorer."""
    
    # Filter out rows with no partitions for the main explorer
    partitions_df = df.filter(pl.col('m_k') > 0)
    
    # Selection for filtering by q values
    q_selector = alt.selection_point(fields=['q_k'])
    
    # Selection for filtering by n values  
    n_selector = alt.selection_point(fields=['n_k'])
    
    # Selection for filtering by m values
    m_selector = alt.selection_point(fields=['m_k'])
    
    # Simple scrollable data table
    data_table = alt.Chart(partitions_df.head(200)).mark_circle(size=30).encode(
        x=alt.X('p:Q', title='Prime p', scale=alt.Scale(type='log')),
        y=alt.Y('q_k:Q', title='Prime base q', scale=alt.Scale(type='log')),
        color=alt.Color('m_k:O', title='m (power of 2)'),
        size=alt.Size('n_k:O', title='n (power of q)', scale=alt.Scale(range=[50, 300])),
        tooltip=['p:Q', 'm_k:Q', 'n_k:Q', 'q_k:Q']
    ).properties(
        title='Partition Data Explorer (p vs q, color=m, size=n)',
        width=500,
        height=400
    )
    
    # Get summary statistics
    max_n = partitions_df.select(pl.col('n_k').max()).item()
    min_n = partitions_df.select(pl.col('n_k').min()).item()
    unique_q = partitions_df.select(pl.col('q_k').n_unique()).item()
    
    # N value distribution (to check if n > 1 exists)
    n_dist_detailed = alt.Chart(partitions_df).mark_bar().encode(
        x=alt.X('n_k:O', title='n values'),
        y=alt.Y('count():Q', title='Frequency'),
        color=alt.condition(n_selector, alt.value('red'), alt.value('steelblue')),
        tooltip=['n_k:O', 'count():Q']
    ).add_params(
        n_selector
    ).properties(
        title='Distribution of n values (click to filter table)',
        width=300,
        height=200
    )
    
    # Q value distribution (for p-adic analysis)
    q_dist = alt.Chart(partitions_df).mark_bar().encode(
        x=alt.X('q_k:O', title='q values (prime bases)', scale=alt.Scale(type='log')),
        y=alt.Y('count():Q', title='Frequency'),
        color=alt.condition(q_selector, alt.value('blue'), alt.value('orange')),
        tooltip=['q_k:O', 'count():Q']
    ).add_params(
        q_selector
    ).properties(
        title='Distribution of q values (click to filter table)',
        width=400,
        height=200
    )
    
    # M value distribution
    m_dist = alt.Chart(partitions_df).mark_bar().encode(
        x=alt.X('m_k:O', title='m values (powers of 2)'),
        y=alt.Y('count():Q', title='Frequency'),
        color=alt.condition(m_selector, alt.value('green'), alt.value('purple')),
        tooltip=['m_k:O', 'count():Q']
    ).add_params(
        m_selector
    ).properties(
        title='Distribution of m values (click to filter table)',
        width=300,
        height=200
    )
    
    # Create summary text showing key statistics  
    n_gt_1_count = partitions_df.filter(pl.col('n_k') > 1).height
    
    summary_text = alt.Chart(alt.Data(values=[{
        'text': f'Data Summary:\n• Max n value: {max_n}\n• n > 1 cases: {n_gt_1_count}\n• Total partitions: {partitions_df.height}\n• Unique q primes: {unique_q}'
    }])).mark_text(
        align='left',
        fontSize=12,
        dx=5,
        dy=-5
    ).encode(
        text='text:N'
    ).properties(
        title='Key Statistics',
        width=200,
        height=150
    )
    
    return data_table, n_dist_detailed, q_dist, m_dist, summary_text


def create_raw_data_table(df, max_primes=200):
    """Create a readable HTML table grouped by prime."""
    partitions_df = df.filter(pl.col('m_k') > 0)
    
    # Group by prime and collect all partitions
    prime_groups = {}
    for row in partitions_df.iter_rows(named=True):
        p = row['p']
        if p not in prime_groups:
            prime_groups[p] = {
                'count': 0,  # Will count partitions for this prime
                'partitions': []
            }
        prime_groups[p]['count'] += 1
        prime_groups[p]['partitions'].append({
            'm': row['m_k'],
            'n': row['n_k'], 
            'q': row['q_k']
        })
    
    # Limit to max_primes and sort by prime value
    sorted_primes = sorted(list(prime_groups.keys()))[:max_primes]
    
    # Convert to HTML table
    html_table = f"""
    <html>
    <head>
        <title>Prime Power Partition Data (Grouped by Prime)</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; font-weight: bold; }}
            tr:nth-child(even) {{ background-color: #f9f9f9; }}
            .prime-cell {{ font-weight: bold; background-color: #e8f4f8; }}
            .partition-list {{ font-family: monospace; }}
            .summary {{ background-color: #e6f3ff; padding: 15px; margin-bottom: 20px; border-radius: 5px; }}
        </style>
    </head>
    <body>
        <h1>Prime Power Partition Data (Grouped by Prime)</h1>
        <div class="summary">
            <h3>Summary Statistics</h3>
            <p><strong>Primes with partitions:</strong> {len(sorted_primes)}</p>
            <p><strong>Max n value:</strong> {partitions_df.select(pl.col('n_k').max()).item()}</p>
            <p><strong>n > 1 cases:</strong> {partitions_df.filter(pl.col('n_k') > 1).height}</p>
            <p><strong>Unique q primes:</strong> {partitions_df.select(pl.col('q_k').n_unique()).item()}</p>
        </div>
        <table>
            <thead>
                <tr>
                    <th>Prime (p)</th>
                    <th>Partition Count</th>
                    <th>All Partitions (m,n,q)</th>
                    <th>Equations</th>
                </tr>
            </thead>
            <tbody>
    """
    
    for p in sorted_primes:
        group = prime_groups[p]
        
        # Format partition list
        partition_list = []
        equation_list = []
        for part in group['partitions']:
            partition_list.append(f"({part['m']},{part['n']},{part['q']})")
            equation_list.append(f"{p} = 2^{part['m']} + {part['q']}^{part['n']}")
        
        partitions_str = "<br>".join(partition_list)
        equations_str = "<br>".join(equation_list)
        
        html_table += f"""
                <tr>
                    <td class="prime-cell">{p}</td>
                    <td>{group['count']}</td>
                    <td class="partition-list">{partitions_str}</td>
                    <td class="partition-list">{equations_str}</td>
                </tr>
        """
    
    html_table += """
            </tbody>
        </table>
    </body>
    </html>
    """
    
    return html_table


def generate_summary_page(df):
    """Generate overview summary page."""
    count_chart, scatter_chart, freq_chart = create_summary_chart(df)
    
    # Add key statistics
    total_primes = df["p"].n_unique()
    largest_prime = df["p"].max()
    total_partitions = df.filter(pl.col('m_k') > 0).height
    
    # Layout: two bar charts on top, scatter plot below
    summary_page = alt.vconcat(
        alt.hconcat(count_chart, freq_chart).resolve_scale(color='independent'),
        scatter_chart
    ).properties(
        title=alt.TitleParams(
            text=[f'Summary: {total_primes} primes, largest: {largest_prime}', 
                  f'Total partitions: {total_partitions}'],
            fontSize=14
        )
    )
    
    return summary_page


def generate_pattern_page(df):
    """Generate pattern analysis page."""
    mn_chart, m_dist, n_dist = create_partition_pattern_chart(df)
    
    pattern_page = alt.hconcat(
        mn_chart,
        alt.vconcat(m_dist, n_dist)
    ).properties(
        title='Partition Patterns Analysis'
    )
    
    return pattern_page


def generate_distribution_page(df):
    """Generate clean distribution analysis."""
    partitions_df = df.filter(pl.col('m_k') > 0)
    
    # Clean N distribution
    n_chart = alt.Chart(partitions_df).mark_bar(color='steelblue').encode(
        x=alt.X('n_k:O', title='n values'),
        y=alt.Y('count():Q', title='Count'),
        tooltip=['n_k:O', 'count():Q']
    ).properties(
        title='Distribution of n values',
        width=300,
        height=250
    )
    
    # Clean Q distribution with limited data points
    q_counts = partitions_df.group_by('q_k').agg(pl.len().alias('count')).sort('count', descending=True).head(20)
    
    q_chart = alt.Chart(q_counts).mark_bar(color='orange').encode(
        x=alt.X('q_k:O', title='q values (top 20)', sort='-y'),
        y=alt.Y('count:Q', title='Count'),
        tooltip=['q_k:O', 'count:Q']
    ).properties(
        title='Top 20 most frequent q values',
        width=400,
        height=250
    )
    
    # M distribution  
    m_chart = alt.Chart(partitions_df).mark_bar(color='green').encode(
        x=alt.X('m_k:O', title='m values'),
        y=alt.Y('count():Q', title='Count'),
        tooltip=['m_k:O', 'count():Q']
    ).properties(
        title='Distribution of m values',
        width=300,
        height=250
    )
    
    # Summary stats
    max_n = partitions_df.select(pl.col('n_k').max()).item()
    n_gt_1 = partitions_df.filter(pl.col('n_k') > 1).height
    
    stats_chart = alt.Chart(alt.Data(values=[{
        'text': f'Key Statistics:\\n• Max n: {max_n}\\n• n > 1: {n_gt_1}\\n• Total: {partitions_df.height}'
    }])).mark_text(
        align='left', fontSize=14, dx=10, dy=10
    ).encode(text='text:N').properties(
        title='Statistics', width=200, height=250
    )
    
    dist_page = alt.vconcat(
        alt.hconcat(n_chart, stats_chart),
        alt.hconcat(q_chart, m_chart)
    ).properties(title='Distribution Analysis')
    
    return dist_page


def generate_dashboard(data_path=None, output_path=None):
    """
    Generate multi-page dashboard with separate HTML files.
    
    Args:
        data_path: Path to data file (pickle)
        output_path: Base path for output files
    """
    # Load data
    df = load_data_for_viz(data_path)
    
    data_dir = Path('data')
    data_dir.mkdir(exist_ok=True)
    
    # Generate individual pages
    summary_page = generate_summary_page(df)
    pattern_page = generate_pattern_page(df)
    dist_page = generate_distribution_page(df)
    
    # Save individual pages
    summary_page.save(str(data_dir / 'summary.html'))
    pattern_page.save(str(data_dir / 'patterns.html'))
    dist_page.save(str(data_dir / 'distributions.html'))
    
    # Generate raw data table
    raw_table_html = create_raw_data_table(df)
    with open(data_dir / 'raw_data.html', 'w') as f:
        f.write(raw_table_html)
    
    # Create index page with links
    index_html = f"""
    <html>
    <head>
        <title>Prime Power Partition Analysis</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; background-color: #f5f5f5; }}
            .container {{ max-width: 800px; margin: 0 auto; background: white; padding: 30px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
            h1 {{ color: #333; text-align: center; }}
            .nav-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin: 30px 0; }}
            .nav-item {{ background: #e8f4f8; padding: 20px; border-radius: 8px; text-align: center; border: 2px solid #d1e7dd; }}
            .nav-item:hover {{ background: #d4e6f1; }}
            .nav-item a {{ text-decoration: none; color: #2c3e50; font-weight: bold; font-size: 16px; }}
            .stats {{ background: #fff3cd; padding: 15px; border-radius: 5px; margin: 20px 0; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Prime Power Partition Analysis Dashboard</h1>
            
            <div class="stats">
                <h3>Dataset Summary</h3>
                <p><strong>Primes analyzed:</strong> {df["p"].n_unique()}</p>
                <p><strong>Largest prime:</strong> {df["p"].max()}</p>
                <p><strong>Total partitions:</strong> {df.filter(pl.col('m_k') > 0).height}</p>
                <p><strong>n > 1 cases:</strong> {df.filter(pl.col('m_k') > 0).filter(pl.col('n_k') > 1).height}</p>
            </div>
            
            <div class="nav-grid">
                <div class="nav-item">
                    <a href="summary.html">[SUMMARY] Overview</a>
                    <p>Partition count distributions and frequency tables</p>
                </div>
                
                <div class="nav-item">
                    <a href="patterns.html">[PATTERNS] Analysis</a>
                    <p>m vs n relationships and pattern distributions</p>
                </div>
                
                <div class="nav-item">
                    <a href="distributions.html">[DISTRIBUTIONS] Value Analysis</a>
                    <p>Clean charts of m, n, and q value frequencies</p>
                </div>
                
                <div class="nav-item">
                    <a href="raw_data.html">[DATA] Raw Table</a>
                    <p>Scrollable table with all partition data</p>
                </div>
            </div>
            
            <p style="text-align: center; color: #666; margin-top: 30px;">
                <em>Each page contains focused, fast-loading visualizations</em>
            </p>
        </div>
    </body>
    </html>
    """
    
    with open(data_dir / 'index.html', 'w') as f:
        f.write(index_html)
    
    print(f"Multi-page dashboard generated:")
    print(f"  [INDEX] {data_dir / 'index.html'}")
    print(f"  [SUMMARY] {data_dir / 'summary.html'}")
    print(f"  [PATTERNS] {data_dir / 'patterns.html'}")
    print(f"  [DISTRIBUTIONS] {data_dir / 'distributions.html'}")
    print(f"  [DATA] {data_dir / 'raw_data.html'}")


if __name__ == "__main__":
    # Generate dashboard from default data
    generate_dashboard()
