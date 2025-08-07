"""
Interactive data viewer using Altair for prime decomposition results.
"""

import altair as alt
import polars as pl
from pathlib import Path
from .utils import load_results, results_to_polars_df, get_default_data_file


def load_data_for_viz(data_path=None):
    """
    Load data and convert to Polars DataFrame for visualization.
    
    Args:
        data_path: Path to data file (if None, uses default)
        
    Returns:
        Polars DataFrame
    """
    try:
        results, metadata = load_results(data_path)
        df = results_to_polars_df(results)
        return df
    except Exception as e:
        raise FileNotFoundError(f"No data found. Please run analysis first. Error: {e}")


def create_summary_chart(df):
    """Create summary charts of partition statistics."""
    # Get unique partition counts per prime (keep in Polars)
    partition_counts_pl = df.group_by('p').agg(pl.col('decomp_count').first())
    
    # Create frequency table for dashboard (in Polars)
    freq_table_pl = (
        partition_counts_pl
        .group_by('decomp_count')
        .agg(pl.len().alias('prime_count'))
        .with_columns(
            (pl.col('prime_count') / partition_counts_pl.height * 100)
            .round(2)
            .alias('percentage')
        )
        .sort('decomp_count')
    )
    
    # Bar chart showing frequency distribution
    count_chart = alt.Chart(freq_table_pl).mark_bar().encode(
        x=alt.X('decomp_count:O', title='Number of Partitions'),
        y=alt.Y('prime_count:Q', title='Number of Primes'),
        color=alt.Color('decomp_count:Q', scale=alt.Scale(scheme='category10')),
        tooltip=['decomp_count:O', 'prime_count:Q', 'percentage:Q']
    ).properties(
        title='Partition Count Frequency Distribution',
        width=400,
        height=300
    )
    
    # Scatter plot of primes vs partition count
    scatter_chart = alt.Chart(partition_counts_pl).mark_circle(size=60).encode(
        x=alt.X('p:Q', title='Prime'),
        y=alt.Y('decomp_count:Q', title='Number of Partitions'),
        color=alt.Color('decomp_count:Q', scale=alt.Scale(scheme='viridis')),
        tooltip=['p:Q', 'decomp_count:Q']
    ).properties(
        title='Prime vs Partition Count',
        width=600,
        height=400
    )
    
    # Frequency table as text chart
    freq_text = alt.Chart(freq_table_pl).mark_text(align='left', fontSize=12).encode(
        y=alt.Y('row_number():O', title=''),
        text=alt.Text('table_text:N')
    ).transform_calculate(
        table_text='datum.decomp_count + " partitions: " + datum.prime_count + " primes (" + datum.percentage + "%)"'
    ).properties(
        title='Frequency Table',
        width=300,
        height=200
    ).resolve_scale(y='independent')
    
    return count_chart, scatter_chart, freq_text


def create_partition_pattern_chart(df):
    """Create charts showing partition patterns."""
    # Filter out empty partitions for pattern analysis (keep in Polars)
    valid_partitions_pl = df.filter(pl.col('decomp_count') > 0)
    
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


def create_data_table(df, max_rows=100):
    """Create an interactive data table."""
    # Show first max_rows entries
    display_df = df.head(max_rows)
    
    # Create selection for highlighting
    click = alt.selection_multi(fields=['p'])
    
    table = alt.Chart(display_df).mark_rect().add_selection(
        click
    ).encode(
        x=alt.X('p:O', title='Prime'),
        y=alt.Y('row_number():O', title='Row'),
        color=alt.condition(click, alt.value('lightblue'), alt.value('white')),
        stroke=alt.value('black'),
        tooltip=['p:Q', 'decomp_count:Q', 'm:Q', 'n:Q', 'q:Q']
    ).properties(
        title=f'Data Table (first {max_rows} rows)',
        width=600,
        height=400
    )
    
    return table


def generate_dashboard(data_path=None, output_path=None):
    """
    Generate complete dashboard with all visualizations.
    
    Args:
        data_path: Path to data file (pickle)
        output_path: Path to save HTML dashboard
    """
    # Load data
    df = load_data_for_viz(data_path)
    
    # Create charts
    count_chart, scatter_chart, freq_text = create_summary_chart(df)
    mn_chart, m_dist, n_dist = create_partition_pattern_chart(df)
    table = create_data_table(df)
    
    # Get summary statistics for title
    total_primes = df["p"].n_unique()
    largest_prime = df["p"].max()
    total_partitions = df.filter(pl.col('decomp_count') > 0).height
    
    # Combine charts
    summary_row = alt.hconcat(count_chart, freq_text, scatter_chart)
    pattern_row = alt.hconcat(mn_chart, alt.vconcat(m_dist, n_dist))
    dashboard = alt.vconcat(
        summary_row,
        pattern_row, 
        table
    ).resolve_scale(
        color='independent'
    ).properties(
        title=alt.TitleParams(
            text=['Prime Power Partition Analysis Dashboard', 
                  f'Analyzed: {total_primes} primes (largest: {largest_prime}) | Total partitions: {total_partitions}'],
            fontSize=16,
            anchor='start'
        )
    )
    
    # Save dashboard
    if output_path is None:
        output_path = Path('data') / 'dashboard.html'
    
    dashboard.save(str(output_path))
    print(f"Dashboard saved to {output_path}")
    
    return dashboard


if __name__ == "__main__":
    # Generate dashboard from default data
    generate_dashboard()
