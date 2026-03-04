"""
Interactive data viewer using Altair for prime decomposition results.

All aggregation happens server-side in Polars before data reaches Altair.
Charts receive pre-computed summary DataFrames (hundreds of rows, not millions).
"""

import altair as alt
import polars as pl
from pathlib import Path
from .utils import get_config

# Altair's default 5000-row limit is too low for scatter plots but we
# should never be sending raw data anyway.  Raise it slightly for safety.
alt.data_transformers.disable_max_rows()

MAX_SCATTER_POINTS = 5000


def load_data_for_viz(data_path=None) -> pl.LazyFrame:
    """Load block data lazily."""
    if data_path is None:
        config = get_config()
        blocks = Path(config.get('blocks_dir', 'data/blocks'))
        pattern = blocks / "pp_b*.parquet"
        return pl.scan_parquet(pattern)
    return pl.scan_parquet(data_path)


def _collect_stats(lf: pl.LazyFrame) -> dict:
    """Compute dataset-level stats in a single streaming pass."""
    stats = (
        lf.select([
            pl.col('p').n_unique().alias('total_primes'),
            pl.col('p').max().alias('largest_prime'),
            (pl.col('m_k') > 0).sum().alias('total_partitions'),
            ((pl.col('m_k') > 0) & (pl.col('n_k') > 1)).sum().alias('n_gt_1'),
            pl.col('n_k').filter(pl.col('m_k') > 0).max().alias('max_n'),
            pl.col('q_k').filter(pl.col('m_k') > 0).n_unique().alias('unique_q'),
        ])
        .collect(streaming=True)
    )
    return stats.row(0, named=True)


# ---------------------------------------------------------------------------
# Summary page
# ---------------------------------------------------------------------------

def _partition_count_freq(lf: pl.LazyFrame) -> pl.DataFrame:
    """Partition-count frequency table (small: one row per distinct count)."""
    return (
        lf.group_by('p')
        .agg((pl.col('m_k') > 0).sum().alias('partition_count'))
        .group_by('partition_count')
        .agg(pl.len().alias('prime_count'))
        .sort('partition_count')
        .collect(streaming=True)
        .with_columns(
            (pl.col('prime_count') / pl.col('prime_count').sum() * 100)
            .round(2).alias('percentage')
        )
    )


def create_summary_chart(lf: pl.LazyFrame):
    freq = _partition_count_freq(lf)

    count_chart = (
        alt.Chart(freq).mark_bar(color='steelblue')
        .encode(
            x=alt.X('partition_count:O', title='Number of Partitions'),
            y=alt.Y('prime_count:Q', title='Number of Primes'),
            tooltip=['partition_count:O', 'prime_count:Q', 'percentage:Q'],
        )
        .properties(title='Partition Count Distribution', width=400, height=300)
    )

    freq_chart = (
        alt.Chart(freq).mark_bar(color='orange')
        .encode(
            x=alt.X('partition_count:O', title='Partition Count'),
            y=alt.Y('prime_count:Q', title='Number of Primes'),
            tooltip=[
                alt.Tooltip('partition_count:O', title='Partitions'),
                alt.Tooltip('prime_count:Q', title='Primes'),
                alt.Tooltip('percentage:Q', title='Percentage', format='.1f'),
            ],
        )
        .properties(title='Frequency by Partition Count', width=300, height=200)
    )

    # Scatter: sample partition counts per prime
    scatter_data = (
        lf.group_by('p')
        .agg((pl.col('m_k') > 0).sum().alias('partition_count'))
        .filter(pl.col('p') > 100)
        .collect(streaming=True)
    )
    if scatter_data.height > MAX_SCATTER_POINTS:
        scatter_data = scatter_data.sample(MAX_SCATTER_POINTS, seed=42)

    scatter_chart = (
        alt.Chart(scatter_data).mark_circle(size=50, opacity=0.7)
        .encode(
            x=alt.X('p:Q', title='Prime (p > 100)', scale=alt.Scale(type='log')),
            y=alt.Y('partition_count:Q', title='Number of Partitions'),
            color=alt.Color('partition_count:Q', scale=alt.Scale(scheme='viridis'),
                            legend=alt.Legend(title='Partition Count')),
            tooltip=['p:Q', 'partition_count:Q'],
        )
        .properties(title='Larger Primes vs Partition Count (sampled)', width=500, height=350)
    )

    return count_chart, scatter_chart, freq_chart


# ---------------------------------------------------------------------------
# Pattern page
# ---------------------------------------------------------------------------

def create_partition_pattern_chart(lf: pl.LazyFrame):
    valid = lf.filter(pl.col('m_k') > 0)

    # m distribution — pre-aggregated
    m_freq = (
        valid.group_by('m_k').agg(pl.len().alias('count'))
        .sort('m_k').collect(streaming=True)
    )
    m_dist = (
        alt.Chart(m_freq).mark_bar()
        .encode(
            x=alt.X('m_k:O', title='m (power of 2)'),
            y=alt.Y('count:Q', title='Frequency'),
            tooltip=['m_k:O', 'count:Q'],
        )
        .properties(title='Distribution of m values', width=300, height=200)
    )

    # n distribution — pre-aggregated
    n_freq = (
        valid.group_by('n_k').agg(pl.len().alias('count'))
        .sort('n_k').collect(streaming=True)
    )
    n_dist = (
        alt.Chart(n_freq).mark_bar()
        .encode(
            x=alt.X('n_k:O', title='n (power of q)'),
            y=alt.Y('count:Q', title='Frequency'),
            tooltip=['n_k:O', 'count:Q'],
        )
        .properties(title='Distribution of n values', width=300, height=200)
    )

    # m × n heatmap — aggregated counts (small grid)
    mn_heat = (
        valid.group_by(['m_k', 'n_k']).agg(pl.len().alias('count'))
        .collect(streaming=True)
    )
    mn_chart = (
        alt.Chart(mn_heat).mark_rect()
        .encode(
            x=alt.X('m_k:O', title='m (power of 2)'),
            y=alt.Y('n_k:O', title='n (power of q)'),
            color=alt.Color('count:Q', scale=alt.Scale(scheme='viridis', type='log'),
                            legend=alt.Legend(title='Count')),
            tooltip=['m_k:O', 'n_k:O', 'count:Q'],
        )
        .properties(title='Partition Patterns: m x n (log-scale color)', width=500, height=400)
    )

    return mn_chart, m_dist, n_dist


# ---------------------------------------------------------------------------
# Distribution page
# ---------------------------------------------------------------------------

def generate_distribution_page(lf: pl.LazyFrame):
    valid = lf.filter(pl.col('m_k') > 0)

    n_freq = (
        valid.group_by('n_k').agg(pl.len().alias('count'))
        .sort('n_k').collect(streaming=True)
    )
    n_chart = (
        alt.Chart(n_freq).mark_bar(color='steelblue')
        .encode(x=alt.X('n_k:O', title='n values'), y=alt.Y('count:Q', title='Count'),
                tooltip=['n_k:O', 'count:Q'])
        .properties(title='Distribution of n values', width=300, height=250)
    )

    q_counts = (
        valid.group_by('q_k').agg(pl.len().alias('count'))
        .sort('count', descending=True).head(20).collect(streaming=True)
    )
    q_chart = (
        alt.Chart(q_counts).mark_bar(color='orange')
        .encode(x=alt.X('q_k:O', title='q values (top 20)', sort='-y'),
                y=alt.Y('count:Q', title='Count'),
                tooltip=['q_k:O', 'count:Q'])
        .properties(title='Top 20 most frequent q values', width=400, height=250)
    )

    m_freq = (
        valid.group_by('m_k').agg(pl.len().alias('count'))
        .sort('m_k').collect(streaming=True)
    )
    m_chart = (
        alt.Chart(m_freq).mark_bar(color='green')
        .encode(x=alt.X('m_k:O', title='m values'), y=alt.Y('count:Q', title='Count'),
                tooltip=['m_k:O', 'count:Q'])
        .properties(title='Distribution of m values', width=300, height=250)
    )

    stats = _collect_stats(lf)
    stats_chart = (
        alt.Chart(alt.Data(values=[{
            'text': (f"Key Statistics:\n"
                     f"  Max n: {stats['max_n']}\n"
                     f"  n > 1: {stats['n_gt_1']:,}\n"
                     f"  Total: {stats['total_partitions']:,}")
        }]))
        .mark_text(align='left', fontSize=14, dx=10, dy=10)
        .encode(text='text:N')
        .properties(title='Statistics', width=200, height=250)
    )

    return alt.vconcat(
        alt.hconcat(n_chart, stats_chart),
        alt.hconcat(q_chart, m_chart),
    ).properties(title='Distribution Analysis')


# ---------------------------------------------------------------------------
# Interactive explorer (sampled)
# ---------------------------------------------------------------------------

def create_interactive_data_explorer(lf: pl.LazyFrame):
    partitions = (
        lf.filter(pl.col('m_k') > 0).collect(streaming=True)
    )

    # Sample for the scatter explorer
    sample = partitions if partitions.height <= MAX_SCATTER_POINTS else partitions.sample(MAX_SCATTER_POINTS, seed=42)

    data_table = (
        alt.Chart(sample).mark_circle(size=30)
        .encode(
            x=alt.X('p:Q', title='Prime p', scale=alt.Scale(type='log')),
            y=alt.Y('q_k:Q', title='Prime base q', scale=alt.Scale(type='log')),
            color=alt.Color('m_k:O', title='m (power of 2)'),
            size=alt.Size('n_k:O', title='n (power of q)', scale=alt.Scale(range=[50, 300])),
            tooltip=['p:Q', 'm_k:Q', 'n_k:Q', 'q_k:Q'],
        )
        .properties(title=f'Partition Data Explorer ({sample.height:,} points)', width=500, height=400)
    )

    # Pre-aggregated distributions for selectors
    n_freq = partitions.group_by('n_k').agg(pl.len().alias('count')).sort('n_k')
    n_sel = alt.selection_point(fields=['n_k'])
    n_dist = (
        alt.Chart(n_freq).mark_bar()
        .encode(
            x=alt.X('n_k:O', title='n values'),
            y=alt.Y('count:Q', title='Frequency'),
            color=alt.condition(n_sel, alt.value('red'), alt.value('steelblue')),
            tooltip=['n_k:O', 'count:Q'],
        )
        .add_params(n_sel)
        .properties(title='n values (click to filter)', width=300, height=200)
    )

    q_top = partitions.group_by('q_k').agg(pl.len().alias('count')).sort('count', descending=True).head(30)
    q_sel = alt.selection_point(fields=['q_k'])
    q_dist = (
        alt.Chart(q_top).mark_bar()
        .encode(
            x=alt.X('q_k:O', title='q values (top 30)', sort='-y'),
            y=alt.Y('count:Q', title='Frequency'),
            color=alt.condition(q_sel, alt.value('blue'), alt.value('orange')),
            tooltip=['q_k:O', 'count:Q'],
        )
        .add_params(q_sel)
        .properties(title='q values (click to filter)', width=400, height=200)
    )

    m_freq = partitions.group_by('m_k').agg(pl.len().alias('count')).sort('m_k')
    m_sel = alt.selection_point(fields=['m_k'])
    m_dist = (
        alt.Chart(m_freq).mark_bar()
        .encode(
            x=alt.X('m_k:O', title='m values'),
            y=alt.Y('count:Q', title='Frequency'),
            color=alt.condition(m_sel, alt.value('green'), alt.value('purple')),
            tooltip=['m_k:O', 'count:Q'],
        )
        .add_params(m_sel)
        .properties(title='m values (click to filter)', width=300, height=200)
    )

    stats = _collect_stats(lf)
    summary_text = (
        alt.Chart(alt.Data(values=[{
            'text': (f"Data Summary:\n"
                     f"  Max n: {stats['max_n']}\n"
                     f"  n > 1: {stats['n_gt_1']:,}\n"
                     f"  Partitions: {stats['total_partitions']:,}\n"
                     f"  Unique q: {stats['unique_q']:,}")
        }]))
        .mark_text(align='left', fontSize=12, dx=5, dy=-5)
        .encode(text='text:N')
        .properties(title='Key Statistics', width=200, height=150)
    )

    return data_table, n_dist, q_dist, m_dist, summary_text


# ---------------------------------------------------------------------------
# Raw data table
# ---------------------------------------------------------------------------

def create_raw_data_table(lf: pl.LazyFrame, max_primes=200):
    """Create a readable HTML table grouped by prime."""
    partitions_df = (
        lf.filter(pl.col('m_k') > 0)
        .sort('p')
        .head(max_primes * 15)  # generous over-fetch, then trim by unique primes
        .collect(streaming=True)
    )

    stats = _collect_stats(lf)

    prime_groups = {}
    for row in partitions_df.iter_rows(named=True):
        p = row['p']
        if p not in prime_groups:
            if len(prime_groups) >= max_primes:
                break
            prime_groups[p] = {'count': 0, 'partitions': []}
        prime_groups[p]['count'] += 1
        prime_groups[p]['partitions'].append({
            'm': row['m_k'], 'n': row['n_k'], 'q': row['q_k']
        })

    sorted_primes = sorted(prime_groups.keys())

    html = f"""<html>
<head>
<title>Prime Power Partition Data</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 20px; }}
table {{ border-collapse: collapse; width: 100%; }}
th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
th {{ background-color: #f2f2f2; }}
tr:nth-child(even) {{ background-color: #f9f9f9; }}
.prime-cell {{ font-weight: bold; background-color: #e8f4f8; }}
.partition-list {{ font-family: monospace; }}
.summary {{ background: #e6f3ff; padding: 15px; margin-bottom: 20px; border-radius: 5px; }}
</style>
</head>
<body>
<h1>Prime Power Partition Data (first {len(sorted_primes)} primes)</h1>
<div class="summary">
<p><strong>Total primes:</strong> {stats['total_primes']:,}</p>
<p><strong>Max n:</strong> {stats['max_n']}</p>
<p><strong>n &gt; 1 cases:</strong> {stats['n_gt_1']:,}</p>
<p><strong>Unique q:</strong> {stats['unique_q']:,}</p>
</div>
<table><thead><tr>
<th>Prime (p)</th><th>Count</th><th>Partitions (m,n,q)</th><th>Equations</th>
</tr></thead><tbody>
"""
    for p in sorted_primes:
        g = prime_groups[p]
        parts = "<br>".join(f"({x['m']},{x['n']},{x['q']})" for x in g['partitions'])
        eqs = "<br>".join(f"{p} = 2^{x['m']} + {x['q']}^{x['n']}" for x in g['partitions'])
        html += f'<tr><td class="prime-cell">{p}</td><td>{g["count"]}</td>'
        html += f'<td class="partition-list">{parts}</td>'
        html += f'<td class="partition-list">{eqs}</td></tr>\n'

    html += "</tbody></table></body></html>"
    return html


# ---------------------------------------------------------------------------
# Page generators
# ---------------------------------------------------------------------------

def generate_summary_page(lf: pl.LazyFrame):
    count_chart, scatter_chart, freq_chart = create_summary_chart(lf)
    stats = _collect_stats(lf)

    return alt.vconcat(
        alt.hconcat(count_chart, freq_chart).resolve_scale(color='independent'),
        scatter_chart,
    ).properties(
        title=alt.TitleParams(
            text=[f"Summary: {stats['total_primes']:,} primes, largest: {stats['largest_prime']:,}",
                  f"Total partitions: {stats['total_partitions']:,}"],
            fontSize=14,
        )
    )


def generate_pattern_page(lf: pl.LazyFrame):
    mn_chart, m_dist, n_dist = create_partition_pattern_chart(lf)

    return alt.hconcat(
        mn_chart,
        alt.vconcat(m_dist, n_dist),
    ).properties(title='Partition Patterns Analysis')


def generate_dashboard(data_path=None, output_path=None):
    """Generate multi-page dashboard with separate HTML files.

    All data aggregation happens in Polars; only small summary frames
    are serialized into the Altair/Vega-Lite JSON specs.
    """
    lf = load_data_for_viz(data_path)
    stats = _collect_stats(lf)

    data_dir = Path('data')
    data_dir.mkdir(exist_ok=True)

    generate_summary_page(lf).save(str(data_dir / 'summary.html'))
    generate_pattern_page(lf).save(str(data_dir / 'patterns.html'))
    generate_distribution_page(lf).save(str(data_dir / 'distributions.html'))

    with open(data_dir / 'raw_data.html', 'w') as f:
        f.write(create_raw_data_table(lf))

    index_html = f"""<html>
<head><title>Prime Power Partition Analysis</title>
<style>
body {{ font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }}
.container {{ max-width: 800px; margin: 0 auto; background: white; padding: 30px;
              border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
h1 {{ color: #333; text-align: center; }}
.nav-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin: 30px 0; }}
.nav-item {{ background: #e8f4f8; padding: 20px; border-radius: 8px; text-align: center;
             border: 2px solid #d1e7dd; }}
.nav-item:hover {{ background: #d4e6f1; }}
.nav-item a {{ text-decoration: none; color: #2c3e50; font-weight: bold; font-size: 16px; }}
.stats {{ background: #fff3cd; padding: 15px; border-radius: 5px; margin: 20px 0; }}
</style></head>
<body><div class="container">
<h1>Prime Power Partition Analysis Dashboard</h1>
<div class="stats">
<h3>Dataset Summary</h3>
<p><strong>Primes analyzed:</strong> {stats['total_primes']:,}</p>
<p><strong>Largest prime:</strong> {stats['largest_prime']:,}</p>
<p><strong>Total partitions:</strong> {stats['total_partitions']:,}</p>
<p><strong>n &gt; 1 cases:</strong> {stats['n_gt_1']:,}</p>
</div>
<div class="nav-grid">
<div class="nav-item"><a href="summary.html">[SUMMARY] Overview</a>
<p>Partition count distributions and frequency tables</p></div>
<div class="nav-item"><a href="patterns.html">[PATTERNS] Analysis</a>
<p>m vs n relationships and pattern distributions</p></div>
<div class="nav-item"><a href="distributions.html">[DISTRIBUTIONS] Value Analysis</a>
<p>Clean charts of m, n, and q value frequencies</p></div>
<div class="nav-item"><a href="raw_data.html">[DATA] Raw Table</a>
<p>Scrollable table with all partition data</p></div>
</div>
<p style="text-align: center; color: #666; margin-top: 30px;">
<em>Charts use server-side aggregation for fast rendering at any dataset size</em></p>
</div></body></html>"""

    with open(data_dir / 'index.html', 'w') as f:
        f.write(index_html)

    print("Multi-page dashboard generated:")
    print(f"  [INDEX] {data_dir / 'index.html'}")
    print(f"  [SUMMARY] {data_dir / 'summary.html'}")
    print(f"  [PATTERNS] {data_dir / 'patterns.html'}")
    print(f"  [DISTRIBUTIONS] {data_dir / 'distributions.html'}")
    print(f"  [DATA] {data_dir / 'raw_data.html'}")


if __name__ == "__main__":
    generate_dashboard()
