//! One-time construction pipeline: parquet blocks → compressed WebGraph.
//!
//! Phase 1: Bulk-copy p columns from sorted parquet blocks → id_to_prime.bin
//! Phase 2: Stream labeled arcs through external sort → BvGraph + parallel labels
//! Phase 3: Same for transpose
//!
//! All phases stream with bounded memory.

use anyhow::{Context, Result, bail};
use clap::Parser;
use dsi_bitstream::prelude::BE;
use indicatif::{ProgressBar, ProgressStyle};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use funbuns_graph::dense_id::DenseIdMap;
use funbuns_graph::io::discover_blocks;
use funbuns_graph::label_codec;

#[derive(Parser)]
#[command(name = "build-graph", about = "Build compressed WebGraph from parquet blocks")]
struct Args {
    /// Directory containing pp_b*.parquet block files
    #[arg(long)]
    blocks_dir: PathBuf,

    /// Output directory for graph files
    #[arg(long)]
    out_dir: PathBuf,

    /// Memory budget for external sorting (bytes, default 4 GiB)
    #[arg(long, default_value = "4294967296")]
    sort_memory: usize,

    /// Skip phase 1 if id_to_prime.bin already exists
    #[arg(long)]
    skip_ids: bool,

    /// Skip forward graph (phase 2) if it already exists
    #[arg(long)]
    skip_forward: bool,

    /// Skip transpose (phase 3) if it already exists
    #[arg(long)]
    skip_transpose: bool,
}

fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    std::fs::create_dir_all(&args.out_dir)
        .with_context(|| format!("creating {}", args.out_dir.display()))?;

    let blocks = discover_blocks(&args.blocks_dir)?;
    if blocks.is_empty() {
        bail!("no pp_b*.parquet files found in {}", args.blocks_dir.display());
    }
    eprintln!("Found {} block files", blocks.len());

    let id_map_path = args.out_dir.join("id_to_prime.bin");

    // ── Phase 1: Dense ID map ──────────────────────────────────────────
    if args.skip_ids && id_map_path.exists() {
        eprintln!("Phase 1: skipped (--skip-ids)");
    } else {
        eprintln!("Phase 1: building dense ID map...");
        let count = build_dense_ids(&blocks, &id_map_path)?;
        eprintln!("Phase 1 done: {} primes → {}", count, id_map_path.display());
    }

    let id_map = DenseIdMap::open(&id_map_path)?;
    let num_nodes = id_map.len();
    eprintln!("ID map: {} primes", num_nodes);

    // ── Phase 2: Forward graph (q → p) ────────────────────────────────
    let fwd_base = args.out_dir.join("forward");
    let fwd_labels = args.out_dir.join("forward.labels.bin");
    if args.skip_forward && fwd_base.with_extension("graph").exists() {
        eprintln!("Phase 2: skipped (--skip-forward)");
    } else {
        eprintln!("Phase 2: building forward graph...");
        build_graph_direction(
            &blocks, &id_map, &fwd_base, &fwd_labels,
            args.sort_memory, &args.out_dir, Direction::Forward,
        )?;
    }

    // ── Phase 3: Transpose (p → q) ────────────────────────────────────
    let rev_base = args.out_dir.join("transpose");
    let rev_labels = args.out_dir.join("transpose.labels.bin");
    if args.skip_transpose && rev_base.with_extension("graph").exists() {
        eprintln!("Phase 3: skipped (--skip-transpose)");
    } else {
        eprintln!("Phase 3: building transpose...");
        build_graph_direction(
            &blocks, &id_map, &rev_base, &rev_labels,
            args.sort_memory, &args.out_dir, Direction::Transpose,
        )?;
    }

    eprintln!("\nBuild complete. Run EF index construction:");
    eprintln!("  webgraph build ef {}", fwd_base.display());
    eprintln!("  webgraph build ef {}", rev_base.display());

    Ok(())
}

#[derive(Clone, Copy)]
enum Direction {
    Forward,   // edge: q → p
    Transpose, // edge: p → q
}

// ═══════════════════════════════════════════════════════════════════════════
// Phase 1: Bulk-copy p columns → id_to_prime.bin
// ═══════════════════════════════════════════════════════════════════════════

/// Read the minimum p value from a block's parquet metadata (column statistics).
/// Falls back to reading the first row if statistics aren't available.
fn block_min_p(path: &Path) -> Result<i64> {
    use parquet::file::reader::{FileReader, SerializedFileReader};

    let file = std::fs::File::open(path)?;
    let reader = SerializedFileReader::new(file)?;
    let metadata = reader.metadata();

    // Try row group statistics first (much cheaper than reading data)
    let mut global_min: Option<i64> = None;
    for rg in 0..metadata.num_row_groups() {
        let rg_meta = metadata.row_group(rg);
        // Find the "p" column index
        for col_idx in 0..rg_meta.num_columns() {
            let col_meta = rg_meta.column(col_idx);
            if col_meta.column_path().string() == "p" {
                if let Some(stats) = col_meta.statistics() {
                    if let parquet::file::statistics::Statistics::Int64(s) = stats {
                        if let Some(&min_val) = s.min_opt() {
                            global_min = Some(global_min.map_or(min_val, |g: i64| g.min(min_val)));
                        }
                    }
                }
            }
        }
    }

    if let Some(min_p) = global_min {
        return Ok(min_p);
    }

    // Fallback: read first batch
    use arrow::array::AsArray;
    use arrow::datatypes::Int64Type;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = std::fs::File::open(path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_batch_size(1)
        .build()?;
    for batch in reader {
        let batch = batch?;
        let p_col = batch.column_by_name("p").context("missing p")?
            .as_primitive::<Int64Type>();
        if !p_col.is_empty() {
            return Ok(p_col.values()[0]);
        }
    }
    bail!("empty block: {}", path.display())
}

fn build_dense_ids(blocks: &[PathBuf], out_path: &Path) -> Result<u64> {
    use arrow::array::AsArray;
    use arrow::datatypes::Int64Type;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    // Precompute min_p for each block, then build a suffix-minimum array.
    // flush_threshold[i] = min(min_p[j] for j in i+1..N) ensures we never
    // flush a prime that a later block might supply in an overlap region.
    let mut min_ps: Vec<i64> = Vec::with_capacity(blocks.len());
    let min_pb = make_pb(blocks.len() as u64, "blocks (reading min_p)");
    for block_path in blocks {
        min_ps.push(block_min_p(block_path)?);
        min_pb.inc(1);
    }
    min_pb.finish();

    // Suffix minimum: safe flush threshold after processing block i
    let mut suffix_min: Vec<i64> = vec![i64::MAX; blocks.len()];
    if blocks.len() >= 2 {
        suffix_min[blocks.len() - 1] = min_ps[blocks.len() - 1];
        for i in (0..blocks.len() - 1).rev() {
            suffix_min[i] = min_ps[i + 1].min(suffix_min[i + 1]);
        }
    }

    let file = std::fs::File::create(out_path)?;
    let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, file);

    let pb = make_pb(blocks.len() as u64, "blocks");
    let mut count: u64 = 0;

    // Pending buffer: primes not yet safe to flush (overlap region)
    let mut pending: Vec<i64> = Vec::new();
    let mut overlaps: u64 = 0;

    for (block_idx, block_path) in blocks.iter().enumerate() {
        let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(block_path)?)?
            .with_batch_size(1_000_000)
            .build()?;

        for batch in reader {
            let batch = batch?;
            let p_col = batch.column_by_name("p")
                .context("missing column p")?
                .as_primitive::<Int64Type>();
            let values = p_col.values();

            // Add unique values to pending
            for &p in values {
                if pending.last() != Some(&p) {
                    pending.push(p);
                }
            }
        }

        // Flush threshold: suffix minimum of all remaining blocks' min_p
        let flush_threshold = if block_idx + 1 < blocks.len() {
            suffix_min[block_idx]
        } else {
            i64::MAX
        };

        // Sort pending (only needed when blocks overlapped into it)
        // Check if it's already sorted first (common case)
        let needs_sort = pending.windows(2).any(|w| w[0] > w[1]);
        if needs_sort {
            overlaps += 1;
            pending.sort_unstable();
        }

        // Deduplicate
        pending.dedup();

        // Partition: flush values < threshold, keep the rest
        let split_pos = pending.partition_point(|&p| p < flush_threshold);

        if split_pos > 0 {
            let bytes = unsafe {
                std::slice::from_raw_parts(
                    pending.as_ptr() as *const u8,
                    split_pos * 8,
                )
            };
            writer.write_all(bytes)?;
            count += split_pos as u64;
        }

        // Keep values >= threshold for next iteration
        pending.drain(..split_pos);

        pb.inc(1);
    }

    // Flush any remaining
    if !pending.is_empty() {
        let bytes = unsafe {
            std::slice::from_raw_parts(
                pending.as_ptr() as *const u8,
                pending.len() * 8,
            )
        };
        writer.write_all(bytes)?;
        count += pending.len() as u64;
    }

    writer.flush()?;
    pb.finish_with_message(format!("{} primes ({} overlap merges)", count, overlaps));
    Ok(count)
}

// ═══════════════════════════════════════════════════════════════════════════
// Phase 2/3: Sort labeled arcs → BvGraph + label file
// ═══════════════════════════════════════════════════════════════════════════

fn build_graph_direction(
    blocks: &[PathBuf],
    id_map: &DenseIdMap,
    graph_basename: &Path,
    label_path: &Path,
    sort_memory: usize,
    out_dir: &Path,
    direction: Direction,
) -> Result<()> {
    let num_nodes = id_map.len();
    let memory_usage = webgraph::utils::MemoryUsage::MemorySize(sort_memory);

    // ── Pass 1: External sort with u8 labels → write label file ────────
    {
        let sort_dir = tempfile::Builder::new()
            .prefix("graph_sort_lbl_")
            .tempdir_in(out_dir)?;
        let codec = label_codec::u8_codec();
        let mut sorter = webgraph::utils::SortPairs::new_labeled(
            memory_usage, sort_dir.path(), codec,
        )?;

        let pb = make_pb(blocks.len() as u64, "blocks (labeled sort)");
        for block_path in blocks {
            push_arcs_from_block(block_path, id_map, direction, &mut sorter)?;
            pb.inc(1);
        }
        pb.finish();

        eprintln!("  Writing labels...");
        let sorted_iter = sorter.iter()?;
        let mut label_writer = BufWriter::with_capacity(4 * 1024 * 1024, std::fs::File::create(label_path)?);
        let mut label_count: u64 = 0;
        for arc in sorted_iter {
            let ((_src, _dst), m) = arc;
            label_writer.write_all(&[m])?;
            label_count += 1;
        }
        label_writer.flush()?;
        eprintln!("  {} labels → {}", label_count, label_path.display());
    }

    // ── Pass 2: External sort without labels → BvGraph compression ─────
    {
        let sort_dir = tempfile::Builder::new()
            .prefix("graph_sort_str_")
            .tempdir_in(out_dir)?;
        let mut sorter = webgraph::utils::SortPairs::new(
            memory_usage, sort_dir.path(),
        )?;

        let pb = make_pb(blocks.len() as u64, "blocks (graph sort)");
        for block_path in blocks {
            push_arcs_unlabeled(block_path, id_map, direction, &mut sorter)?;
            pb.inc(1);
        }
        pb.finish();

        eprintln!("  Compressing...");
        let sorted_iter = sorter.iter()?;
        let graph = webgraph::graphs::arc_list_graph::ArcListGraph::new(
            num_nodes,
            sorted_iter.map(|(pair, _)| pair),
        );

        webgraph::graphs::bvgraph::BvComp::with_basename(graph_basename)
            .comp_graph::<BE>(&graph)?;

        eprintln!("  Graph → {}.graph", graph_basename.display());
    }

    Ok(())
}

/// Push labeled arcs from a single block into the sorter.
fn push_arcs_from_block<C: webgraph::utils::BatchCodec<Label = u8>>(
    block_path: &PathBuf,
    id_map: &DenseIdMap,
    direction: Direction,
    sorter: &mut webgraph::utils::SortPairs<C>,
) -> Result<()> {
    use arrow::array::AsArray;
    use arrow::datatypes::Int64Type;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(block_path)?)?
        .with_batch_size(1_000_000)
        .build()?;

    for batch in reader {
        let batch = batch?;
        let p_col = batch.column_by_name("p").context("missing p")?.as_primitive::<Int64Type>();
        let q_col = batch.column_by_name("q_k").context("missing q_k")?.as_primitive::<Int64Type>();
        let m_col = batch.column_by_name("m_k").context("missing m_k")?.as_primitive::<Int64Type>();

        let p_vals = p_col.values();
        let q_vals = q_col.values();
        let m_vals = m_col.values();

        for i in 0..batch.num_rows() {
            let q = q_vals[i];
            if q == 0 { continue; }

            let p_id = id_map.prime_to_id(p_vals[i] as u64)
                .with_context(|| format!("p={} not in ID map", p_vals[i]))?;
            let q_id = id_map.prime_to_id(q as u64)
                .with_context(|| format!("q={} not in ID map", q))?;
            let m = m_vals[i] as u8;

            let (src, dst) = match direction {
                Direction::Forward => (q_id, p_id),
                Direction::Transpose => (p_id, q_id),
            };

            sorter.push_labeled(src, dst, m)?;
        }
    }

    Ok(())
}

/// Push unlabeled arcs from a single block into the sorter.
fn push_arcs_unlabeled(
    block_path: &PathBuf,
    id_map: &DenseIdMap,
    direction: Direction,
    sorter: &mut webgraph::utils::SortPairs,
) -> Result<()> {
    use arrow::array::AsArray;
    use arrow::datatypes::Int64Type;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(block_path)?)?
        .with_batch_size(1_000_000)
        .build()?;

    for batch in reader {
        let batch = batch?;
        let p_col = batch.column_by_name("p").context("missing p")?.as_primitive::<Int64Type>();
        let q_col = batch.column_by_name("q_k").context("missing q_k")?.as_primitive::<Int64Type>();

        let p_vals = p_col.values();
        let q_vals = q_col.values();

        for i in 0..batch.num_rows() {
            let q = q_vals[i];
            if q == 0 { continue; }

            let p_id = id_map.prime_to_id(p_vals[i] as u64).unwrap();
            let q_id = id_map.prime_to_id(q as u64).unwrap();

            let (src, dst) = match direction {
                Direction::Forward => (q_id, p_id),
                Direction::Transpose => (p_id, q_id),
            };
            sorter.push(src, dst)?;
        }
    }

    Ok(())
}

fn make_pb(total: u64, item: &str) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(ProgressStyle::default_bar()
        .template(&format!(
            "{{spinner:.green}} [{{bar:40.cyan/blue}}] {{pos}}/{{len}} {} ({{eta}})", item
        ))
        .unwrap()
        .progress_chars("=>-"));
    pb
}
