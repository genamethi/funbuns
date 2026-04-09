//! Parquet block scanning utilities.

use anyhow::{Context, Result};
use std::path::PathBuf;

/// Extract max_prime from filename like `pp_b001_p7368791.parquet` → 7368791.
/// Returns 0 if the pattern doesn't match.
fn parse_max_prime(path: &std::path::Path) -> u64 {
    path.file_name()
        .and_then(|n| n.to_str())
        .and_then(|n| {
            let after_p = n.rsplit_once("_p")?.1;
            let digits = after_p.strip_suffix(".parquet")?;
            digits.parse::<u64>().ok()
        })
        .unwrap_or(0)
}

/// Discover block parquet files in a directory, sorted by the max_prime
/// embedded in the filename (not lexicographic).
pub fn discover_blocks(data_dir: &std::path::Path) -> Result<Vec<PathBuf>> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(data_dir)
        .with_context(|| format!("reading {}", data_dir.display()))?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with("pp_b") && n.ends_with(".parquet"))
                .unwrap_or(false)
        })
        .collect();

    paths.sort_by_key(|p| parse_max_prime(p));
    Ok(paths)
}
