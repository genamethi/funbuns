//! Mmap'd graph access for the prime power partition lattice.
//!
//! Loads the compressed BvGraph (forward + transpose) and dense ID map
//! produced by `build-graph`. All data is memory-mapped; only accessed
//! pages are loaded into RAM.

use memmap2::Mmap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::OnceLock;
use webgraph::traits::RandomAccessLabeling;

// ───────────────────────────────────────────────────────────────────────
// Dense ID map
// ───────────────────────────────────────────────────────────────────────

pub struct DenseIdMap {
    _mmap: Mmap,
    primes: &'static [u64],
}

impl DenseIdMap {
    pub fn open(path: &std::path::Path) -> anyhow::Result<Self> {
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let count = mmap.len() / 8;
        let primes: &'static [u64] = unsafe {
            std::slice::from_raw_parts(mmap.as_ptr() as *const u64, count)
        };
        Ok(DenseIdMap { _mmap: mmap, primes })
    }

    #[inline]
    #[allow(dead_code)]
    pub fn len(&self) -> usize { self.primes.len() }

    #[inline]
    pub fn id_to_prime(&self, id: usize) -> u64 { self.primes[id] }

    #[inline]
    pub fn prime_to_id(&self, prime: u64) -> Option<usize> {
        self.primes.binary_search(&prime).ok()
    }
}

// ───────────────────────────────────────────────────────────────────────
// Concrete graph type (Mmap load, BE, dynamic codes)
// ───────────────────────────────────────────────────────────────────────

// The type returned by BvGraph::with_basename("x").load() with default
// Mmap mode and BE endianness.
type MmapBvGraph = webgraph::graphs::bvgraph::BvGraph<
    webgraph::graphs::bvgraph::DynCodesDecoderFactory<
        dsi_bitstream::prelude::BigEndian,
        webgraph::utils::MmapHelper<u32>,
        webgraph::graphs::bvgraph::EF,
    >,
>;

// ───────────────────────────────────────────────────────────────────────
// Global graph handle
// ───────────────────────────────────────────────────────────────────────

struct GraphHandle {
    id_map: DenseIdMap,
    forward: MmapBvGraph,
    transpose: MmapBvGraph,
    label_fwd: Mmap,
    label_rev: Mmap,
    /// Cumulative degree offsets for forward graph (for label indexing).
    /// cum_deg_fwd[i] = sum of outdegrees of nodes 0..i.
    cum_deg_fwd: Vec<u64>,
    /// Cumulative degree offsets for transpose graph.
    cum_deg_rev: Vec<u64>,
}

static GRAPH: OnceLock<Result<GraphHandle, String>> = OnceLock::new();

fn graph_dir() -> PathBuf {
    if let Ok(dir) = std::env::var("FUNBUNS_GRAPH_DIR") {
        return PathBuf::from(dir);
    }
    PathBuf::from("/media/extssd/research/dioph.pp/data/graph")
}

fn load_graph() -> Result<GraphHandle, String> {
    use webgraph::traits::SequentialLabeling;

    let dir = graph_dir();

    let id_map = DenseIdMap::open(&dir.join("id_to_prime.bin"))
        .map_err(|e| format!("loading id_map: {e}"))?;

    let forward: MmapBvGraph = webgraph::graphs::bvgraph::BvGraph::with_basename(dir.join("forward"))
        .load()
        .map_err(|e| format!("loading forward graph: {e}"))?;

    let transpose: MmapBvGraph = webgraph::graphs::bvgraph::BvGraph::with_basename(dir.join("transpose"))
        .load()
        .map_err(|e| format!("loading transpose graph: {e}"))?;

    // Build cumulative degree arrays for label indexing
    let n = forward.num_nodes();
    let mut cum_deg_fwd = Vec::with_capacity(n + 1);
    let mut acc: u64 = 0;
    cum_deg_fwd.push(0);
    for i in 0..n {
        acc += forward.outdegree(i) as u64;
        cum_deg_fwd.push(acc);
    }

    let mut cum_deg_rev = Vec::with_capacity(n + 1);
    acc = 0;
    cum_deg_rev.push(0);
    for i in 0..n {
        acc += transpose.outdegree(i) as u64;
        cum_deg_rev.push(acc);
    }

    let label_fwd = {
        let f = File::open(dir.join("forward.labels.bin"))
            .map_err(|e| format!("opening forward labels: {e}"))?;
        unsafe { Mmap::map(&f).map_err(|e| format!("mmap forward labels: {e}"))? }
    };

    let label_rev = {
        let f = File::open(dir.join("transpose.labels.bin"))
            .map_err(|e| format!("opening transpose labels: {e}"))?;
        unsafe { Mmap::map(&f).map_err(|e| format!("mmap transpose labels: {e}"))? }
    };

    Ok(GraphHandle {
        id_map, forward, transpose, label_fwd, label_rev, cum_deg_fwd, cum_deg_rev,
    })
}

fn get_graph() -> Result<&'static GraphHandle, String> {
    GRAPH.get_or_init(load_graph).as_ref().map_err(|e| e.clone())
}

// ───────────────────────────────────────────────────────────────────────
// Public query API
// ───────────────────────────────────────────────────────────────────────

/// Successors: primes p where this prime q is an ancestor (q → p edges).
/// Returns vec of (target_prime, m).
pub fn successors(prime: u64) -> Result<Vec<(u64, u8)>, String> {
    use webgraph::traits::RandomAccessGraph;

    let g = get_graph()?;
    let id = g.id_map.prime_to_id(prime)
        .ok_or_else(|| format!("prime {} not in graph", prime))?;

    let edge_start = g.cum_deg_fwd[id] as usize;
    let succs: Vec<usize> = g.forward.successors(id).collect();

    let mut result = Vec::with_capacity(succs.len());
    for (i, &target_id) in succs.iter().enumerate() {
        let m = g.label_fwd.get(edge_start + i).copied().unwrap_or(0);
        result.push((g.id_map.id_to_prime(target_id), m));
    }
    Ok(result)
}

/// Predecessors (ancestors): primes q that appear in this prime's decompositions.
/// Returns vec of (source_prime, m).
pub fn predecessors(prime: u64) -> Result<Vec<(u64, u8)>, String> {
    use webgraph::traits::RandomAccessGraph;

    let g = get_graph()?;
    let id = g.id_map.prime_to_id(prime)
        .ok_or_else(|| format!("prime {} not in graph", prime))?;

    let edge_start = g.cum_deg_rev[id] as usize;
    let preds: Vec<usize> = g.transpose.successors(id).collect();

    let mut result = Vec::with_capacity(preds.len());
    for (i, &source_id) in preds.iter().enumerate() {
        let m = g.label_rev.get(edge_start + i).copied().unwrap_or(0);
        result.push((g.id_map.id_to_prime(source_id), m));
    }
    Ok(result)
}

/// Follow the ancestry chain back to a k=0 source.
/// Returns vec of (prime, m) from start to terminal ancestor.
pub fn ancestry_chain(prime: u64) -> Result<Vec<(u64, u8)>, String> {
    use webgraph::traits::RandomAccessGraph;

    let g = get_graph()?;
    let mut chain = Vec::new();
    let mut current = prime;

    loop {
        let id = g.id_map.prime_to_id(current)
            .ok_or_else(|| format!("prime {} not in graph", current))?;

        let edge_start = g.cum_deg_rev[id] as usize;
        let preds: Vec<usize> = g.transpose.successors(id).collect();

        if preds.is_empty() {
            chain.push((current, 0u8));
            break;
        }

        let m = g.label_rev.get(edge_start).copied().unwrap_or(0);
        chain.push((current, m));
        current = g.id_map.id_to_prime(preds[0]);

        if chain.len() > 100 {
            return Err("chain exceeded 100 steps".to_string());
        }
    }

    Ok(chain)
}
