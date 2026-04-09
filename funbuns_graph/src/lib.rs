//! funbuns_graph: compressed DAG storage for the prime power partition lattice.
//!
//! The graph encodes edges q → p where p = 2^m + q^n, with m stored as
//! a u8 edge label. Storage uses WebGraph's BV compression format with
//! memory-mapped access.
//!
//! On-disk layout (all under a configurable base directory):
//!   id_to_prime.bin         — sorted Vec<u64>, dense ID → prime value
//!   forward.graph/.ef       — BvGraph: successors (q → p edges)
//!   forward.labels.bin      — flat u8 array, m values parallel to edge order
//!   forward.label_offsets.bin — cumulative edge offsets per node (u64)
//!   transpose.graph/.ef     — BvGraph: predecessors (p → q edges)
//!   transpose.labels.bin    — flat u8 array, m values for transpose
//!   transpose.label_offsets.bin

pub mod dense_id;
pub mod io;
pub mod label_codec;
