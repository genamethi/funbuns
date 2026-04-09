//! Dense ID mapping: prime ↔ dense index.
//!
//! The mapping is a flat sorted array of little-endian u64 primes in
//! `id_to_prime.bin`. Memory-mapped for random access.
//!
//! - Forward lookup (prime → id): slice::binary_search, O(log N).
//! - Inverse lookup (id → prime): direct index, O(1).

use anyhow::{Context, Result, bail};
use memmap2::Mmap;
use std::fs::File;
use std::path::Path;

/// Memory-mapped dense ID ↔ prime mapping.
pub struct DenseIdMap {
    _mmap: Mmap,
    primes: &'static [u64],
}

impl DenseIdMap {
    /// Open an existing id_to_prime.bin file.
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)
            .with_context(|| format!("opening {}", path.display()))?;
        let mmap = unsafe { Mmap::map(&file)? };
        let byte_len = mmap.len();
        if byte_len % 8 != 0 {
            bail!("id_to_prime.bin size {} is not a multiple of 8", byte_len);
        }
        let count = byte_len / 8;

        // Safe: page-aligned mmap on x86_64, written as native LE u64s
        let primes: &'static [u64] = unsafe {
            std::slice::from_raw_parts(mmap.as_ptr() as *const u64, count)
        };

        Ok(DenseIdMap { _mmap: mmap, primes })
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.primes.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.primes.is_empty()
    }

    #[inline]
    pub fn id_to_prime(&self, id: usize) -> u64 {
        self.primes[id]
    }

    #[inline]
    pub fn prime_to_id(&self, prime: u64) -> Option<usize> {
        self.primes.binary_search(&prime).ok()
    }
}
