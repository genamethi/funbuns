# funbuns

Prime power partition analysis: given a prime p, find all representations
p = 2^m + q^n where q is an odd prime and m, n >= 1.

## Git workflow

- **`master`** -- stable baseline
- **`claude-assisted`** -- completed features and changes (main working branch)
- **`liminal`** -- intermediate/in-progress work; commit here frequently

**Commit discipline:** commit all changes to `liminal` regularly throughout
a session (every meaningful chunk of work, not just at the end). When a
feature or change is complete and verified, merge or commit to
`claude-assisted`. Never commit directly to `master` without discussion.

## Environment

Everything runs through **pixi** (never pip, never conda directly):
```
pixi run <task>          # run a defined task
pixi install             # sync environment after pixi.toml changes
pixi add <pkg>           # add conda dependency
pixi add --pypi <pkg>    # add PyPI dependency
```
Config lives in `pixi.toml`. The editable install (`funbuns = { path = ".", editable = true }`)
registers console_scripts via `setup.py`.

### System tools
| Tool | Location | Notes |
|------|----------|-------|
| pixi | `~/.pixi/bin/pixi` | Package/task manager |
| cargo/rustc | `~/.cargo/bin/` | 1.93.1 -- builds the native plugin |
| uv | `~/.local/bin/uv` | Used by pixi's PyPI backend |
| SageMath 10.7 | via pixi (conda) | Number theory computations |
| DuckDB 1.5+ | via pixi (pypi) | Query backend for partition data |
| Polars 1.38+ | via pixi (conda) | All dataframe operations (never pandas) |
| lua 5.4 | `/usr/bin/lua` | Session logging hooks |
| djvutxt | `/usr/bin/djvutxt` | DJVU text extraction (djvulibre) |
| calibredb | `/usr/bin/calibredb` | Research library management |
| jq | `/usr/bin/jq` | JSON processing |

### Build the Rust plugin
```
cargo build --release    # in funbuns_native/
# NOT pixi run build, NOT maturin
```
The resulting `.so` is loaded as a Polars plugin by `native_expr.py`.

## CLI entry points

Three separate entry points keep concerns separated:

### `funbuns` -- mathematical analysis (src/funbuns/__main__.py)
```
funbuns -n 1000000              # generate partitions for 1M primes
funbuns --ladic                 # l-adic Diophantine analysis
funbuns --spectral              # FFT obstruction spectrum
funbuns --baker-circle 149      # archimedean H^1 visualization
funbuns --explore --explore-q 3 # recurrence structure exploration
funbuns --remainder             # remainder profiling (uses Rust full_profile)
funbuns --zipf                  # Zipf/Mandelbrot on q_k frequencies
funbuns --fixed-mod             # fixed-modulus ring analysis
funbuns --partitions --partition-k 5  # query primes with k=5 decompositions
funbuns --view                  # Altair dashboard
```

### `funbuns-admin` -- infrastructure (src/funbuns/admin.py)
```
funbuns-admin db build           # one-time DuckDB index build
funbuns-admin db sync            # incremental sync with new parquet
funbuns-admin db status          # show database stats
funbuns-admin serve [--port N]   # FastAPI partition browser (default: 8081)
funbuns-admin notebook [--port N] # Jupyter (default: 8888)
```

### `bmgr` -- block management (src/funbuns/block_manager.py)
```
pixi run bmgr                    # basic block status
pixi run bmgr-integrity          # data integrity check
pixi run bmgr-integrate          # integration check
pixi run bmgr-diagnose           # diagnose issues
pixi run bmgr-show-runs          # show computation run history
```

### Pixi tasks (pixi.toml)
```
pixi run serve                   # start web server
pixi run notebook                # start Jupyter
pixi run build-db / sync-db / db-status
pixi run ladic / remainder / zipf / spectral / explore / local-global
pixi run baker-circle            # baker-circle for p=149
pixi run build-native            # cargo build --release
```

## Data

Latest cached stats (logged as `dataset_summary` events after each
`bmgr --integrate-check` and `sync-db`):
```
jq -s '[.[] | select(.event=="dataset_summary")] | last' logs/bmgr.jsonl
jq -s '[.[] | select(.event=="dataset_summary")] | last' logs/admin.jsonl
```

- **Block parquet files**: `/media/extssd/research/dioph.pp/data/blocks/pp_b*.parquet`
  - Schema: `{p: u64, m_k: u32, n_k: u32, q_k: u64}`
  - Obstructed primes have q_k = 0
  - 2026-03-0?: 2,675 blocks, ~1.17B primes
  - 2026-04-06: 20,289 blocks, ~10.1B primes, max p ≈ 254.7B
- **DuckDB**: `data/funbuns.duckdb`
  - `partition_counts` TABLE (indexed): p, k
  - `decompositions` VIEW: zero-copy scan over parquet files
  - Temp directory on SSD: `/media/extssd/research/dioph.pp/data/duckdb_tmp/`
- **Config**: `pixi.toml [tool.funbuns]` has buffer_size, data paths, ports

## Algorithm and library rules

- **Never use pandas.** Polars only. Prefer lazy mode + streaming collect.
- **Never use Python loops for arithmetic.** Use SageMath (PARI/FLINT-backed),
  the Rust plugin (GMP-backed via `rug`), or vectorized Polars expressions.
- **Never use `next_prime()` or `prime_pi()` in loops.** Use rank-based
  reasoning with O(1) or O(log N) unrank calls.
- **Don't try to out-optimize PARI/FLINT/GMP.** These are already optimal
  per-call. Reduce call counts via mathematical structure instead.
- **For non-trivial Polars queries**, show the graphviz plan and get approval
  before writing the implementation.
- **No percentages or distributions** unless they reveal algebraic structure.
  Focus on raw data and structural patterns.
- **Don't mess with distributions without mathematical motivation.**
- **Verbose/progress output by default** -- don't gate it behind `-v`.
- **JSONL for event/metadata logging** (one JSON object per line).
- **Use `cargo build --release` directly** for the native plugin, not
  pixi or maturin.

## Research philosophy

This is exploratory mathematical research, not production software. The
goal is to understand the structure of prime power partitions through
computational experiment, not to ship features. Approaches are driven by
number-theoretic motivation (Baker's method, Shorey-Tijdeman, Catalan
conjecture, cohomological obstruction theory).

Key mathematical results so far:
- At most 2 solutions (m, n) per (p, q) pair (S-unit equation bound)
- Only p=11, q=3 achieves exactly 2 solutions (empirically verified across 602M pairs)
- ~17.3% of primes are obstructed (no decomposition exists)
- k distribution follows approximately Poisson shape (but with algebraic structure)

Current research directions (ranked):
1. Local-global gap (H^1) -- archimedean Baker circle DONE, q-adic analysis next
2. H^2 interaction -- cup products and derived obstructions
3. q-adic tower -- Iwasawa-style limit behavior
4. Engineering cleanup -- stale data, deprecated APIs

## File overview (26 source files)

### Core pipeline
| File | Role |
|------|------|
| `src/funbuns/core.py` | Parallel prime partition generation (imap_unordered, index-based dispatch) |
| `src/funbuns/block_manager.py` | Block file management, integrity checks, run history |
| `src/funbuns/block_catalog.py` | Block metadata catalog |
| `src/funbuns/dataprep.py` | Data preparation utilities |
| `src/funbuns/data_integrity.py` | Data integrity validation |
| `src/funbuns/utils.py` | Config loading, JournalWriter (JSONL), get_data_dir() |

### Rust native plugin
| File | Role |
|------|------|
| `funbuns_native/src/lib.rs` | Polars plugin: omega, big_omega, mu, dominant_share/q/exp, is_prime_power, largest_prime_factor, v_ell, full_profile. Uses `rug` (GMP). |
| `src/funbuns/native_expr.py` | Python bindings for the Rust plugin expressions |

### Analysis modules
| File | Role |
|------|------|
| `src/funbuns/ladic.py` | l-adic analysis, gap-filling deep dives (SageMath factor()) |
| `src/funbuns/spectral.py` | Obstruction indicator FFT, clock superposition |
| `src/funbuns/baker_circle.py` | Baker circle / archimedean H^1 visualization |
| `src/funbuns/remainder.py` | Incremental remainder profiling (uses full_profile) |
| `src/funbuns/zipf.py` | Bounded-memory Zipf/Mandelbrot fitting on q_k frequencies |
| `src/funbuns/fixed_mod.py` | Fixed-modulus ring analysis (needs SageMath rewrite) |
| `src/funbuns/data_exploration.py` | q-chains, bipartite adjacency, gap anatomy |

### Infrastructure
| File | Role |
|------|------|
| `src/funbuns/__main__.py` | CLI entry point (math analysis only) |
| `src/funbuns/admin.py` | Infrastructure CLI: db, serve, notebook |
| `src/funbuns/webserver.py` | FastAPI partition browser (port 8081) |
| `src/funbuns/querydb.py` | DuckDB backend (partition_counts + decompositions) |
| `src/funbuns/viewer.py` | Altair dashboard generator |
| `src/funbuns/run_ingester.py` | Run ingestion utilities |

### Notebooks and scripts
| File | Role |
|------|------|
| `notebooks/exploration.ipynb` | Interactive analysis (DuckDB + SageMath + VegaFusion) |
| `scripts/check_pq_bound.py` | Verifies at-most-2 solutions per (p,q) pair |
| `scripts/bench_core.py` | Core pipeline benchmarking |

## Tests

**No tests exist yet.** Priority areas for test coverage:

### Data integrity
- Duplicate row detection across block files
- Schema validation (correct column names, types, no nulls in p)
- Block file naming consistency (pp_b{N}.parquet numbering)
- Obstructed prime marker correctness (q_k=0 rows)

### End-to-end pipeline
- Small pathological input: known primes with known decompositions
- Round-trip: generate -> write parquet -> read back -> verify
- Graceful exit handling (SIGINT/SIGTERM during generation)
- Resume correctness (picks up exactly where it left off)

### Concurrency
- No row collisions in core.py multiprocessing (imap_unordered)
- No partial writes on process kill
- Memory limits respected (streaming collect, bounded counters)

### CLI
- All flags actually invoke their intended code paths
- `--partition-k 0` returns obstructed primes
- `funbuns-admin db status` works on fresh vs built database
- Error messages for missing database, missing parquet dir

### Query correctness
- DuckDB partition_counts.k matches actual row count in decompositions
- `decompositions_up_to()` returns correct k values
- Filter expressions parse correctly: `2*i+1`, `3..10`, `3,5,7`
- Filtered queries return same results as unfiltered + post-filter

### Native plugin
- Rust functions agree with SageMath for small inputs
- full_profile struct fields are consistent (single factorization)
- Edge cases: p=2 (even prime), p=3 (smallest odd prime), large primes near u64 max

## MCP server: research-reader

Provides access to the research library at `~/fluid/research/library/`.
Server source: `~/fluid/research/library/.mcp-server/main.py` (PEP 723 inline deps, run via `uv run --script`).

Tools:
- `read_djvu(path, pages?)` -- extract text via djvutxt
- `read_epub(path, chapter?)` -- extract text via ebooklib; omit chapter for TOC
- `list_library()` -- list all PDF/DJVU/EPUB files with sizes
- `search_library(query)` -- filename substring search
- PDFs: use Claude Code's native `Read` tool (supports page ranges)

Library managed by Calibre (`calibredb --with-library`).

## Related projects

| Project | Location | Approach |
|---------|----------|----------|
| **factorsums** | `../factorsums/` | SageMath-based. Original p^j*q^k + p^l*q^m formulation. Hierarchical prime power detection, canonical form conventions. Uses pandas (legacy). |
| **facsums** (pppart) | `../facsums/` | NumPy-vectorized pipeline. Bitshift power-of-2 detection `(n & (n-1)) == 0`, multi-stage mask filtering, upfront validity masks. Hit combinatorial explosion in partition pair generation. Also contains HallFreePartitions (coprime two-term partitions). |

Both are predecessors. funbuns differs by:
- Fixing one base to 2 (p = 2^m + q^n), avoiding combinatorial explosion
- Using Polars instead of NumPy/pandas
- Rust plugin for expensive per-row operations (GMP-backed)
- DuckDB for indexed queries over billion-row datasets
- Cohomological framing (obstruction theory, Baker's method)

## Pending work

See memory files for details. Key items:
1. `fixed_mod.py` SageMath rewrite (replace pure-Python arithmetic)
2. Phase 7 cleanup (delete stale data: zipf parquet 3.2GB, ladic_blocks 9GB+)
3. Viewer deprecation (`streaming=True` -> `engine="streaming"`)
4. `--paranoid` mode (recompute from source, don't trust metadata)
5. DuckDB partial results investigation (q=3 counts truncated)
