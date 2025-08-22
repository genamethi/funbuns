# Data integrity and block management strategy
_Exported on 8/13/2025 at 21:50:04 MST from Cursor (1.4.5)_

---

**User**

I think we could separate out data integrity and block management, but they'll be tightly woven together with utils.py. In fact, we might find that separation of concerns is blurred a bit. What's your prognonsis for a three unit approach with those? What is left without a neat classification? What is redundant? Where is the overlap/difficult to categorize parts? This bug keeps persisting and I think it has to do with a failure to properly integrate data/runs with data/blocks. At the moment the previous runs have been cleared out. I know most of the core.py logic is sound, I'm not worried about that, main.py or utils.py except for any code that has to do with integrating run data into the blocks. It appears there's a logical mismatch we need to resolve between start index and start prime, but that doesn't occur when I resume from block 55. Review the most recent chat data and get me a plan. Keep in mind teh @pixi.toml @utils.py and organization of files (use of resourcelib, pathlib and so on) Let me know if you have any planning questions.

---

**Cursor**

Quick status: Reviewed your latest trace, `pixi.toml`, and `utils.py` excerpts alongside the recent chat notes. The persistent failure points to the runs→blocks integration and resume state derivation. Below is a tight three-unit plan, where concerns are separated but allowed to blur at the integration seams. I’ll keep it NCIED and aligned with your config/path style and vectorized approach [[memory:5786165]].

### Three-unit architecture prognosis

- **A. Block management (storage topology and lifecycle)**
  - Scope: converting `data/runs/` into `data/blocks/`, sizing blocks, numbering, deduping by row keys, filename accuracy, gap/overlap checks, and append-to-last-block semantics.
  - Core invariants:
    - Blocks are disjoint in prime coverage and sorted by `p`.
    - No duplicate rows in a block (unique on `['p','m_k','n_k','q_k']`).
    - Filenames reflect max `p` in the file; numbering is contiguous; last block may be partially filled.
  - Primary responsibilities:
    - Catalog/coverage tooling for both runs and blocks.
    - Deterministic, idempotent conversion with safety checks when appending to last block.
    - Reject resumption if unconverted runs remain (gate to protect resume state).

- **B. Data integrity (schema, content, and invariants)**
  - Scope: schema validation, dedup across blocks, per-prime partition invariants, “zero-partition” rules, global uniqueness of `p`, and consistency of measured counts.
  - Core invariants:
    - Global `p` uniqueness across the set of blocks (no overlaps).
    - Consistent schema across files; column presence and types stable.
    - Computed counts are reproducible via Polars lazy scans (no Python loops per workspace rule).
  - Primary responsibilities:
    - Integrity scanners and reports (read-only by default).
    - Canonical index–prime mapping: measured `start_idx = n_unique(p)` from blocks.
    - Alert on coverage holes, overlaps, and schema drift.

- **C. Resume/state management (bridge from storage to runtime)**
  - Scope: deriving `init_p` and `start_idx` from storage; emitting a canonical “resume sentinel”; runtime gating based on presence/absence of runs/blocks; mode interactions (monolithic vs blocks vs runs).
  - Core invariants:
    - Resume derives only from blocks (never from runs) when separate mode is active.
    - A single resume sentinel captures: last prime, cumulative unique count, last block path/index, timestamp.
  - Primary responsibilities:
    - Read-only audit that recalculates state directly from the blocks (Polars scan/glob).
    - Explicit failure when runs are present but not integrated into blocks.
    - Stable behavior regardless of filename numbering oddities; sentinel is the source of truth.

### What doesn’t fit neatly
- **Performance/timing**: `TimingCollector` and digest outputs. Useful across units; not purely integrity nor block mgmt nor resume.
- **Environment capture**: versions/build matrices, CPU/memory, Pixi lock snapshots. Orthogonal to data content.
- **Exploratory analysts**: summary and viewer-ish utilities; valuable for operations but not core to the three units.

### Redundancies to trim
- **Overlap in “listing and counting”**: multiple snippets enumerate and summarize blocks/runs. Consolidate into `block_catalog` and `run_catalog` to be the single sources.
- **Dual resume logic**: `resume_p` and ad-hoc summaries replicate work. Centralize into resume audit + sentinel emission, then make resume consume the sentinel exclusively.
- **Block management behaviors**: `block_manager.py`, `integrate_block_manager.py`, and `convert_runs_to_blocks_auto()` share intent. Promote one implementation (the vectorized, idempotent path) and deprecate the rest into a thin wrapper or remove once replaced.

### Overlap/blur that’s expected
- **runs→blocks conversion touches both A and B**: dedup and set-based logic belong to integrity, but the file lifecycle and numbering are block management. Keep the conversion primitive in Block Mgmt, but enforce dedup/uniqueness with Integrity scanners before/after conversion.
- **Resume audit relies on Block Mgmt invariants**: state derivation is meaningless without validated blocks; expect C to depend on A+B outputs.

### Bug-focused hypotheses to validate
- **Directory/config mismatch**: `pixi.toml` has `blocks_dir = "data"` while code often assumes `data/blocks` (e.g., `resume_p()` uses `data/blocks`, but `show_run_files_summary()` lists `data`). Normalize and make all directory usage sourced from config (including `runs_dir`) to eliminate split-brain.
- **Resume gating violation**: logs show “Resuming from … from block files” and “Data mode: Separate run files” in the same session. If any unconverted runs exist, `resume_p()` should refuse to proceed, because start state would not account for them. Even if you cleared runs now, the earlier mismatch suggests the gate was missing when the regression happened.
- **Empty feeder input due to resume mismatch**: the reshape error indicates an empty primes series. If `init_p`/`start_idx` is computed from blocks but the runtime tries to produce primes beginning at an inconsistent location, the computed batch slice could be empty. This disappears when starting at block 55, which implies those files are internally consistent and the sentinel at that cut works, but earlier blocks or the conversion had a coverage hole or overlap.
- **Dedup semantics vs. expected cardinality**: conversion dedups by `['p','m_k','n_k','q_k']`. `resume_p()` counts `n_unique(p)`. If the block coverage is discontinuous (holes or overlaps), `start_idx` can drift relative to the intended generation index. Audit needs to assert both contiguity by block order and monotonicity of max `p` across files.

### Plan (minimal, deterministic, vectorized)

- **Phase 1: Read-only audits**
  - Resume audit: compute `last_prime`, `start_idx = n_unique(p)`, and last block metadata directly from blocks. Fail if any runs exist.
  - Block catalog: per-block min/max `p`, `rows`, `unique p`, detect holes/overlaps and filename–content mismatch.
  - Coverage diff: compare prime sets of runs vs blocks; must be empty before resume.
- **Phase 2: Resume sentinel**
  - Emit a single sentinel file (Parquet/JSON) from the audits: `{last_prime, start_idx, last_block_path, last_block_max_p, last_block_num, timestamp}`. Resume uses this exclusively.
- **Phase 3: Config normalization**
  - Add `blocks_dir = "data/blocks"` and `runs_dir = "data/runs"` to `pixi.toml` and make all utilities route through config consistently (no hard-coded subpaths).
  - Align any lingering helpers (`show_run_files_summary`) to respect config.
- **Phase 4: Conversion guardrails**
  - If `use_separate_runs=True`: refuse resume when `runs_dir` is non-empty; require conversion first.
  - After conversion, integrity scan must pass before sentinel update.
- **Phase 5: Consolidation**
  - Deprecate overlapping temp scripts in favor of the toolbelt: `block_catalog`, `run_catalog`, `coverage_diff`, `resume_audit`, `integrity_scan`, `resume_sentinel`. Keep legacy scripts in `temp-scripts/` for reference, not deletion [[memory:5915914]].

### Planning questions
- Can we standardize `blocks_dir` to `data/blocks` and introduce `runs_dir` in `pixi.toml`, then refactor all directory access to come from config first, env vars second, and fallbacks last?
- For the sentinel, do you prefer JSON (easier to diff) or Parquet (schema stability, vectorized reads), or both?
- Should resume strictly refuse to run if runs are present (to prevent the mixed “block resume + run mode” state that led to the mismatch)?
- When you “resume from block 55,” is that via a specific CLI flag or by pruning to a known-good subset? If there’s a documented point-in-time that works, I’ll make the audits diff that state against the current to isolate where divergence began.
- Any constraints on block size target (currently ~500k primes in the conversion tool), or should we lift this into config as `target_block_prime_count`?

- Consolidated the three-unit plan (Block Mgmt, Integrity, Resume/State) with explicit invariants and responsibilities.
- Identified misconfig and overlap hotspots (`blocks_dir` vs hard-coded paths; mixed mode resume; conversion dedup vs unique-prime counts).
- Proposed a read-only audit-first toolbelt and a single resume sentinel as the source of truth, with config normalization and conversion gating to prevent future regressions.

