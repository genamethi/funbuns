# Ingest cutover plan (step 7)

This document captures the implementation plan agreed on during the
2026-04-14 session, immediately after the iceberg migration's step 6
(legacy `blocks/` retirement). It is the last design milestone of the
multi-step migration; see `iceberg_prototype_findings.md` for step 1
findings and `iceberg_data_setup.md` for the stable write-side schema.

## Migration state (2026-04-14)

The original 7-step iceberg migration plan:

1. Prototype — **done**
2. Schema lock — **done** (`iceberg_schema.py`)
3. Compactor — **done** (504 batches, 10.1B primes, k_max=16, k-sum invariant verified)
4. Parallel validation — **declared satisfied** by reconciliation during compaction (5 spot-checked blocks across full p range, k-sum invariant, dedup audit)
5. Rust read-path retarget (`funbuns_graph`) — **deferred** pending a webgraph-vs-alternatives evaluation
6. Retire `blocks/` — **done** (130GB deleted from `/media/extssd/research/dioph.pp/data/blocks/`)
7. **Ingest path cutover** — this plan

DuckDB retirement (tracked in issue #2) is a parallel track, not gated
on step 7. Webserver is kept functional-ish in place until notebooks
replace it; see issue #2 for scope.

## Decision: option X (direct-to-iceberg from `PPConsumer`)

Rejected alternatives:

- **Option Y** — keep a `runs/` or `staging/` directory as a crash-safety
  buffer, with periodic drain into iceberg. More complex, two codepaths.
- **Option Z** — in-memory buffer for the whole run, single `write_batch`
  at end. Loses all work on crash; not production-safe at the user's
  scale (smallest run is 100M primes).

Option X was chosen because it matches the "clean solutions, low risk,
easy reconfiguration" design mentality: one codepath, no staging zoo.
The tiny-snapshot concern is real but mitigated by tuning the flush
threshold, not by adding infrastructure.

## Flush semantics

Reuse the existing `buffer_size` config knob in `pixi.toml`
(`[tool.funbuns].buffer_size`, currently `10_000_000` rows). At
`PARTITION_DISTRIBUTION.avg_rows_per_prime = 1.7`, that's ~5.9M primes
per flush.

Snapshot counts at this setting:

- 100M-prime run (smallest) → ~17 snapshots
- 1B-prime run → ~170 snapshots
- 10B-prime run → ~1000 snapshots

All within iceberg comfort zone. Discomfort begins at tens of thousands
of snapshots per table, which is orders of magnitude away.

User-confirmed constraints:

- Smallest run: 100M primes
- Core batch size: 1M primes (best IPC balance)
- No preference on exact flush size — "I'm not as sure about [flushing
  behavior], but that's good"

## Implementation order

Each step is its own commit on `liminal`.

### Step 1. `IcebergWriter` + shared shaping helper

Add to `src/funbuns/iceberg_schema.py`:

- `shape_for_write(raw: pl.DataFrame, *, batch_id: int) -> tuple[pl.DataFrame, pl.DataFrame]`
  Factor out the raw `{p, m_k, n_k, q_k}` → `(primes_df, decomp_df)`
  transformation that currently lives in `scripts/iceberg_compactor.py`
  (`_build_batch_frames`). Same dedup-aware logic: `.unique(subset=["p",
  "m_k", "n_k", "q_k"])`, `q_k > 0` filter for decomp, `group_by(p).agg(k)`
  for primes. Returns frames in the dtypes the validators expect.
- `class IcebergWriter` — owns a `SqlCatalog` handle and a
  `next_batch_id` counter. Constructor initializes the counter via the
  same manifest-scan pattern as `_resume_state` in the compactor (reads
  `readable_metrics.batch_id.upper_bound` via `tbl.inspect.files()`).
  `flush(raw_df: pl.DataFrame)` method calls `shape_for_write` then
  `write_batch`, increments the counter.

### Step 2. Retrofit compactor to shared helper

Update `scripts/iceberg_compactor.py` to call `shape_for_write` instead
of its local `_build_batch_frames`. Verify behavior unchanged via a
dry-run against any single batch. This is a sanity check that the
factoring is correct before the live path depends on it.

### Step 3. Wire `__main__.py` / `utils.py` to the new writer

- `utils.resume_p` — replace block-filename scan with
  `max(primes.p.upper_bound)` via the iceberg manifest. Same
  `tbl.inspect.files()` pattern used throughout.
- `utils.append_data` — delete.
- `utils.setup_resume_mode` / `setup_analysis_mode` — simplified. The
  "append callback" is now an `IcebergWriter.flush` bound method. `temp`
  mode semantics need a decision: does `--temp` still make sense when
  ingest is committing directly to a catalog? Simplest answer: `--temp`
  creates a throwaway iceberg catalog under `get_temp_dir()`.
- `__main__._check_block_data` — delete.
- `__main__.main` — analysis-mode guard dispatch stays; block-data
  guard call sites are removed. Generation path constructs an
  `IcebergWriter`, passes `writer.flush` as the callback to `PPManager`.
- `utils.convert_runs_to_blocks_auto` — delete.
- `__main__` `--init` path — simplified; same writer, same callback.

### Step 4. Delete dead modules and retire tasks

- Delete `src/funbuns/block_catalog.py` (entirely dead)
- Delete `src/funbuns/run_ingester.py` (entirely dead)
- Delete `src/funbuns/block_manager.py` (70% dead, 30% retargetable;
  cleaner to delete outright and build a thinner iceberg integrity tool
  later if needed)
- Delete `src/funbuns/utils.py` dead helpers: `get_default_data_file`,
  `get_temp_data_file`, `show_run_files_summary`,
  `convert_runs_to_blocks_auto`
- Retire pixi tasks: `bmgr`, `bmgr-integrate`, `bmgr-integrity`,
  `bmgr-diagnose`, `bmgr-show-runs`
- `compact` task can stay as a "bulk rebuild" reference or go; either is
  fine

### Step 5. Import audit and cleanup

Every remaining `from .block_catalog import`, `from .block_manager
import`, `from .run_ingester import` in the codebase gets chased and
removed. Run `pixi run python -c "import funbuns"` and fix whatever
explodes.

## Impact classification (preserved reference)

Legend: 🟢 unchanged / 🟡 trivial retarget / 🟠 substantial rewrite /
🔴 dead / 🟣 design-blocked (resolved with option X).

### `__main__.py`

| Section | Status | Notes |
|---|---|---|
| CLI parser, flag plumbing | 🟢 | Unchanged shape |
| `_check_block_data()` | 🔴 | Delete |
| `--ladic-gap`, `--baker-circle`, `--genpp` | 🟢 | Standalone, no data dependency |
| `--view` | 🟣 | Issue #2 (DuckDB retirement) |
| `--ladic`, `--spectral`, `--clocks`, `--fixed-mod`, `--remainder`, `--zipf`, `--explore` | 🟠 | Dispatch stays; each module needs its own retarget to `polars.scan_iceberg` (separate cutover work, not part of step 7) |
| Generation path (`-n`, `-i`) | 🟡 → implemented in step 3 |

### `core.py`

| Section | Status | Notes |
|---|---|---|
| `PPBatchProcessor`, `worker_batch`, `PPBatchFeeder`, `PPManager` | 🟢 | Pure compute, no changes |
| `PPConsumer` | 🟡 | No code change; only the injected `save_callback` target changes |

**core.py itself needs zero edits.** The cutover is entirely in the
callback it receives.

### `block_manager.py`

| Section | Status |
|---|---|
| `BlockManager` class | 🔴 |
| `--convert`, `--show-runs`, `--integrate-check`, `--analyze`, `--summary` | 🔴 |
| `--integrity`, `--diagnose`, `--prefix-check`, `--audit-prefix` | 🟠 (deferred) |
| `--truncate-from-block` / `-prime` | 🟠 (deferred; would become iceberg `ExpireSnapshots`) |

Decision: delete outright in step 4. If an integrity tool is wanted
later, it will be easier to write fresh against the iceberg `primes`
table than to retarget this.

### `block_catalog.py`
🔴 entirely dead. Delete. `tbl.inspect.files()` replaces every function.

### `run_ingester.py`
🔴 entirely dead under option X. Delete.

### `utils.py`

| Function | Status |
|---|---|
| `get_log_dir`, `setup_logging`, `JournalWriter` | 🟢 |
| `get_config`, `get_data_dir`, `get_config_file`, `get_temp_dir` | 🟢 |
| `PARTITION_SCHEMA`, `PARTITION_DISTRIBUTION` | 🟢 |
| `get_default_data_file`, `get_temp_data_file` | 🔴 |
| `resume_p` | 🟡 (retarget to manifest scan) |
| `append_data` | 🔴 |
| `setup_analysis_mode`, `setup_resume_mode` | 🟡 (simplify, drop runs-dir references) |
| `show_run_files_summary` | 🔴 |
| `convert_runs_to_blocks_auto` | 🔴 |

### `funbuns_native`
🟢 **entirely unaffected.** The Rust plugin is a pure Polars expression
provider; it doesn't know anything about data sources. No changes needed
for step 7.

### Notebooks
🟡 swap `DuckDB + parquet glob` cells for `polars.scan_iceberg`.
Separate thread — not part of step 7 critical path.

### Pixi tasks

| Task | Action |
|---|---|
| `serve`, `notebook`, `build-db`, `sync-db`, `db-status` | issue #2 |
| `bmgr*` | delete in step 4 |
| `ladic`, `remainder`, `zipf`, `spectral`, `explore`, `local-global`, `baker-circle` | unchanged; underlying modules retarget separately |
| `compact` | keep as reference (cheap) or delete |
| `build-native`, `build-graph-bin`, `build-graph` | 🟢 |
| `profile*`, `trace*` (dev feature) | 🟢 |

## Deferred decisions (not blocking step 7)

- **Whether analysis modules retarget themselves** — `ladic.py`,
  `spectral.py`, `remainder.py`, `zipf.py`, `data_exploration.py`,
  `fixed_mod.py` all currently scan `blocks/`. Post-step-7, they'll all
  fail at import or first call. User has indicated they'd rather shift
  exploration to notebooks anyway, so these may be retargeted
  opportunistically rather than as a bulk port.
- **bmgr's diagnostic subset reborn as iceberg tool** — the intent of
  `--integrity`, `--prefix-check`, `--audit-prefix` is valid against the
  iceberg primes table. Much simpler code (already deduped and sorted).
  Deferred until there's an actual need.
- **DuckDB retirement (issue #2)** — webserver, `querydb.py`, `admin.py`
  db commands. Parallel track.
- **Rust graph retarget** — pending webgraph-vs-alternatives evaluation.
- **Notebook rewrite** — separate thread, "notebooks replace webserver"
  pivot.

## Session state at plan creation

- Branch: `liminal` (local, ahead of `origin/liminal` by ~10 commits,
  ahead of `origin/claude-assisted` substantially)
- Last commit: `a1e03f5 Iceberg migration: schema module, compactor,
  data setup doc`
- Working tree has pre-existing unrelated changes in `funbuns_graph/`,
  `notebooks/exploration.ipynb`, `src/funbuns/__main__.py`,
  `src/funbuns/core.py`, and untracked scripts. These are **not** part
  of step 7 and should be left alone during the cutover.
- Issue #2 filed: https://github.com/genamethi/funbuns/issues/2
- Legacy `blocks/` deleted (130GB freed)

Implementation approval given by the user at end of session; `/exit`
was invoked before any code was written. **Next session resumes at
step 1.**
