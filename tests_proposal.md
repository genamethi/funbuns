# Test Proposal: funbuns core pipeline

Status: DRAFT -- all tests reviewed for soundness. Ready for user sign-off
before implementation.

Scope: core.py, utils.py, __main__.py, block_manager.py, block_catalog.py,
data_integrity.py, dataprep.py, run_ingester.py. NOT db, analysis, or
peripheral tools.

## Known issues to address alongside tests

- `detect_overlaps_between_blocks()` joins on just `p`, which flags shared
  primes across blocks as "overlaps." This is wrong -- a prime's partition
  rows can legitimately span blocks. The real concern is ROW duplication:
  identical (p, m_k, n_k, q_k) tuples. This function needs reworking or
  removal. (Tracked as block manager annotation task.)

- **No signal handling in PPManager.** ctrl+c kills workers abruptly, loses
  in-flight results, may leave partial run files. Fix: SIGINT handler in
  `run_gen()` that terminates pool, flushes received results, prints summary.
  Workers should ignore SIGINT (pool initializer sets SIG_IGN). SIGTSTP
  (ctrl+z) is a follow-up: print PID + fg instructions, then re-raise.

- **JournalWriter not wired into generation pipeline.** Only zipf, spectral,
  and remainder use it. PPManager.run_gen(), funbuns-admin, and bmgr produce
  no structured log entries. `setup_logging()` (stdlib logging to
  `logs/decomp_analysis.log`) is only called in __main__.py generation path.
  Proposal: each entry point gets its own journal file or shares one with
  distinct module tags. Event labels should cover: invocation (entry point,
  args, timestamp), batch completion, flush, run completion, errors, resume
  point. Different files per entry point:
    - `logs/funbuns.jsonl` -- generation + analysis runs
    - `logs/admin.jsonl` -- db, serve, notebook
    - `logs/bmgr.jsonl` -- block management operations

- **Flush strategy needs rethinking.** `__main__.py:298` sets
  `buffer_size = batch_size * 2`, coupling two independent concerns.
  batch_size controls IPC granularity (larger = less overhead, user
  reports 100K much faster than 10K). buffer_size controls disk write
  frequency. With batch_size=1M and 12 workers, each batch returns ~2M
  rows, so flush fires on nearly every result -- excessive I/O.
  Conversely, smaller batch sizes delay flushing even when memory is
  available.
  - **Desired behavior:** use ~70% of available memory before flushing,
    without flushing too often. The flush trigger should be
    memory-aware, not row-count-coupled-to-batch-size.
  - **Empirical data (1.3B primes, 2026-03-27):**
    - avg_k = 1.88 (trending toward 2 at scale)
    - rows_per_prime = 2.06 (includes k=0 marker rows)
    - Per-batch memory at 12 workers:
      - batch=100K: ~150 MB peak (workers + queue + buffer)
      - batch=1M: ~1.5 GB peak (numpy arrays only; ~3-4 GB with
        SageMath Integer objects in prime_range lists)
    - Current `_memory_pressure()` (psutil > 50%) exists as safety
      valve but threshold is too conservative and not the primary
      trigger.

- **Stale `PARTITION_DISTRIBUTION` constant.** `utils.py:189` has
  `avg_rows_per_prime = 1.7`. Actual value from 1.3B primes is 2.06.
  This affects `process_batch` initial array allocation (line 65:
  `estimated_rows = len(prime_list) * 1.7 * 1.2`). With the real ratio,
  the 1.2x headroom barely covers it, causing more `_grow_array()`
  doublings than intended. The other constants are also stale:
  - `avg_partitions_per_prime`: stated 2.05, actual avg_k for k>0 is
    2.28
  - `zero_probability`: stated 0.173, actual 17.46% (close enough)
  - `max_observed_partitions`: stated 14, actual max k=15

- **No paranoid mode or automatic integrity escalation.** Current
  validation layers (interval merge, Dusart bounds, optional PARI
  prime_pi) cannot detect isolated missing primes within a block.
  Desired: a `--paranoid` flag that enumerates all primes in the
  covered range and verifies each appears in the data. Additionally,
  automatic escalation during run ingestion: track rolling delta of
  observed count vs Li(x); if |delta| exceeds C·√x·ln(x) (Schoenfeld
  1976, RH-conditional), auto-trigger paranoid. At our scale,
  breaching this bound is a near-certain data integrity signal.
  See xfail tests X6-X8.

- **Inconsistent directory creation.** `get_data_dir()` creates `data/` on
  fallback, `get_log_dir()` creates `logs/`, `append_data()` creates
  `runs/`. But `blocks_dir()` does NOT create the directory. If
  `FUNBUNS_DATA_DIR` points to a nonexistent path, some functions fail
  silently and others raise.

## Oracle strategy

The core.py tests use a reference that is algorithmically independent:
- `is_prime_power(proof=True, get_data=True)` as the oracle (core uses
  `proof=False`)
- Seeded `random_prime()` for reproducible test primes across ranges

## Pytest configuration

### pytest.ini
```ini
[pytest]
addopts = -ra -v --strict-markers
testpaths = tests
pythonpath = src
markers =
    slow: long-running or subprocess integration tests
    sage: requires SageMath (PARI/FLINT)
    xfail: expected failures (broken functions, unimplemented features)
norecursedirs = .git .pixi data notebooks funbuns_native build dist *.egg-info logs
filterwarnings =
    ignore::DeprecationWarning
```
`--strict-markers` prevents marker typos. `pythonpath = src` handles
imports. Specific deprecation filters (Polars streaming API, SageMath)
added as they surface.

### Dependencies
- `pytest-mock` (ships `mocker` fixture)
- `pytest-timeout` (optional, for `@pytest.mark.timeout(N)`)

### Test structure
- Class-based organization with descriptive docstrings (pppart+adics
  pattern): `class TestResumeP: """Resume from block filenames."""`
- Helper functions prefixed with `_` for test data creation
  (e.g., `_make_block_parquet(tmp_path, primes, partitions)`)
- No conftest.py initially -- fixtures local to test files. Extract to
  conftest if shared across 3+ files.
- `xfail` tests for known broken behavior (e.g., T7:
  `detect_overlaps_between_blocks` joins on p only). These document
  the defect and will start passing when the fix lands.

### Patterns (from facsums + pppart+adics)
- Known-value verification (static expected sets, assert set equality)
- Boundary conditions (smallest valid input, edge of valid range)
- Error message content checks (`pytest.raises` + `str(excinfo.value)`)
- Performance timeouts (`@pytest.mark.timeout(N)` or `time.time()`)
- Output parsing helpers for CLI tests (capsys + structured parsing)
- Subprocess integration tests for CLI entry points (marked `slow`)
- Comprehensive edge cases for file I/O: empty files, corrupt data,
  missing columns, large numbers

## Test list

### core.py (5 tests)

**T1: Triple identity** [APPROVED]
- `process_batch` takes a list of SageMath Integer primes (not indices).
  `worker_batch` is the index-based interface that resolves via
  `Primes().unrank()` + `prime_range()`.
- Generate seeded random primes via `random_prime(2^k, lbound=2^(k-1),
  proof=True)` at k=21,25,31,35,40 (5 primes per range, 25 total).
- Run `PPBatchProcessor().process_batch([p])` for each.
- For each output row where q_k > 0: assert `2**m_k + q_k**n_k == p`,
  `Integer(q_k).is_prime(proof=True)`, `Integer(p).is_prime(proof=True)`.
- For q_k == 0: assert obstructed.

**T2: Obstructed primes** [APPROVED, revised]
- Static list of ~25 known k=0 primes drawn from different magnitude ranges
  (small, medium, large). Obtain these by querying existing data or
  computing with paranoid-style verification.
- For each: run `process_batch([p])`, assert output is single row
  (p, 0, 0, 0).
- Separately, create a temp script that verifies each obstructed prime
  paranoid-style: for every m in `1..floor(log2(p))`, confirm
  `(p - 2^m)` is NOT a prime power via `is_prime_power(proof=True)`.
- This script runs once to validate the static list, not on every pytest run.

**T3: worker_batch index dispatch** [APPROVED]
- Call `worker_batch(start_idx=10, count=20)`.
- Verify output p values match
  `prime_range(Primes().unrank(10), Primes().unrank(30))`.

**T4: PPConsumer flush behavior** [APPROVED]
- Use `pytest-mock` (`mocker` fixture) for mock `save_callback`.
- Add DataFrames until `buffer_size` is exceeded.
- Assert callback fires at threshold.
- Call `finalize()`, assert remaining buffer flushed.

**T5: PPBatchFeeder arithmetic** [REVIEWED]
- Construct feeder with known init_p, num_primes, batch_size.
- Assert `generate_batches()` yields contiguous `(start_idx, count)`
  tuples covering exactly num_primes.
- Edge case: `num_primes % batch_size != 0` -> `ValueError`.
- Note: constructor calls `prime_pi(init_p)` (SageMath), so needs `sage`
  marker. Test with small init_p (e.g., 29) to keep fast.

### data_integrity.py (5 tests)

**T6: detect_duplicates_in_block** [REVIEWED]
- Write temp parquet with known duplicate `(p, m_k, n_k, q_k)` rows.
- Assert returns correct count.
- No duplicates -> 0.
- Truncated/corrupt file -> raises `IOError`.
- Implementation detail: uses `df.unique(keys)` on all 4 EXPECTED_KEYS.
  Test should verify dedup is on full row, not just p.

**T7: cross-block row duplication** [DEFERRED]
- `detect_overlaps_between_blocks()` joins on `p` only -- fundamentally
  wrong for this purpose. Shared p values across blocks are normal; only
  identical (p, m_k, n_k, q_k) row tuples are actual duplicates.
- This function needs rewriting before a meaningful test can be written.
- Mark as `xfail` or defer until the block manager annotation task
  corrects the function.
- When corrected, test plan:
  - Two blocks with an exact duplicate row (all 4 cols) -> detected.
  - Two blocks sharing p values but distinct (m_k, n_k, q_k) -> no error.

**T8: detect_gaps** [REVIEWED]
- Construct `BlockInfo` list with an intentional hole (gap_end -
  gap_start > natural gap bound at ~5*(ln p)^2).
- Assert `detect_gaps()` finds it.
- Contiguous ranges -> empty list.
- Single block -> empty list (needs len(merged) >= 2).
- Note: `detect_gaps` takes a `List[BlockInfo]` directly, no disk I/O.
  Construct `BlockInfo` objects with synthetic min_prime/max_prime.
- **Coarseness:** detect_gaps is layer 1 -- it catches inter-block
  gaps that exceed the Cramér bound (5x safety factor). Gaps below
  that bound are invisible at this layer. This is by design: cheap
  O(B) first pass over block metadata only.
- Test a sub-threshold gap (e.g., gap of 150 at p ~ 10^6, where
  bound is ~5*(13.8)^2 = 953) -> NOT detected. This documents the
  intended coarseness, not a bug.

**T9: validate_completeness_fast** [REVIEWED]
- Pass constructed `BlockInfo` lists via the `infos` parameter (avoids
  disk I/O and dependency on real block files).
- **Layer 1 (gaps):** Gap between blocks -> INCOMPLETE, returns early.
- **Layer 2 (structural):** No gaps + overlapping blocks + starts at
  p<=3 + no corrupt -> COMPLETE. This relies on deterministic
  generation: prime_range is exhaustive, so overlapping blocks
  guarantee full coverage without counting. No SageMath, no data reads.
- **Layer 3 (counting via Dusart):** No gaps, no overlaps ->
  per_block_sum is exact unique count (no double-counting). Compare
  against Dusart (2010) unconditional bounds on pi(actual_max).
  Within bounds -> COMPLETE. Outside -> `needs_exact=True`.
  Below Dusart upper validity threshold (actual_max < 2,953,652,287)
  -> always `needs_exact=True`.
- **Layer 4 (exact, separate function):** `verify_count_exact()` calls
  PARI `prime_pi()` -- O(x^{2/3}), fast for x < 10^13, essentially
  free at our scale (~25B). Only invoked by `comprehensive_diagnosis`
  with `--verbose` when `needs_exact=True`.
- **Blind spot:** no layer checks intra-block gaps (block claims
  range [3, 100] but is missing p=47 internally). Layer 3 catches
  this only if enough primes are missing to breach Dusart bounds.
  Layer 4 catches any count mismatch. Isolated missing primes within
  the Dusart error margin are invisible until layer 4.
- Test cases:
  - Case 1 (layer 2): overlapping, starts at 3, no corrupt -> COMPLETE.
  - Case 2 (layer 1): gap between blocks -> INCOMPLETE.
  - Case 3 (layer 3): below Dusart threshold -> `needs_exact=True`.
  - Case 4: all corrupt -> empty valid list, complete=False.
  - Case 5 (layer 3): above Dusart threshold, per_block_sum within
    bounds -> COMPLETE. per_block_sum outside bounds -> needs_exact.

**T10: _dusart_pi_bounds** [REVIEWED]
- Pure math function, no side effects. Verify bounds bracket
  `prime_pi(x)` for known x values.
- Test at: x = 10^8 (above lower threshold 88,783 but below upper
  threshold 2,953,652,287 -- lower bound valid, upper bound valid
  only by convention), 10^10, 10^11 (above both thresholds).
- Assert `lower <= prime_pi(x) <= upper` for valid x.
- Below lower threshold (x < 88,783): function returns values but
  lower bound is NOT proven valid. Document this; don't assert
  bracketing.
- Needs `sage` marker for `prime_pi` oracle.
- Note: the formula is `pi(x) ~ (x/ln x)(1 + 1/ln x + c/ln^2 x)`
  with c=2.0 (lower) and c=2.334 (upper). These are Dusart's
  Theorem 6.9 coefficients. The test validates the implementation
  against the theorem, not the theorem itself.

### block_catalog.py (3 tests)

**T11: _parse_block_filename** [REVIEWED]
- Pure regex, no side effects. Test cases:
  - Valid: `pp_b001_p7249729.parquet` -> `(1, 7249729)`.
  - Invalid: `random.parquet` -> `(None, None)`.
  - Edge: `pp_b000_p2.parquet` -> `(0, 2)`.
  - Large numbers: `pp_b2675_p25165843009.parquet`.
  - Directory component ignored (uses `path.name`).

**T12: _fast_block_bounds** [REVIEWED]
- Write temp parquet with known p values (include duplicates to verify
  `n_unique` differs from row count).
- Assert `(min_p, max_p, rows, unique)` match expected.
- Corrupt file (e.g., write random bytes to .parquet) -> returns
  `(None, None, None, None)`.
- Missing "p" column -> returns `(None, None, None, None)` (exception
  caught).

**T13: sorted_blocks_by_data** [REVIEWED]
- Write 3+ temp parquets with out-of-order filenames but known data
  ranges (e.g., b003 has smallest primes, b001 has largest).
- Pass file list to `sorted_blocks_by_data(files=...)`.
- Assert result sorted by min_prime.
- Corrupt file sorts to end (min_prime=None -> float("inf")).

### utils.py (4 tests)

**T14: resume_p** [REVIEWED]
- Set `FUNBUNS_DATA_DIR` env var to temp dir with `blocks/` subdir.
- Create empty parquet files with known `pp_b{N}_p{max}.parquet` names.
- Assert `resume_p()` returns max prime from filenames.
- No block files -> `None`.
- Files with unparseable names (no pp_b pattern) -> `None` (best_p
  stays 0).

**T15: append_data** [REVIEWED]
- Set `FUNBUNS_DATA_DIR` to temp dir.
- Write a DataFrame via `append_data()`.
- Assert run file created in `{temp}/runs/`, readable, matches input
  schema and data.
- Filename format: `pparts_run_{timestamp}_{pid}.parquet`.
- Write two DataFrames rapidly -> two distinct files (PID + microsecond
  timestamp prevents collision).

**T16: JournalWriter** [REVIEWED]
- Construct with explicit temp path.
- Write 3 events via `.log(module, event, **payload)`.
- Read back JSONL lines.
- Assert each line parses as JSON with keys: `ts`, `module`, `event`.
- Assert payload keys present (e.g., `log("core", "batch_done",
  count=100)` -> `"count": 100` in the line).
- Assert `ts` is valid ISO 8601 with UTC timezone.

**T17: get_data_dir hierarchy** [REVIEWED]
- Use `monkeypatch` to control env vars and config file.
- Case 1: `FUNBUNS_DATA_DIR` set -> returns that path.
- Case 2: Env var unset, pixi.toml has `data_dir` in
  `[tool.funbuns.directories]` -> returns config value.
- Case 3: Env var unset, no config -> returns `Path('data')` and
  creates it.
- Note: `get_config()` reads `pixi.toml` from CWD (`get_config_file()`
  returns `Path('pixi.toml')`). Test should use `monkeypatch.chdir()`
  to a temp dir with a synthetic pixi.toml, or mock `get_config`.

### run_ingester.py (2 tests)

**T18: integrate_runs_into_blocks** [REVIEWED]
- Set `FUNBUNS_DATA_DIR` to temp dir. Create `runs/` and `blocks/` subdirs.
- Write 3 run parquets with known data (some duplicate rows across files).
- Call `integrate_runs_into_blocks(target_prime_count=...,
  delete_run_files=False)`.
- Assert: blocks created in `blocks/`, data sorted by p, duplicates
  removed (by full row via `.unique()`), all primes accounted for.
- Call again with `delete_run_files=True` -> run files removed.
- Note: function calls `list_block_files()` and `blocks_dir()` which
  use `get_data_dir()` internally. The env var override is sufficient.
- **Run file deletion safety:** currently, `delete_run_files=True`
  deletes run files after block writes complete (line 200-205), but
  there is no integrity verification between writing blocks and
  deleting runs. If the blocks are corrupt (partial write, disk full),
  the run data is gone. A pending integrity check should prevent
  deletion. See X9.

**T19: partial last block absorption** [REVIEWED]
- Setup: temp dir with one undersized block (fewer unique primes than
  `target_prime_count`) + new run files.
- Call `integrate_runs_into_blocks(target_prime_count=...)`.
- Assert: the old undersized block is deleted (line 103: `unlink()`),
  its data is merged with run data into new blocks.
- Assert: new blocks contain union of old block + run data, deduplicated.
- Note: the function checks filename max_p vs content max_p (line 83).
  The test block's filename must have correct max_p.
- **Crash safety concern:** the old undersized block is deleted at
  line 103 BEFORE new blocks are written (lines 178-185). A crash
  between these points loses both the old block and the run data.
  See X9.

### dataprep.py (1 test)

**T22: prepare_prime_powers** [REVIEWED, new]
- Call `prepare_prime_powers(n=100, max_power=10)` with
  `FUNBUNS_DATA_DIR` set to temp dir.
- Assert output parquet exists, has column "1" (primes) and columns
  "2" through "10" (powers).
- Verify a few known values: e.g., prime 7 -> column "3" = 343.
- Verify overflow handling: large prime^high_power that exceeds Int64
  should be 0 (bounded mode).
- Needs `sage` marker.

### __main__.py (2 tests)

**T20: CLI flag routing** [REVIEWED]
- Patch `sys.argv` and mock target functions.
- `['funbuns', '--ladic']` -> `run_ladic_analysis` called (needs
  `_check_block_data` mocked to return True).
- `['funbuns', '--spectral']` -> spectral functions called.
- `['funbuns', '--partitions']` -> `run_exploration` called with
  `partitions=True`.
- `['funbuns', '--baker-circle', '149']` -> `run_baker_circle(149, ...)`
  called (no block data check needed).
- `['funbuns']` (no args) -> `parser.print_help()` called, function
  returns.
- `['funbuns', '--genpp', '100']` -> `prepare_prime_powers(100)` called,
  returns early before block data check.
- Note: many analysis modes do lazy imports (e.g., `from .ladic import
  run_ladic_analysis`). Mock at the module level or patch the import.

**T21: argument group coverage** [REVIEWED]
- Test the argparse setup independently by calling
  `parser.parse_args(...)` with various flag combos.
- **Implication rules:**
  - `--ladic-limit 50` implies `--ladic` (line 126-127).
  - `--fixed-mod-limit 100` implies `--fixed-mod`.
  - `--remainder-limit 200` implies `--remainder`.
- **Explore mode triggers** (any of these triggers 'explore' in
  analysis_modes, lines 159-163):
  - `--explore`, `--explore-q Q`, `--explore-m M`, `--tree`,
    `--local-global`, `--partitions`, `--partition-k K`,
    `--partition-p P`, `--partition-max-p P`.
  - `--explore-q 3` without `--explore` -> triggers explore, but
    `args.explore` stays False. `run_exploration` receives
    `q=3, explore=False` -- verify it handles this.
- **`--local-global`** says "implies --explore, default q=3" in help
  but only triggers at analysis_modes level (line 160), does NOT set
  `args.explore=True` or `args.explore_q=3`. The `run_exploration`
  function receives `local_global=True` and must handle defaults
  internally. Test that this works end-to-end.
- **`--tree` says "requires --explore-q"** in help but no enforcement
  in argparse. `--tree` alone triggers explore mode without a q value.
  Test: `['funbuns', '--tree']` -> should either error or have
  `run_exploration` handle the missing q gracefully.
- **`-vv` / `-d` alias quirk:** `-vv` is a single flag alias for
  `--debug`, NOT `-v -v`. So `funbuns -vv` sets debug=True but
  verbose=False. `funbuns -v -v` is an error (duplicate `-v`). This
  is probably unintentional. Document behavior.
- **`--partition-k 0`** -> should query obstructed primes (k=0 means
  no partitions). Verify this routes correctly.
- `-n` without analysis flags -> generation mode.
- `--ladic --spectral` -> both run sequentially (no mutual exclusion).

### xfail tests (known issues, expected to fail until fixed)

**X1: graceful SIGINT exit** [xfail]
- Spawn `PPManager.run_gen()` in a subprocess with a small batch.
- Send SIGINT after first batch completes.
- Assert: process exits cleanly (returncode 0 or specific exit code),
  no zombie workers, any received results flushed to disk.
- Currently: unhandled KeyboardInterrupt, workers killed, data lost.
- Marked `@pytest.mark.slow` + `@pytest.mark.xfail(reason="no signal
  handling in PPManager")`.

**X2: generation pipeline journal logging** [xfail]
- Run a small generation (`-n 1000 -b 1000`).
- Assert journal file exists with entries for: run_start (args, init_p),
  batch_complete (count, primes_processed), flush (rows, file), run_end
  (total_primes, duration).
- Currently: no JournalWriter calls in core.py or PPManager. Only
  `logging.info` in `append_data` when verbose=True.
- Marked `@pytest.mark.xfail(reason="JournalWriter not wired into
  generation pipeline")`.

**X3: funbuns-admin and bmgr logging** [xfail]
- Invoke `funbuns-admin db status` and `bmgr --integrity`.
- Assert journal entries with correct module tags (`admin`, `bmgr`).
- Currently: neither entry point calls `setup_logging()` or
  `JournalWriter`.
- Marked `@pytest.mark.xfail(reason="no logging in admin/bmgr entry
  points")`.

**X4: buffer_size independent of batch_size** [xfail]
- Assert that `PPConsumer.buffer_size` does not scale linearly with
  batch_size. Or: with batch_size=1M, buffer_size should still be a
  reasonable fixed value (e.g., <=500K rows), not 2M.
- Currently: `__main__.py:298` hardcodes `buffer_size = batch_size * 2`.
- Tests the design invariant, not a runtime failure.
- Marked `@pytest.mark.xfail(reason="buffer_size coupled to
  batch_size")`.

**X5: blocks_dir() missing directory** [xfail]
- Set `FUNBUNS_DATA_DIR` to a nonexistent path.
- Call `blocks_dir()`. It returns a Path but does not create it.
- Call `append_data()`. It creates `runs/` under the same root.
- Assert inconsistency: `runs/` exists, `blocks/` does not.
- Currently: `blocks_dir()` is the only data-path function that does
  not `mkdir`.
- Marked `@pytest.mark.xfail(reason="inconsistent directory creation")`.

**X6: paranoid mode — intra-block missing prime detection** [xfail]
- Construct a block covering a known prime range but with one prime
  deliberately omitted from the data.
- Call a paranoid verification function that enumerates all primes in
  [min_p, max_p] and checks each appears in the block.
- Assert: the missing prime is identified.
- Currently: no such function exists. Layers 1-3 of
  `validate_completeness_fast` cannot detect isolated intra-block
  omissions (they check coverage intervals and aggregate counts, not
  individual primes). Layer 4 (`verify_count_exact`) detects the
  count mismatch but not *which* prime is missing.
- Paranoid mode should work both within a single block and across
  block boundaries — the check is against the prime enumeration, not
  block structure.
- Marked `@pytest.mark.xfail(reason="paranoid mode not implemented")`.

**X7: automatic paranoid escalation via Schoenfeld bound** [xfail]
- During run ingestion, track a rolling delta: observed unique prime
  count vs Li(x) (logarithmic integral).
- Graduated response:
  1. Delta within Dusart bounds -> no action (current behavior).
  2. Delta exceeds expected variance over rolling window -> warning
     logged, recommend `--paranoid`.
  3. Delta exceeds C·√x·ln(x) (Schoenfeld 1976: under RH,
     |π(x) - Li(x)| < √x·ln(x)/(8π) for x ≥ 2657) -> auto-trigger
     paranoid without `--paranoid` flag.
- The Schoenfeld bound is RH-conditional, but at our scale (x ~ 25B)
  it's astronomically unlikely to be violated by arithmetic alone.
  Breaching it is a near-certain signal of data loss, not a refutation
  of RH.
- Test setup: construct blocks with a known prime range, remove enough
  primes to push the delta past the Schoenfeld threshold, run
  ingestion, assert paranoid mode auto-triggered.
- Currently: no rolling delta tracking, no Li(x) comparison, no
  graduated escalation. `_memory_pressure` is the only automatic
  trigger in the pipeline, and it's for flushing, not integrity.
- Marked `@pytest.mark.xfail(reason="automatic paranoid escalation
  not implemented")`.

**X8: paranoid mode is optimal for exactness** [xfail]
- When paranoid is triggered (manually or automatically), it should
  use PARI `prime_range()` to enumerate the expected primes in the
  range, then do a set-difference against the observed primes.
  O(n) in the number of primes, O(1) per prime via hash lookup.
- Assert: for a small known range (e.g., primes up to 10^6), paranoid
  returns the exact set of missing primes (if any) and the exact set
  of unexpected entries (non-primes, out-of-range values).
- This is the most expensive check but still optimal for the task —
  you can't verify membership without enumerating.
- Marked `@pytest.mark.xfail(reason="paranoid mode not implemented")`
  + `@pytest.mark.sage`.

**X9: run file deletion before integrity verification** [xfail]
- `integrate_runs_into_blocks` with `delete_run_files=True` deletes
  run files immediately after block writes, with no integrity check
  in between. If the new blocks are corrupt (partial write, disk full,
  crash), the source data is gone.
- Worse: partial block absorption (line 103) deletes the old undersized
  block BEFORE writing new blocks. A crash between delete and write
  loses both the old block data and any run data not yet written.
- Test: simulate a write failure (mock `write_parquet` to raise after
  partial writes). Assert run files are NOT deleted when block writes
  fail. Assert old block is NOT deleted until new blocks are confirmed.
- Desired behavior: delete run files only after verifying the new
  blocks contain all expected data (at minimum: row count matches,
  or a fast integrity check passes). If an integrity check is
  pending/scheduled, defer deletion entirely.
- Marked `@pytest.mark.xfail(reason="run files deleted before
  integrity verification")`.
