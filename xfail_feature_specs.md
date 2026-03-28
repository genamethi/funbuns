# Xfail Feature Specifications

Each section describes a feature corresponding to one or more `xfail` tests.
The tests are already written and will pass when the feature is correctly
implemented. The implementing agent should:

1. Implement the feature per the spec below.
2. Run `pixi run python -m pytest tests/ -v` and confirm the relevant
   xfail(s) now show `XPASS(strict)`.
3. Remove the `@pytest.mark.xfail(...)` decorator from each passing test.
4. Re-run the full suite to confirm 0 failures.

All xfails use `strict=True` — an XPASS is a hard failure until the
decorator is removed.

---

## X1: Graceful SIGINT exit (test_core.py::TestGracefulSIGINT)

**File:** `src/funbuns/core.py` (PPManager.run_gen)

**Problem:** ctrl+c during generation produces a KeyboardInterrupt traceback,
kills workers mid-computation, and loses any completed but unflushed results.

**Spec:**
- Install a SIGINT handler in `run_gen()` before entering the pool context.
- Worker processes should ignore SIGINT (set `signal.SIG_IGN` in the pool
  initializer function) so they don't crash independently.
- On SIGINT, the main process should:
  1. Stop accepting new results from `imap_unordered`.
  2. Call `pool.terminate()` to stop workers.
  3. Call `consumer.finalize()` to flush any buffered results to disk.
  4. Print a summary (primes processed, batches completed, files written).
  5. Exit with return code 0 (not re-raise KeyboardInterrupt).
- No `KeyboardInterrupt` traceback should appear in stdout or stderr.

**Test contract:**
- Subprocess running `funbuns -n 10000 -b 1000` receives SIGINT after
  run files start appearing on disk.
- Asserts: `returncode == 0`, no "KeyboardInterrupt" in output,
  `>= 1` parquet files in `runs/`, total rows `> 0`.

**Implementation freedom:** The handler mechanism, pool shutdown sequence,
and summary format are up to the implementer. The test only checks
observable behavior (exit code, output, files on disk).

---

## X2: Generation pipeline journal logging (test_core.py::TestGenerationJournalLogging)

**File:** `src/funbuns/__main__.py` and/or `src/funbuns/core.py`

**Problem:** The generation pipeline produces no structured log entries.
JournalWriter exists in utils.py but is only used by zipf, spectral,
and remainder modules.

**Spec:**
- When `funbuns -n ... -b ...` runs, create a JSONL journal at
  `{data_dir}/logs/funbuns.jsonl`.
- Required events (each is one JSON line with at minimum `ts`, `module`,
  `event` keys):
  - `run_start`: emitted before pool creation. Include args, init_p.
  - `run_end`: emitted after `consumer.finalize()`. Include total_primes,
    batches_processed, duration.
- Additional events (batch_complete, flush) are encouraged but not tested.

**Test contract:**
- Mocks PPManager to avoid actual computation.
- Asserts: `logs/funbuns.jsonl` exists, contains entries with
  `"run_start"` and `"run_end"` event values.

---

## X3: Admin/bmgr journal logging (test_main.py::TestAdminBmgrLogging)

**File:** `src/funbuns/admin.py`

**Problem:** `funbuns-admin` and `bmgr` produce no structured log entries.

**Spec:**
- `funbuns-admin` commands should log to `{data_dir}/logs/admin.jsonl`.
- Each entry must have `module: "admin"` (or similar identifying tag).
- At minimum: log invocation (subcommand, timestamp) and completion.

**Test contract:**
- Invokes `funbuns-admin db status` (mocked DB).
- Asserts: `logs/admin.jsonl` exists, contains entry with
  `module == "admin"`.

---

## X4: Memory-aware flush strategy (test_main.py::TestBufferSizeIndependence)

**File:** `src/funbuns/__main__.py` (line ~298)

**Problem:** `buffer_size = batch_size * 2` couples flush frequency to IPC
granularity. With batch_size=1M and 12 workers, this means buffer_size=2M,
which triggers a flush on nearly every batch return — excessive I/O.

**Spec:**
- `buffer_size` passed to PPConsumer must not scale linearly with
  `batch_size`.
- Target: ~70% memory utilization before flushing. The flush trigger
  should be memory-aware (e.g., psutil-based, or a fixed sensible cap),
  not a multiple of batch_size.
- With batch_size=1M, buffer_size should be <= 500K rows.

**Test contract:**
- Mocks PPManager, sets batch_size=1M via CLI args.
- Asserts: the `buffer_size` arg passed to PPManager is `<= 500_000`.

---

## X5: Consistent directory creation (test_utils.py::TestBlocksDirCreation)

**File:** `src/funbuns/utils.py` or wherever `blocks_dir()` is defined

**Problem:** `append_data()` auto-creates `runs/` via `mkdir(exist_ok=True)`,
but `blocks_dir()` does not create `blocks/`. Inconsistent behavior.

**Spec:**
- `blocks_dir()` (or equivalent) should create the directory if it
  doesn't exist, same as `append_data` does for `runs/`.

**Test contract:**
- Sets FUNBUNS_DATA_DIR to a fresh empty path.
- Calls `append_data()` (creates `runs/`), then checks `blocks/` exists.
- Currently fails because `blocks/` is never auto-created.

---

## X6: Paranoid intra-block verification (test_data_integrity.py::TestParanoidIntraBlock)

**File:** `src/funbuns/data_integrity.py`

**Problem:** Layers 1-3 of `validate_completeness_fast` check coverage
intervals and aggregate counts. They cannot detect a single missing prime
within a block. No function exists to enumerate and verify individual primes.

**Spec:**
- Implement `paranoid_verify_block(path) -> dict` in `data_integrity.py`.
- Given a parquet file, determine min_p and max_p from the data.
- Enumerate all primes in [min_p, max_p] (via SageMath `prime_range` or
  equivalent).
- Return `{"missing_primes": list[int], ...}` — primes in the range that
  do not appear in the block's `p` column.

**Test contract:**
- Creates a block with primes 2..29 but omits 23.
- Asserts: `23 in result["missing_primes"]`.

---

## X7: Automatic paranoid escalation (test_data_integrity.py::TestSchoenfeldEscalation)

**File:** `src/funbuns/data_integrity.py`

**Problem:** No mechanism to automatically trigger paranoid verification
when observed prime counts diverge suspiciously from expected values.

**Spec:**
- Implement `check_paranoid_escalation(observed_count, max_prime) -> dict`.
- Compare `observed_count` against expected pi(max_prime) using Dusart/
  Schoenfeld bounds.
- Schoenfeld (1976): under RH, |pi(x) - Li(x)| < sqrt(x)*ln(x)/(8*pi)
  for x >= 2657. Breaching this at our scale is near-certain data loss.
- Return `{"trigger_paranoid": bool, "reason": str}`.
- When delta exceeds Schoenfeld bound: `trigger_paranoid=True`,
  `reason="schoenfeld"`.

**Test contract:**
- Calls with observed_count=50, max_prime=100000 (pi(100000) ~ 9592).
- Asserts: `trigger_paranoid is True`, `reason == "schoenfeld"`.

---

## X8: Exact missing-prime enumeration (test_data_integrity.py::TestParanoidOptimal)

**File:** `src/funbuns/data_integrity.py`
**Marker:** `@pytest.mark.sage`

**Problem:** No way to get the exact set of missing primes across a range.

**Spec:**
- Implement `paranoid_verify_range(path, min_p, max_p) -> dict`.
- Uses `sage.all.prime_range(min_p, max_p+1)` to enumerate expected primes.
- Compares against observed primes in the parquet file.
- Returns `{"missing_primes": list[int], ...}`.
- O(n) in number of primes, O(1) per prime via set lookup.

**Test contract:**
- Creates a block covering 2..997 with 5 specific primes removed
  ({23, 149, 373, 509, 877}).
- Asserts: `set(result["missing_primes"]) == {23, 149, 373, 509, 877}`.

---

## X9: Run file deletion safety (test_run_ingester.py::TestRunFileDeletionSafety)

**File:** `src/funbuns/run_ingester.py`

**Problem:** Two crash-unsafe patterns:
1. `delete_run_files=True` deletes run files immediately after block writes,
   with no integrity verification in between.
2. Partial block absorption (line ~103) calls `.unlink()` on the old
   undersized block BEFORE writing its replacement.

**Spec (test a):**
- After writing new blocks and before deleting run files, perform an
  integrity check (at minimum: verify new blocks exist and row counts match).
- The test instruments `write_parquet` and `Path.unlink` to record the
  event sequence and looks for an `"integrity_check"` event between
  writes and deletes.

**Spec (test b):**
- During partial block absorption, do NOT delete the old block until the
  new replacement block is confirmed written to disk.
- If the new block write fails (IOError), the old block must survive.

**Test contracts:**
- **(a)** Runs integration with delete_run_files=True. Asserts event
  sequence contains `"integrity_check"` between `"write_block"` and
  `"delete_run"`.
- **(b)** Mocks write_parquet to fail on new block writes. Asserts old
  block file still exists on disk after the failure.

---

## Overflow detection in dataprep (test_dataprep.py::TestPreparePrimePowers)

**File:** `src/funbuns/dataprep.py`

**Problem:** `prepare_prime_powers` uses `pl.col("1").pow(k) <= int64_max`
to detect overflow, but Polars `.pow()` silently wraps on Int64 overflow.
Values that wrap to positive pass the guard undetected.

**Spec:**
- Replace the overflow detection with a method that catches all overflows,
  not just those that wrap to negative. Options:
  - Compute in Float64 first, compare against threshold, then cast.
  - Use a logarithmic pre-check: `k * log2(p) > 63` implies overflow.
  - Compute in UInt64 and check for wrap-around.

**Test contract:**
- Calls `prepare_prime_powers(n=100, max_power=64, use_bounded=True)`.
- Asserts: column "64" for prime 97 is 0 (97^64 >> 2^63).
