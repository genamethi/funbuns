# Xfail Feature Specifications

Each section describes a feature with a corresponding `xfail` test already
in the codebase. The test is written and will pass when the feature is
correctly implemented. After implementation:

1. Run `pixi run python -m pytest tests/ -v`.
2. The relevant xfail(s) will show `XPASS(strict)` — this is a hard failure.
3. Remove the `@pytest.mark.xfail(...)` decorator from each passing test.
4. Re-run the full suite: 0 failures confirms correctness.

All xfails use `strict=True`.

---

## X1: Graceful SIGINT exit

**Location:** `test_core.py::TestGracefulSIGINT`
**Modifies:** `src/funbuns/core.py` (PPManager.run_gen)

**Problem:** ctrl+c during generation produces a KeyboardInterrupt traceback,
kills workers mid-computation, and loses any completed but unflushed results.

**Spec:**
- Install a SIGINT handler in `run_gen()` before entering the pool context.
- Worker processes should ignore SIGINT (set `signal.SIG_IGN` in the pool
  initializer) so they don't crash independently.
- On SIGINT, the main process should:
  1. Stop accepting new results from `imap_unordered`.
  2. Terminate the pool.
  3. Flush any buffered results to disk via `consumer.finalize()`.
  4. Exit with return code 0 (no re-raised KeyboardInterrupt).
- No `KeyboardInterrupt` traceback should appear in stdout or stderr.
- Implementation details (handler mechanism, pool shutdown sequence,
  summary format) are up to you.

**Exact test the implementation must pass:**

The test spawns a subprocess running `funbuns -n 10000 -b 1000`. It waits
for parquet files to appear in `{tmp_path}/runs/` (proving generation is
producing data), then sends SIGINT. Assertions:

```python
# 1. Clean exit, not KeyboardInterrupt crash (1) or signal death (-2)
assert proc.returncode == 0

# 2. No traceback leaked
combined = stdout.decode() + stderr.decode()
assert "KeyboardInterrupt" not in combined

# 3. Completed results flushed to disk
run_files = list(runs_dir.glob("*.parquet"))
assert len(run_files) >= 1

# 4. Flushed data is valid
total_rows = sum(pl.read_parquet(f).height for f in run_files)
assert total_rows > 0
```

---

## X2: Generation pipeline journal logging

**Location:** `test_core.py::TestGenerationJournalLogging`
**Modifies:** `src/funbuns/__main__.py` and/or `src/funbuns/core.py`

**Problem:** The generation pipeline produces no structured log entries.
`JournalWriter` exists in `utils.py` but is only used by zipf, spectral,
and remainder modules.

**Spec:**
- When `funbuns -n ... -b ...` runs, create a JSONL journal at
  `{data_dir}/logs/funbuns.jsonl`.
- Each line is a JSON object with at minimum `ts`, `module`, `event` keys.
- Required events:
  - `run_start`: emitted before pool creation. Include args, init_p.
  - `run_end`: emitted after consumer finalize. Include total_primes, duration.
- Use the existing `JournalWriter` class from `utils.py`.

**Exact test:**

The test mocks `PPManager` (no real computation) and checks that journal
entries are written during the `main()` flow:

```python
# After running main() with mocked PPManager:
journal = tmp_path / "logs" / "funbuns.jsonl"
assert journal.exists(), "No journal file created"

entries = [json.loads(line) for line in journal.read_text().strip().split("\n")]
events = [e["event"] for e in entries]
assert "run_start" in events
assert "run_end" in events
```

---

## X3: Admin/bmgr journal logging

**Location:** `test_main.py::TestAdminBmgrLogging`
**Modifies:** `src/funbuns/admin.py`

**Problem:** `funbuns-admin` and `bmgr` produce no structured log entries.

**Spec:**
- `funbuns-admin` commands should log to `{data_dir}/logs/admin.jsonl`.
- Each entry must have a `module` key with value `"admin"`.
- At minimum: log invocation (subcommand, timestamp) and completion.

**Exact test:**

```python
# After invoking funbuns-admin db status (with mocked QueryDB):
journal = tmp_path / "logs" / "admin.jsonl"
assert journal.exists(), "No admin journal file created"

entries = [json.loads(line) for line in journal.read_text().strip().split("\n")]
modules = [e.get("module") for e in entries]
assert "admin" in modules
```

---

## X4: Memory-aware flush strategy

**Location:** `test_main.py::TestBufferSizeIndependence`
**Modifies:** `src/funbuns/__main__.py` (line ~298)

**Problem:** `buffer_size = batch_size * 2` couples flush frequency to IPC
granularity. With batch_size=1M, buffer_size=2M triggers a flush on nearly
every batch return.

**Spec:**
- `buffer_size` passed to PPConsumer must not scale linearly with batch_size.
- Target: ~70% memory utilization before flushing.
- With batch_size=1M, buffer_size must be <= 500K rows.
- The flush trigger should be memory-aware (e.g., psutil-based) or a
  fixed sensible cap — not a multiple of batch_size.

**Exact test:**

The test mocks PPManager, passes `batch_size=1_000_000` via CLI, and
inspects what buffer_size was forwarded:

```python
# Extract buffer_size from PPManager constructor call
call_args, call_kwargs = MockManager.call_args
buffer_size = call_kwargs.get("buffer_size")
if buffer_size is None:
    # Fallback: scan positional args for the 2M value
    for arg in call_args:
        if isinstance(arg, int) and arg == 2_000_000:
            buffer_size = arg
            break
assert buffer_size is not None, "Could not find buffer_size in PPManager call"
assert buffer_size <= 500_000
```

---

## X5: Consistent directory creation

**Location:** `test_utils.py::TestBlocksDirCreation`
**Modifies:** `src/funbuns/utils.py` (or wherever blocks_dir is defined)

**Problem:** `append_data()` auto-creates `runs/` via `mkdir(exist_ok=True)`,
but the blocks directory path function does not create `blocks/`.

**Spec:**
- When `get_data_dir()` is called (or when any function first accesses the
  blocks directory), `blocks/` should be auto-created if missing, consistent
  with how `runs/` is handled.

**Exact test:**

```python
# FUNBUNS_DATA_DIR set to fresh empty path
data_dir = get_data_dir()
blocks = data_dir / "blocks"

# append_data creates runs/ automatically
df = pl.DataFrame({"p": [7], "m_k": [1], "n_k": [1], "q_k": [5]}, schema=PARTITION_SCHEMA)
append_data(df)

runs = data_dir / "runs"
assert runs.exists()      # passes today
assert blocks.exists()    # FAILS today — this is what must be fixed
```

---

## X6: Paranoid intra-block verification

**Location:** `test_data_integrity.py::TestParanoidIntraBlock`
**Modifies:** `src/funbuns/data_integrity.py`

**Problem:** Layers 1-3 of `validate_completeness_fast` check coverage
intervals and aggregate counts. They cannot detect a single missing prime
within a block.

**Spec:**
- Implement `paranoid_verify_block(path: Path) -> dict` in `data_integrity.py`.
- Read the parquet file, determine min_p and max_p from the `p` column.
- Enumerate all primes in [min_p, max_p] (via SageMath `prime_range` or
  equivalent).
- Return a dict containing at least `"missing_primes": list[int]` — primes
  in the expected range that don't appear in the data.

**Exact test:**

```python
from funbuns.data_integrity import paranoid_verify_block

# Block has primes 2..29 but omits 23
primes = [2, 3, 5, 7, 11, 13, 17, 19, 29]
# ... written to parquet ...

result = paranoid_verify_block(path)
assert 23 in result["missing_primes"]
```

---

## X7: Automatic paranoid escalation via Schoenfeld bound

**Location:** `test_data_integrity.py::TestSchoenfeldEscalation`
**Modifies:** `src/funbuns/data_integrity.py`

**Problem:** No mechanism to automatically trigger paranoid verification
when observed prime counts diverge from expected values.

**Background:** Schoenfeld (1976) proved that under the Riemann Hypothesis,
`|pi(x) - Li(x)| < sqrt(x) * ln(x) / (8 * pi)` for `x >= 2657`. At our
data scale (x ~ 25 billion), breaching this bound is near-certain evidence
of data loss, not a refutation of RH.

**Spec:**
- Implement `check_paranoid_escalation(observed_count: int, max_prime: int) -> dict`.
- Compare observed_count against expected pi(max_prime).
- When the deficit exceeds the Schoenfeld bound, return
  `{"trigger_paranoid": True, "reason": "schoenfeld"}`.

**Exact test:**

```python
from funbuns.data_integrity import check_paranoid_escalation

# pi(100000) ~ 9592, observed is 50 — massive deficit
result = check_paranoid_escalation(observed_count=50, max_prime=100_000)
assert result["trigger_paranoid"] is True
assert result["reason"] == "schoenfeld"
```

---

## X8: Exact missing-prime enumeration

**Location:** `test_data_integrity.py::TestParanoidOptimal`
**Modifies:** `src/funbuns/data_integrity.py`
**Requires:** SageMath

**Problem:** No way to get the exact set of missing primes across a range.

**Spec:**
- Implement `paranoid_verify_range(path: Path, min_p: int, max_p: int) -> dict`.
- Use `sage.all.prime_range(min_p, max_p + 1)` to enumerate expected primes.
- Compare against observed primes via set difference.
- Return `{"missing_primes": list[int], ...}`.
- O(n) in number of primes, O(1) per prime via set lookup.

**Exact test:**

```python
from funbuns.data_integrity import paranoid_verify_range
from sage.all import prime_range as sage_prime_range

all_primes = [int(p) for p in sage_prime_range(1000)]
removed = {23, 149, 373, 509, 877}
present = [p for p in all_primes if p not in removed]
# ... present written to parquet at path ...

result = paranoid_verify_range(path, min_p=2, max_p=997)
assert set(result["missing_primes"]) == removed
```

---

## X9: Run file deletion safety

**Location:** `test_run_ingester.py::TestRunFileDeletionSafety`
**Modifies:** `src/funbuns/run_ingester.py`

**Problem:** Two crash-unsafe patterns:
1. `delete_run_files=True` deletes run files immediately after block writes,
   with no integrity verification.
2. Partial block absorption (~line 103) calls `.unlink()` on the old
   undersized block BEFORE writing its replacement. A crash between delete
   and write loses both.

**Spec (test a — integrity check before deletion):**
- After writing new blocks and before deleting run files, perform an
  integrity check. The test instruments `write_parquet` and `Path.unlink`
  to record the event sequence. Your implementation must cause an event
  tagged `"integrity_check"` to appear in the call log between block
  writes and run file deletes.
- Concretely: the test monkey-patches `pl.DataFrame.write_parquet` and
  `Path.unlink` with wrappers that append `("write_block", path)` and
  `("delete_run", path)` to a list. Your code must append
  `("integrity_check", ...)` to the same mechanism, OR you can take a
  different approach as long as the event sequence `[..., "write_block",
  ..., "integrity_check", ..., "delete_run", ...]` is observable. The
  simplest way: after writing blocks and before deleting runs, call a
  function that the test's `tracking_write`/`tracking_unlink` wrappers
  will see. See the note below.

**Implementation note on test a:** The test patches `pl.DataFrame.write_parquet`
and `Path.unlink` globally. It records events as `("write_block", path)` and
`("delete_run", path)` respectively. It then checks:
```python
events = [e[0] for e in call_log]
assert "integrity_check" in events
```
The cleanest way to satisfy this: between block writes and run deletes,
call a verification function that itself calls `Path.exists()` or reads
the new blocks. If you want the test to see the event, you could add
a call like `Path(some_path).stat()` that the test could intercept — but
the current test only checks the event list for the string `"integrity_check"`.
The most direct approach: have `integrate_runs_into_blocks` call a method
that logs to the same call_log. Since the test patches at the class level,
the simplest fix is to have the ingester verify blocks exist and are readable
before deleting, and to append to a hookable/observable event stream.

**Alternatively**, you may modify the test to use a different observation
mechanism — as long as the behavioral guarantee holds (integrity check
happens between write and delete).

**Spec (test b — old block survives failed write):**
- During partial block absorption, do NOT delete the old block until the
  replacement is confirmed on disk.
- If the replacement write fails (IOError), the old block must still exist.

**Exact test b:**

```python
# Old block at blocks/pp_b001_p13.parquet
# New run data in runs/run_001.parquet
# write_parquet patched to raise IOError on any path containing "pp_b"

try:
    integrate_runs_into_blocks(target_prime_count=10, delete_run_files=False, verbose=False)
except IOError:
    pass

# Old block must survive
assert (blocks / "pp_b001_p13.parquet").exists()
```

---

## Overflow detection in dataprep

**Location:** `test_dataprep.py::TestPreparePrimePowers::test_overflow_bounded`
**Modifies:** `src/funbuns/dataprep.py`

**Problem:** `prepare_prime_powers` uses `pl.col("1").pow(k) <= int64_max`
to detect overflow, but Polars `.pow()` silently wraps on Int64 overflow.
Values that wrap to positive pass the guard undetected.

**Spec:**
- Replace the overflow detection with a method that catches all overflows.
  Options:
  - Logarithmic pre-check: `k * log2(p) > 63` implies overflow.
  - Compute in Float64 first, compare against threshold, then cast.
  - Any other approach that correctly identifies overflow.

**Exact test:**

```python
result = prepare_prime_powers(n=100, max_power=64, use_bounded=True)
df = pl.read_parquet(result)

# 97^64 vastly exceeds 2^63-1; should be 0 in bounded mode
row97 = df.filter(pl.col("1") == 97)
assert row97["64"].item() == 0
```
