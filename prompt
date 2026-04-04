# Feature Specifications

These are behavioral requirements for funbuns. Each describes a problem and
the desired behavior. Implementation approach is up to the developer.

After making changes, run the full test suite to verify:
```
pixi run python -m pytest tests/ -v
```

---

## F1: Graceful SIGINT during generation

**Problem:** Pressing ctrl+c during `funbuns -n ... -b ...` produces a
KeyboardInterrupt traceback, kills worker processes mid-computation, and
loses any results that were completed but not yet written to disk.

**Desired behavior:**
- When the user sends SIGINT during generation, the process should exit
  cleanly with return code 0.
- No KeyboardInterrupt traceback should appear in stdout or stderr.
- Worker processes should not crash independently from the signal.
- Any results that were already computed and buffered must be flushed to
  disk before exit.
- The data written to disk must be valid and readable.

---

## F2: Generation pipeline journal logging

**Problem:** The generation pipeline (`funbuns -n ... -b ...`) produces no
structured log entries. Other modules (zipf, spectral, remainder) already
use `JournalWriter` from `utils.py`, but generation does not.

**Desired behavior:**
- When generation runs, it should produce a JSONL journal file at
  `{data_dir}/logs/funbuns.jsonl`.
- Each line must be a JSON object containing at least `ts`, `module`, and
  `event` keys.
- A `run_start` event must be emitted before computation begins.
- A `run_end` event must be emitted after computation completes.

---

## F3: Admin journal logging

**Problem:** The `funbuns-admin` entry point produces no structured log
entries.

**Desired behavior:**
- `funbuns-admin` commands should produce JSONL log entries at
  `{data_dir}/logs/admin.jsonl`.
- Each entry must include a `module` key with value `"admin"`.

---

## F4: Memory-aware flush strategy

**Problem:** The buffer size for the generation consumer is currently
`batch_size * 2`. This means with a large batch size (e.g. 1M), the buffer
is 2M rows, which triggers a flush on nearly every batch return — excessive
I/O that defeats the purpose of buffering.

**Desired behavior:**
- The flush buffer size must not scale linearly with batch size.
- For large batch sizes, the buffer should remain at a reasonable fixed size
  or be determined by available memory, not by a multiplier on batch_size.

---

## F5: Consistent directory creation

**Problem:** `append_data()` auto-creates the `runs/` directory via
`mkdir(exist_ok=True)`, but the `blocks/` directory is not similarly
auto-created. This is inconsistent.

**Desired behavior:**
- The `blocks/` subdirectory under the data directory should be
  auto-created if it does not exist, consistent with how `runs/` is handled.

---

## F6: Paranoid intra-block verification

**Problem:** The existing integrity checks (`validate_completeness_fast`)
verify coverage intervals and aggregate counts, but cannot detect a single
missing prime within a block.

**Desired behavior:**
- Given a block parquet file, it should be possible to verify that every
  prime in the range [min_p, max_p] of that block is actually present in
  the data.
- Missing primes (those expected in the range but absent from the data)
  should be identified and reported.

---

## F7: Automatic paranoid escalation via Schoenfeld bound

**Problem:** There is no mechanism to automatically flag when observed prime
counts diverge suspiciously from expected values.

**Background:** Schoenfeld (1976) proved that under the Riemann Hypothesis,
`|pi(x) - Li(x)| < sqrt(x) * ln(x) / (8 * pi)` for `x >= 2657`. At our
data scale, breaching this bound is near-certain evidence of data loss, not
a refutation of RH.

**Desired behavior:**
- Given an observed prime count and the maximum prime in a dataset, it
  should be possible to determine whether the deficit exceeds what the
  Schoenfeld bound allows.
- When the bound is breached, the system should flag the need for a full
  paranoid verification and indicate that the Schoenfeld bound was the
  trigger.

---

## F8: Exact missing-prime enumeration over a range

**Problem:** No way to get the exact set of missing primes across an
arbitrary range of a parquet file.

**Desired behavior:**
- Given a parquet file and a prime range [min_p, max_p], enumerate all
  primes expected in that range (using SageMath) and report exactly which
  ones are missing from the data.

---

## F9: Run file deletion safety

**Problem:** Two crash-unsafe patterns in run ingestion:

1. When `delete_run_files=True`, run files are deleted immediately after
   block writes with no verification that the new blocks actually contain
   the expected data. A crash or corruption during the write means both the
   run files and the data are lost.

2. During partial block absorption, the old undersized block is deleted
   before its replacement is written. A crash between delete and write
   loses both the old block and the unwritten replacement.

**Desired behavior:**
- Run files must not be deleted until the new blocks they were integrated
  into have been verified as written and readable.
- During partial block absorption, the old block must not be removed until
  its replacement has been confirmed on disk. If the replacement write
  fails, the old block must survive.

---

## F10: Overflow detection in prime power table generation

**Problem:** `prepare_prime_powers` uses `pl.col("1").pow(k) <= int64_max`
to detect overflow, but Polars `.pow()` silently wraps on Int64 overflow.
Values that wrap back to positive pass the guard undetected.

**Desired behavior:**
- Overflow detection must catch all overflows, including those that wrap
  to positive values. For example, 97^64 vastly exceeds 2^63-1 and must
  be detected as an overflow (zeroed out in bounded mode), not treated as
  a valid value.
