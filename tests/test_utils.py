"""Tests for utils.py: resume_p, append_data, JournalWriter, get_data_dir."""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import polars as pl
import pytest

from funbuns.utils import (
    JournalWriter,
    append_data,
    get_data_dir,
    resume_p,
)


def _touch_parquet(path: Path):
    """Create a minimal valid parquet file (empty schema is fine for filename tests)."""
    pl.DataFrame({"p": [1]}).write_parquet(path)


class TestResumeP:
    """T14: Resume from block filenames without reading parquet data."""

    def test_returns_max_prime(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUNBUNS_DATA_DIR", str(tmp_path))
        blocks = tmp_path / "blocks"
        blocks.mkdir()
        _touch_parquet(blocks / "pp_b001_p1000003.parquet")
        _touch_parquet(blocks / "pp_b002_p2000003.parquet")
        _touch_parquet(blocks / "pp_b003_p3000017.parquet")

        assert resume_p() == 3000017

    def test_no_block_files(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUNBUNS_DATA_DIR", str(tmp_path))
        blocks = tmp_path / "blocks"
        blocks.mkdir()
        # Empty directory
        assert resume_p() is None

    def test_no_blocks_dir(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUNBUNS_DATA_DIR", str(tmp_path))
        # No blocks/ subdirectory at all
        assert resume_p() is None

    def test_unparseable_filenames(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUNBUNS_DATA_DIR", str(tmp_path))
        blocks = tmp_path / "blocks"
        blocks.mkdir()
        _touch_parquet(blocks / "random_file.parquet")
        _touch_parquet(blocks / "not_a_block.parquet")
        # No valid pp_b pattern -> best_p stays 0 -> returns None
        assert resume_p() is None


class TestAppendData:
    """T15: Write DataFrames to run files in data/runs/."""

    def test_creates_run_file(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUNBUNS_DATA_DIR", str(tmp_path))
        df = pl.DataFrame({
            "p": [7, 11, 13],
            "m_k": [1, 1, 1],
            "n_k": [1, 1, 1],
            "q_k": [5, 9, 11],
        }).cast({"p": pl.Int64, "m_k": pl.Int64, "n_k": pl.Int64, "q_k": pl.Int64})

        append_data(df)

        runs = tmp_path / "runs"
        assert runs.exists()
        run_files = list(runs.glob("pparts_run_*.parquet"))
        assert len(run_files) == 1

        # Read back and verify
        result = pl.read_parquet(run_files[0])
        assert result.shape == (3, 4)
        assert set(result.columns) == {"p", "m_k", "n_k", "q_k"}
        assert result["p"].to_list() == [7, 11, 13]

    def test_distinct_files_on_rapid_writes(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUNBUNS_DATA_DIR", str(tmp_path))
        df = pl.DataFrame({
            "p": [7], "m_k": [1], "n_k": [1], "q_k": [5],
        }).cast({"p": pl.Int64, "m_k": pl.Int64, "n_k": pl.Int64, "q_k": pl.Int64})

        append_data(df)
        append_data(df)

        runs = tmp_path / "runs"
        run_files = list(runs.glob("pparts_run_*.parquet"))
        assert len(run_files) == 2
        # Filenames should differ (microsecond timestamp + PID)
        names = [f.name for f in run_files]
        assert names[0] != names[1]

    def test_filename_contains_pid(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FUNBUNS_DATA_DIR", str(tmp_path))
        df = pl.DataFrame({
            "p": [7], "m_k": [1], "n_k": [1], "q_k": [5],
        }).cast({"p": pl.Int64, "m_k": pl.Int64, "n_k": pl.Int64, "q_k": pl.Int64})

        append_data(df)

        run_files = list((tmp_path / "runs").glob("pparts_run_*.parquet"))
        assert len(run_files) == 1
        assert str(os.getpid()) in run_files[0].name


class TestJournalWriter:
    """T16: JSONL event logging."""

    def test_writes_jsonl(self, tmp_path):
        journal_path = tmp_path / "test_journal.jsonl"
        jw = JournalWriter(path=journal_path)

        jw.log("core", "batch_done", count=100, primes=50)
        jw.log("ingester", "flush", rows=5000)
        jw.log("core", "run_end", total=1000)

        lines = journal_path.read_text().strip().split("\n")
        assert len(lines) == 3

        for line in lines:
            entry = json.loads(line)
            assert "ts" in entry
            assert "module" in entry
            assert "event" in entry

    def test_payload_keys(self, tmp_path):
        journal_path = tmp_path / "test_journal.jsonl"
        jw = JournalWriter(path=journal_path)
        jw.log("core", "batch_done", count=100, file="run_001.parquet")

        entry = json.loads(journal_path.read_text().strip())
        assert entry["module"] == "core"
        assert entry["event"] == "batch_done"
        assert entry["count"] == 100
        assert entry["file"] == "run_001.parquet"

    def test_timestamp_is_utc_iso8601(self, tmp_path):
        journal_path = tmp_path / "test_journal.jsonl"
        jw = JournalWriter(path=journal_path)
        jw.log("test", "ping")

        entry = json.loads(journal_path.read_text().strip())
        ts = entry["ts"]
        # Should parse as ISO 8601 with timezone
        parsed = datetime.fromisoformat(ts)
        assert parsed.tzinfo is not None
        # Should be UTC
        assert parsed.tzinfo == timezone.utc or "+" in ts or "Z" in ts


class TestGetDataDir:
    """T17: Data directory hierarchy (env var > config > default)."""

    def test_env_var_takes_priority(self, tmp_path, monkeypatch):
        target = tmp_path / "custom_data"
        target.mkdir()
        monkeypatch.setenv("FUNBUNS_DATA_DIR", str(target))
        assert get_data_dir() == target

    def test_config_fallback(self, tmp_path, monkeypatch):
        """With env var unset, reads from pixi.toml [tool.funbuns.directories]."""
        monkeypatch.delenv("FUNBUNS_DATA_DIR", raising=False)
        monkeypatch.chdir(tmp_path)

        config_dir = tmp_path / "configured_data"
        config_dir.mkdir()
        pixi_toml = tmp_path / "pixi.toml"
        pixi_toml.write_text(f"""
[tool.funbuns.directories]
data_dir = "{config_dir}"
""")
        result = get_data_dir()
        assert result == config_dir

    def test_default_fallback(self, tmp_path, monkeypatch):
        """No env var, no config -> defaults to data/ in CWD."""
        monkeypatch.delenv("FUNBUNS_DATA_DIR", raising=False)
        monkeypatch.chdir(tmp_path)

        # Write a pixi.toml without data_dir
        pixi_toml = tmp_path / "pixi.toml"
        pixi_toml.write_text("[tool.funbuns]\nbuffer_size = 10000\n")

        result = get_data_dir()
        assert result == Path("data")
        assert (tmp_path / "data").exists()  # Should be created
