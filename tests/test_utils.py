"""Tests for utils.py: JournalWriter, get_data_dir.

Legacy tests for block-filename-based resume_p, append_data, and
blocks_dir auto-creation were removed in the iceberg ingest cutover
(steps 3-5). resume_p now reads iceberg manifest state; see
test_iceberg_commit_seq.py for coverage of the replacement path.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from funbuns.utils import JournalWriter, get_data_dir


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
        parsed = datetime.fromisoformat(ts)
        assert parsed.tzinfo is not None
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

        pixi_toml = tmp_path / "pixi.toml"
        pixi_toml.write_text("[tool.funbuns]\nbuffer_size = 10000\n")

        result = get_data_dir()
        assert result == Path("data")
        assert (tmp_path / "data").exists()
