"""
Funbuns: Prime power partition analysis package.

Studying primes p that can be expressed as p = 2^m + q^n where q is prime.
"""

import tomllib
from pathlib import Path


def _read_version() -> str:
    """Read version from pixi.toml (single source of truth)."""
    try:
        pixi = Path(__file__).resolve().parents[2] / "pixi.toml"
        with open(pixi, "rb") as f:
            return tomllib.load(f)["workspace"]["version"]
    except (FileNotFoundError, KeyError):
        return "0.0.0"


__version__ = _read_version()
VERSION = tuple(int(x) for x in __version__.split("."))

from .core import PPBatchProcessor, worker_batch, PPBatchFeeder, PPConsumer
from .core import PPManager
from .utils import setup_logging, resume_p, get_config

__all__ = ['PPBatchProcessor', 'PPBatchFeeder', 'PPConsumer', 'PPManager', 'setup_logging', 'resume_p', 'get_config']
