"""
Funbuns: Prime power partition analysis package.

Studying primes p that can be expressed as p = 2^m + q^n where q is prime.
"""

import os


def _read_version() -> str:
    """ pixi run and pixi shell both add project version to the environment. """
    return os.getenv("PIXI_PROJECT_VERSION") or "0.0.0"


__version__ = _read_version()
## I suspect this isn't needed in the __init__ and can be moved to the test that actually uses it.
    ##TODO confirm and move to test file, delete here.
VERSION = tuple(int(x) for x in __version__.split("."))

from . import _patches  # noqa: F401  — applies pyiceberg sort_order_id fix on import
from .core import PPBatchProcessor, worker_batch, PPBatchFeeder, PPConsumer
from .core import PPManager
from .utils import setup_logging, resume_p, get_config

__all__ = ['PPBatchProcessor', 'PPBatchFeeder', 'PPConsumer', 'PPManager', 'setup_logging', 'resume_p', 'get_config']
