"""
Funbuns: Prime power partition analysis package.

Studying primes p that can be expressed as p = 2^m + q^n where q is prime.
"""

__version__ = "0.1.0"

from .core import PPBatchProcessor, worker_batch, PPBatchFeeder, PPConsumer, run_gen
from .utils import setup_logging, resume_p, append_data, get_config, get_default_data_file

__all__ = ['PPBatchProcessor', 'PPBatchFeeder', 'PPConsumer', 'run_gen', 'setup_logging', 'resume_p', 'append_data', 'get_config', 'get_default_data_file']
