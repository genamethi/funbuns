"""
Funbuns: Prime decomposition analysis package.

Studying primes p that can be expressed as p = 2^m + q^n where q is prime.
"""

__version__ = "0.1.0"

from .core import decomp, analyze, batch
from .utils import save_results, load_results, setup_logging, get_completed_primes, get_default_data_file

__all__ = ['decomp', 'analyze', 'batch', 'save_results', 'load_results', 'setup_logging', 'get_completed_primes', 'get_default_data_file']
