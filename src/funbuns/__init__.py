"""
Funbuns: Prime power partition analysis package.

Studying primes p that can be expressed as p = 2^m + q^n where q is prime.
"""

__version__ = "0.1.0"

from .core import decomp, PPFeeder, PPProducer, PPConsumer
from .utils import setup_logging, resume_p, append_data, get_config, get_default_data_file

__all__ = ['decomp', 'PPFeeder', 'PPProducer', 'PPConsumer', 'setup_logging', 'resume_p', 'append_data', 'get_config', 'get_default_data_file']
