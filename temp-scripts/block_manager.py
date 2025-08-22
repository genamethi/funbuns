#!/usr/bin/env python3
"""
Comprehensive block management for prime partition data.
Handles block organization, naming, sizing, and analysis without aggregation.
"""

import polars as pl
from pathlib import Path
import argparse
import shutil
from typing import List, Tuple, Optional
from funbuns.utils import convert_runs_to_blocks_auto
from funbuns.data_integrity import quick_integrity_report
from funbuns.block_manager import main

if __name__ == "__main__":
    main()


