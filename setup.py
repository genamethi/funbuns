"""
Setup configuration for funbuns package.
"""

import tomllib
from pathlib import Path

from setuptools import setup, find_packages

with open(Path(__file__).parent / "pixi.toml", "rb") as f:
    _version = tomllib.load(f)["workspace"]["version"]

setup(
    name="funbuns",
    version=_version,
    description="Prime decomposition analysis: p = 2^m + q^n",
    author="erpage159",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.12",
    install_requires=[
        "sagemath",
        "polars",
        "tqdm",
        "psutil",
        "scipy",
    ],
    entry_points={
        "console_scripts": [
            "funbuns=funbuns.__main__:main",
            "funbuns-admin=funbuns.admin:main",
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Mathematics",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
)
