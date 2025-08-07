"""
Setup configuration for funbuns package.
"""

from setuptools import setup, find_packages

setup(
    name="funbuns",
    version="0.1.0",
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
    ],
    entry_points={
        "console_scripts": [
            "calcpp=funbuns.__main__:main",
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
