"""Packaging for the automated ETL and data quality pipeline."""

from pathlib import Path

from setuptools import find_packages, setup

_here = Path(__file__).parent
_long_description = (_here / "README.md").read_text() if (_here / "README.md").exists() else ""
_requirements = [
    line.strip()
    for line in (_here / "requirements.txt").read_text().splitlines()
    if line.strip() and not line.startswith("#")
]

setup(
    name="automated-etl-data-quality-pipeline",
    version="1.0.0",
    description="Config-driven ETL pipeline with a data quality monitoring framework.",
    long_description=_long_description,
    long_description_content_type="text/markdown",
    author="Godfrey Njoro",
    python_requires=">=3.11",
    packages=find_packages(include=["src", "src.*", "flows", "flows.*"]),
    install_requires=_requirements,
    entry_points={
        "console_scripts": [
            "etl-run=flows.etl_flow:main",
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3.11",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
