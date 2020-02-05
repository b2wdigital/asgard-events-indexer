from codecs import open  # To use a consistent encoding
from os import path

from setuptools import (  # Always prefer setuptools over distutils
    find_packages,
    setup,
)

here = path.abspath(path.dirname(__file__))

setup(
    name="asgard-events-indexer",
    version="0.3.0",
    description="Asgard Events Indexer",
    long_description="",
    url="https://github.com/B2W-BIT/asgard-events-indexer",
    # Author details
    author="Dalton Matos",
    author_email="dalton.matos@b2wdigital.com",
    license="MIT",
    classifiers=["Programming Language :: Python :: 3.7"],
    packages=find_packages(exclude=["contrib", "docs", "tests*"]),
    test_suite="tests",
    install_requires=[],
    entry_points={},
)
