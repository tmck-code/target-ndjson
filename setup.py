#!/usr/bin/env python
from setuptools import setup

setup(
    name="target-ndjson",
    version="0.1.0",
    description="Singer.io target for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["target_ndjson"],
    install_requires=[
        "singer-python>=5.0.12",
    ],
    entry_points="""
    [console_scripts]
    target-ndjson=target_ndjson:main
    """,
    packages=["target_ndjson"],
    package_data = {},
    include_package_data=True,
)
