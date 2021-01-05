#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-linear",
    version="0.1.1",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_linear"],
    install_requires=[
        "singer-python>=5.0.12",
        "gql"
    ],
    entry_points="""
    [console_scripts]
    tap-linear=tap_linear:main
    """,
    packages=["tap_linear"],
    package_data = {
        "schemas": ["tap_linear/schemas/*.json"]
    },
    include_package_data=True,
)
