#!/usr/bin/env python
from setuptools import setup, find_packages

PROJECT = "dynamodb_meta_store"
VERSION = "0.4.1"

try:
    long_description = open("./README.md", "rt").read()
except IOError:
    long_description = ""

setup(
    name=PROJECT,
    version=VERSION,

    description="Store metadata using DynamoDB as backend",
    # long_description=long_description,

    author="Sergey Mazin",
    author_email="sergey.mazin@hotmail.com",

    url="https://github.com/sergeymazin/dynamodb-meta-store",
    download_url="https://github.com/sergeymazin/dynamodb-meta-store/tarball/master",

    keywords=["dynamodb", "aws", "config", "metadata"],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Intended Audience :: Developers",
        "Environment :: Console",
    ],
    platforms=["Any"],
    install_requires=[
        "boto3"
    ],
    extras_require={
        "dev": [
            "pre-commit",
            "flake8",
            "flake8-quotes",
            "twine",
        ]
    },
    packages=find_packages(),
)
