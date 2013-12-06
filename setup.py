#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name = "dynamomite",
    version = "0.0.1",
    author = "Ben Hagen",
    author_email = "benhagen@gmail.com",
    description = "AWS DynamoDB as Python dicts. Yay!",
    license = "BSD",
    keywords = "aws dynamo",
    url = "https://github.com/benhagen/dynamomite",
    packages=find_packages(),
    requires=['boto',]
)
