import os
from setuptools import setup, find_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name="dso",
    version="0.0.2",
    author="Patrick Shechet",
    author_email="patrick.shechet@gmail.com",
    description=("A Darned Simple ORM"),
    license="BSD",
    packages=find_packages(),
    long_description=read("README.md"),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3 :: Only",
    ],
)
