import pathlib

from setuptools import find_packages, setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()


setup(
    name="dsorm",
    version="0.0.9",
    author="Patrick Shechet",
    author_email="patrick.shechet@gmail.com",
    description=("A Data Structure ORM"),
    license="BSD",
    packages=find_packages(),
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/kajuberdut/dsorm",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3 :: Only",
    ],
)
