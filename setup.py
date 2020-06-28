from setuptools import setup, find_packages

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name = "pyracmon",
    version = "0.0.2.1",
    author = "sozuberry",
    author_email = "sozuberry@gmail.com",
    description = "Python O/R Mapping extension for DB-API 2.0",
    long_description = long_description,
    long_description_content_type = "text/markdown",
    url = "https://github.com/sozu/py-pyracmon",
    packages = find_packages(),
    classifiers = [
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
