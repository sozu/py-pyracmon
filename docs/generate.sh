#!/bin/bash

cd `dirname $0`
cd ..

SPHINX_APIDOC_OPTIONS=members sphinx-apidoc -t docs/template/ -f -e -a -o docs/src/ . tests setup.py

sphinx-build docs/src docs/_build