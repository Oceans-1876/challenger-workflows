#!/usr/bin/env bash

set -x

mypy workflows
black workflows --check
isort --check-only workflows
isort .
flake8
