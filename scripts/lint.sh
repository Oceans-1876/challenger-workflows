#!/usr/bin/env bash

set -x

mypy workflows
black workflows --check
isort --check-only workflows
flake8
