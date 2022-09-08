#!/bin/sh -e

set -x

# Sort imports one per line, so autoflake can remove unused imports
isort --force-single-line-imports workflows
autoflake --remove-all-unused-imports --recursive --remove-unused-variables --in-place --exclude=__init__.py workflows
black workflows
isort workflows
