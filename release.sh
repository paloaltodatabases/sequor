#!/bin/bash
set -e  # Exit on error

# Clean build artifacts
rm -rf dist/

# Build package
python -m build

# Upload to PyPI
python -m twine upload dist/*

echo "Release completed successfully."