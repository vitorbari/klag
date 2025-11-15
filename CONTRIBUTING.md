# Contributing to klag

Thank you for your interest in contributing to klag! This document provides guidelines and instructions for contributing.

## Code of Conduct

Please note that this project adheres to the Contributor Covenant Code of Conduct. By participating, you are expected to uphold this code. Please report unacceptable behavior.

## How to Contribute

### Reporting Bugs

- Check if the bug has already been reported in the Issues section
- Use the bug report template when creating a new issue
- Include detailed steps to reproduce the bug
- Include any relevant logs or screenshots
- Specify your environment (OS, Python version, Kafka version)

### Feature Requests

- Check if the feature has already been requested in the Issues section
- Use the feature request template
- Clearly describe the problem the feature would solve
- Suggest an approach for implementing the feature, if possible

### Pull Requests

1. Fork the repository
2. Create a branch for your changes
3. Add or modify code and tests
4. Run tests locally to ensure they pass
5. Submit a pull request using the PR template

## Development Setup

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/klag.git
cd klag

# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest
```

## Coding Style

- Follow PEP 8 for Python code style
- Include type annotations
- Write meaningful docstrings in the Google style
- Keep lines under 100 characters when possible

## Testing

- Add tests for new features
- Update tests for modified code
- Ensure all tests pass before submitting PR
- Aim for good test coverage for new code

## Commit Messages

- Use clear, descriptive commit messages
- Start with a short summary line (50 chars or less)
- Follow with a more detailed explanation if necessary
- Reference relevant issue numbers

## Releasing

Only project maintainers can create releases. The process includes:

1. Update version in `klag/__init__.py` and `setup.py`
2. Update CHANGELOG.md
3. Create a new GitHub release with release notes
4. Update Homebrew formula if needed

Thank you for contributing to klag!