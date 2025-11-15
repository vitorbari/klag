#!/usr/bin/env python3
"""
Main entry point for klag.

This module allows running klag as a package: python -m klag
"""

import sys
from .cli import KlagCLI


def main():
    """Execute the klag command line interface."""
    cli = KlagCLI()
    cli.run()


if __name__ == "__main__":
    sys.exit(main())