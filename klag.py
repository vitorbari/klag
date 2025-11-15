#!/usr/bin/env python3
"""
klag - Kafka Consumer Lag Visualizer

A simplified tool for visualizing Kafka consumer group lag with color-coded indicators.

Features:
- Text and bar chart visualization modes
- Color-coded lag indicators based on lag size
- Change tracking indicators in watch mode
- Topic filtering with regex support
- Consistent sorting by topic and partition
"""

import sys
from klag.__main__ import main

if __name__ == "__main__":
    sys.exit(main())