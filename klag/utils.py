"""
Utility functions for the klag tool.

This module provides various helper functions used throughout the application.
"""

import sys
import time
import shutil
import threading
import itertools
from typing import Optional, Tuple


class Colors:
    """ANSI color codes for terminal output."""
    RESET = "\033[0m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"
    BOLD = "\033[1m"


class Spinner:
    """
    A loading spinner animation for command-line interfaces.

    Provides visual feedback during long-running operations.
    """

    def __init__(self):
        """Initialize the spinner state."""
        self.running = False
        self.thread = None

    def start(self, message: str = "Loading...") -> None:
        """
        Start the spinner animation with the given message.

        Args:
            message: The message to display next to the spinner
        """
        self.running = True

        def spin():
            spinner_chars = itertools.cycle(['⣾', '⣽', '⣻', '⢿', '⡿', '⣟', '⣯', '⣷'])
            while self.running:
                sys.stdout.write(f"\r{Colors.CYAN}{next(spinner_chars)}{Colors.RESET} {message}")
                sys.stdout.flush()
                time.sleep(0.1)

        self.thread = threading.Thread(target=spin, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        """Stop the spinner and clear the line."""
        self.running = False

        # Wait for the spinner thread to notice the flag change
        # This prevents race conditions where we clear the line but the spinner redraws
        if self.thread and self.thread.is_alive():
            time.sleep(0.15)

        # Clear only the current line (not the entire terminal width)
        term_width, _ = get_terminal_size()
        sys.stdout.write('\r' + ' ' * term_width + '\r')
        sys.stdout.flush()


def get_terminal_size() -> Tuple[int, int]:
    """
    Get the terminal size.

    Returns:
        A tuple of (columns, lines)
    """
    terminal_size = shutil.get_terminal_size((80, 20))
    return terminal_size.columns, terminal_size.lines


def clear_line() -> None:
    """Clear the current terminal line."""
    term_width, _ = get_terminal_size()
    sys.stdout.write('\r' + ' ' * term_width + '\r')
    sys.stdout.flush()