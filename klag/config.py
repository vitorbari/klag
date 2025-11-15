"""
Configuration handling module for klag.

This module handles loading and processing configuration from various sources.
"""

import os
import yaml
from typing import Dict, Any, Optional, List


def extract_bootstrap_server_from_properties(config_file: str) -> Optional[str]:
    """
    Extract bootstrap.servers property from a Kafka client properties file.

    Args:
        config_file: Path to the Kafka client properties file

    Returns:
        The bootstrap server value if found, None otherwise
    """
    if not config_file or not os.path.exists(os.path.expanduser(config_file)):
        return None

    try:
        with open(os.path.expanduser(config_file), 'r') as f:
            for line in f:
                # Skip comments and empty lines
                line = line.strip()
                if not line or line.startswith('#'):
                    continue

                # Look for bootstrap.servers property
                if line.startswith('bootstrap.servers='):
                    return line.split('=', 1)[1].strip()
    except Exception as e:
        print(f"Warning: Failed to read bootstrap server from {config_file}: {e}")

    return None


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Load configuration from a YAML file.

    Looks for configuration files in the following order:
    1. Specified path (if provided)
    2. ./klag.yaml (current directory)
    3. ~/.config/klag/klag.yaml (user config directory)
    4. /etc/klag/klag.yaml (system config directory)

    Args:
        config_path: Optional path to a specific configuration file

    Returns:
        A dictionary containing the parsed configuration or an empty dict if no config found
    """
    # Default locations to check for config files
    default_paths = [
        "./klag.yaml",  # Current directory
        os.path.expanduser("~/.config/klag/klag.yaml"),  # User config directory
        "/etc/klag/klag.yaml"  # System config directory
    ]

    # If config path is provided, check it first
    if config_path:
        default_paths.insert(0, config_path)

    # Try to load from each path
    for path in default_paths:
        try:
            if os.path.exists(path):
                with open(path, 'r') as f:
                    return yaml.safe_load(f) or {}
        except Exception as e:
            print(f"Warning: Failed to load config from {path}: {e}")

    # Return empty config if no config file found
    return {}