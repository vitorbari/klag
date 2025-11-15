"""Tests for the configuration module."""

import os
import tempfile
from unittest.mock import patch, mock_open
import pytest
import yaml

from klag.config import load_config, extract_bootstrap_server_from_properties


class MockIterableFile:
    """A mock file-like object that can be iterated over line by line."""

    def __init__(self, lines):
        """Initialize with a list of lines."""
        self.lines = lines
        self.index = 0

    def __iter__(self):
        """Return self as iterator."""
        return self

    def __next__(self):
        """Return the next line or raise StopIteration."""
        if self.index < len(self.lines):
            line = self.lines[self.index]
            self.index += 1
            return line
        raise StopIteration

    def __enter__(self):
        """Context manager enter."""
        return self

    def __exit__(self, *args):
        """Context manager exit."""
        pass


class TestConfig:
    """Tests for the configuration module."""

    def test_load_config_from_specified_path(self, temp_config_file, sample_config):
        """Test loading config from a specified file path."""
        # Load the config from the temp file
        config = load_config(temp_config_file)

        # Check that we have the expected configuration structure
        assert 'environments' in config
        assert 'test' in config['environments']
        assert config['environments']['test']['bootstrap_server'] == 'test-server:9092'
        assert config['default_group'] == 'test-group'
        assert config['defaults']['mode'] == 'text'
        assert config['defaults']['watch_interval'] == 10

    @patch('os.path.exists')
    @patch('builtins.open')
    @patch('yaml.safe_load')
    def test_load_config_search_paths(self, mock_yaml_load, mock_open_func, mock_exists):
        """Test that config is searched for in the expected locations."""
        # Make the second path (user config) exist, others return False
        def mock_exists_side_effect(path):
            if '/.config/klag/' in path:
                return True
            return False

        mock_exists.side_effect = mock_exists_side_effect

        # Mock the YAML parsing result directly
        mock_yaml_load.return_value = {
            'environments': {
                'test': {
                    'bootstrap_server': 'test-server:9092'
                }
            }
        }

        # Call the function - it should find the ~/.config path
        config = load_config()

        # Verify the config was loaded correctly
        assert 'environments' in config
        assert 'test' in config['environments']
        assert config['environments']['test']['bootstrap_server'] == 'test-server:9092'

        # Verify that paths were checked in the right order
        assert mock_exists.call_count >= 2

        # First path should be the current directory
        first_call_arg = mock_exists.call_args_list[0][0][0]
        assert first_call_arg.endswith('klag.yaml')

        # Second path should include the user config
        second_call_arg = mock_exists.call_args_list[1][0][0]
        assert '/.config/klag/' in second_call_arg

    @patch('builtins.open')
    @patch('os.path.exists')
    @patch('os.path.expanduser')
    def test_extract_bootstrap_server_from_properties(self, mock_expanduser, mock_exists, mock_open_func):
        """Test extracting bootstrap server from client properties file."""
        # Setup mocks
        mock_exists.return_value = True
        mock_expanduser.side_effect = lambda x: x  # Just return the same path

        # Create a mock file with bootstrap.servers
        mock_file = MockIterableFile([
            "# Kafka client properties\n",
            "bootstrap.servers=kafka-server:9092\n",
            "security.protocol=PLAINTEXT\n"
        ])
        mock_open_func.return_value = mock_file

        # Call the function with debug
        bootstrap_server = extract_bootstrap_server_from_properties('/fake/path/to/client.properties')

        # Debug what's happening
        print(f"Mock exists called with: {[call[0][0] for call in mock_exists.call_args_list]}")
        print(f"Mock open called with: {[call[0][0] for call in mock_open_func.call_args_list]}")
        print(f"Bootstrap server result: {bootstrap_server}")

        # Check that the bootstrap server was extracted correctly
        assert bootstrap_server == 'kafka-server:9092'

    @patch('builtins.open')
    @patch('os.path.exists')
    def test_extract_bootstrap_server_missing(self, mock_exists, mock_open_func):
        """Test handling when bootstrap.servers is not in properties file."""
        # Setup mocks
        mock_exists.return_value = True

        # Create a mock file without bootstrap.servers
        mock_file = MockIterableFile([
            "# Kafka client properties\n",
            "security.protocol=PLAINTEXT\n"
        ])
        mock_open_func.return_value = mock_file

        # Should return None when bootstrap.servers is not found
        bootstrap_server = extract_bootstrap_server_from_properties('/fake/path/to/client.properties')
        assert bootstrap_server is None

    @patch('builtins.open')
    def test_extract_bootstrap_server_file_not_found(self, mock_open_func):
        """Test handling when properties file is not found."""
        # Mock file open to raise FileNotFoundError
        mock_open_func.side_effect = FileNotFoundError("No such file")

        # Should return None when file is not found
        bootstrap_server = extract_bootstrap_server_from_properties('/non/existent/file.properties')
        assert bootstrap_server is None