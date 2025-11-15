"""Tests for the CLI module."""

import argparse
import sys
from unittest.mock import patch, MagicMock
import pytest

from klag.cli import parse_args, KlagCLI


class TestParseArgs:
    """Tests for the parse_args function."""

    def test_parse_args_default(self):
        """Test parsing args with defaults."""
        with patch('sys.argv', ['klag.py']):
            args = parse_args()
            assert args.bootstrap_server is None
            assert args.group is None
            assert args.config is None
            assert args.env is None
            assert args.mode is None
            assert args.filter is None
            assert args.watch is None
            assert args.list_environments is False

    def test_parse_args_with_values(self):
        """Test parsing args with specified values."""
        with patch('sys.argv', [
            'klag.py',
            '-b', 'kafka:9092',
            '-g', 'test-group',
            '-c', '/path/to/config.properties',
            '-e', 'staging',
            '-m', 'text',
            '-f', 'topic.*',
            '-w', '10'
        ]):
            args = parse_args()
            assert args.bootstrap_server == 'kafka:9092'
            assert args.group == 'test-group'
            assert args.config == '/path/to/config.properties'
            assert args.env == 'staging'
            assert args.mode == 'text'
            assert args.filter == 'topic.*'
            assert args.watch == 10

    def test_parse_args_watch_no_value(self):
        """Test parsing args with watch flag but no value."""
        with patch('sys.argv', ['klag.py', '-w']):
            args = parse_args()
            assert args.watch is True


class TestKlagCLI:
    """Tests for the KlagCLI class."""

    @patch('klag.cli.parse_args')
    @patch('klag.cli.load_config')
    def test_init(self, mock_load_config, mock_parse_args):
        """Test KlagCLI initialization."""
        # Setup the mocks
        mock_args = MagicMock()
        mock_args.config_file = None
        mock_parse_args.return_value = mock_args
        mock_load_config.return_value = {'default': 'config'}

        # Create CLI instance
        cli = KlagCLI()

        # Check that it has the expected attributes
        assert hasattr(cli, 'kafka_client')
        assert hasattr(cli, 'lag_tracker')
        assert hasattr(cli, 'args')
        assert hasattr(cli, 'config')
        assert cli.config == {'default': 'config'}

        # Check that the mocks were called
        mock_parse_args.assert_called_once()
        mock_load_config.assert_called_once_with(None)

    @patch('klag.cli.parse_args')
    @patch('klag.cli.load_config')
    def test_resolve_parameters_from_env(self, mock_load_config, mock_parse_args):
        """Test resolving parameters from environment config."""
        # Setup the mocks
        mock_args = MagicMock()
        mock_args.bootstrap_server = None
        mock_args.config = None
        mock_args.group = None
        mock_args.env = 'test_env'
        mock_args.mode = None
        mock_args.config_file = None
        mock_args.list_environments = False

        mock_parse_args.return_value = mock_args
        mock_load_config.return_value = {
            'environments': {
                'test_env': {
                    'bootstrap_server': 'env-server:9092',
                    'config_file': '~/config.properties'
                }
            },
            'default_group': 'default-group',
            'defaults': {
                'mode': 'bar'
            }
        }

        # Create CLI instance and resolve parameters
        cli = KlagCLI()
        bootstrap_server, group, config_file, mode = cli.resolve_parameters()

        # Check that parameters were resolved correctly
        assert bootstrap_server == 'env-server:9092'
        assert group == 'default-group'
        assert config_file is not None  # Expanded path
        assert mode == 'bar'

    @patch('klag.cli.parse_args')
    @patch('klag.cli.load_config')
    def test_resolve_parameters_from_args(self, mock_load_config, mock_parse_args):
        """Test that command-line args take precedence."""
        # Setup the mocks
        mock_args = MagicMock()
        mock_args.bootstrap_server = 'arg-server:9092'
        mock_args.config = '/arg/config.properties'
        mock_args.group = 'arg-group'
        mock_args.env = 'test_env'
        mock_args.mode = 'text'
        mock_args.config_file = None
        mock_args.list_environments = False

        mock_parse_args.return_value = mock_args
        mock_load_config.return_value = {
            'environments': {
                'test_env': {
                    'bootstrap_server': 'env-server:9092',
                    'config_file': '~/config.properties'
                }
            },
            'default_group': 'default-group',
            'defaults': {
                'mode': 'bar'
            }
        }

        # Create CLI instance and resolve parameters
        cli = KlagCLI()
        bootstrap_server, group, config_file, mode = cli.resolve_parameters()

        # Check that args take precedence
        assert bootstrap_server == 'arg-server:9092'
        assert group == 'arg-group'
        assert config_file == '/arg/config.properties'
        assert mode == 'text'

    @patch('klag.cli.parse_args')
    @patch('klag.cli.load_config')
    @patch('klag.cli.extract_bootstrap_server_from_properties')
    def test_resolve_parameters_from_properties(self, mock_extract, mock_load_config, mock_parse_args):
        """Test extracting bootstrap server from properties file."""
        # Setup the mocks
        mock_args = MagicMock()
        mock_args.bootstrap_server = None
        mock_args.config = '/path/to/config.properties'
        mock_args.group = 'arg-group'
        mock_args.env = None
        mock_args.mode = None
        mock_args.config_file = None
        mock_args.list_environments = False

        mock_parse_args.return_value = mock_args
        mock_load_config.return_value = {
            'defaults': {
                'mode': 'bar'
            }
        }
        mock_extract.return_value = 'props-server:9092'

        # Create CLI instance and resolve parameters
        cli = KlagCLI()
        bootstrap_server, group, config_file, mode = cli.resolve_parameters()

        # Check that bootstrap server was extracted from properties
        assert bootstrap_server == 'props-server:9092'
        assert group == 'arg-group'
        assert config_file == '/path/to/config.properties'
        assert mode == 'bar'

        # Check that extract was called
        mock_extract.assert_called_once_with('/path/to/config.properties')

    @patch('klag.cli.parse_args')
    @patch('klag.cli.load_config')
    def test_resolve_parameters_missing_bootstrap(self, mock_load_config, mock_parse_args):
        """Test error when bootstrap server is not provided."""
        # Setup the mocks
        mock_args = MagicMock()
        mock_args.bootstrap_server = None
        mock_args.config = None
        mock_args.group = 'arg-group'
        mock_args.env = None
        mock_args.mode = None
        mock_args.config_file = None
        mock_args.list_environments = False

        mock_parse_args.return_value = mock_args
        mock_load_config.return_value = {}

        # Create CLI instance
        cli = KlagCLI()

        # Should exit when bootstrap server is missing
        with pytest.raises(SystemExit) as excinfo:
            cli.resolve_parameters()

        assert excinfo.value.code == 1

    @patch('klag.cli.parse_args')
    @patch('klag.cli.load_config')
    def test_resolve_parameters_missing_group(self, mock_load_config, mock_parse_args):
        """Test error when group is not provided."""
        # Setup the mocks
        mock_args = MagicMock()
        mock_args.bootstrap_server = 'server:9092'
        mock_args.config = None
        mock_args.group = None
        mock_args.env = None
        mock_args.mode = None
        mock_args.config_file = None
        mock_args.list_environments = False

        mock_parse_args.return_value = mock_args
        mock_load_config.return_value = {}

        # Create CLI instance
        cli = KlagCLI()

        # Should exit when group is missing
        with pytest.raises(SystemExit) as excinfo:
            cli.resolve_parameters()

        assert excinfo.value.code == 1

    @patch('klag.cli.parse_args')
    @patch('klag.cli.load_config')
    @patch('builtins.print')
    def test_handle_listing_commands(self, mock_print, mock_load_config, mock_parse_args):
        """Test handling of --list-environments command."""
        # Setup the mocks
        mock_args = MagicMock()
        mock_args.list_environments = True
        mock_args.config_file = None

        mock_parse_args.return_value = mock_args
        mock_load_config.return_value = {
            'environments': {
                'staging': {},
                'production': {}
            }
        }

        # Create CLI instance
        cli = KlagCLI()

        # Run the method
        result = cli.handle_listing_commands()

        # Check that listing was handled
        assert result is True
        mock_print.assert_any_call("Available environments:")

    @patch('klag.cli.parse_args')
    @patch('klag.cli.load_config')
    def test_run_with_listing(self, mock_load_config, mock_parse_args):
        """Test run method with listing command."""
        # Setup the mocks
        mock_args = MagicMock()
        mock_args.list_environments = True
        mock_args.config_file = None

        mock_parse_args.return_value = mock_args
        mock_load_config.return_value = {
            'environments': {
                'staging': {},
                'production': {}
            }
        }

        # Create CLI instance with mock for handle_listing_commands
        cli = KlagCLI()
        cli.handle_listing_commands = MagicMock(return_value=True)

        # Run the CLI
        cli.run()

        # Check that handle_listing_commands was called
        cli.handle_listing_commands.assert_called_once()

    @patch('klag.cli.parse_args')
    @patch('klag.cli.load_config')
    def test_run_normal_mode(self, mock_load_config, mock_parse_args):
        """Test run method in normal mode."""
        # Setup the mocks
        mock_args = MagicMock()
        mock_args.bootstrap_server = 'server:9092'
        mock_args.config = None
        mock_args.group = 'test-group'
        mock_args.env = None
        mock_args.mode = 'bar'
        mock_args.config_file = None
        mock_args.list_environments = False
        mock_args.watch = None
        mock_args.filter = None

        mock_parse_args.return_value = mock_args
        mock_load_config.return_value = {}

        # Create CLI instance with mocks
        cli = KlagCLI()
        cli.handle_listing_commands = MagicMock(return_value=False)
        cli.kafka_client = MagicMock()
        cli.kafka_client.fetch_consumer_group_info.return_value = "output"
        cli.kafka_client.parse_consumer_groups_output.return_value = []

        # Mock the visualizer
        mock_visualizer = MagicMock()
        with patch('klag.cli.VisualizerFactory.create_visualizer', return_value=mock_visualizer):
            # Run the CLI
            cli.run()

        # Check that methods were called as expected
        cli.handle_listing_commands.assert_called_once()
        cli.kafka_client.fetch_consumer_group_info.assert_called_once_with('server:9092', 'test-group', None)
        cli.kafka_client.parse_consumer_groups_output.assert_called_once_with("output", None)
        mock_visualizer.create_summary.assert_called_once()
        mock_visualizer.visualize.assert_called_once()

    @patch('klag.cli.parse_args')
    @patch('klag.cli.load_config')
    @patch('klag.cli.os.system')
    @patch('klag.cli.time.sleep')
    @patch('klag.cli.signal.signal')
    def test_watch_mode(self, mock_signal, mock_sleep, mock_system, mock_load_config, mock_parse_args):
        """Test watch mode functionality."""
        # Setup the mocks
        mock_args = MagicMock()
        mock_args.bootstrap_server = 'server:9092'
        mock_args.config = None
        mock_args.group = 'test-group'
        mock_args.filter = None

        mock_parse_args.return_value = mock_args

        # Create CLI instance with mocks
        cli = KlagCLI()
        cli.kafka_client = MagicMock()
        cli.kafka_client.fetch_consumer_group_info.return_value = "output"
        cli.kafka_client.parse_consumer_groups_output.return_value = []

        # Mock visualizer
        mock_visualizer = MagicMock()

        with patch('klag.cli.VisualizerFactory.create_visualizer', return_value=mock_visualizer):
            # Mock datetime (imported locally within the watch_mode method)
            with patch('datetime.datetime') as mock_datetime:
                # Setup datetime.now() to return a fixed value
                mock_datetime.now.return_value.strftime.return_value = "2025-01-01 12:00:00"

                # Setup to exit watch mode after first iteration
                mock_sleep.side_effect = KeyboardInterrupt()

                # Run watch mode with a watch interval of 5 seconds
                with pytest.raises(SystemExit) as excinfo:
                    cli.watch_mode('server:9092', 'test-group', None, 'bar', 5)

                # Check that exit was clean with code 0
                assert excinfo.value.code == 0

        # Verify calls
        mock_signal.assert_called()  # Signal handler should be registered
        cli.kafka_client.fetch_consumer_group_info.assert_called_once_with('server:9092', 'test-group', None)
        cli.kafka_client.parse_consumer_groups_output.assert_called_once()
        mock_system.assert_called_once()  # Should call os.system to clear the screen
        mock_visualizer.create_summary.assert_called_once()
        mock_visualizer.visualize.assert_called_once()

    @patch('klag.cli.parse_args')
    @patch('klag.cli.load_config')
    def test_run_with_watch_mode(self, mock_load_config, mock_parse_args):
        """Test run method with watch mode enabled."""
        # Setup the mocks
        mock_args = MagicMock()
        mock_args.bootstrap_server = 'server:9092'
        mock_args.config = None
        mock_args.group = 'test-group'
        mock_args.env = None
        mock_args.mode = 'bar'
        mock_args.config_file = None
        mock_args.list_environments = False
        mock_args.watch = 5
        mock_args.filter = None

        mock_parse_args.return_value = mock_args
        mock_load_config.return_value = {
            'defaults': {
                'mode': 'bar'
            }
        }

        # Create CLI instance with mocks
        cli = KlagCLI()
        cli.handle_listing_commands = MagicMock(return_value=False)
        cli.watch_mode = MagicMock()  # Mock watch_mode to avoid actual execution

        # Run the CLI
        cli.run()

        # Check that methods were called as expected
        cli.handle_listing_commands.assert_called_once()
        cli.watch_mode.assert_called_once_with('server:9092', 'test-group', None, 'bar', 5)