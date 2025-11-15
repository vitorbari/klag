"""
Command line interface for klag.

Handles command-line argument parsing and dispatch to appropriate functionality.
"""

import os
import sys
import time
import signal
import argparse
from typing import Dict, Any, Optional, List, Tuple

from .config import load_config, extract_bootstrap_server_from_properties
from .kafka import KafkaClient
from .visualization import LagTracker, VisualizerFactory
from .utils import Colors, clear_line


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns:
        Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(
        description="Visualize Kafka consumer group lag with color-coded indicators and multiple visualization options"
    )
    parser.add_argument("-b", "--bootstrap-server",
                      help="Kafka bootstrap server")
    parser.add_argument("-g", "--group",
                      help="Consumer group name")
    parser.add_argument("-c", "--config",
                      help="Client config properties file path")
    parser.add_argument("-e", "--env", "--environment",
                      help="Environment name (from config file, e.g., 'staging' or 'live')")
    parser.add_argument("-y", "--config-file",
                      help="Path to klag config file")
    parser.add_argument("-m", "--mode", choices=["text", "bar"],
                      help="Visualization mode (default: bar)")
    parser.add_argument("-f", "--filter",
                      help="Filter topics by pattern (supports exact match or regex)")
    parser.add_argument("-w", "--watch", type=int, nargs="?", const=True,
                      help="Watch mode, refresh every N seconds (default from config)")
    parser.add_argument("--list-environments", action="store_true",
                      help="List available environments from config file")
    return parser.parse_args()


class KlagCLI:
    """
    Main CLI class for the klag tool.

    Handles the command-line interface and coordinates the different components.
    """

    def __init__(self):
        """Initialize the CLI handler."""
        self.kafka_client = KafkaClient()
        self.lag_tracker = LagTracker()
        self.args = parse_args()
        self.config = load_config(self.args.config_file)

    def resolve_parameters(self) -> Tuple[str, str, Optional[str], str]:
        """
        Resolve and validate command-line parameters.

        Returns:
            A tuple of (bootstrap_server, group, config_file, mode)

        Raises:
            SystemExit: If required parameters are missing
        """
        # Handle environment selection
        bootstrap_server = self.args.bootstrap_server
        config_file = self.args.config
        group = self.args.group

        if self.args.env and 'environments' in self.config and self.args.env in self.config['environments']:
            env_config = self.config['environments'][self.args.env]

            # Only use environment values if command line args were not provided
            if not bootstrap_server:
                bootstrap_server = env_config.get('bootstrap_server')
            if not config_file:
                config_file = os.path.expanduser(env_config.get('config_file', ''))

            # Print selected environment
            print(f"Using environment: {self.args.env}")

        # If no group is specified, use the default group if available
        if not group and 'default_group' in self.config and self.config['default_group']:
            group = self.config['default_group']

        # Try to extract bootstrap server from client.properties if not provided
        if not bootstrap_server and config_file:
            bootstrap_server = extract_bootstrap_server_from_properties(config_file)
            if bootstrap_server:
                print(f"Using bootstrap server from client properties: {bootstrap_server}")

        # Check required parameters
        if not bootstrap_server:
            print("Error: Bootstrap server not found. Please provide it via --bootstrap-server argument,", file=sys.stderr)
            print("in the klag.yaml environment config, or in the client.properties file.", file=sys.stderr)
            sys.exit(1)

        # Group is required for regular operation (not for list-environments command)
        if not self.args.list_environments and not group:
            print("Error: --group is required for this operation", file=sys.stderr)
            sys.exit(1)

        # Set mode from args or defaults
        defaults = self.config.get('defaults', {})
        mode = self.args.mode if self.args.mode else defaults.get('mode', 'bar')

        return bootstrap_server, group, config_file, mode

    def handle_listing_commands(self) -> bool:
        """
        Handle list-environments and list-groups commands.

        Returns:
            True if a listing command was handled, False otherwise
        """
        # Handle listing environments
        if self.args.list_environments:
            if 'environments' in self.config:
                print("Available environments:")
                for env_name in self.config['environments'].keys():
                    print(f"  - {env_name}")
            else:
                print("No environments found in config file.")
            return True


        return False

    def watch_mode(self, bootstrap_server: str, group: str,
                   config_file: Optional[str], mode: str,
                   watch_interval: int) -> None:
        """
        Run klag in watch mode, with periodic refresh.

        Args:
            bootstrap_server: Kafka bootstrap server
            group: Consumer group name
            config_file: Optional client config file
            mode: Visualization mode
            watch_interval: Refresh interval in seconds
        """
        # Handle Ctrl+C gracefully
        def signal_handler(sig, frame):
            print("\nExiting watch mode...")
            sys.exit(0)

        signal.signal(signal.SIGINT, signal_handler)

        try:
            # Initial run - no need to wait
            first_run = True

            while True:
                # Only wait on subsequent runs
                if not first_run:
                    # Show a waiting message with countdown
                    for remaining in range(watch_interval, 0, -1):
                        sys.stdout.write(f"\r{Colors.CYAN}⏱{Colors.RESET} Refreshing in {remaining} seconds...")
                        sys.stdout.flush()
                        signal.signal(signal.SIGINT, signal_handler)  # Re-register the signal handler
                        time.sleep(1)  # Wait for 1 second

                    # Clear the waiting message line
                    clear_line()
                else:
                    first_run = False

                # Get data for the specified group
                output = self.kafka_client.fetch_consumer_group_info(bootstrap_server, group, config_file)
                data = self.kafka_client.parse_consumer_groups_output(output, self.args.filter)

                # Set display name
                display_name = f"Group: {group}"

                # If filter is simple (no regex special chars), show it in display name
                if self.args.filter and not any(c in self.args.filter for c in "^$.*+?()[]{}|\\"):
                    display_name = f"Group: {group}, Topic filter: {self.args.filter}"

                # Sort data consistently by topic then partition
                data = sorted(data, key=lambda x: (x["topic"], x["partition"]))

                # Now that we have the data ready, clear the screen
                os.system('cls' if os.name == 'nt' else 'clear')

                # Show timestamp and information
                from datetime import datetime
                print(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print(display_name)

                # Create visualizer
                visualizer = VisualizerFactory.create_visualizer(mode, self.lag_tracker)
                print(visualizer.create_summary(data))
                print()

                # Visualize based on mode
                # Pass topic filter flag and whether to show changes
                # Only show changes on subsequent runs (after the first run)
                show_changes = not first_run
                visualizer.visualize(data, False, show_changes)

                # Store the current data for the next run comparison
                # Do this after visualization but before waiting for next run
                self.lag_tracker.store_lag_data(data)

                # Add a message about how to exit
                print(f"\nPress Ctrl+C to exit.")
        except KeyboardInterrupt:
            print("\nExiting watch mode...")
            sys.exit(0)

    def run(self) -> None:
        """
        Run the klag CLI application.
        """
        # Handle listing commands
        if self.handle_listing_commands():
            return

        # Resolve parameters
        bootstrap_server, group, config_file, mode = self.resolve_parameters()

        # Handle watch mode parameter with default value
        watch_interval = None
        if self.args.watch is not None:
            # If just -w was provided without a value, use the default_watch value
            defaults = self.config.get('defaults', {})
            default_watch = defaults.get('watch_interval', 5)

            if self.args.watch is True:
                watch_interval = default_watch
                print(f"Using default watch interval of {default_watch} seconds")
            else:
                # If a specific value was provided (e.g., -w 10), use that value
                watch_interval = self.args.watch

        # If watch mode is enabled, handle it specially
        if watch_interval:
            self.watch_mode(bootstrap_server, group, config_file, mode, watch_interval)
            return

        # Normal (non-watch) mode
        # Get data for the specified group
        output = self.kafka_client.fetch_consumer_group_info(bootstrap_server, group, config_file)
        data = self.kafka_client.parse_consumer_groups_output(output, self.args.filter)

        # Set display name
        display_name = f"Group: {group}"

        # Apply regex filter if specified
        if self.args.filter:
            # If filter is simple (no regex special chars), show it in display name
            if not any(c in self.args.filter for c in "^$.*+?()[]{}|\\\\"):
                display_name = f"Group: {group}, Topic filter: {self.args.filter}"

        # Sort data consistently by topic then partition
        data = sorted(data, key=lambda x: (x["topic"], x["partition"]))

        # Create visualizer
        visualizer = VisualizerFactory.create_visualizer(mode, self.lag_tracker)

        # Print summary with the appropriate display name
        print(display_name)
        print(visualizer.create_summary(data))
        print()

        # Visualize based on mode
        # Non-watch mode never shows changes (show_changes=False)
        visualizer.visualize(data, False, False)