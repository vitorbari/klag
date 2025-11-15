"""
Kafka interaction module for klag.

Handles interactions with Kafka clusters via command-line tools.
"""

import re
import sys
import subprocess
from typing import Dict, List, Optional, Any

from .utils import Spinner, get_terminal_size


class KafkaClient:
    """
    Handles interactions with Kafka clusters.

    This class wraps around the kafka-consumer-groups command line tool to
    fetch and parse consumer group information.
    """

    def __init__(self):
        """Initialize the KafkaClient."""
        self.spinner = Spinner()

    def fetch_consumer_group_info(self, bootstrap_server: str, group: str,
                                 config_file: Optional[str] = None) -> str:
        """
        Fetch consumer group information from Kafka.

        Args:
            bootstrap_server: Kafka bootstrap server address
            group: Consumer group name
            config_file: Optional path to client config properties file

        Returns:
            Raw output from the kafka-consumer-groups command

        Raises:
            SystemExit: If the command fails
        """
        cmd = ["kafka-consumer-groups",
              "--bootstrap-server", bootstrap_server,
              "--group", group,
              "--describe"]

        if config_file:
            cmd.extend(["--command-config", config_file])

        # Start the loading spinner
        self.spinner.start(f"Retrieving lag information for group: {group}...")

        try:
            # Print the command being executed (for debugging)
            cmd_str = " ".join(cmd)

            # Run the command and check its exit status
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)

            # Stop spinner before returning
            self.spinner.stop()

            # Check that we have valid output with actual data
            # The output should have a header line and at least one data line
            output_lines = result.stdout.strip().split('\n')
            if not result.stdout or len(output_lines) < 2:
                # Clear the line using the terminal width
                term_width, _ = get_terminal_size()
                sys.stderr.write('\r' + ' ' * term_width + '\r')
                sys.stderr.flush()

                print("\nWarning: kafka-consumer-groups returned no data.", file=sys.stderr)
                print(f"Command executed: {cmd_str}", file=sys.stderr)
                if result.stdout:
                    print(f"Output: {result.stdout.strip()}", file=sys.stderr)
                print("This may indicate that the consumer group doesn't exist or has no active topics.", file=sys.stderr)
                print("Try checking if the group name is correct or if it has any active consumers.", file=sys.stderr)
                sys.exit(1)

            return result.stdout

        except subprocess.CalledProcessError as e:
            # Stop spinner before showing error
            self.spinner.stop()

            # Clear the line using the terminal width
            term_width, _ = get_terminal_size()
            sys.stderr.write('\r' + ' ' * term_width + '\r')
            sys.stderr.flush()

            print("\nError running kafka-consumer-groups command:", file=sys.stderr)
            print(f"Command executed: {' '.join(cmd)}", file=sys.stderr)
            print(f"Exit code: {e.returncode}", file=sys.stderr)

            if e.stderr:
                print(f"Error details: {e.stderr.strip()}", file=sys.stderr)

            # Provide suggestions based on common error codes
            if e.returncode == 1:
                print("\nPossible causes:", file=sys.stderr)
                print("- The specified consumer group doesn't exist", file=sys.stderr)
                print("- Connection issues to the Kafka cluster", file=sys.stderr)
                print("- Authorization failures (check credentials in client.properties)", file=sys.stderr)

            sys.exit(1)

        except FileNotFoundError:
            # Stop spinner before showing error
            self.spinner.stop()

            # Clear the line using the terminal width
            term_width, _ = get_terminal_size()
            sys.stderr.write('\r' + ' ' * term_width + '\r')
            sys.stderr.flush()

            print("\nError: kafka-consumer-groups command not found.", file=sys.stderr)
            print("Make sure Kafka tools are installed and in your PATH.", file=sys.stderr)
            print("Installation instructions:", file=sys.stderr)
            print("  - Homebrew: brew install kafka", file=sys.stderr)
            print("  - Manual: Download from https://kafka.apache.org/downloads", file=sys.stderr)
            sys.exit(1)

        except Exception as e:
            # Catch any other unexpected errors
            self.spinner.stop()

            # Clear the line using the terminal width
            term_width, _ = get_terminal_size()
            sys.stderr.write('\r' + ' ' * term_width + '\r')
            sys.stderr.flush()

            print(f"\nUnexpected error running kafka-consumer-groups: {str(e)}", file=sys.stderr)
            print(f"Command attempted: {' '.join(cmd)}", file=sys.stderr)
            sys.exit(1)

    def parse_consumer_groups_output(self, output: str,
                                    filter_regex: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Parse the output from kafka-consumer-groups command.

        Args:
            output: Raw output from the command
            filter_regex: Optional regex to filter topics

        Returns:
            A list of dictionaries with parsed consumer group data

        Raises:
            SystemExit: If no data is returned or parsing fails
        """
        self.spinner.start("Processing consumer group data...")

        try:
            lines = output.strip().split("\n")
            if len(lines) < 2:
                # Clear the line using the terminal width
                term_width, _ = get_terminal_size()
                sys.stderr.write('\r' + ' ' * term_width + '\r')
                sys.stderr.flush()

                print("\nNo data returned from kafka-consumer-groups", file=sys.stderr)
                print("This could be because:", file=sys.stderr)
                print("  - The consumer group does not exist", file=sys.stderr)
                print("  - The consumer group exists but has no active consumers", file=sys.stderr)
                print("  - There are no topics assigned to this consumer group", file=sys.stderr)
                print("\nTry checking:", file=sys.stderr)
                print("  - If the group name is correct", file=sys.stderr)
                print("  - If any consumers are currently running with this group", file=sys.stderr)
                sys.exit(1)

            # Get the header line to determine column positions
            header = lines[0]

            # Find the positions of the important columns
            try:
                # Convert header to lowercase for case-insensitive matching
                header_lower = header.lower()

                # Find the position of each column
                group_pos = header_lower.find("group")
                topic_pos = header_lower.find("topic")
                partition_pos = header_lower.find("partition")
                current_offset_pos = header_lower.find("current-offset")
                log_end_pos = header_lower.find("log-end-offset")
                lag_pos = header_lower.find("lag")
                consumer_id_pos = header_lower.find("consumer-id")

                # For debugging
                if group_pos < 0 or topic_pos < 0 or partition_pos < 0 or current_offset_pos < 0 or log_end_pos < 0 or lag_pos < 0:
                    print("Warning: Could not find all expected columns in header:", file=sys.stderr)
                    print(f"Header: {header}", file=sys.stderr)
                    print(f"Positions: GROUP={group_pos}, TOPIC={topic_pos}, PARTITION={partition_pos}, CURRENT-OFFSET={current_offset_pos}, LOG-END-OFFSET={log_end_pos}, LAG={lag_pos}", file=sys.stderr)
            except ValueError as e:
                print(f"Error parsing header: {e}", file=sys.stderr)
                sys.exit(1)

            # Skip the header line
            data_lines = lines[1:]

            # Parse the data
            parsed_data = []
            for line in data_lines:
                try:
                    # Only process if the line is long enough to contain all needed fields
                    if len(line) <= lag_pos:
                        continue

                    # Extract fields based on their positions
                    group = line[:topic_pos].strip()
                    topic = line[topic_pos:partition_pos].strip()

                    # For numeric fields, extract and convert carefully
                    partition_str = line[partition_pos:current_offset_pos].strip()
                    partition = int(partition_str)

                    # Handle cases where offsets or lag might be represented as '-'
                    current_offset_str = line[current_offset_pos:log_end_pos].strip()
                    current_offset = 0 if current_offset_str == '-' else int(current_offset_str)

                    log_end_str = line[log_end_pos:lag_pos].strip()
                    log_end_offset = 0 if log_end_str == '-' else int(log_end_str)

                    # For LAG, we need to be careful with the end position
                    if consumer_id_pos > lag_pos:
                        lag_str = line[lag_pos:consumer_id_pos].strip()
                    else:
                        # If consumer_id_pos is not found or incorrect, just take the rest and split
                        rest = line[lag_pos:].strip()
                        lag_str = rest.split()[0]

                    # Handle dash for lag as well
                    lag = 0 if lag_str == '-' else int(lag_str)

                    # Apply filter if specified
                    if filter_regex and not re.search(filter_regex, topic):
                        continue

                    parsed_data.append({
                        "group": group,
                        "topic": topic,
                        "partition": partition,
                        "current_offset": current_offset,
                        "log_end_offset": log_end_offset,
                        "lag": lag
                    })
                except (ValueError, IndexError) as e:
                    print(f"Warning: Could not parse line: {line}", file=sys.stderr)
                    print(f"Error: {e}", file=sys.stderr)
                    continue

            self.spinner.stop()
            return parsed_data

        except Exception as e:
            self.spinner.stop()
            print(f"Error parsing consumer groups output: {e}", file=sys.stderr)
            sys.exit(1)