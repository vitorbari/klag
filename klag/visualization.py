"""
Visualization module for klag.

Provides different visualization strategies for displaying Kafka consumer lag.
"""

from typing import Dict, List, Any, Optional
from abc import ABC, abstractmethod
from datetime import datetime

from .utils import Colors, get_terminal_size


class LagTracker:
    """
    Tracks lag changes over time to show trends.

    Stores previous lag values and calculates changes for visualization.
    """

    def __init__(self):
        """Initialize the lag tracker."""
        self.previous_lag_data: Dict = {}

    def store_lag_data(self, data: List[Dict[str, Any]]) -> None:
        """
        Store current lag data for comparison in the next refresh.

        Args:
            data: List of consumer group data items
        """
        self.previous_lag_data = {}

        for item in data:
            group = item["group"]
            topic = item["topic"]
            partition = item["partition"]
            lag = item["lag"]

            # Create nested dictionaries as needed
            if group not in self.previous_lag_data:
                self.previous_lag_data[group] = {}
            if topic not in self.previous_lag_data[group]:
                self.previous_lag_data[group][topic] = {}

            # Store the lag value
            self.previous_lag_data[group][topic][partition] = lag

    def get_change_indicator(self, item: Dict[str, Any]) -> str:
        """
        Get a change indicator for a specific item compared to previous data.

        Args:
            item: A consumer group data item

        Returns:
            A formatted string with change indicator and value
        """
        group = item["group"]
        topic = item["topic"]
        partition = item["partition"]
        current_lag = item["lag"]

        # Check if we have previous data for this item
        if (group in self.previous_lag_data and
            topic in self.previous_lag_data[group] and
            partition in self.previous_lag_data[group][topic]):

            previous_lag = self.previous_lag_data[group][topic][partition]
            difference = current_lag - previous_lag

            if difference > 0:
                # Lag increased
                return f"{Colors.RED}▲{Colors.RESET} +{difference}"
            elif difference < 0:
                # Lag decreased
                return f"{Colors.GREEN}▼{Colors.RESET} {difference}"
            else:
                # No change
                return f"{Colors.WHITE}■{Colors.RESET} 0"
        else:
            # No previous data
            return f"{Colors.WHITE}■{Colors.RESET} NEW"


class Visualizer(ABC):
    """
    Abstract base class for visualization strategies.

    Defines the interface that all visualization implementations must follow.
    """

    def __init__(self, lag_tracker: LagTracker):
        """
        Initialize the visualizer.

        Args:
            lag_tracker: Lag tracker for showing changes
        """
        self.lag_tracker = lag_tracker

    @abstractmethod
    def visualize(self, data: List[Dict[str, Any]], topic_mode: bool = False,
                 show_changes: bool = False) -> None:
        """
        Visualize the consumer group lag data.

        Args:
            data: The consumer group data to visualize
            topic_mode: Whether to focus on a specific topic
            show_changes: Whether to show change indicators
        """
        pass

    def get_color_for_lag(self, lag: int) -> str:
        """
        Get the appropriate color for a lag value.

        Args:
            lag: The lag value

        Returns:
            ANSI color code for the lag value
        """
        if lag == 0:
            return Colors.GREEN
        elif lag < 100:
            return Colors.CYAN
        elif lag < 1000:
            return Colors.BLUE
        elif lag < 10000:
            return Colors.YELLOW
        else:
            return Colors.RED

    def create_summary(self, data: List[Dict[str, Any]]) -> str:
        """
        Create a summary of the lag data.

        Args:
            data: The consumer group data

        Returns:
            A summary string
        """
        if not data:
            return "No data available"

        total_lag = sum(item["lag"] for item in data)
        topics = set(item["topic"] for item in data)
        partitions = len(data)
        groups = set(item["group"] for item in data)

        lagging_partitions = sum(1 for item in data if item["lag"] > 0)

        if len(groups) > 1:
            return f"Summary: {len(groups)} groups, {len(topics)} topics, {partitions} partitions, {lagging_partitions} lagging, {total_lag} total lag"
        else:
            return f"Summary: {len(topics)} topics, {partitions} partitions, {lagging_partitions} lagging, {total_lag} total lag"


class TextVisualizer(Visualizer):
    """
    Text-based visualization of consumer group lag.

    Displays lag information in a structured text format organized by topic and group.
    """

    def visualize(self, data: List[Dict[str, Any]], topic_mode: bool = False,
                 show_changes: bool = False) -> None:
        """
        Visualize the data in text format with group and topic as headers.

        Args:
            data: The consumer group data to visualize
            topic_mode: Whether to focus on a specific topic
            show_changes: Whether to show change indicators
        """
        term_width, _ = get_terminal_size()

        # Group data by topic for better organization
        topics = {}
        for item in data:
            topic = item["topic"]
            if topic not in topics:
                topics[topic] = []
            topics[topic].append(item)

        # Create header format for partitions
        if show_changes:
            header = f"{Colors.BOLD}{'PART':<6} {'LAG':<10} {'CHANGE':<15}{Colors.RESET}"
        else:
            header = f"{Colors.BOLD}{'PART':<6} {'LAG':<10}{Colors.RESET}"

        # Print data by topic in sorted order
        for topic, items in sorted(topics.items()):
            # Print topic header
            print(f"\n{Colors.BOLD}{Colors.CYAN}Topic: {topic}{Colors.RESET}")

            # Group items by consumer group
            groups = {}
            for item in items:
                group = item["group"]
                if group not in groups:
                    groups[group] = []
                groups[group].append(item)

            # Print data by group
            for group, group_items in sorted(groups.items()):
                # Sort partitions within the group
                group_items = sorted(group_items, key=lambda x: x["partition"])

                # Print group as subheader
                print(f"{Colors.BOLD}Group: {group}{Colors.RESET}")
                print(header)

                # Print each partition
                for item in group_items:
                    partition = item["partition"]
                    lag = item["lag"]

                    # Get appropriate color for lag value
                    color = self.get_color_for_lag(lag)

                    # Format the partition row
                    line = f"{partition:<6} {color}{lag:<10}{Colors.RESET}"

                    # Add change indicator if needed
                    if show_changes:
                        change_indicator = self.lag_tracker.get_change_indicator(item)
                        line += f" {change_indicator:<15}"

                    print(line)

                # Add a separator between groups
                print("─" * min(40, term_width))


class BarVisualizer(Visualizer):
    """
    Bar chart visualization of consumer group lag.

    Displays lag information as ASCII bar charts organized by topic.
    """

    def visualize(self, data: List[Dict[str, Any]], topic_mode: bool = False,
                 show_changes: bool = False) -> None:
        """
        Visualize the data with ASCII bar charts.

        Args:
            data: The consumer group data to visualize
            topic_mode: Whether to focus on a specific topic
            show_changes: Whether to show change indicators
        """
        term_width, _ = get_terminal_size()

        # Find the maximum lag for scaling
        max_lag = max([item["lag"] for item in data] + [1])

        # Calculate available width for the bar
        info_width = 25  # Partition + lag value + spacing
        # If showing changes, we need more space for the CHANGE column
        if show_changes:
            info_width += 15  # Add space for CHANGE column
        bar_max_width = term_width - info_width

        if topic_mode:
            # Group by consumer group for topic-centric view
            groups = {}
            for item in data:
                group = item["group"]
                if group not in groups:
                    groups[group] = []
                groups[group].append(item)

            # Topic name is the same for all items in topic mode
            topic_name = data[0]["topic"] if data else "Unknown Topic"

            # Print topic as main header
            print(f"\n{Colors.BOLD}Topic: {topic_name}{Colors.RESET}")

            # Print data by consumer group
            for group, items in sorted(groups.items()):
                # Sort partitions within the group
                items = sorted(items, key=lambda x: x["partition"])

                # Print group as header
                print(f"\n{Colors.BOLD}Group: {group}{Colors.RESET}")

                # Update header to include CHANGE column if showing changes
                if show_changes:
                    header = f"{'PART':<4} {'LAG':<10} {'BAR':<{bar_max_width}} {'CHANGE':<15}"
                else:
                    header = f"{'PART':<4} {'LAG':<10} {'BAR'}"
                print(f"{Colors.BOLD}{header}{Colors.RESET}")

                # Print each partition
                for item in items:
                    partition = item["partition"]
                    lag = item["lag"]

                    # Calculate bar width proportionally
                    if max_lag > 0:
                        bar_width = int((lag / max_lag) * bar_max_width)
                    else:
                        bar_width = 0

                    # Get appropriate color for lag value
                    color = self.get_color_for_lag(lag)

                    # Create the bar
                    bar = "█" * min(bar_width, bar_max_width)

                    # Get change indicator if needed
                    if show_changes:
                        change_indicator = self.lag_tracker.get_change_indicator(item)
                        print(f"{partition:<4} {lag:<10} {color}{bar:<{bar_max_width}}{Colors.RESET} {change_indicator:<15}")
                    else:
                        print(f"{partition:<4} {lag:<10} {color}{bar}{Colors.RESET}")

                # Add a visual separator between groups
                print("─" * min(80, term_width))
        else:
            # Original group-centric view (group by topic)
            # Print header
            if show_changes:
                header = f"{'PART':<4} {'LAG':<10} {'BAR':<{bar_max_width}} {'CHANGE':<15}"
            else:
                header = f"{'PART':<4} {'LAG':<10} {'BAR'}"

            # Group by topic for better visualization
            topics = {}
            for item in data:
                topic = item["topic"]
                if topic not in topics:
                    topics[topic] = []
                topics[topic].append(item)

            # Print data by topic (maintain sorted order)
            for topic, items in sorted(topics.items()):
                # Sort partitions within the topic
                items = sorted(items, key=lambda x: x["partition"])

                # Print topic as header
                print(f"\n{Colors.BOLD}{topic}{Colors.RESET}")
                print(f"{Colors.BOLD}{header}{Colors.RESET}")

                # Print each partition
                for item in items:
                    partition = item["partition"]
                    lag = item["lag"]

                    # Calculate bar width proportionally
                    if max_lag > 0:
                        bar_width = int((lag / max_lag) * bar_max_width)
                    else:
                        bar_width = 0

                    # Get appropriate color for lag value
                    color = self.get_color_for_lag(lag)

                    # Create the bar
                    bar = "█" * min(bar_width, bar_max_width)

                    # Get change indicator if needed
                    if show_changes:
                        change_indicator = self.lag_tracker.get_change_indicator(item)
                        print(f"{partition:<4} {lag:<10} {color}{bar:<{bar_max_width}}{Colors.RESET} {change_indicator:<15}")
                    else:
                        print(f"{partition:<4} {lag:<10} {color}{bar}{Colors.RESET}")

                # Add a visual separator between topics
                print("─" * min(80, term_width))


class VisualizerFactory:
    """
    Factory for creating visualizers.

    Provides a way to create the appropriate visualizer based on the requested mode.
    """

    @staticmethod
    def create_visualizer(mode: str, lag_tracker: LagTracker) -> Visualizer:
        """
        Create a visualizer instance for the specified mode.

        Args:
            mode: The visualization mode ("text" or "bar")
            lag_tracker: The lag tracker to use

        Returns:
            A visualizer instance

        Raises:
            ValueError: If the mode is not supported
        """
        if mode == "text":
            return TextVisualizer(lag_tracker)
        elif mode == "bar":
            return BarVisualizer(lag_tracker)
        else:
            raise ValueError(f"Unsupported visualization mode: {mode}")