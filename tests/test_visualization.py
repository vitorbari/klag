"""Tests for the visualization module."""

import io
from unittest.mock import patch, MagicMock
import pytest

from klag.visualization import LagTracker, TextVisualizer, BarVisualizer, VisualizerFactory
from klag.utils import Colors


class TestLagTracker:
    """Tests for the LagTracker class."""

    def test_init(self):
        """Test LagTracker initialization."""
        tracker = LagTracker()
        assert hasattr(tracker, 'previous_lag_data')
        assert tracker.previous_lag_data == {}

    def test_store_lag_data(self, sample_consumer_group_data):
        """Test storing lag data."""
        tracker = LagTracker()
        tracker.store_lag_data(sample_consumer_group_data)

        # Check that data was stored correctly
        assert 'test-group' in tracker.previous_lag_data
        assert 'topic-1' in tracker.previous_lag_data['test-group']
        assert 0 in tracker.previous_lag_data['test-group']['topic-1']
        assert tracker.previous_lag_data['test-group']['topic-1'][0] == 100

    def test_get_change_indicator_increased(self):
        """Test change indicator when lag increases."""
        tracker = LagTracker()

        # Setup previous data
        tracker.previous_lag_data = {
            'test-group': {
                'topic-1': {
                    0: 50  # Previous lag was 50
                }
            }
        }

        # Current lag is 100 (increased by 50)
        item = {
            'group': 'test-group',
            'topic': 'topic-1',
            'partition': 0,
            'lag': 100
        }

        # Get change indicator
        indicator = tracker.get_change_indicator(item)

        # Check that it shows an increase
        assert f"{Colors.RED}▲{Colors.RESET}" in indicator
        assert "+50" in indicator

    def test_get_change_indicator_decreased(self):
        """Test change indicator when lag decreases."""
        tracker = LagTracker()

        # Setup previous data
        tracker.previous_lag_data = {
            'test-group': {
                'topic-1': {
                    0: 150  # Previous lag was 150
                }
            }
        }

        # Current lag is 100 (decreased by 50)
        item = {
            'group': 'test-group',
            'topic': 'topic-1',
            'partition': 0,
            'lag': 100
        }

        # Get change indicator
        indicator = tracker.get_change_indicator(item)

        # Check that it shows a decrease
        assert f"{Colors.GREEN}▼{Colors.RESET}" in indicator
        assert "-50" in indicator

    def test_get_change_indicator_no_change(self):
        """Test change indicator when lag doesn't change."""
        tracker = LagTracker()

        # Setup previous data
        tracker.previous_lag_data = {
            'test-group': {
                'topic-1': {
                    0: 100  # Previous lag was 100
                }
            }
        }

        # Current lag is still 100 (no change)
        item = {
            'group': 'test-group',
            'topic': 'topic-1',
            'partition': 0,
            'lag': 100
        }

        # Get change indicator
        indicator = tracker.get_change_indicator(item)

        # Check that it shows no change
        assert f"{Colors.WHITE}■{Colors.RESET}" in indicator
        assert "0" in indicator

    def test_get_change_indicator_new_item(self):
        """Test change indicator for new item with no previous data."""
        tracker = LagTracker()

        # No previous data

        # Current item
        item = {
            'group': 'test-group',
            'topic': 'topic-1',
            'partition': 0,
            'lag': 100
        }

        # Get change indicator
        indicator = tracker.get_change_indicator(item)

        # Check that it shows as new
        assert f"{Colors.WHITE}■{Colors.RESET}" in indicator
        assert "NEW" in indicator


class TestVisualizerBase:
    """Base tests for all visualizer classes."""

    def test_get_color_for_lag(self):
        """Test color selection based on lag value."""
        tracker = LagTracker()
        visualizer = TextVisualizer(tracker)  # Using TextVisualizer as concrete class

        # Test various lag values and their colors
        assert visualizer.get_color_for_lag(0) == Colors.GREEN
        assert visualizer.get_color_for_lag(50) == Colors.CYAN
        assert visualizer.get_color_for_lag(500) == Colors.BLUE
        assert visualizer.get_color_for_lag(5000) == Colors.YELLOW
        assert visualizer.get_color_for_lag(15000) == Colors.RED

    def test_create_summary(self, sample_consumer_group_data):
        """Test creating a summary of lag data."""
        tracker = LagTracker()
        visualizer = TextVisualizer(tracker)  # Using TextVisualizer as concrete class

        # Create summary
        summary = visualizer.create_summary(sample_consumer_group_data)

        # Check that the summary contains expected information
        assert "3 partitions" in summary
        assert "2 lagging" in summary  # 2 partitions have lag > 0
        assert "150 total lag" in summary  # Sum of all lags


class TestTextVisualizer:
    """Tests for the TextVisualizer class."""

    @patch('sys.stdout', new_callable=io.StringIO)
    def test_visualize(self, mock_stdout, sample_consumer_group_data):
        """Test text visualization output."""
        tracker = LagTracker()
        visualizer = TextVisualizer(tracker)

        # Call visualize
        visualizer.visualize(sample_consumer_group_data)

        # Get output
        output = mock_stdout.getvalue()

        # Check that output contains expected elements
        assert "Topic: topic-1" in output
        assert "Topic: topic-2" in output
        assert "Group: test-group" in output
        assert "PART" in output
        assert "LAG" in output


class TestBarVisualizer:
    """Tests for the BarVisualizer class."""

    @patch('sys.stdout', new_callable=io.StringIO)
    def test_visualize(self, mock_stdout, sample_consumer_group_data):
        """Test bar visualization output."""
        tracker = LagTracker()
        visualizer = BarVisualizer(tracker)

        # Call visualize
        visualizer.visualize(sample_consumer_group_data)

        # Get output
        output = mock_stdout.getvalue()

        # Check that output contains expected elements
        assert "topic-1" in output
        assert "topic-2" in output
        assert "BAR" in output


class TestVisualizerFactory:
    """Tests for the VisualizerFactory class."""

    def test_create_text_visualizer(self):
        """Test creating a TextVisualizer."""
        tracker = LagTracker()
        visualizer = VisualizerFactory.create_visualizer("text", tracker)

        # Check that we got the right type
        assert isinstance(visualizer, TextVisualizer)

    def test_create_bar_visualizer(self):
        """Test creating a BarVisualizer."""
        tracker = LagTracker()
        visualizer = VisualizerFactory.create_visualizer("bar", tracker)

        # Check that we got the right type
        assert isinstance(visualizer, BarVisualizer)

    def test_create_invalid_visualizer(self):
        """Test error handling for invalid visualizer type."""
        tracker = LagTracker()

        # Should raise ValueError for invalid type
        with pytest.raises(ValueError) as excinfo:
            VisualizerFactory.create_visualizer("invalid", tracker)

        # Check error message
        assert "Unsupported visualization mode" in str(excinfo.value)