"""Tests for the Kafka client module."""

import subprocess
import sys
from unittest.mock import patch, MagicMock
import pytest

from klag.kafka import KafkaClient


class TestKafkaClient:
    """Tests for the KafkaClient class."""

    def test_init(self):
        """Test KafkaClient initialization."""
        client = KafkaClient()
        assert hasattr(client, 'spinner')

    @patch('subprocess.run')
    def test_fetch_consumer_group_info_success(self, mock_run):
        """Test fetching consumer group info successfully."""
        # Setup the mock
        mock_result = MagicMock()
        mock_result.stdout = "HEADER\nDATA1\nDATA2"
        mock_result.returncode = 0
        mock_run.return_value = mock_result

        # Create client and call method
        client = KafkaClient()
        result = client.fetch_consumer_group_info('localhost:9092', 'test-group')

        # Check that subprocess.run was called correctly
        mock_run.assert_called_once_with(
            ["kafka-consumer-groups",
             "--bootstrap-server", "localhost:9092",
             "--group", "test-group",
             "--describe"],
            capture_output=True, text=True, check=True
        )

        # Check result
        assert result == "HEADER\nDATA1\nDATA2"

    @patch('subprocess.run')
    def test_fetch_consumer_group_info_with_config(self, mock_run):
        """Test fetching consumer group info with config file."""
        # Setup the mock
        mock_result = MagicMock()
        mock_result.stdout = "HEADER\nDATA1\nDATA2"
        mock_result.returncode = 0
        mock_run.return_value = mock_result

        # Create client and call method with config file
        client = KafkaClient()
        result = client.fetch_consumer_group_info(
            'localhost:9092',
            'test-group',
            '/path/to/config.properties'
        )

        # Check that subprocess.run was called with config file
        mock_run.assert_called_once_with(
            ["kafka-consumer-groups",
             "--bootstrap-server", "localhost:9092",
             "--group", "test-group",
             "--describe",
             "--command-config", "/path/to/config.properties"],
            capture_output=True, text=True, check=True
        )

        # Check result
        assert result == "HEADER\nDATA1\nDATA2"

    @patch('subprocess.run')
    def test_fetch_consumer_group_info_no_data(self, mock_run):
        """Test handling when no data is returned."""
        # Setup the mock to return less than 2 lines
        mock_result = MagicMock()
        mock_result.stdout = "HEADER"
        mock_run.return_value = mock_result

        # Create client and call method
        client = KafkaClient()

        # Should exit with code 1
        with pytest.raises(SystemExit) as excinfo:
            client.fetch_consumer_group_info('localhost:9092', 'test-group')

        assert excinfo.value.code == 1

    @patch('subprocess.run')
    def test_fetch_consumer_group_info_command_error(self, mock_run):
        """Test handling when command fails."""
        # Setup the mock to raise CalledProcessError
        error = subprocess.CalledProcessError(1, 'cmd')
        error.stderr = "Error details"
        mock_run.side_effect = error

        # Create client and call method
        client = KafkaClient()

        # Should exit with code 1
        with pytest.raises(SystemExit) as excinfo:
            client.fetch_consumer_group_info('localhost:9092', 'test-group')

        assert excinfo.value.code == 1

    @patch('subprocess.run')
    def test_fetch_consumer_group_info_command_not_found(self, mock_run):
        """Test handling when command is not found."""
        # Setup the mock to raise FileNotFoundError
        mock_run.side_effect = FileNotFoundError("No such file or directory")

        # Create client and call method
        client = KafkaClient()

        # Should exit with code 1
        with pytest.raises(SystemExit) as excinfo:
            client.fetch_consumer_group_info('localhost:9092', 'test-group')

        assert excinfo.value.code == 1

    def test_parse_consumer_groups_output(self, sample_kafka_output):
        """Test parsing kafka-consumer-groups command output."""
        client = KafkaClient()
        result = client.parse_consumer_groups_output(sample_kafka_output)

        # Check the parsed data
        assert len(result) == 3
        assert result[0]['topic'] == 'topic-1'
        assert result[0]['partition'] == 0
        assert result[0]['lag'] == 100
        assert result[1]['topic'] == 'topic-1'
        assert result[1]['partition'] == 1
        assert result[1]['lag'] == 50
        assert result[2]['topic'] == 'topic-2'
        assert result[2]['partition'] == 0
        assert result[2]['lag'] == 0

    def test_parse_consumer_groups_output_with_filter(self, sample_kafka_output):
        """Test parsing output with topic filter."""
        client = KafkaClient()

        # Filter for topic-1
        result = client.parse_consumer_groups_output(sample_kafka_output, "topic-1")

        # Check that only topic-1 entries are included
        assert len(result) == 2
        assert all(item['topic'] == 'topic-1' for item in result)

        # Filter for topic-2
        result = client.parse_consumer_groups_output(sample_kafka_output, "topic-2")

        # Check that only topic-2 entries are included
        assert len(result) == 1
        assert result[0]['topic'] == 'topic-2'

    def test_parse_consumer_groups_output_with_regex_filter(self, sample_kafka_output):
        """Test parsing output with regex topic filter."""
        client = KafkaClient()

        # Filter for topics that end with a digit
        result = client.parse_consumer_groups_output(sample_kafka_output, ".*-\\d$")

        # All topics should match
        assert len(result) == 3

        # Filter for topics that end with 1
        result = client.parse_consumer_groups_output(sample_kafka_output, ".*-1$")

        # Only topic-1 should match
        assert len(result) == 2
        assert all(item['topic'] == 'topic-1' for item in result)