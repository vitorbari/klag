"""Test fixtures and configuration for klag tests."""

import os
import tempfile
import pytest
import yaml
from unittest.mock import patch

# Sample consumer group data for testing
@pytest.fixture
def sample_consumer_group_data():
    """Return sample consumer group data for tests."""
    return [
        {
            "group": "test-group",
            "topic": "topic-1",
            "partition": 0,
            "current_offset": 1000,
            "log_end_offset": 1100,
            "lag": 100
        },
        {
            "group": "test-group",
            "topic": "topic-1",
            "partition": 1,
            "current_offset": 2000,
            "log_end_offset": 2050,
            "lag": 50
        },
        {
            "group": "test-group",
            "topic": "topic-2",
            "partition": 0,
            "current_offset": 3000,
            "log_end_offset": 3000,
            "lag": 0
        }
    ]

# Sample kafka-consumer-groups command output
@pytest.fixture
def sample_kafka_output():
    """Return sample output from kafka-consumer-groups command."""
    return """
CONSUMER-GROUP  TOPIC           PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID
test-group      topic-1         0          1000            1100            100             -
test-group      topic-1         1          2000            2050            50              -
test-group      topic-2         0          3000            3000            0               -
"""

# Sample klag configuration
@pytest.fixture
def sample_config():
    """Return a sample klag configuration."""
    return {
        'environments': {
            'local': {
                'bootstrap_server': 'localhost:9092',
                'config_file': '~/.config/kafka/local/client.properties'
            },
            'staging': {
                'config_file': '~/.config/kafka/staging/client.properties'
            }
        },
        'default_group': 'consumer-group-1',
        'defaults': {
            'mode': 'bar',
            'watch_interval': 5
        }
    }

@pytest.fixture
def temp_config_file():
    """Create a temporary config file for testing."""
    with tempfile.NamedTemporaryFile('w+', suffix='.yaml', delete=False) as f:
        yaml.dump({
            'environments': {
                'test': {
                    'bootstrap_server': 'test-server:9092',
                    'config_file': '/path/to/test/client.properties'
                }
            },
            'default_group': 'test-group',
            'defaults': {
                'mode': 'text',
                'watch_interval': 10
            }
        }, f)

    try:
        yield f.name
    finally:
        # Clean up the temporary file
        if os.path.exists(f.name):
            os.unlink(f.name)

# Mock for client.properties file
@pytest.fixture
def mock_client_properties(monkeypatch):
    """Mock reading a client.properties file."""
    def mock_open_properties(*args, **kwargs):
        class MockFile:
            def __init__(self):
                self.lines = [
                    "# Kafka client properties\n",
                    "bootstrap.servers=kafka-server:9092\n",
                    "security.protocol=PLAINTEXT\n"
                ]
                self.line_index = 0

            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

            def __iter__(self):
                return self

            def __next__(self):
                if self.line_index < len(self.lines):
                    line = self.lines[self.line_index]
                    self.line_index += 1
                    return line
                raise StopIteration

        return MockFile()

    with patch('builtins.open', mock_open_properties):
        yield