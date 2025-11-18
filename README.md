# klag - Kafka Consumer Lag Visualizer

A simple command-line tool that wraps around the `kafka-consumer-groups` command, providing cleaner and more intuitive visualization of Kafka consumer group lag.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
![Python Version](https://img.shields.io/badge/python-3.6%2B-blue)

## Features

- **Simplified interface** compared to the official kafka-consumer-groups tool
- **Multiple visualization modes**:
  - `bar`: ASCII bar charts showing lag by topic and partition
  - `text`: Structured text output organized by topic and partition
- **Environment support** for quick switching between Kafka clusters
- **Watch mode** to continuously monitor lag
- **Consistent sorting** by topic and partition for easy reading
- **Topic filtering** by regex pattern
- **Color-coded output** with different colors based on lag severity
- **Flexible topic filtering** with regex pattern support

## Installation

1. Clone the repository:
```bash
git clone https://github.com/vitorbari/klag.git
cd klag
```

2. Ensure you have Python 3.6+ installed

3. Install dependencies:

   **Option A: Using a Virtual Environment (Recommended)**
   ```bash
   # Create a virtual environment
   python3 -m venv venv

   # Activate the virtual environment
   source venv/bin/activate  # On Linux/macOS
   # or
   venv\Scripts\activate     # On Windows

   # Install dependencies
   pip install pyyaml
   ```

   **Option B: Using pipx (for command-line applications)**
   ```bash
   # Install pipx if not already installed
   brew install pipx  # On macOS
   # or
   python3 -m pip install --user pipx  # On other systems

   # Install klag with its dependencies
   pipx install --spec git+https://github.com/vitorbari/klag.git pyyaml
   ```

   **Option C: System-wide Installation (may require additional flags)**
   ```bash
   # On modern Python installations (PEP 668 compliant)
   pip3 install --user pyyaml  # Install in user space

   # If you encounter "externally-managed-environment" errors and understand the risks:
   pip3 install --user pyyaml  # Recommended user space installation
   # OR (not recommended for system Python)
   pip3 install --break-system-packages pyyaml  # Override protection, use with caution
   ```

4. Install Kafka tools (if not already installed):
   - macOS: `brew install kafka`
   - Linux: Follow [Apache Kafka installation guide](https://kafka.apache.org/downloads)
   - Ensure `kafka-consumer-groups` is in your PATH

5. Make the script executable:
```bash
chmod +x klag.py
```

6. (Optional) Create a symbolic link in your path for easy access:

   If using a virtual environment, you may want to create a wrapper script instead:
   ```bash
   # Create a symlink if installed system-wide or with --user
   ln -s $(pwd)/klag.py /usr/local/bin/klag

   # If using a virtual environment, create a wrapper script:
   echo '#!/bin/bash
   source "'$(pwd)'/venv/bin/activate"
   "'$(pwd)'/klag.py" "$@"' > /usr/local/bin/klag
   chmod +x /usr/local/bin/klag
   ```

> **Note about PEP 668**: Modern Python installations implement PEP 668, which prevents pip from modifying system Python packages to avoid conflicts with the system package manager. This is why you might see "externally-managed-environment" errors. Using virtual environments is the recommended approach to avoid these issues.

## Configuration

Create a configuration file at one of these locations:
- `./klag.yaml` (current directory)
- `~/.config/klag/klag.yaml` (user config directory)
- `/etc/klag/klag.yaml` (system config directory)

Example configuration:

```yaml
# klag configuration
environments:
  local:
    # bootstrap_server is optional if defined in client.properties
    bootstrap_server: "localhost:9092"
    config_file: "~/.config/kafka/local/client.properties"

  staging:
    # You can omit bootstrap_server if it's in client.properties
    config_file: "~/.config/kafka/staging/client.properties"

  production:
    # If both are specified, bootstrap_server takes precedence
    bootstrap_server: "kafka-production-server:9092"
    config_file: "~/.config/kafka/production/client.properties"

# Default consumer group to monitor
default_group: "consumer-group-1"

# Default visualization settings
defaults:
  mode: "bar"  # text or bar
  watch_interval: 5  # seconds for watch mode
```

## Usage

### Basic Usage

```bash
# Use configuration from config file
klag.py -e staging -g consumer-group-1

# Specify all parameters manually
klag.py -b kafka-staging-server:9092 \
        -c ~/.config/kafka/staging/client.properties \
        -g consumer-group-1
```

### Visualization Modes

```bash
# Bar chart visualization (default)
klag.py -e staging -m bar

# Structured text output
klag.py -e staging -m text
```

### Watch Mode

```bash
# Update every 5 seconds (default)
klag.py -e staging -w 5

# Update every 10 seconds
klag.py -e staging -w 10
```

### Topic Filtering

The `-f/--filter` option supports both exact matches and regex patterns:

```bash
# Simple exact topic match (no regex special characters)
klag.py -e staging -f "orders"

# Filter with regex pattern (match topics starting with "orders")
klag.py -e staging -f "orders.*"

# Complex topic name with dots
klag.py -e staging -f "public.customer.foo.bar"

# Strict regex match (must match exactly, no substrings)
klag.py -e staging -f "^public\.customer\.foo\.bar$"

# Match multiple topics using regex OR operator
klag.py -e staging -f "(orders|payments)"

# Combined with visualization modes
klag.py -e staging -f "orders.*" -m text

# Combined with watch mode
klag.py -e staging -f "orders.*" -w 5
```

### List Available Environments

```bash
# List available environments
klag.py --list-environments
```


## Command-line Options

```
  -h, --help            show this help message and exit
  -b BOOTSTRAP_SERVER, --bootstrap-server BOOTSTRAP_SERVER
                        Kafka bootstrap server
  -g GROUP, --group GROUP
                        Consumer group name (required, except for --list-environments/--list-groups)
  -c CONFIG, --config CONFIG
                        Client config properties file path
  -e ENV, --env ENV, --environment ENV
                        Environment name (from config file, e.g., 'staging' or 'live')
  -y CONFIG_FILE, --config-file CONFIG_FILE
                        Path to klag config file
  -m {text,bar}, --mode {text,bar}
                        Visualization mode (default: bar)
  -f FILTER, --filter FILTER
                        Filter topics by pattern (supports exact match or regex)
  -w [WATCH], --watch [WATCH]
                        Watch mode, refresh every N seconds (default from config)
  --list-environments   List available environments from config file
```

## Requirements

- Python 3.6+
- PyYAML
- Kafka tools (kafka-consumer-groups must be in your PATH)

## Client Configuration

The `-c/--config` option points to a client properties file that klag passes to the `kafka-consumer-groups` command. This is a standard Kafka client configuration file used for configuring security settings, connection parameters, and other client behavior options. The same format is used across all Kafka tools.

### Bootstrap Server Resolution

Klag uses the following order to determine which bootstrap server to use:

1. Command-line argument (`-b/--bootstrap-server`) if provided
2. Environment configuration in klag.yaml (`bootstrap_server` field) if present
3. Client properties file (`bootstrap.servers` property) if defined
4. If none of the above are found, klag will show an error

This flexible approach allows you to:
- Specify the bootstrap server in just one place (client.properties)
- Override the bootstrap server for specific environments in klag.yaml
- Override both with a command-line argument when needed

### Creating a client.properties file

For basic usage, create a file with the following content:

```properties
# Basic properties
bootstrap.servers=your-kafka-server:9092
```

For detailed information about all available configuration options, refer to the [Confluent Admin Client Configuration documentation](https://docs.confluent.io/platform/current/installation/configuration/admin-configs.html#bootstrap-servers).

For secure clusters, add authentication and SSL settings:

```properties
# Security settings
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="your-username" password="your-password";

# SSL settings (if needed)
ssl.truststore.location=/path/to/truststore.jks
ssl.truststore.password=truststore-password
```

Common locations to store client properties files:
- `~/.config/kafka/{environment}/client.properties` (recommended for multiple environments)
- `/etc/kafka/client.properties` (system-wide)

## Development

### Running Tests

The project includes a test suite built with pytest. To run the tests:

1. Install the test dependencies:
```bash
pip3 install pytest pytest-cov
```

2. Run the tests:
```bash
# Run all tests
pytest

# Run tests with coverage report
pytest --cov=klag

# Run a specific test file
pytest tests/test_config.py
```

The test suite includes:
- Unit tests for configuration handling
- Tests for Kafka client functionality with mock outputs
- Visualization component tests
- CLI interface tests

## Project Structure

```
klag/
├── __init__.py        # Package version and metadata
├── __main__.py        # Entry point for module execution
├── cli.py             # Command-line interface handling
├── config.py          # Configuration loading and processing
├── kafka.py           # Kafka client functionality
├── utils.py           # Utility functions and helpers
└── visualization.py   # Visualization engines for different modes
```

## Contributing

Contributions are welcome! Please check out our [Contributing Guidelines](CONTRIBUTING.md) for details on how to get started.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.