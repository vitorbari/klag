# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

klag (Kafka Lag) is a command-line utility for visualizing Kafka consumer group lag with more intuitive and cleaner output than the standard Kafka tools. It wraps around the `kafka-consumer-groups` command and provides multiple visualization modes.

## Installation

### Installation for Users

#### Option A: Using a Virtual Environment (Recommended)

```bash
# Clone the repository
git clone https://github.com/vitorbari/klag.git
cd klag

# Create a virtual environment
python3 -m venv venv

# Activate the virtual environment
source venv/bin/activate  # On Linux/macOS
# or
venv\Scripts\activate     # On Windows

# Install dependencies
pip install pyyaml

# Make executable
chmod +x klag.py

# Optionally create a wrapper script
echo '#!/bin/bash
source "'$(pwd)'/venv/bin/activate"
"'$(pwd)'/klag.py" "$@"' > /usr/local/bin/klag
chmod +x /usr/local/bin/klag
```

#### Option B: Using pipx (for Command Line Applications)

```bash
# Install pipx if not already installed
brew install pipx  # On macOS
# or
python3 -m pip install --user pipx  # On other systems

# Install klag with its dependencies
pipx install --spec git+https://github.com/vitorbari/klag.git pyyaml
```

#### Option C: System-wide Installation

```bash
# Clone the repository
git clone https://github.com/vitorbari/klag.git
cd klag

# Install dependencies in user space
pip3 install --user pyyaml

# Make executable
chmod +x klag.py

# Optionally create symbolic link
ln -s $(pwd)/klag.py /usr/local/bin/klag
```

> **Note about PEP 668**: Modern Python installations implement PEP 668, which prevents pip from modifying system packages. If you encounter an "externally-managed-environment" error, use one of the virtual environment options above.

### Development Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/klag.git
cd klag

# Create a virtual environment
python3 -m venv venv

# Activate the virtual environment
source venv/bin/activate  # On Linux/macOS
# or
venv\Scripts\activate     # On Windows

# Install dependencies
pip install pyyaml

# Install development dependencies
pip install pytest pytest-cov

# Create local config for development
cp klag.yaml.example klag.yaml
# Edit klag.yaml with your specific Kafka settings
```

### Running the Tool

```bash
# Using environment configuration
./klag.py -e staging

# Manual configuration
./klag.py -b kafka-bootstrap-server:9092 -g consumer-group-name

# With watch mode (continuous refresh every 5 seconds)
./klag.py -e staging -w 5

# Different visualization modes
./klag.py -e staging -m bar  # Bar chart (default)
./klag.py -e staging -m heat  # Heat map
./klag.py -e staging -m text  # Text mode

# Filtering and sorting
./klag.py -e staging -f "topic-pattern"  # Filter topics by regex
./klag.py -e staging -s lag  # Sort by lag (default)
./klag.py -e staging -s topic  # Sort by topic
./klag.py -e staging -s part  # Sort by partition

# Listing configuration
./klag.py --list-environments
./klag.py --list-groups

# Raw output mode
./klag.py -e staging --raw
```

## Architecture

The klag tool is organized around these main components:

1. **Main Script (`klag.py`)**: The core application that:
   - Parses command-line arguments
   - Loads configuration from YAML files
   - Executes and parses Kafka commands
   - Visualizes lag data in different formats

2. **Configuration System**:
   - YAML-based configuration files loaded from multiple possible locations
   - Supports environment definitions for different Kafka clusters
   - Defines default consumer groups and visualization preferences

3. **Visualization Engine**:
   - Text mode: Tabular display with color-coded lag values
   - Bar chart: ASCII bar charts showing relative lag values
   - Heat map: Color-coded visualization of lag by topic and partition

4. **Command Execution**:
   - Wraps around `kafka-consumer-groups` command
   - Parses structured data from command output
   - Provides filtering and sorting capabilities

5. **Watch Mode**:
   - Continuous monitoring with configurable refresh interval
   - Terminal clearing and redrawing for live updates

## Key Files

- `klag.py`: Main Python script containing all functionality
- `klag.yaml.example`: Example configuration file (template)
- `.gitignore`: Prevents committing local configuration files

## Configuration

The configuration file can be placed in one of these locations:
- `./klag.yaml` (current directory)
- `~/.config/klag/klag.yaml` (user config directory)
- `/etc/klag/klag.yaml` (system config directory)

Example configuration:
```yaml
environments:
  staging:
    bootstrap_server: "kafka-staging-server:9092"
    config_file: "~/.config/kafka/staging/client.properties"

  production:
    bootstrap_server: "kafka-production-server:9092"
    config_file: "~/.config/kafka/production/client.properties"

default_groups:
  - "consumer-group-1"
  - "consumer-group-2"

defaults:
  mode: "bar"  # text, bar, or heat
  sort: "lag"  # lag, topic, or part
  watch_interval: 5  # seconds for watch mode
```

## Dependencies

- Python 3.6+
- PyYAML (install via `pip3 install pyyaml`)
- Kafka tools (`kafka-consumer-groups` must be in your PATH)

## Notes

- Local config files should be gitignored
- Use `klag.yaml.example` as a template for local development