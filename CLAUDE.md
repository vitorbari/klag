# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

klag (Kafka Lag) is a command-line utility for visualizing Kafka consumer group lag with more intuitive and cleaner output than the standard Kafka tools. It wraps around the `kafka-consumer-groups` command and provides multiple visualization modes.

## Distribution

klag is distributed via Homebrew. The Homebrew formula handles installation of all dependencies (Python, PyYAML, Kafka) and places configuration files in the appropriate locations.

### Installation for Users

```bash
# Install via Homebrew
brew install klag
```

### Development Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/klag.git
cd klag

# Install dependencies manually if needed
pip3 install pyyaml

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
- `/usr/local/etc/klag/klag.yaml` (Homebrew on Intel Macs)
- `/opt/homebrew/etc/klag/klag.yaml` (Homebrew on Apple Silicon)

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

- Python 3.6+ (automatically installed by Homebrew)
- PyYAML (automatically installed by Homebrew)
- Kafka tools (`kafka-consumer-groups` must be in your PATH, installed by Homebrew)

## Homebrew Formula

The Homebrew formula for klag handles installation of all dependencies and configuration. Key elements of the formula:

1. **Dependencies**:
   - `depends_on "python@3"`
   - `depends_on "kafka"`
   - PyYAML installed via resources

2. **Installation**:
   - Installs the executable script
   - Installs example config to Homebrew's etc directory
   - Respects existing configurations during upgrades

3. **Development**:
   - Local config files should be gitignored
   - Use `klag.yaml.example` as a template for local development