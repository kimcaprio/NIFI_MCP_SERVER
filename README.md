# NiFi MCP Server

A natural language-based Model Context Protocol (MCP) Server for Apache NiFi interaction and flow generation.

## Overview

The NiFi MCP Server allows users to interact with Apache NiFi through natural language queries. It processes user queries, extracts intent, and executes appropriate NiFi API operations.

## Features

- Natural language query processing with intent detection
- Operations for process groups, processors, connections, and templates
- Flow control (start/stop/status) for NiFi components
- Search capabilities across all NiFi component types
- Documentation access for processors and other components
- Streamlit chat interface for easy interaction
- Configuration options for NiFi connection and authentication

## Installation

### Prerequisites

- Python 3.8 or higher
- Apache NiFi instance

### Setup

1. Clone the repository
2. Create and activate a virtual environment
3. Install dependencies: `pip install -r requirements.txt`
4. Configure: `cp config.example.yaml config.yaml`

## Usage

### Quick Start

Run both the server and UI with a single command:

```
python run.py
```

Options:
- `--server-only`: Run only the server component
- `--ui-only`: Run only the UI component 
- `--server-host`: Specify server host
- `--server-port`: Specify server port
- `--ui-port`: Specify UI port
- `--log-level`: Set log level (debug, info, warning, error, critical)

### Manual Start

Start the server: `python -m uvicorn nifi_mcp_server.server:app --host 0.0.0.0 --port 8000`

Start the chat UI: `streamlit run nifi_chat_ui/app.py`

## Example Queries

- "List all process groups"
- "Show processors in the main process group"
- "Create a new process group called 'Data Processing'"
- "Search for GetFile processors"
- "What is the status of my flow?"
- "Start the Data Processing flow"
- "Stop all processors in the root group"
- "Show documentation for GetFile processor"
- "List connections in the ETL process group"
- "Find all templates with 'database' in the name"
- "Monitor the current flow status"
