#!/usr/bin/env python3
"""
Run script for NiFi MCP Server and Chat UI
This script can start both the server and UI components.
"""

import os
import sys
import argparse
import subprocess
import time
import signal
import yaml
from pathlib import Path

# Default configuration
DEFAULT_CONFIG = {
    "server": {
        "host": "0.0.0.0",
        "port": 8000,
        "log_level": "info"
    },
    "ui": {
        "port": 8501
    }
}

def load_config():
    """Load configuration from config.yaml file."""
    config = DEFAULT_CONFIG.copy()
    config_path = Path("config.yaml")
    
    if config_path.exists():
        try:
            with open(config_path, "r") as f:
                yaml_config = yaml.safe_load(f)
                
            # Update with file values
            if yaml_config and "server" in yaml_config:
                config["server"].update(yaml_config.get("server", {}))
            if yaml_config and "ui" in yaml_config:
                config["ui"].update(yaml_config.get("ui", {}))
        except Exception as e:
            print(f"Error loading config: {e}")
    
    return config

def run_server(host, port, log_level):
    """Run the FastAPI server."""
    cmd = [
        sys.executable, "-m", "uvicorn", 
        "nifi_mcp_server.server:app", 
        "--host", host, 
        "--port", str(port),
        "--log-level", log_level
    ]
    return subprocess.Popen(cmd)

def run_ui(port):
    """Run the Streamlit UI."""
    env = os.environ.copy()
    env["MCP_SERVER_URL"] = f"http://localhost:{DEFAULT_CONFIG['server']['port']}"
    cmd = [sys.executable, "-m", "streamlit", "run", "nifi_chat_ui/app.py", "--server.port", str(port)]
    return subprocess.Popen(cmd, env=env)

def main():
    parser = argparse.ArgumentParser(description="Run NiFi MCP Server and Chat UI")
    parser.add_argument("--server-only", action="store_true", help="Run only the server component")
    parser.add_argument("--ui-only", action="store_true", help="Run only the UI component")
    parser.add_argument("--server-host", help="Server host")
    parser.add_argument("--server-port", type=int, help="Server port")
    parser.add_argument("--ui-port", type=int, help="UI port")
    parser.add_argument("--log-level", help="Log level", choices=["debug", "info", "warning", "error", "critical"])
    
    args = parser.parse_args()
    config = load_config()
    
    # Override with command line arguments
    if args.server_host:
        config["server"]["host"] = args.server_host
    if args.server_port:
        config["server"]["port"] = args.server_port
    if args.ui_port:
        config["ui"]["port"] = args.ui_port
    if args.log_level:
        config["server"]["log_level"] = args.log_level
    
    processes = []
    
    try:
        # Start server if requested
        if not args.ui_only:
            print(f"Starting server on {config['server']['host']}:{config['server']['port']}...")
            server_process = run_server(
                config["server"]["host"], 
                config["server"]["port"],
                config["server"]["log_level"]
            )
            processes.append(server_process)
            time.sleep(2)  # Give the server time to start
        
        # Start UI if requested
        if not args.server_only:
            print(f"Starting UI on port {config['ui']['port']}...")
            ui_process = run_ui(config["ui"]["port"])
            processes.append(ui_process)
        
        # Wait for termination
        for process in processes:
            process.wait()
            
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        # Clean up processes
        for process in processes:
            try:
                process.send_signal(signal.SIGINT)
                process.wait(timeout=5)
            except:
                process.kill()
        
        print("Shutdown complete.")

if __name__ == "__main__":
    main() 