from fastapi import FastAPI, HTTPException, Request, Response, Depends
import json
import yaml
import os
from typing import Dict, Any, List, Optional, Union
from loguru import logger
from pydantic import BaseModel

from .nifi_api import NiFiAPIClient
from .nlp_processor import NLProcessor, QueryContext, QueryResult

# MCP Server App
app = FastAPI(title="NiFi MCP Server", description="Model Context Protocol Server for Apache NiFi")

# Configuration for NiFi connection and other settings
CONFIG = {
    "nifi": {
        "url": os.environ.get("NIFI_URL", "http://localhost:8080/nifi-api"),
        "auth_type": os.environ.get("NIFI_AUTH_TYPE", "none"),
        "username": os.environ.get("NIFI_USERNAME", ""),
        "password": os.environ.get("NIFI_PASSWORD", ""),
        "token": os.environ.get("NIFI_TOKEN", ""),
        "ssl_verify": os.environ.get("NIFI_SSL_VERIFY", "true").lower() == "true",
    },
    "openai": {
        "api_key": os.environ.get("OPENAI_API_KEY", ""),
        "model": os.environ.get("OPENAI_MODEL", "gpt-3.5-turbo"),
    }
}

# Load config from file if it exists
CONFIG_FILE = os.environ.get("NIFI_MCP_CONFIG", "config.yaml")
if os.path.exists(CONFIG_FILE):
    try:
        with open(CONFIG_FILE, "r") as f:
            file_config = yaml.safe_load(f)
            # Update CONFIG with file values, preserving env var overrides
            if file_config:
                for section, values in file_config.items():
                    if section not in CONFIG:
                        CONFIG[section] = {}
                    for key, value in values.items():
                        if os.environ.get(f"{section.upper()}_{key.upper()}") is None:
                            CONFIG[section][key] = value
    except Exception as e:
        logger.error(f"Error loading config from {CONFIG_FILE}: {str(e)}")

# Initialize NiFi API client
nifi_client = NiFiAPIClient(
    base_url=CONFIG["nifi"]["url"],
    auth_type=CONFIG["nifi"]["auth_type"],
    username=CONFIG["nifi"]["username"],
    password=CONFIG["nifi"]["password"],
    token=CONFIG["nifi"]["token"],
    ssl_verify=CONFIG["nifi"]["ssl_verify"]
)

# Initialize NLP processor
nlp_processor = NLProcessor(
    api_key=CONFIG["openai"]["api_key"],
    model=CONFIG["openai"]["model"]
)

# Model for tool request
class ToolRequest(BaseModel):
    name: str
    parameters: Dict[str, Any] = {}

# Model for chat request
class ChatRequest(BaseModel):
    query: str
    session_id: Optional[str] = None
    user_id: Optional[str] = None
    context: Optional[Dict[str, Any]] = None

# Model for tool response
class ToolResponse(BaseModel):
    status: str
    tool: str
    result: Any

@app.get("/")
async def root():
    return {"message": "NiFi MCP Server", "status": "running"}

@app.post("/mcp/tool")
async def handle_tool_call(request: ToolRequest):
    """Handle MCP tool calls from the client."""
    try:
        logger.debug(f"Received tool call: {request.name}")
        
        # Route to appropriate tool handler based on tool name
        if request.name == "nifi_query":
            # Handle natural language query to NiFi
            query = request.parameters.get("query", "")
            result = await process_natural_language_query(query)
            return {
                "status": "success",
                "tool": request.name,
                "result": result.dict()
            }
        
        elif request.name == "process_groups_list":
            # Get process groups - directly use the NiFi API
            parent_id = request.parameters.get("parent_id", "root")
            from .tools.process_groups import list_process_groups
            result = await list_process_groups(nifi_client, parent_id)
            return {
                "status": "success", 
                "tool": request.name,
                "result": result
            }
        
        elif request.name == "process_group_details":
            # Get process group details
            pg_id = request.parameters.get("pg_id")
            if not pg_id:
                raise ValueError("Process group ID is required")
            
            from .tools.process_groups import get_process_group_details
            result = await get_process_group_details(nifi_client, pg_id)
            return {
                "status": "success",
                "tool": request.name,
                "result": result
            }
        
        elif request.name == "flow_status":
            # Get flow status
            pg_id = request.parameters.get("pg_id", "root")
            from .tools.flow_control import get_flow_status
            result = await get_flow_status(nifi_client, pg_id)
            return {
                "status": "success",
                "tool": request.name,
                "result": result
            }
            
        elif request.name == "start_component":
            # Start a component
            component_id = request.parameters.get("component_id")
            component_type = request.parameters.get("component_type")
            
            if not component_id or not component_type:
                raise ValueError("Component ID and type are required")
                
            from .tools.flow_control import start_component
            result = await start_component(nifi_client, component_id, component_type)
            return {
                "status": "success",
                "tool": request.name,
                "result": result
            }
            
        elif request.name == "stop_component":
            # Stop a component
            component_id = request.parameters.get("component_id")
            component_type = request.parameters.get("component_type")
            
            if not component_id or not component_type:
                raise ValueError("Component ID and type are required")
                
            from .tools.flow_control import stop_component
            result = await stop_component(nifi_client, component_id, component_type)
            return {
                "status": "success",
                "tool": request.name,
                "result": result
            }
            
        # Add more tool handlers here
            
        else:
            # Unknown tool
            return {
                "status": "error",
                "tool": request.name,
                "result": f"Unknown tool: {request.name}"
            }
    
    except Exception as e:
        logger.error(f"Error processing tool call: {str(e)}")
        return {
            "status": "error",
            "tool": request.name,
            "result": str(e)
        }

@app.post("/api/chat")
async def chat_endpoint(request: ChatRequest):
    """Handle natural language chat requests."""
    try:
        result = await process_natural_language_query(request.query, request.session_id, request.user_id)
        return result.dict()
    except Exception as e:
        logger.error(f"Error processing chat request: {str(e)}")
        return {
            "original_query": request.query,
            "response": f"Error: {str(e)}",
            "error": str(e)
        }

async def process_natural_language_query(query: str, session_id: Optional[str] = None, 
                                       user_id: Optional[str] = None) -> QueryResult:
    """Process a natural language query and execute the appropriate action."""
    # Create a query context
    context = QueryContext(
        query=query,
        nifi_url=CONFIG["nifi"]["url"],
        session_id=session_id,
        user_id=user_id
    )
    
    # Process the query using the NLP processor
    result = await nlp_processor.process_query(context)
    
    # If we have a detected intent, execute the corresponding action
    if result.detected_intent and result.detected_intent.intent_type != "unknown":
        intent = result.detected_intent
        action_result = None
        
        try:
            # Route to appropriate action handler
            if intent.intent_type == "list_process_groups":
                from .tools.process_groups import list_process_groups
                parent_id = intent.parameters.get("parent_group", "root")
                # Convert name to ID if needed (simplified for now)
                if parent_id != "root" and not parent_id.startswith("process-group-"):
                    parent_id = "root"  # For simplicity, use root if not an ID
                
                action_result = await list_process_groups(nifi_client, parent_id)
                result.action_taken = f"Listed process groups in {parent_id}"
            
            elif intent.intent_type == "get_processor_details":
                # This would need to search for the processor by name first
                result.action_taken = "Get processor details action"
                # TODO: Implement processor details lookup
            
            elif intent.intent_type == "create_process_group":
                from .tools.process_groups import create_process_group
                name = intent.parameters.get("name", "New Process Group")
                parent_id = "root"  # Default to root for simplicity
                
                action_result = await create_process_group(
                    nifi_client, 
                    parent_id, 
                    name, 
                    position_x=100, 
                    position_y=100
                )
                result.action_taken = f"Created process group '{name}'"
            
            elif intent.intent_type == "get_flow_status":
                from .tools.flow_control import get_flow_status
                pg_id = intent.parameters.get("process_group", "root")
                
                action_result = await get_flow_status(nifi_client, pg_id)
                result.action_taken = f"Retrieved flow status for {pg_id}"
                
                # Enhance the response with results
                if action_result.get("status") == "success":
                    status_info = action_result.get("component_status", {})
                    flow_info = action_result.get("flow_status", {})
                    
                    running = status_info.get("running", 0)
                    total = status_info.get("total", 0)
                    
                    queued = flow_info.get("queued", "0 B")
                    
                    result.response = f"Your flow has {running} of {total} components running. " + \
                                     f"Currently {queued} of data is queued in the flow."
            
            elif intent.intent_type == "start_component":
                from .tools.flow_control import start_component
                
                # This is simplified - in a real implementation, you'd need to search for the component first
                component_name = intent.parameters.get("name", "")
                
                if component_name:
                    # For demo purposes, assume it's the root process group
                    component_id = "root"
                    component_type = "process-group"
                    
                    action_result = await start_component(nifi_client, component_id, component_type)
                    result.action_taken = f"Started component '{component_name}'"
                else:
                    result.error = "No component name provided to start"
                    result.response = "I need to know which component to start. Please specify a processor or process group name."
            
            elif intent.intent_type == "stop_component":
                from .tools.flow_control import stop_component
                
                # This is simplified - in a real implementation, you'd need to search for the component first
                component_name = intent.parameters.get("name", "")
                
                if component_name:
                    # For demo purposes, assume it's the root process group
                    component_id = "root"
                    component_type = "process-group"
                    
                    action_result = await stop_component(nifi_client, component_id, component_type)
                    result.action_taken = f"Stopped component '{component_name}'"
                else:
                    result.error = "No component name provided to stop"
                    result.response = "I need to know which component to stop. Please specify a processor or process group name."
            
            # Add more action handlers as needed
            
            # Update the response with action results if available
            if action_result:
                if action_result.get("status") == "error":
                    result.error = action_result.get("message", "Unknown error")
                    result.response = f"I encountered an error: {result.error}"
                else:
                    # Enhance the response with results
                    result.context_updates["action_result"] = action_result
        
        except Exception as e:
            logger.error(f"Error executing action: {str(e)}")
            result.error = str(e)
            result.response = f"I understood that you want to {intent.intent_type.replace('_', ' ')}, but I encountered an error: {str(e)}"
    
    return result
