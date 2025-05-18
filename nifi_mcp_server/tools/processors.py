from typing import Dict, Any, List, Optional
from ..nifi_api import NiFiAPIClient
from loguru import logger

# Processor Tools

async def list_processors(nifi_client: NiFiAPIClient, pg_id: str = "root") -> Dict[str, Any]:
    """List processors in a process group.
    
    Args:
        nifi_client: NiFi API client instance
        pg_id: ID of the parent process group (default: root)
        
    Returns:
        Dictionary with processor information
    """
    try:
        response = nifi_client.get(f"/flow/process-groups/{pg_id}")
        processors = response.get("processGroupFlow", {}).get("flow", {}).get("processors", [])
        
        # Extract relevant information
        result = [
            {
                "id": p.get("id"),
                "name": p.get("name"),
                "type": p.get("component", {}).get("type"),
                "state": p.get("status", {}).get("runStatus"),
                "input": p.get("inputRequirement"),
                "position": {
                    "x": p.get("position", {}).get("x"),
                    "y": p.get("position", {}).get("y")
                }
            }
            for p in processors
        ]
        
        return {
            "status": "success",
            "parent_id": pg_id,
            "processors": result,
            "count": len(result)
        }
    except Exception as e:
        logger.error(f"Error listing processors: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def get_processor_details(nifi_client: NiFiAPIClient, processor_id: str) -> Dict[str, Any]:
    """Get detailed information about a specific processor.
    
    Args:
        nifi_client: NiFi API client instance
        processor_id: Processor ID
        
    Returns:
        Dictionary with detailed processor information
    """
    try:
        response = nifi_client.get(f"/processors/{processor_id}")
        component = response.get("component", {})
        
        # Get status information
        status_response = nifi_client.get(f"/processors/{processor_id}/status")
        status = status_response.get("processorStatus", {})
        
        return {
            "status": "success",
            "id": processor_id,
            "name": component.get("name"),
            "type": component.get("type"),
            "bundle": component.get("bundle"),
            "state": status.get("runStatus"),
            "properties": component.get("properties", {}),
            "relationships": [
                {
                    "name": rel.get("name"),
                    "description": rel.get("description"),
                    "auto_terminate": rel.get("autoTerminate", False)
                }
                for rel in component.get("relationships", [])
            ],
            "input_requirement": component.get("inputRequirement"),
            "execution_stats": {
                "bytesRead": status.get("bytesRead", 0),
                "bytesWritten": status.get("bytesWritten", 0),
                "tasks_completed": status.get("taskCount", 0),
                "tasks_duration_ns": status.get("taskNanoseconds", 0),
                "input": status.get("input", "0"),
                "output": status.get("output", "0"),
                "activeThreadCount": status.get("activeThreadCount", 0),
            }
        }
    except Exception as e:
        logger.error(f"Error getting processor details: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def search_processors(nifi_client: NiFiAPIClient, search_term: str, 
                          pg_id: str = "root", recursive: bool = True) -> Dict[str, Any]:
    """Search for processors by name or type.
    
    Args:
        nifi_client: NiFi API client instance
        search_term: Term to search for
        pg_id: ID of the parent process group (default: root)
        recursive: Whether to search recursively in child process groups
        
    Returns:
        Dictionary with matching processors
    """
    try:
        search_term_lower = search_term.lower()
        matches = []
        
        # Get processors in the current process group
        response = nifi_client.get(f"/flow/process-groups/{pg_id}")
        processors = response.get("processGroupFlow", {}).get("flow", {}).get("processors", [])
        
        # Find matches in the current group
        for processor in processors:
            name = processor.get("name", "").lower()
            processor_type = processor.get("component", {}).get("type", "").lower()
            
            if search_term_lower in name or search_term_lower in processor_type:
                matches.append({
                    "id": processor.get("id"),
                    "name": processor.get("name"),
                    "type": processor.get("component", {}).get("type"),
                    "pg_id": pg_id,
                    "state": processor.get("status", {}).get("runStatus")
                })
        
        # If recursive, also search in child process groups
        if recursive:
            child_groups = response.get("processGroupFlow", {}).get("flow", {}).get("processGroups", [])
            
            for child_group in child_groups:
                child_id = child_group.get("id")
                child_results = await search_processors(nifi_client, search_term, child_id, recursive)
                
                if child_results.get("status") == "success":
                    matches.extend(child_results.get("processors", []))
        
        return {
            "status": "success",
            "search_term": search_term,
            "processors": matches,
            "count": len(matches)
        }
    except Exception as e:
        logger.error(f"Error searching processors: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def create_processor(nifi_client: NiFiAPIClient, pg_id: str, name: str, processor_type: str,
                         position_x: int = 0, position_y: int = 0) -> Dict[str, Any]:
    """Create a new processor.
    
    Args:
        nifi_client: NiFi API client instance
        pg_id: Process group ID to add the processor to
        name: Name of the new processor
        processor_type: Type of processor (e.g., org.apache.nifi.processors.standard.GetFile)
        position_x: X position (default: 0)
        position_y: Y position (default: 0)
        
    Returns:
        Dictionary with new processor information
    """
    try:
        # First, get the bundle info for the processor type
        bundles_response = nifi_client.get(f"/flow/processor-types/{processor_type}")
        bundle_info = bundles_response.get("processorTypes", [])[0].get("bundle")
        
        if not bundle_info:
            return {
                "status": "error",
                "message": f"Could not find bundle information for processor type: {processor_type}"
            }
        
        # Prepare the request body
        request_body = {
            "component": {
                "name": name,
                "type": processor_type,
                "bundle": bundle_info,
                "position": {
                    "x": position_x,
                    "y": position_y
                }
            },
            "revision": {
                "version": 0
            }
        }
        
        # Make the API call
        response = nifi_client.post(f"/process-groups/{pg_id}/processors", request_body)
        
        return {
            "status": "success",
            "id": response.get("id"),
            "name": response.get("component", {}).get("name"),
            "type": response.get("component", {}).get("type"),
            "uri": response.get("uri")
        }
    except Exception as e:
        logger.error(f"Error creating processor: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def update_processor_state(nifi_client: NiFiAPIClient, processor_id: str, state: str) -> Dict[str, Any]:
    """Update the state of a processor.
    
    Args:
        nifi_client: NiFi API client instance
        processor_id: ID of the processor to update
        state: New state (RUNNING, STOPPED, DISABLED)
        
    Returns:
        Dictionary with update status
    """
    try:
        # Get current processor info to get the current revision
        current_info = nifi_client.get(f"/processors/{processor_id}")
        
        # Prepare the request body
        request_body = {
            "component": {
                "id": processor_id,
                "state": state
            },
            "revision": current_info.get("revision")
        }
        
        # Make the API call
        response = nifi_client.put(f"/processors/{processor_id}", request_body)
        
        return {
            "status": "success",
            "id": processor_id,
            "name": response.get("component", {}).get("name"),
            "state": response.get("component", {}).get("state"),
            "message": f"Processor state updated to {state}"
        }
    except Exception as e:
        logger.error(f"Error updating processor state: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

async def update_processor_properties(nifi_client: NiFiAPIClient, processor_id: str, 
                                    properties: Dict[str, str]) -> Dict[str, Any]:
    """Update properties of a processor.
    
    Args:
        nifi_client: NiFi API client instance
        processor_id: ID of the processor to update
        properties: Dictionary of properties to update
        
    Returns:
        Dictionary with update status
    """
    try:
        # Get current processor info to get the current revision and properties
        current_info = nifi_client.get(f"/processors/{processor_id}")
        current_properties = current_info.get("component", {}).get("properties", {})
        
        # Merge the new properties with existing ones
        updated_properties = {**current_properties, **properties}
        
        # Prepare the request body
        request_body = {
            "component": {
                "id": processor_id,
                "properties": updated_properties
            },
            "revision": current_info.get("revision")
        }
        
        # Make the API call
        response = nifi_client.put(f"/processors/{processor_id}", request_body)
        
        return {
            "status": "success",
            "id": processor_id,
            "name": response.get("component", {}).get("name"),
            "updated_properties": properties,
            "message": "Processor properties updated successfully"
        }
    except Exception as e:
        logger.error(f"Error updating processor properties: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

# Additional processor operations can be added here
